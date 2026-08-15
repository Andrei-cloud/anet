package anet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// ValidationNone disables connection validation.
	ValidationNone ValidationStrategy = "none"
	// ValidationPing sends a simple ping to validate connection.
	ValidationPing ValidationStrategy = "ping"
	// ValidationRead attempts to read with timeout to validate connection.
	ValidationRead ValidationStrategy = "read"
)

// ErrClosing indicates the pool is shutting down.
var ErrClosing = errors.New("pool is closing")

// ValidationStrategy defines how connections should be validated.
type ValidationStrategy string

// PoolConfig contains configuration options for a connection pool.
type PoolConfig struct {
	// DialTimeout is the timeout for creating new connections. Default is 5s.
	DialTimeout time.Duration
	// IdleTimeout is how long a connection can remain idle before being closed. Default is 60s.
	IdleTimeout time.Duration
	// ValidationInterval is how often to validate idle connections. Default is 30s.
	ValidationInterval time.Duration
	// KeepAliveInterval is the interval for TCP keepalive. Default is 30s.
	KeepAliveInterval time.Duration
	// ValidationStrategy defines how to validate connections. Default is ValidationRead.
	ValidationStrategy ValidationStrategy
	// ValidationTimeout is the timeout for connection validation operations. Default is 1s.
	ValidationTimeout time.Duration
	// MaxValidationAttempts is the maximum number of validation attempts before discarding connection. Default is 3.
	MaxValidationAttempts int
}

func (c *PoolConfig) applyDefaults() {
	if c.DialTimeout == 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = 60 * time.Second
	}
	if c.KeepAliveInterval == 0 {
		c.KeepAliveInterval = 30 * time.Second
	}
	if c.ValidationStrategy == "" {
		c.ValidationStrategy = ValidationRead
	}
	if c.ValidationTimeout == 0 {
		c.ValidationTimeout = 1 * time.Second
	}
	if c.MaxValidationAttempts == 0 {
		c.MaxValidationAttempts = 3
	}
}

// Pool manages a collection of reusable connections.
type Pool interface {
	Get() (PoolItem, error)
	GetWithContext(context.Context) (PoolItem, error)
	Release(PoolItem)
	Put(PoolItem)
	Len() int
	Cap() int
	Close()
}

// PoolItem represents a closeable resource managed by the pool.
type PoolItem interface {
	Close() error
}

// Factory creates new pool items.
type Factory func(string) (PoolItem, error)

// pool implements the Pool interface.
type pool struct {
	addr        string
	capacity    uint32
	count       atomic.Uint32
	queue       chan PoolItem
	factoryFunc Factory
	closing     atomic.Bool
	logger      *os.File
	config      *PoolConfig
	stopChan    chan struct{}
	mu          sync.RWMutex
}

// DefaultPoolConfig returns the default configuration.
func DefaultPoolConfig() *PoolConfig {
	cfg := &PoolConfig{
		DialTimeout:           5 * time.Second,
		IdleTimeout:           60 * time.Second,
		ValidationInterval:    30 * time.Second,
		KeepAliveInterval:     30 * time.Second,
		ValidationStrategy:    ValidationRead,
		ValidationTimeout:     1 * time.Second,
		MaxValidationAttempts: 3,
	}
	return cfg
}

// NewPoolList creates a list of Pool interfaces from a slice of addresses.
func NewPoolList(poolCap uint32, f Factory, addrs []string, config *PoolConfig) []Pool {
	pools := make([]Pool, 0, len(addrs))
	for _, addr := range addrs {
		p := NewPool(poolCap, f, addr, config)
		pools = append(pools, p)
	}

	return pools
}

// NewPool creates a new connection pool.
func NewPool(poolCap uint32, f Factory, addr string, config *PoolConfig) Pool {
	cfg := &PoolConfig{}
	if config != nil {
		*cfg = *config
	}
	cfg.applyDefaults()

	p := &pool{
		addr:        addr,
		capacity:    poolCap,
		queue:       make(chan PoolItem, poolCap),
		factoryFunc: f,
		logger:      os.Stderr,
		config:      cfg,
		stopChan:    make(chan struct{}),
	}
	p.closing.Store(false)

	// Start background validation if interval is set.
	if p.config.ValidationInterval > 0 && p.config.ValidationStrategy != ValidationNone {
		go p.validateIdleConnections()
	}

	return p
}

// validateIdleConnections periodically validates idle connections.
func (p *pool) validateIdleConnections() {
	ticker := time.NewTicker(p.config.ValidationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopChan:
			return
		case <-ticker.C:
			p.validateConnectionSubset()
		}
	}
}

// validateConnectionSubset validates a small subset of idle connections.
func (p *pool) validateConnectionSubset() {
	if p.closing.Load() {
		return
	}
	if p.config.ValidationStrategy == ValidationNone {
		return
	}

	maxToCheck := 5
checkLoop:
	for range maxToCheck {
		var item PoolItem
		p.mu.RLock()
		if p.closing.Load() {
			p.mu.RUnlock()
			return
		}
		select {
		case item = <-p.queue:
			p.mu.RUnlock()
		default:
			p.mu.RUnlock()
			break checkLoop
		}

		if item == nil {
			continue
		}

		if p.validateConnection(item) {
			p.returnOrRelease(item)
		} else {
			p.Release(item)
		}
	}
}

// returnOrRelease tries to return item to pool, releases if pool is full or closed.
func (p *pool) returnOrRelease(item PoolItem) {
	if item == nil {
		return
	}

	p.mu.RLock()
	if p.closing.Load() {
		p.mu.RUnlock()
		p.Release(item)
		return
	}

	select {
	case p.queue <- item:
		p.mu.RUnlock()
	default:
		p.mu.RUnlock()
		p.Release(item)
	}
}

// Get retrieves an item from the pool with optimized fast path.
func (p *pool) Get() (PoolItem, error) {
	if p.closing.Load() {
		return nil, ErrClosing
	}

	// Fast path: try to get an existing connection from the queue under read lock.
	p.mu.RLock()
	if p.closing.Load() {
		p.mu.RUnlock()
		return nil, ErrClosing
	}
	select {
	case item := <-p.queue:
		p.mu.RUnlock()
		if item == nil {
			return nil, ErrClosing
		}
		return item, nil
	default:
		p.mu.RUnlock()
	}

	// Try to create a new connection if under capacity.
	for {
		current := p.count.Load()
		if current >= p.capacity {
			break
		}
		if p.count.CompareAndSwap(current, current+1) {
			item, err := p.factoryFunc(p.addr)
			if err != nil {
				p.count.Add(^uint32(0))
				return nil, err
			}
			if p.closing.Load() {
				p.Release(item)
				return nil, ErrClosing
			}
			return item, nil
		}
	}

	// Wait for a connection to become available or pool closing.
	select {
	case item := <-p.queue:
		if item == nil {
			return nil, ErrClosing
		}
		return item, nil
	case <-p.stopChan:
		return nil, ErrClosing
	}
}

// GetWithContext retrieves an item with context cancellation support.
func (p *pool) GetWithContext(ctx context.Context) (PoolItem, error) {
	if p.closing.Load() {
		return nil, ErrClosing
	}

	// Fast path: try to get an existing connection immediately under read lock.
	p.mu.RLock()
	if p.closing.Load() {
		p.mu.RUnlock()
		return nil, ErrClosing
	}
	select {
	case item := <-p.queue:
		p.mu.RUnlock()
		if item == nil {
			return nil, ErrClosing
		}
		return item, nil
	default:
		p.mu.RUnlock()
	}

	// Try to create a new connection if under capacity.
	for {
		current := p.count.Load()
		if current >= p.capacity {
			break
		}
		if p.count.CompareAndSwap(current, current+1) {
			item, err := p.factoryFunc(p.addr)
			if err != nil {
				p.count.Add(^uint32(0))
				return nil, err
			}
			if p.closing.Load() {
				p.Release(item)
				return nil, ErrClosing
			}
			return item, nil
		}
	}

	// Wait for an available connection, context cancellation, or pool shutdown.
	select {
	case item := <-p.queue:
		if item == nil {
			return nil, ErrClosing
		}
		return item, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.stopChan:
		return nil, ErrClosing
	}
}

// Put returns an item to the pool.
func (p *pool) Put(item PoolItem) {
	if item == nil {
		return
	}

	p.mu.RLock()
	if p.closing.Load() {
		p.mu.RUnlock()
		p.Release(item)
		return
	}

	select {
	case p.queue <- item:
		p.mu.RUnlock()
	default:
		p.mu.RUnlock()
		p.Release(item)
	}
}

// Release closes an item and decrements pool count.
func (p *pool) Release(item PoolItem) {
	if item != nil {
		p.count.Add(^uint32(0))
		if err := item.Close(); err != nil {
			if p.logger != nil {
				if _, err := fmt.Fprintf(p.logger, "Error closing pool item: %v\n", err); err != nil {
					_, _ = fmt.Fprintf(os.Stderr, "Error writing to logger: %v\n", err)
				}
			}
		}
	}
}

// Close closes the pool and all its items safely without channel races.
func (p *pool) Close() {
	if !p.closing.CompareAndSwap(false, true) {
		return
	}

	close(p.stopChan)

	p.mu.Lock()
	var itemsToClose []PoolItem
drainLoop:
	for {
		select {
		case item := <-p.queue:
			if item != nil {
				itemsToClose = append(itemsToClose, item)
			}
		default:
			break drainLoop
		}
	}
	p.mu.Unlock()

	for _, item := range itemsToClose {
		p.Release(item)
	}
}

// Len returns the current number of items created in the pool.
func (p *pool) Len() int {
	return int(p.count.Load())
}

// Cap returns the capacity of the pool.
func (p *pool) Cap() int {
	return int(p.capacity)
}

// validateConnection validates a connection based on the configured strategy.
func (p *pool) validateConnection(item PoolItem) bool {
	if item == nil {
		return false
	}

	conn, ok := item.(interface{ SetDeadline(time.Time) error })
	if !ok {
		return p.validateConnectionBasic(item)
	}

	deadline := time.Now().Add(p.config.ValidationTimeout)
	if err := conn.SetDeadline(deadline); err != nil {
		return false
	}

	defer func() {
		_ = conn.SetDeadline(time.Time{})
	}()

	return p.validateConnectionWithStrategy(item)
}

func (p *pool) validateConnectionBasic(_ PoolItem) bool {
	return true
}

func (p *pool) validateConnectionWithStrategy(item PoolItem) bool {
	var lastErr error

	for attempt := 0; attempt < p.config.MaxValidationAttempts; attempt++ {
		var err error

		switch p.config.ValidationStrategy {
		case ValidationNone:
			return true
		case ValidationPing:
			err = p.validatePing(item)
		case ValidationRead:
			err = p.validateRead(item)
		default:
			return true
		}

		if err == nil {
			return true
		}

		lastErr = err
		if attempt < p.config.MaxValidationAttempts-1 {
			time.Sleep(10 * time.Millisecond)
		}
	}

	if p.logger != nil && lastErr != nil {
		_, _ = fmt.Fprintf(p.logger, "Connection validation failed after %d attempts: %v\n",
			p.config.MaxValidationAttempts, lastErr)
	}

	return false
}

func (p *pool) validatePing(item PoolItem) error {
	if conn, ok := item.(net.Conn); ok {
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			var b [1]byte
			_ = tcpConn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
			n, err := tcpConn.Read(b[:])
			_ = tcpConn.SetReadDeadline(time.Time{})

			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					return nil
				}
				return err
			}

			if n > 0 {
				return fmt.Errorf("unexpected data during ping validation")
			}
		}
	}
	return nil
}

func (p *pool) validateRead(item PoolItem) error {
	reader, ok := item.(io.Reader)
	if !ok {
		return nil
	}

	if conn, ok := item.(net.Conn); ok {
		oldDeadline := time.Now().Add(p.config.ValidationTimeout)
		_ = conn.SetReadDeadline(oldDeadline)
		defer func() { _ = conn.SetReadDeadline(time.Time{}) }()
	}

	var b [1]byte
	n, err := reader.Read(b[:])
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return nil
		}
		return err
	}

	if n > 0 {
		return fmt.Errorf("unexpected data during read validation")
	}

	return nil
}
