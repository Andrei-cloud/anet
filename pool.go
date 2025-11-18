package anet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
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
}

// DefaultPoolConfig returns the default configuration.
func DefaultPoolConfig() *PoolConfig {
	return &PoolConfig{
		DialTimeout:           5 * time.Second,
		IdleTimeout:           60 * time.Second,
		ValidationInterval:    30 * time.Second,
		KeepAliveInterval:     30 * time.Second,
		ValidationStrategy:    ValidationRead,
		ValidationTimeout:     1 * time.Second,
		MaxValidationAttempts: 3,
	}
}

// NewPoolList creates a list of Pool interfaces from a slice of addresses.
func NewPoolList(poolCap uint32, f Factory, addrs []string, config *PoolConfig) []Pool {
	if config == nil {
		config = DefaultPoolConfig()
	}
	pools := make([]Pool, 0, len(addrs))
	for _, addr := range addrs {
		p := NewPool(poolCap, f, addr, config)
		pools = append(pools, p)
	}

	return pools
}

// NewPool creates a new connection pool.
func NewPool(poolCap uint32, f Factory, addr string, config *PoolConfig) Pool {
	if config == nil {
		config = DefaultPoolConfig()
	}
	p := &pool{
		addr:        addr,
		capacity:    poolCap,
		queue:       make(chan PoolItem, poolCap),
		factoryFunc: f,
		logger:      os.Stderr,
		config:      config,
		stopChan:    make(chan struct{}),
	}
	p.closing.Store(false)

	// Start background validation if interval is set.
	if p.config.ValidationInterval > 0 {
		go p.validateIdleConnections()
	}

	return p
}

// validateIdleConnections periodically validates idle connections - simplified.
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
	// Check closing status without lock first
	if p.closing.Load() {
		return
	}

	// Skip validation if strategy is none
	if p.config.ValidationStrategy == ValidationNone {
		return
	}

	// Only validate a small subset to avoid performance impact
	maxToCheck := 5 // Limit validation to avoid overhead
checkLoop:
	for range maxToCheck {
		select {
		case item := <-p.queue:
			if item == nil {
				continue
			}

			// Validate the connection
			if p.validateConnection(item) {
				// Connection is healthy, return to pool
				p.returnOrRelease(item)
			} else {
				// Connection is unhealthy, release it
				p.Release(item)
			}
		default:
			break checkLoop
		}
	}
}

// returnOrRelease tries to return item to pool, releases if pool is full.
func (p *pool) returnOrRelease(item PoolItem) {
	select {
	case p.queue <- item:
		// Successfully returned to pool
	default:
		p.Release(item)
	}
}

// Get retrieves an item from the pool with optimized fast path.
func (p *pool) Get() (PoolItem, error) {
	if p.closing.Load() {
		return nil, ErrClosing
	}

	// Fast path: try to get an existing connection from the queue.
	select {
	case item := <-p.queue:
		if item == nil {
			return nil, ErrClosing
		}
		// Skip validation in fast path - let actual usage detect issues

		return item, nil
	default:
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

			return item, nil
		}
		// CAS failed, retry loop
	}

	// Wait for a connection to become available.
	item := <-p.queue
	if item == nil {
		return nil, ErrClosing
	}

	return item, nil
}

// GetWithContext retrieves an item with minimal context checking.
func (p *pool) GetWithContext(ctx context.Context) (PoolItem, error) {
	if p.closing.Load() {
		return nil, ErrClosing
	}

	// Ultra-fast path: try to get without any validation or context check
	select {
	case item := <-p.queue:
		if item == nil {
			return nil, ErrClosing
		}

		return item, nil
	default:
	}

	// Try to create a new connection if under capacity
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

			return item, nil
		}
		// CAS failed, retry loop
	}

	// Wait for an available connection or context cancellation
	select {
	case item := <-p.queue:
		if item == nil {
			return nil, ErrClosing
		}

		return item, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Put returns an item to the pool with minimal validation.
func (p *pool) Put(item PoolItem) {
	if item == nil {
		return
	}

	if p.closing.Load() {
		p.Release(item)
		return
	}

	// Skip validation in Put - just add back to pool
	select {
	case p.queue <- item:
	default:
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

// Close closes the pool and all its items with minimal locking.
func (p *pool) Close() {
	// Fast check to avoid lock if already closing.
	if !p.closing.CompareAndSwap(false, true) {
		return // Already closing or closed
	}

	// Signal stop to background goroutine
	close(p.stopChan)

	// Close the queue to signal no more items
	close(p.queue)

	// Collect and close items without holding locks
	itemsToClose := make([]PoolItem, 0, cap(p.queue))
	for item := range p.queue {
		if item != nil {
			itemsToClose = append(itemsToClose, item)
		}
	}

	// Release items outside any locks
	for _, item := range itemsToClose {
		p.Release(item)
	}
}

// Len returns the current number of items in the pool.
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

	// Extract connection from PoolItem
	conn, ok := item.(interface{ SetDeadline(time.Time) error })
	if !ok {
		// Item doesn't support deadlines, use basic validation
		return p.validateConnectionBasic(item)
	}

	// Set deadline for validation
	deadline := time.Now().Add(p.config.ValidationTimeout)
	if err := conn.SetDeadline(deadline); err != nil {
		return false
	}

	// Restore deadline after validation
	defer func() {
		// Reset deadline to no timeout
		_ = conn.SetDeadline(time.Time{})
	}()

	return p.validateConnectionWithStrategy(item)
}

// validateConnectionBasic performs basic validation without timeout support.
func (p *pool) validateConnectionBasic(_ PoolItem) bool {
	switch p.config.ValidationStrategy {
	case ValidationNone:
		return true
	case ValidationPing, ValidationRead:
		// For items without deadline support, we can't safely validate
		// Return true to avoid unnecessary connection churn
		return true
	default:
		return true
	}
}

// validateConnectionWithStrategy performs validation based on the configured strategy.
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

		// Brief pause between attempts
		if attempt < p.config.MaxValidationAttempts-1 {
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Log validation failure if we have a logger
	if p.logger != nil {
		_, _ = fmt.Fprintf(p.logger, "Connection validation failed after %d attempts: %v\n",
			p.config.MaxValidationAttempts, lastErr)
	}

	return false
}

// validatePing attempts to check if the connection is alive using TCP-level checks.
func (p *pool) validatePing(item PoolItem) error {
	// Try to cast to net.Conn for TCP-specific checks
	if conn, ok := item.(net.Conn); ok {
		// For TCP connections, we can try to read with a very small buffer
		// This will detect closed connections without consuming data
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			// Use a 1-byte buffer to check connection state
			var b [1]byte
			_ = tcpConn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
			n, err := tcpConn.Read(b[:])
			_ = tcpConn.SetReadDeadline(time.Time{}) // Reset deadline

			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					// Timeout is expected for healthy connections with no data
					return nil
				}
				// Other errors indicate connection problems
				return err
			}

			if n > 0 {
				// Unexpected data - this might indicate a protocol issue
				// The connection is alive, but has unexpected data.
				// We should consider it invalid to avoid protocol desync.
				return fmt.Errorf("unexpected data during ping validation")
			}
		}
	}

	// For non-TCP connections or if TCP checks fail, just return success
	// to avoid false positives
	return nil
}

// validateRead attempts to perform a minimal read to check connection health.
func (p *pool) validateRead(item PoolItem) error {
	// Cast to io.Reader if possible
	reader, ok := item.(io.Reader)
	if !ok {
		// Item doesn't support reading, consider it valid
		return nil
	}

	// Try to read with a very short deadline
	if conn, ok := item.(net.Conn); ok {
		oldDeadline := time.Now().Add(p.config.ValidationTimeout)
		_ = conn.SetReadDeadline(oldDeadline)
		defer func() { _ = conn.SetReadDeadline(time.Time{}) }() // Reset deadline
	}

	// Attempt to read a single byte
	var b [1]byte
	n, err := reader.Read(b[:])
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			// Timeout is expected for healthy connections with no pending data
			return nil
		}
		// Other errors (EOF, connection reset, etc.) indicate problems
		return err
	}

	if n > 0 {
		// We read data, which means the connection is alive
		// However, for a request-response protocol, idle connections shouldn't have data.
		// We consider this a validation failure to prevent protocol desync.
		return fmt.Errorf("unexpected data during read validation")
	}

	return nil
}
