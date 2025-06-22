package anet

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

// ErrClosing indicates the pool is shutting down.
var ErrClosing = errors.New("pool is closing")

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
		DialTimeout:        5 * time.Second,
		IdleTimeout:        60 * time.Second,
		ValidationInterval: 30 * time.Second,
		KeepAliveInterval:  30 * time.Second,
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

	// Only validate a small subset to avoid performance impact
	maxToCheck := 5 // Limit validation to avoid overhead
checkLoop:
	for i := 0; i < maxToCheck; i++ {
		select {
		case item := <-p.queue:
			if item == nil {
				continue
			}
			// Just put it back - let usage detect broken connections
			p.returnOrRelease(item)
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
	current := p.count.Load()
	if current < p.capacity {
		if p.count.CompareAndSwap(current, current+1) {
			item, err := p.factoryFunc(p.addr)
			if err != nil {
				p.count.Add(^uint32(0))
				return nil, err
			}

			return item, nil
		}
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
	current := p.count.Load()
	if current < p.capacity {
		if p.count.CompareAndSwap(current, current+1) {
			item, err := p.factoryFunc(p.addr)
			if err != nil {
				p.count.Add(^uint32(0))
				return nil, err
			}

			return item, nil
		}
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
