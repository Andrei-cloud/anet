package anet

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
)

// ErrClosing indicates the pool is shutting down.
var ErrClosing = errors.New("pool is closing")

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
	mu          sync.RWMutex // Changed from sync.Mutex to sync.RWMutex
	addr        string
	capacity    uint32
	count       atomic.Uint32
	queue       chan PoolItem
	factoryFunc Factory
	closing     atomic.Bool // Changed from bool to atomic.Bool for lock-free access
	logger      *os.File
}

// NewPoolList creates a list of Pool interfaces from a slice of addresses.
func NewPoolList(poolCap uint32, f Factory, addrs []string) []Pool {
	pools := make([]Pool, 0, len(addrs))
	for _, addr := range addrs {
		p := NewPool(poolCap, f, addr)
		pools = append(pools, p)
	}

	return pools
}

// NewPool creates a new connection pool.
func NewPool(poolCap uint32, f Factory, addr string) Pool {
	p := &pool{
		addr:        addr,
		capacity:    poolCap,
		queue:       make(chan PoolItem, poolCap),
		factoryFunc: f,
		logger:      os.Stderr,
	}
	// Initialize atomic fields explicitly
	p.closing.Store(false)

	return p
}

// validateConnection performs basic connection health check.
func (p *pool) validateConnection(item PoolItem) bool {
	if item == nil {
		return false
	}

	if conn, ok := item.(net.Conn); ok {
		if conn == nil {
			return false
		}

		if tcpConn, ok := conn.(*net.TCPConn); ok {
			if err := tcpConn.SetKeepAlive(true); err != nil {
				return false
			}
		}
	}

	return true
}

// Get retrieves an item from the pool.
func (p *pool) Get() (PoolItem, error) {
	if p.closing.Load() {
		return nil, ErrClosing
	}

	// Try to get an existing connection from the queue
	select {
	case item := <-p.queue:
		if item == nil {
			return nil, ErrClosing
		}

		if p.validateConnection(item) {
			return item, nil
		}

		p.Release(item)
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

	// Wait for a connection to become available
	item := <-p.queue
	if item == nil {
		return nil, ErrClosing
	}

	if p.validateConnection(item) {
		return item, nil
	}

	p.Release(item)

	return p.Get() // Recursively try again if connection is invalid
}

// GetWithContext retrieves an item with context awareness.
func (p *pool) GetWithContext(ctx context.Context) (PoolItem, error) {
	if p.closing.Load() {
		return nil, ErrClosing
	}

	// Fast path: try to get an existing connection
	select {
	case item := <-p.queue:
		if item == nil {
			return nil, ErrClosing
		}
		if p.validateConnection(item) {
			return item, nil
		}
		p.Release(item)
	case <-ctx.Done():
		return nil, ctx.Err()
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
	for {
		select {
		case item := <-p.queue:
			if item == nil {
				return nil, ErrClosing
			}
			if p.validateConnection(item) {
				return item, nil
			}
			p.Release(item)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Put returns an item to the pool.
func (p *pool) Put(item PoolItem) {
	if item == nil {
		return
	}

	if p.closing.Load() {
		p.Release(item)

		return
	}

	if !p.validateConnection(item) {
		p.Release(item)

		return
	}

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

// Close closes the pool and all its items.
func (p *pool) Close() {
	// Fast check to avoid lock if already closing
	if p.closing.Load() {
		return
	}

	p.mu.Lock()
	// Double check after acquiring lock
	if p.closing.Load() {
		p.mu.Unlock()

		return
	}
	p.closing.Store(true)
	close(p.queue)

	// Collect items to close outside the lock
	itemsToClose := make([]PoolItem, 0, cap(p.queue))
	for item := range p.queue {
		if item != nil {
			itemsToClose = append(itemsToClose, item)
		}
	}
	p.mu.Unlock()

	// Release items outside the lock
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
