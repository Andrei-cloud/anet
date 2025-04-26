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
	mu          sync.Mutex
	addr        string
	capacity    uint32
	count       atomic.Uint32
	queue       chan PoolItem
	factoryFunc Factory
	closing     bool
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
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		return nil, ErrClosing
	}
	p.mu.Unlock()

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

	for {
		item := <-p.queue
		if item == nil {
			return nil, ErrClosing
		}

		if p.validateConnection(item) {
			return item, nil
		}

		p.Release(item)
	}
}

// GetWithContext retrieves an item with context awareness.
func (p *pool) GetWithContext(ctx context.Context) (PoolItem, error) {
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		return nil, ErrClosing
	}
	p.mu.Unlock()

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

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

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

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case item := <-p.queue:
			if item == nil {
				return nil, ErrClosing
			}

			if p.validateConnection(item) {
				return item, nil
			}

			p.Release(item)
		}
	}
}

// Put returns an item to the pool.
func (p *pool) Put(item PoolItem) {
	if item == nil {
		return
	}

	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		p.Release(item)
		return
	}

	if !p.validateConnection(item) {
		p.mu.Unlock()
		p.Release(item)
		return
	}

	select {
	case p.queue <- item:
		p.mu.Unlock()
	default:
		p.mu.Unlock()
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
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		return
	}
	p.closing = true
	close(p.queue)

	itemsToClose := make([]PoolItem, 0, cap(p.queue))
	for item := range p.queue {
		itemsToClose = append(itemsToClose, item)
	}
	p.mu.Unlock()

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
