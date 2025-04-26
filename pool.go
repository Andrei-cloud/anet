package anet

import (
	"context"
	"errors"
	"log"
	"os"
	"sync"
	"sync/atomic"
)

var ErrClosing = errors.New("pool is closing")

type Pool interface {
	Get() (PoolItem, error)
	GetWithContext(context.Context) (PoolItem, error)
	Release(PoolItem)
	Put(PoolItem)
	Len() int
	Cap() int
	Close()
}

type PoolItem interface {
	Close() error
}

type Factory func(string) (PoolItem, error)

type pool struct {
	mu          sync.Mutex
	addr        string
	capacity    uint32
	count       atomic.Uint32 // Change to atomic.Uint32 for better sync
	queue       chan PoolItem
	factoryFunc Factory
	closing     bool
	logger      *log.Logger
}

// NewPoolList creates a list of Pool interfaces.
func NewPoolList(poolCap uint32, f Factory, addrs []string) []Pool {
	pools := make([]Pool, 0, len(addrs))

	for _, addr := range addrs {
		p := NewPool(poolCap, f, addr)
		pools = append(pools, p)
	}

	return pools
}

// NewPool creates a new connection pool. Returns the Pool interface.
func NewPool(poolCap uint32, f Factory, addr string) Pool {
	return &pool{
		addr:        addr,
		capacity:    poolCap,
		queue:       make(chan PoolItem, poolCap),
		factoryFunc: f,
		logger:      log.New(os.Stderr, "pool: ", log.LstdFlags),
	}
}

// Get retrieves an item from the pool.
func (p *pool) Get() (PoolItem, error) {
	// Check closing state
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		return nil, ErrClosing
	}
	p.mu.Unlock()

	// Grow pool if under capacity
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

	// Try non-blocking queue read
	select {
	case item := <-p.queue:
		if item == nil {
			return nil, ErrClosing
		}
		return item, nil
	default:
	}

	// Block until an item is available
	item := <-p.queue
	if item == nil {
		return nil, ErrClosing
	}
	return item, nil
}

// GetWithContext retrieves an item from the pool with context awareness.
func (p *pool) GetWithContext(ctx context.Context) (PoolItem, error) {
	// Check closing state
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		return nil, ErrClosing
	}
	p.mu.Unlock()

	// Context cancellation before attempting grow
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Grow pool if under capacity
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

	// Try non-blocking queue read or check context
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case item := <-p.queue:
		if item == nil {
			return nil, ErrClosing
		}
		return item, nil
	default:
	}

	// Block until an item is available or context done
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case item := <-p.queue:
		if item == nil {
			return nil, ErrClosing
		}
		return item, nil
	}
}

// Put returns an item to the pool.
func (p *pool) Put(item PoolItem) {
	if item == nil { // Prevent putting nil items back
		return
	}

	p.mu.Lock()
	if p.closing {
		p.mu.Unlock() // Unlock before potentially blocking Release->item.Close()
		p.Release(item)
		return
	}

	// Try a non-blocking send. If the queue is full, release the item.
	select {
	case p.queue <- item:
		// Item successfully put back into the queue
		p.mu.Unlock()
	default:
		// Queue is full, release the item instead of blocking
		p.mu.Unlock() // Unlock before potentially blocking Release->item.Close()
		p.Release(item)
	}
}

// Release closes an item and decrements the pool count.
func (p *pool) Release(item PoolItem) {
	if item != nil {
		// Decrement count using atomic operations
		p.count.Add(^uint32(0))
		if err := item.Close(); err != nil {
			// Log error only if logger is configured
			if p.logger != nil {
				p.logger.Printf("Error closing pool item: %v", err)
			}
		}
	}
}

// Close closes the pool and all its items.
func (p *pool) Close() {
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock() // Already closing, release lock and return
		return
	}
	p.closing = true
	close(p.queue) // Close the channel to signal waiters

	itemsToClose := make([]PoolItem, 0, cap(p.queue)) // Use cap instead of len
	// Drain the queue into a temporary slice while lock is held
	for item := range p.queue {
		itemsToClose = append(itemsToClose, item)
	}
	p.mu.Unlock() // Release the lock *before* closing items

	// Now close the items without holding the pool lock
	for _, item := range itemsToClose {
		p.Release(item) // Release will decrement count and call item.Close()
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
