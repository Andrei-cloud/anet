package anet

import (
	"context"
	"errors"
	"log" // Added import
	"os"  // Added import
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
	sync.Mutex
	addr            string
	capacity, count uint32
	queue           chan PoolItem
	factoryFunc     Factory
	closing         bool
	logger          *log.Logger
}

// NewPoolList creates a list of Pool interfaces.
func NewPoolList(poolCap uint32, f Factory, addrs []string) []Pool { // Return []Pool
	pools := make([]Pool, 0, len(addrs))

	for _, addr := range addrs {
		p := NewPool(poolCap, f, addr)
		pools = append(pools, p)
	}

	return pools // Return slice of interfaces
}

// NewPool creates a new connection pool. Returns the Pool interface.
func NewPool(poolCap uint32, f Factory, addr string) Pool { // Added return type Pool
	return &pool{
		addr:        addr,
		count:       0,
		capacity:    poolCap,
		queue:       make(chan PoolItem, poolCap),
		factoryFunc: f,
		logger:      log.New(os.Stderr, "pool: ", log.LstdFlags), // Re-enabled logger
	}
}

// Get retrieves an item from the pool.
func (p *pool) Get() (PoolItem, error) {
	if p.closing {
		return nil, ErrClosing
	}

	select {
	// Try non-blocking read first
	case item := <-p.queue:
		if item == nil { // Channel closed while waiting
			return nil, ErrClosing
		}

		return item, nil
	default:
		// Queue is empty or temporarily blocked, proceed to check capacity
	}

	p.Lock()
	if p.count < p.capacity {
		// Increment count *before* factory call
		atomic.AddUint32(&p.count, 1)
		p.Unlock() // Unlock before potentially slow factory call

		item, err := p.factoryFunc(p.addr)
		if err != nil {
			atomic.AddUint32(&p.count, ^uint32(0)) // Decrement count on error
			return nil, err                        // Factory failed
		}

		// Successfully created a new item
		return item, nil
	}
	// Capacity is reached, must wait for an item to be returned
	p.Unlock()

	// Blocking wait for an item from the queue
	item := <-p.queue
	if item == nil { // Check if channel was closed while waiting
		return nil, ErrClosing
	}

	return item, nil
}

// GetWithContext retrieves an item from the pool with context awareness.
func (p *pool) GetWithContext(ctx context.Context) (PoolItem, error) {
	if p.closing {
		return nil, ErrClosing
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	// Try non-blocking read first
	case item := <-p.queue:
		if item == nil { // Channel closed
			return nil, ErrClosing
		}

		return item, nil
	default:
		// Queue is empty or temporarily blocked
	}

	p.Lock()
	if p.count < p.capacity {
		// Increment count *before* factory call
		atomic.AddUint32(&p.count, 1)
		p.Unlock() // Unlock before potentially slow factory call

		item, err := p.factoryFunc(p.addr) // Consider context for factory? Maybe not needed.
		if err != nil {
			atomic.AddUint32(&p.count, ^uint32(0)) // Decrement count on error
			return nil, err
		}

		// Successfully created a new item
		return item, nil
	}
	// Capacity reached, must wait for an item to be returned
	p.Unlock()

	// Blocking wait with context awareness
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case item := <-p.queue:
		if item == nil { // Check if channel was closed while waiting
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

	p.Lock()
	// Ensure unlock happens even if p.closing is true early
	// defer p.Unlock() // Moved unlock inside branches

	if p.closing {
		p.Unlock() // Unlock before potentially blocking Release->item.Close()
		p.Release(item)
		return
	}

	// Try a non-blocking send. If the queue is full, release the item.
	select {
	case p.queue <- item:
		// Item successfully put back into the queue
		p.Unlock()
	default:
		// Queue is full, release the item instead of blocking
		p.Unlock() // Unlock before potentially blocking Release->item.Close()
		p.Release(item)
	}
}

// Release closes an item and decrements the pool count.
func (p *pool) Release(item PoolItem) {
	if item != nil {
		// Decrement count *before* closing the item, in case Close() is slow/blocks
		atomic.AddUint32(&p.count, ^uint32(0)) // Decrement count using atomic operation
		if err := item.Close(); err != nil {
			// Log error only if logger is configured
			if p.logger != nil {
				p.logger.Printf("Error closing pool item: %v", err)
			}
		}
		// Removed redundant atomic decrement here
	}
}

// Close closes the pool and all its items.
func (p *pool) Close() {
	p.Lock()
	if p.closing {
		p.Unlock() // Already closing, release lock and return
		return
	}
	p.closing = true
	close(p.queue) // Close the channel to signal waiters

	itemsToClose := make([]PoolItem, 0, len(p.queue)) // Buffer size hint
	// Drain the queue into a temporary slice while lock is held
	for item := range p.queue {
		itemsToClose = append(itemsToClose, item)
	}
	p.Unlock() // Release the lock *before* closing items

	// Now close the items without holding the pool lock
	for _, item := range itemsToClose {
		p.Release(item) // Release will decrement count and call item.Close()
	}
}

// Len returns the current number of items in the pool.
func (p *pool) Len() int { return int(p.count) }

// Cap returns the capacity of the pool.
func (p *pool) Cap() int { return int(p.capacity) }
