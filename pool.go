package anet

import (
	"context"
	"errors"
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
func NewPool(poolCap uint32, f Factory, addr string) Pool { // Return Pool interface
	return &pool{
		addr:        addr,
		count:       0,
		capacity:    poolCap,
		queue:       make(chan PoolItem, poolCap),
		factoryFunc: f,
	}
}

// Get retrieves an item from the pool.
func (p *pool) Get() (PoolItem, error) {
	var item PoolItem
	var err error
	if p.closing {
		return nil, ErrClosing
	}

	// If the queue is empty and we can create more connections.
	if len(p.queue) == 0 && p.count < p.capacity {
		p.Lock() // Lock to safely check count and potentially create.
		// Double-check count after acquiring lock.
		if p.count < p.capacity {
			if item, err = p.factoryFunc(p.addr); err != nil {
				p.Unlock()

				return nil, err
			}
			// Increment count *before* adding to queue to avoid race condition
			// where another goroutine might see count < cap but item not yet in queue.
			atomic.AddUint32(&p.count, 1)
			p.Unlock()      // Unlock before potentially blocking channel send.
			p.queue <- item // Add the new item to the queue.
		} else {
			p.Unlock() // Unlock if we didn't create an item.
		}
	}

	// Block until an item is available from the queue.
	item = <-p.queue

	return item, nil
}

// GetWithContext retrieves an item from the pool with context awareness.
func (p *pool) GetWithContext(ctx context.Context) (PoolItem, error) {
	var item PoolItem
	var err error
	if p.closing {
		return nil, ErrClosing
	}

	if len(p.queue) == 0 && p.count < p.capacity {
		if item, err = p.factoryFunc(p.addr); err != nil {
			return nil, err
		}

		p.queue <- item
		atomic.AddUint32(&p.count, 1)
	}

	select {
	case <-ctx.Done():

		return nil, ctx.Err()
	case item = <-p.queue:
	}

	return item, nil
}

// Put returns an item to the pool.
func (p *pool) Put(item PoolItem) {
	p.Lock()
	defer p.Unlock()
	if p.closing {
		p.Release(item)

		return
	}

	p.queue <- item
}

// Release closes an item and decrements the pool count.
func (p *pool) Release(item PoolItem) {
	if item != nil {
		if err := item.Close(); err != nil {
			// Optionally log the error here
			// p.logger.Printf("Error closing pool item: %v", err)
		}
		atomic.AddUint32(&p.count, ^uint32(0))
	}
}

// Close closes the pool and all its items.
func (p *pool) Close() {
	p.Lock()
	defer p.Unlock()
	p.closing = true
	close(p.queue) // Close the channel to signal waiters

	// Drain remaining items and close them
	for item := range p.queue {
		p.Release(item)
	}
}

// Len returns the current number of items in the pool.
func (p *pool) Len() int { return int(p.count) }

// Cap returns the capacity of the pool.
func (p *pool) Cap() int { return int(p.capacity) }
