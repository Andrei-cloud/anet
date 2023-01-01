package anet

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

var (
	ErrClosing = errors.New("pool is closing")
)

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
	addr        string
	cap, count  uint32
	queue       chan PoolItem
	factoryFunc Factory
	closing     bool
}

func NewPoolList(cap uint32, f Factory, addrs []string) *[]Pool {
	var pools []Pool

	for _, addr := range addrs {
		p := NewPool(cap, f, addr)
		pools = append(pools, p)
	}

	return &pools
}

func NewPool(cap uint32, f Factory, addr string) *pool {
	return &pool{
		addr:        addr,
		count:       0,
		cap:         cap,
		queue:       make(chan PoolItem, cap),
		factoryFunc: f,
	}
}

func (p *pool) Get() (item PoolItem, err error) {
	if p.closing {
		return nil, ErrClosing
	}
	if len(p.queue) == 0 && p.count < p.cap {
		if item, err = p.factoryFunc(p.addr); err != nil {
			return nil, err
		}

		p.queue <- item
		atomic.AddUint32(&p.count, 1)
	}
	select {
	case item = <-p.queue:
	default:
	}
	return item, nil
}

func (p *pool) GetWithContext(ctx context.Context) (item PoolItem, err error) {
	if p.closing {
		return nil, ErrClosing
	}
	if len(p.queue) == 0 && p.count < p.cap {
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

func (p *pool) Put(item PoolItem) {
	p.Lock()
	defer p.Unlock()
	if p.closing {
		p.Release(item)
		return
	}

	p.queue <- item
}

func (p *pool) Release(item PoolItem) {
	if item != nil {
		item.Close()
		atomic.AddUint32(&p.count, ^uint32(0))
	}
}

func (p *pool) Close() {
	p.Lock()
	defer p.Unlock()
	p.closing = true
	for len(p.queue) > 0 {
		item := <-p.queue
		p.Release(item)
	}
}

func (p *pool) Len() int { return int(p.count) }
func (p *pool) Cap() int { return int(p.cap) }
