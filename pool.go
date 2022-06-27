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
	Len() uint32
	Close()
}

type PoolItem interface {
	Close() error
}

type Factory func() (PoolItem, error)

type pool struct {
	sync.Mutex
	cap, count  uint32
	queue       chan PoolItem
	factoryFunc Factory
	closing     bool
}

func NewPool(cap uint32, f Factory) *pool {
	return &pool{
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
		if item, err = p.factoryFunc(); err != nil {
			return nil, err
		}
		atomic.AddUint32(&p.count, 1)
		return item, nil
	}
	return <-p.queue, nil
}

func (p *pool) GetWithContext(ctx context.Context) (item PoolItem, err error) {
	if p.closing {
		return nil, ErrClosing
	}
	if len(p.queue) == 0 && p.count < p.cap {
		if item, err = p.factoryFunc(); err != nil {
			return nil, err
		}

		atomic.AddUint32(&p.count, 1)
		return item, nil
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case item = <-p.queue:
		return item, nil
	}
}

func (p *pool) Put(item PoolItem) {
	if p.closing {
		item.Close()
		atomic.AddUint32(&p.count, ^uint32(0))
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
		item.Close()
		atomic.AddUint32(&p.count, ^uint32(0))
	}
}

func (p *pool) Len() uint32 { return p.count }
