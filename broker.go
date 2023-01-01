package anet

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"

	"golang.org/x/sync/errgroup"
)

var (
	ErrTimeout        = fmt.Errorf("timeout on response")
	ErrQuit           = fmt.Errorf("broker is quiting")
	ErrClosingBroker  = fmt.Errorf("broker is closing")
	ErrMaxLenExceeded = fmt.Errorf("max length exceeded")
)

type Broker interface {
	Send(*[]byte) ([]byte, error)
	SendContext(context.Context, *[]byte) ([]byte, error)
}

type Logger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
}

type PendingList sync.Map

type broker struct {
	mu           sync.Mutex
	workers      int
	recvQueue    chan PoolItem
	compool      []Pool
	requestQueue chan *Task
	pending      sync.Map
	quit         chan struct{}
}

func NewBroker(p []Pool, n int, l Logger) *broker {
	return &broker{
		workers:      n,
		compool:      p,
		recvQueue:    make(chan PoolItem, n),
		requestQueue: make(chan *Task, n),
		quit:         make(chan struct{}),
	}
}

func (b *broker) Start() error {
	eg := &errgroup.Group{}

	for i := 0; i < b.workers; i++ {
		eg.Go(func() error {
			return b.loop()
		})
	}

	return eg.Wait()
}

func (b *broker) Close() {
	close(b.quit)
	close(b.requestQueue)
	close(b.recvQueue)
	for _, p := range b.compool {
		p.Close()
	}
	b.pending.Range(func(_ any, value any) bool {
		close(value.(*Task).response)
		value.(*Task).errCh <- ErrClosingBroker
		close(value.(*Task).errCh)
		return true
	})
}

func (b *broker) Send(req *[]byte) ([]byte, error) {
	var (
		resp []byte
		err  error
	)
	task := b.newTask(req)

	b.requestQueue <- task

	select {
	case resp = <-task.response:
	case err = <-task.errCh:
		b.failPending(task)
		return nil, err
	}

	return resp, nil
}

func (b *broker) SendContext(ctx context.Context, req *[]byte) ([]byte, error) {
	var (
		resp []byte
		err  error
	)

	task := b.newTask(req)

	b.requestQueue <- task

	select {
	case resp = <-task.response:
	case err = <-task.errCh:
		b.failPending(task)
		return nil, err
	case <-ctx.Done():
		b.failPending(task)
		return nil, err
	}

	return resp, nil
}

func (b *broker) pickConnPool() Pool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.activePool()[rand.Intn(len(b.compool))]
}

func (b *broker) activePool() []Pool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.compool
}

func (b *broker) loop() error {
	var (
		cmd  []byte
		resp []byte
		task *Task
	)
	for {
		select {
		case <-b.quit:
			return ErrQuit
		case task = <-b.requestQueue:
			p := b.pickConnPool()
			wr, err := p.Get()
			if err != nil {
				select {
				case task.errCh <- err:
				default:
				}
				continue
			}

			cmd = b.addTask(task)

			err = Write(wr.(io.Writer), cmd)
			if err != nil {
				select {
				case task.errCh <- err:
				default:
				}
				p.Release(wr)
				continue
			}

			resp, err = Read(wr.(io.Reader))
			if err != nil {
				p.Release(wr)
				continue
			}
			b.respondPending(resp)
			p.Put(wr)
		}
	}
}
