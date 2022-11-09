package anet

import (
	"context"
	"fmt"
	"io"
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
	sendWorkers  int
	recvWorkers  int
	recvQueue    chan PoolItem
	compool      Pool
	requestQueue chan *Task
	pending      sync.Map
	quit         chan struct{}
}

func NewBroker(p Pool, n int, l Logger) *broker {
	return &broker{
		sendWorkers:  n,
		compool:      p,
		recvWorkers:  p.Cap(),
		recvQueue:    make(chan PoolItem, p.Cap()),
		requestQueue: make(chan *Task, n),
		quit:         make(chan struct{}),
	}
}

func (b *broker) Start() error {
	eg := &errgroup.Group{}

	for i := 0; i < b.sendWorkers; i++ {
		eg.Go(func() error {
			return b.writeloop()
		})
		eg.Go(func() error {
			return b.readloop()
		})
	}

	return eg.Wait()
}

func (b *broker) Close() {
	close(b.quit)
	close(b.requestQueue)
	close(b.recvQueue)
	b.compool.Close()
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

func (b *broker) writeloop() error {
	var (
		cmd  []byte
		task *Task
	)
	for {
		select {
		case <-b.quit:
			return ErrQuit
		case task = <-b.requestQueue:
			w, err := b.compool.Get()
			if err != nil {
				select {
				case <-task.errCh:
				default:
					task.errCh <- err
				}
				continue
			}

			cmd = b.addTask(task)

			err = Write(w.(io.Writer), cmd)
			if err != nil {
				select {
				case <-task.errCh:
				default:
					task.errCh <- err
				}
				b.compool.Release(w)
				continue
			}

			b.recvQueue <- w
		}
	}
}

func (b *broker) readloop() error {
	var (
		err  error
		resp []byte
	)
	for {
		select {
		case <-b.quit:
			return ErrQuit
		case r := <-b.recvQueue:
			resp, err = Read(r.(io.Reader))
			if err != nil {
				b.compool.Release(r)
				continue
			}
			b.respondPending(resp)
			b.compool.Put(r)
		}
	}
}
