package anet

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

var ErrTimeout = fmt.Errorf("timeout on response")

const taskIDSize = 4

type Broker interface {
	Send([]byte) ([]byte, error)
}

type Logger interface {
	Log(keyvals ...interface{}) error
}

type Task struct {
	taskID   string
	request  *[]byte
	response chan []byte
	errCh    chan error
}

type PendingList map[string]*Task

type broker struct {
	sync.Mutex
	workers      int
	connPool     Pool
	requestQueue chan *Task
	pending      PendingList
	quit         chan struct{}

	logger Logger

	timeout time.Duration
}

func NewBroker(cp Pool, n int, l Logger) *broker {
	return &broker{
		workers:      n,
		connPool:     cp,
		requestQueue: make(chan *Task, n),
		pending:      make(PendingList),
		quit:         make(chan struct{}),

		logger:  l,
		timeout: 5 * time.Second,
	}
}

func (b *broker) Log(keyvals ...interface{}) {
	if b.logger != nil {
		b.logger.Log(keyvals...)
	}
}

func (b *broker) Start(ctx context.Context) {
	eg := &errgroup.Group{}

	for i := 0; i < b.workers; i++ {
		eg.Go(func() error {
			return b.worker(ctx)
		})
	}
	if err := eg.Wait(); err != nil {
		b.Log("error", err)
	}
}

func (b *broker) Close() {
	close(b.quit)
	b.connPool.Close()
	for _, t := range b.pending {
		close(t.response)
	}
	close(b.requestQueue)
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
		return resp, nil
	case err = <-task.errCh:
		b.failPending(task)
		return nil, err
	case <-time.After(b.timeout):
		return nil, ErrTimeout
	}
}

func (b *broker) newTask(r *[]byte) *Task {
	return &Task{
		taskID:   randString(taskIDSize),
		request:  r,
		response: make(chan []byte),
		errCh:    make(chan error),
	}
}

func (b *broker) addTask(task *Task) []byte {
	{
		b.Lock()
		b.pending[task.taskID] = task
		b.Unlock()
	}
	return append([]byte(task.taskID), *task.request...)
}

func (b *broker) worker(ctx context.Context) (err error) {
	var (
		msg []byte
		c   net.Conn
	)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task := <-b.requestQueue:
			msg, err = Encode(b.addTask(task))
			if err != nil {
				b.Log("err", err)
				task.errCh <- err
				continue
			}
			conn, err := b.connPool.GetWithContext(ctx)
			if err != nil {
				b.Log("err", err)
				task.errCh <- err
				b.connPool.Release(conn)
				continue
			}
			c = conn.(net.Conn)

			_, err = c.Write(msg)
			if err != nil {
				b.Log("err", err)
				task.errCh <- err
				b.connPool.Release(conn)
				continue
			}
			b.Log(c.RemoteAddr(), " -> ", string(msg))

			msg, err = Decode(bufio.NewReader(c))
			if err != nil {
				b.Log("err", err)
				task.errCh <- err
				b.connPool.Release(conn)
				continue
			}
			b.Log(c.RemoteAddr(), " <- ", string(msg))
			b.respondPending(msg)
			b.connPool.Put(conn)
		}

	}
}

func (b *broker) respondPending(msg []byte) {
	var (
		task *Task
		ok   bool
	)
	header := string((msg)[:taskIDSize])
	response := (msg)[taskIDSize:]
	b.Lock()
	if task, ok = b.pending[header]; !ok {
		b.Log(fmt.Sprintf("pending task for %s not found; response descarded", header))
		b.Unlock()
		return
	}
	b.Unlock()
	task.response <- response
	{
		b.Lock()
		delete(b.pending, header)
		b.Unlock()
	}
	close(task.response)
	close(task.errCh)
}

func (b *broker) failPending(task *Task) {
	{
		b.Lock()
		delete(b.pending, task.taskID)
		b.Unlock()
	}
	close(task.response)
	close(task.errCh)
}
