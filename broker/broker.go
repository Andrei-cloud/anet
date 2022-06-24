package broker

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/andrei-cloud/anet/pool"
	"golang.org/x/sync/errgroup"
)

var ErrTimeout = fmt.Errorf("timeout on response")

const taskIDSize = 5

type Broker interface {
	SendReceive(*[]byte) (chan *[]byte, error)
}

type Task struct {
	taskID    string
	request   []byte
	response  chan []byte
	errCh     chan error
	timestamp time.Time
}

type PendingList map[string]*Task

type broker struct {
	sync.Mutex
	workers      int
	connPool     pool.Pool
	requestQueue chan *Task
	pending      PendingList
	quit         chan struct{}

	timeout time.Duration
}

func NewBroker(cp pool.Pool, n int) *broker {
	return &broker{
		workers:      n,
		connPool:     cp,
		requestQueue: make(chan *Task, n),
		pending:      make(PendingList),
		quit:         make(chan struct{}),
		timeout:      5 * time.Second,
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
		log.Fatal("Error", err)
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

func (b *broker) Send(req []byte) ([]byte, error) {
	var (
		resp []byte
		err  error
	)
	task := b.newTask(req)

	b.requestQueue <- task

	select {
	case resp = <-task.response:
	case err = <-task.errCh:
		return nil, err
	case <-time.After(b.timeout):
		return nil, ErrTimeout
	}

	return resp, nil
}

func (b *broker) newTask(r []byte) *Task {
	return &Task{
		taskID:    randString(taskIDSize),
		request:   r,
		response:  make(chan []byte),
		errCh:     make(chan error),
		timestamp: time.Now(),
	}
}

func (b *broker) addTask(task *Task) []byte {
	b.Lock()
	b.pending[task.taskID] = task
	b.Unlock()
	return append([]byte(task.taskID), task.request...)
}

func (b *broker) worker(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task := <-b.requestQueue:
			out, err := Encode(b.addTask(task))
			if err != nil {
				log.Println(err)
				return err
			}
			conn, err := b.connPool.GetWithContext(ctx)
			if err != nil {
				log.Println("error getting connection", err)
				return err
			}
			c := conn.(net.Conn)

			n, err := c.Write(out)
			if err != nil {
				if err == io.EOF {
					log.Printf("connection %s is closed\n", c.RemoteAddr())
					return err
				}
				return err
			}
			log.Printf("write %d bytes to %s\n", n, c.RemoteAddr())
			log.Printf("%s -> %s\n", c.RemoteAddr(), out)

			resp, err := Decode(bufio.NewReader(c))
			if err != nil {
				if err == io.EOF {
					log.Printf("connection %s is closed\n", c.RemoteAddr())
					return err
				}
				log.Println(err)
				return err
			}
			log.Printf("read %d bytes from %s\n", n, c.RemoteAddr())
			log.Printf("%s <- %s\n", c.RemoteAddr(), resp)
			b.respondPending(resp)
			b.connPool.Put(conn)
		}

	}
}

func (b *broker) respondPending(msg []byte) {
	var (
		task *Task
		ok   bool
	)
	header := string((msg)[:5])
	response := (msg)[5:]
	b.Lock()
	defer b.Unlock()
	if task, ok = b.pending[header]; !ok {
		log.Printf("pending task for %s not found; response descarded\n", header)
		return
	}
	defer close(task.response)
	task.response <- response
	delete(b.pending, header)
}
