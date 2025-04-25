package anet

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

var (
	ErrTimeout          = errors.New("timeout on response")
	ErrQuit             = errors.New("broker is quiting")
	ErrClosingBroker    = errors.New("broker is closing")
	ErrMaxLenExceeded   = errors.New("max length exceeded")
	ErrNoPoolsAvailable = errors.New("no connection pools available")
)

type Broker interface {
	Send(*[]byte) ([]byte, error)
	SendContext(context.Context, *[]byte) ([]byte, error)
}

type Logger interface {
	Print(v ...any)
	Printf(format string, v ...any)
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
	logger       Logger
	rng          *rand.Rand
}

// NoopLogger provides a default logger that does nothing.
type NoopLogger struct{}

// Print does nothing.
func (l *NoopLogger) Print(_ ...any) {}

// Printf does nothing.
func (l *NoopLogger) Printf(_ string, _ ...any) {}

// NewBroker creates a new message broker. Returns the Broker interface.
func NewBroker(p []Pool, n int, l Logger) Broker {
	if l == nil {
		l = &NoopLogger{}
	}
	rngSource := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(rngSource)

	return &broker{
		workers:      n,
		compool:      p,
		recvQueue:    make(chan PoolItem, n),
		requestQueue: make(chan *Task, n),
		quit:         make(chan struct{}),
		logger:       l,
		rng:          rng,
	}
}

func (b *broker) Start() error {
	eg := &errgroup.Group{}
	b.logger.Printf("Broker starting with %d workers...", b.workers)

	for i := 0; i < b.workers; i++ {
		workerID := i
		eg.Go(func() error {
			b.logger.Printf("Worker %d starting loop", workerID)
			err := b.loop(workerID)
			b.logger.Printf("Worker %d finished loop: %v", workerID, err)

			return err
		})
	}

	err := eg.Wait()
	b.logger.Printf("Broker stopped. Final error: %v", err)

	return err
}

func (b *broker) Close() {
	b.logger.Print("Broker closing...")
	close(b.quit)

	for _, p := range b.compool {
		p.Close()
	}

	b.pending.Range(func(key any, value any) bool {
		task := value.(*Task)
		select {
		case task.errCh <- ErrClosingBroker:
		default:
		}
		close(task.response)
		close(task.errCh)
		b.pending.Delete(key)

		return true
	})
	b.logger.Print("Broker closed.")
}

func (b *broker) Send(req *[]byte) ([]byte, error) {
	var (
		resp []byte
		err  error
	)
	task := b.newTask(context.Background(), req)

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

	task := b.newTask(ctx, req)

	select {
	case b.requestQueue <- task:
	default:
		return nil, ErrClosingBroker
	}

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
	if len(b.compool) == 0 {
		return nil
	}

	return b.activePool()[b.rng.Intn(len(b.compool))]
}

func (b *broker) activePool() []Pool {
	return b.compool
}

func (b *broker) loop(workerID int) error {
	var (
		cmd  []byte
		resp []byte
		p    Pool
		wr   PoolItem
		err  error
	)
	for {
		select {
		case <-b.quit:
			b.logger.Printf("Worker %d received quit signal", workerID)
			return ErrQuit
		case task, ok := <-b.requestQueue:
			if !ok {
				b.logger.Printf("Worker %d: requestQueue closed, exiting loop.", workerID)
				return nil
			}
			taskCtx := task.Context()
			b.logger.Printf("Worker %d received task %s", workerID, string(task.taskID))

			p = b.pickConnPool()
			if p == nil {
				err = ErrNoPoolsAvailable
				b.logger.Printf(
					"Worker %d: Error picking pool for task %s: %v",
					workerID,
					string(task.taskID),
					err,
				)
				b.trySendError(task, err)

				continue
			}

			wr, err = p.GetWithContext(taskCtx)
			if err != nil {
				b.logger.Printf(
					"Worker %d: Error getting connection for task %s from pool: %v",
					workerID,
					string(task.taskID),
					err,
				)
				b.trySendError(task, err)

				continue
			}
			b.logger.Printf(
				"Worker %d: Acquired connection for task %s",
				workerID,
				string(task.taskID),
			)

			cmd = b.addTask(task)

			err = Write(wr.(io.Writer), cmd)
			if err != nil {
				b.logger.Printf(
					"Worker %d: Error writing to connection for task %s: %v",
					workerID,
					string(task.taskID),
					err,
				)
				b.trySendError(task, err)
				p.Release(wr)

				continue
			}
			b.logger.Printf(
				"Worker %d: Successfully wrote %d bytes for task %s",
				workerID,
				len(cmd),
				string(task.taskID),
			)

			resp, err = Read(wr.(io.Reader))
			if err != nil {
				b.logger.Printf(
					"Worker %d: Error reading from connection for task %s: %v",
					workerID,
					string(task.taskID),
					err,
				)
				p.Release(wr)
				b.failPending(task)

				continue
			}
			b.logger.Printf(
				"Worker %d: Successfully read %d bytes for task %s",
				workerID,
				len(resp),
				string(task.taskID),
			)

			b.respondPending(resp)
			p.Put(wr)
			b.logger.Printf("Worker %d: Completed task %s", workerID, string(task.taskID))
		}
	}
}

func (b *broker) trySendError(task *Task, err error) {
	select {
	case task.errCh <- err:
	default:
		b.logger.Printf("Failed to send error to task %s: %v", string(task.taskID), err)
		b.failPending(task)
	}
}

// newTask creates a new task, storing the context.
func (b *broker) newTask(ctx context.Context, r *[]byte) *Task {
	taskIDBytes := make([]byte, taskIDSize)
	b.mu.Lock()
	_, _ = b.rng.Read(taskIDBytes)
	b.mu.Unlock()

	return &Task{
		ctx:      ctx,
		taskID:   taskIDBytes,
		request:  r,
		response: make(chan []byte, 1),
		errCh:    make(chan error, 1),
	}
}
