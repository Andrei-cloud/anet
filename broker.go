package anet

import (
	"context"
	"encoding/hex" // Import hex package
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
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
	Start() error
	Close()
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
	//nolint:containedctx // Necessary for task cancellation within the broker queue.
	ctx     context.Context
	cancel  context.CancelFunc
	logger  Logger
	rng     *rand.Rand
	wg      sync.WaitGroup
	closing atomic.Bool
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
	ctx, cancel := context.WithCancel(context.Background())

	return &broker{
		workers:      n,
		compool:      p,
		recvQueue:    make(chan PoolItem, n),
		requestQueue: make(chan *Task, n),
		ctx:          ctx,
		cancel:       cancel,
		logger:       l,
		rng:          rng,
	}
}

func (b *broker) Start() error {
	eg := &errgroup.Group{}
	b.logger.Printf("Broker starting with %d workers...", b.workers)

	for i := 0; i < b.workers; i++ {
		workerID := i
		b.wg.Add(1)
		eg.Go(func() error {
			defer b.wg.Done()
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

// Close shuts down the broker and associated connection pools.
func (b *broker) Close() {
	b.mu.Lock()
	if b.closing.Load() {
		b.mu.Unlock()
		return // Already closing
	}
	b.closing.Store(true)
	b.cancel() // signal loop via context cancel
	b.mu.Unlock()

	// Wait for all workers to finish processing their current task (if any)
	b.wg.Wait()

	// Now that workers are stopped, safely close the request channel
	// This prevents workers from trying to send results to a closed channel
	close(b.requestQueue)

	// Fail any remaining pending tasks that were never picked up by a worker
	b.pending.Range(func(key any, value any) bool {
		task, ok := value.(*Task)
		if !ok {
			// Log or handle the unexpected type if necessary
			b.logger.Printf("Unexpected type in pending map for key %v", key)

			return true // Continue ranging
		}

		select {
		case task.errCh <- ErrClosingBroker:
		default:
		}
		close(task.response)
		close(task.errCh)
		b.pending.Delete(key)

		return true
	})

	// Close the underlying connection pools
	for _, p := range b.compool {
		p.Close()
	}

	b.logger.Print("Broker closed.")
}

func (b *broker) Send(req *[]byte) ([]byte, error) {
	var (
		resp []byte
		err  error
	)
	task := b.newTask(context.Background(), req)

	// Lock to check closing status and potentially send
	b.mu.Lock()
	if b.closing.Load() {
		b.mu.Unlock()
		// Use the specific error for closing broker
		return nil, ErrClosingBroker
	}

	// Add task to pending list *before* sending to queue, under lock
	b.pending.Store(string(task.taskID), task)

	// Use select to handle potential blocking or closed channel during shutdown
	select {
	case b.requestQueue <- task:
		// Successfully sent task
		b.mu.Unlock() // Unlock *after* successful send
	case <-b.ctx.Done():
		// Broker quit while trying to send
		b.mu.Unlock()       // Unlock before failing
		b.failPending(task) // Clean up the task
		return nil, ErrClosingBroker
	default:
		// This case might happen if the queue is full and we don't want to block indefinitely
		// Or if the channel was closed between the `if b.closing` check and now.
		b.mu.Unlock()
		b.failPending(task)          // Clean up the task
		return nil, ErrClosingBroker // Treat as if broker is closing
	}

	// Wait for response or error outside the lock
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

	// Lock to check closing status and potentially send
	b.mu.Lock()
	if b.closing.Load() {
		b.mu.Unlock()
		// Use the specific error for closing broker
		return nil, ErrClosingBroker
	}

	// Add task to pending list *before* sending to queue, under lock
	b.pending.Store(string(task.taskID), task)

	// Use select to handle potential blocking or closed channel during shutdown
	select {
	case b.requestQueue <- task:
		// Successfully sent task
		b.mu.Unlock() // Unlock *after* successful send
	case <-b.ctx.Done():
		// Broker quit while trying to send
		b.mu.Unlock()       // Unlock before failing
		b.failPending(task) // Clean up the task
		return nil, ErrClosingBroker
	case <-ctx.Done():
		// Context was canceled while trying to send
		b.mu.Unlock()       // Unlock before failing
		b.failPending(task) // Clean up the task
		return nil, ctx.Err()
	default:
		// This case might happen if the queue is full and we don't want to block indefinitely
		// Or if the channel was closed between the `if b.closing` check and now.
		b.mu.Unlock()
		b.failPending(task)          // Clean up the task
		return nil, ErrClosingBroker // Treat as if broker is closing
	}

	// Wait for response or error outside the lock
	select {
	case resp = <-task.response:
		return resp, nil
	case err = <-task.errCh:
		b.failPending(task)
		return nil, err
	case <-ctx.Done():
		b.failPending(task)
		return nil, ctx.Err()
	}
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

// loop is the main worker loop for processing tasks.
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
		case <-b.ctx.Done(): // Prioritize quit signal
			b.logger.Printf("Worker %d received quit signal", workerID)
			return ErrQuit
		case task, ok := <-b.requestQueue:
			if !ok {
				// Task channel closed, likely during shutdown
				b.logger.Printf("Worker %d: requestQueue closed, exiting loop.", workerID)
				return nil
			}
			taskCtx := task.Context()
			b.logger.Printf("Worker %d received task %s", workerID, hex.EncodeToString(task.taskID))

			p = b.pickConnPool()
			if p == nil {
				err = ErrNoPoolsAvailable
				b.logger.Printf(
					"Worker %d: Error picking pool for task %s: %v",
					workerID,
					hex.EncodeToString(task.taskID),
					err,
				)
				b.trySendError(task, err)

				continue
			}

			// Get connection with context awareness from the task
			if taskCtx != nil {
				wr, err = p.GetWithContext(taskCtx)
			} else {
				wr, err = p.Get()
			}

			if err != nil {
				// Check if the error is due to the task's context being done
				if taskCtx != nil && errors.Is(err, taskCtx.Err()) {
					b.logger.Printf(
						"Task %s context done while getting connection: %v",
						hex.EncodeToString(task.taskID),
						err,
					)
					// Don't call trySendError here, the SendContext will handle the context error
				} else {
					b.logger.Printf(
						"Worker %d: Error getting connection for task %s from pool: %v",
						workerID,
						hex.EncodeToString(task.taskID),
						err,
					)
					b.trySendError(task, fmt.Errorf("failed to get connection: %w", err))
				}

				continue
			}
			b.logger.Printf(
				"Worker %d: Acquired connection for task %s",
				workerID,
				hex.EncodeToString(task.taskID),
			)

			// Ensure wr is not nil before type assertion
			netConn, ok := wr.(net.Conn)
			if !ok {
				b.logger.Printf(
					"Worker task %s: PoolItem is not a net.Conn",
					hex.EncodeToString(task.taskID),
				)
				// Use errors.New for static error messages
				b.trySendError(task, errors.New("internal error: pool item is not net.Conn"))
				p.Release(wr) // Release the invalid item

				continue
			}

			cmd = b.addTask(task)

			err = Write(netConn, cmd)
			if err == nil {
				resp, err = Read(netConn)
			}

			// Decide whether to put back or release based on error
			if err != nil {
				b.logger.Printf(
					"Worker %d: Error reading from connection for task %s: %v",
					workerID,
					hex.EncodeToString(task.taskID),
					err,
				)
				b.trySendError(task, fmt.Errorf("connection error: %w", err))
				p.Release(wr) // Release connection on error
			} else {
				b.logger.Printf(
					"Worker %d: Successfully read %d bytes for task %s",
					workerID,
					len(resp),
					hex.EncodeToString(task.taskID),
				)
				b.respondPending(resp)
				p.Put(wr) // Put connection back on success
				b.logger.Printf(
					"Worker %d: Completed task %s",
					workerID,
					hex.EncodeToString(task.taskID),
				)
			}
		}
	}
}

func (b *broker) trySendError(task *Task, err error) {
	select {
	case task.errCh <- err:
	default:
		b.logger.Printf("Failed to send error to task %s: %v", hex.EncodeToString(task.taskID), err)
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

// addTask prepares the command bytes including the task ID header.
// It no longer adds the task to the pending map, as that's now done in Send/SendContext.
func (b *broker) addTask(task *Task) []byte {
	cmd := make([]byte, taskIDSize+len(*task.request))
	copy(cmd[:taskIDSize], task.taskID)
	copy(cmd[taskIDSize:], *task.request)

	return cmd
}
