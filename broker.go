package anet

import (
	"context"
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
	// ErrTimeout indicates a response was not received within the deadline.
	ErrTimeout = errors.New("timeout on response")

	// ErrQuit indicates the broker is shutting down.
	ErrQuit = errors.New("broker is quiting")

	// ErrClosingBroker indicates the broker is in the process of closing.
	ErrClosingBroker = errors.New("broker is closing")

	// ErrNoPoolsAvailable indicates no connection pools are available.
	ErrNoPoolsAvailable = errors.New("no connection pools available")
)

// Broker coordinates sending requests and receiving responses over pooled connections.
type Broker interface {
	Send(*[]byte) ([]byte, error)
	SendContext(context.Context, *[]byte) ([]byte, error)
	Start() error
	Close()
}

// Logger handles structured logging for the broker.
type Logger interface {
	Print(v ...any)                 // Info level.
	Printf(format string, v ...any) // Info level formatted.
	Infof(format string, v ...any)  // Info level with formatting.
	Warnf(format string, v ...any)  // Warning level.
	Errorf(format string, v ...any) // Error level.
}

type PendingList sync.Map

type broker struct {
	mu           sync.Mutex
	connMu       sync.RWMutex // Add mutex for connection operations.
	workers      int
	recvQueue    chan PoolItem
	compool      []Pool
	requestQueue chan *Task
	pending      sync.Map
	activeConns  sync.Map // map[string]net.Conn.
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

// Infof does nothing.
func (l *NoopLogger) Infof(_ string, _ ...any) {}

// Warnf does nothing.
func (l *NoopLogger) Warnf(_ string, _ ...any) {}

// Errorf does nothing.
func (l *NoopLogger) Errorf(_ string, _ ...any) {}

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
	b.logger.Infof("Broker starting with %d workers...", b.workers)

	for i := 0; i < b.workers; i++ {
		workerID := i
		b.wg.Add(1)
		eg.Go(func() error {
			defer b.wg.Done()
			err := b.loop(workerID)
			return err
		})
	}

	err := eg.Wait()
	if err != nil && !errors.Is(err, ErrQuit) {
		b.logger.Errorf("Broker stopped with error: %v", err)
	} else {
		b.logger.Infof("Broker stopped gracefully.")
	}

	return err
}

// Close shuts down the broker and associated connection pools.
func (b *broker) Close() {
	b.mu.Lock()
	if b.closing.Load() {
		b.mu.Unlock()
		return
	}
	b.closing.Store(true)
	b.cancel()
	b.mu.Unlock()

	// Close all active connections immediately.
	b.connMu.Lock()
	b.activeConns.Range(func(key, value any) bool {
		if conn, ok := value.(net.Conn); ok {
			if err := conn.Close(); err != nil {
				// Use key as string after type assertion
				if keyStr, ok := key.(string); ok {
					b.logger.Warnf(
						"Error closing connection for task %s: %v",
						keyStr,
						err,
					)
				}
			}
		}
		b.activeConns.Delete(key)

		return true
	})
	b.connMu.Unlock()

	// Wait for workers to finish with a timeout.
	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		b.logger.Warnf("Timeout waiting for workers to finish, forcing close.")
	}

	// Now that workers are stopped, safely close the request channel.
	close(b.requestQueue)

	// Fail any remaining pending tasks.
	b.pending.Range(func(key, value any) bool {
		task, ok := value.(*Task)
		if !ok {
			return true
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

	// Close the underlying connection pools.
	for i, p := range b.compool {
		p.Close()
		_ = i
	}

	b.logger.Print("Broker closed.")
}

func (b *broker) Send(req *[]byte) ([]byte, error) {
	var (
		resp []byte
		err  error
	)
	task := b.newTask(context.Background(), req)

	b.mu.Lock()
	if b.closing.Load() {
		b.mu.Unlock()
		return nil, ErrClosingBroker
	}

	b.pending.Store(string(task.taskID), task)

	select {
	case b.requestQueue <- task:
		b.mu.Unlock()
	case <-b.ctx.Done():
		b.mu.Unlock()
		b.failPending(task)
		return nil, ErrClosingBroker
	default:
		b.mu.Unlock()
		b.failPending(task)
		return nil, ErrClosingBroker
	}

	select {
	case resp = <-task.response:
		return resp, nil
	case err = <-task.errCh:
		return nil, err
	}
}

func (b *broker) SendContext(ctx context.Context, req *[]byte) ([]byte, error) {
	var (
		resp []byte
		err  error
	)

	task := b.newTask(ctx, req)

	b.mu.Lock()
	if b.closing.Load() {
		b.mu.Unlock()
		return nil, ErrClosingBroker
	}

	b.pending.Store(string(task.taskID), task)

	select {
	case b.requestQueue <- task:
		b.mu.Unlock()
	case <-b.ctx.Done():
		b.mu.Unlock()
		b.failPending(task)
		return nil, ErrClosingBroker
	case <-ctx.Done():
		b.mu.Unlock()
		b.failPending(task)
		return nil, ctx.Err()
	default:
		b.mu.Unlock()
		b.failPending(task)
		return nil, ErrClosingBroker
	}

	select {
	case resp = <-task.response:
		return resp, nil
	case err = <-task.errCh:
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
	for {
		select {
		case <-b.ctx.Done():
			return ErrQuit
		case task, ok := <-b.requestQueue:
			if !ok {
				return nil
			}

			if b.closing.Load() {
				b.trySendError(task, ErrClosingBroker)
				continue
			}

			taskCtx := task.Context()

			b.connMu.RLock()
			p := b.pickConnPool()
			b.connMu.RUnlock()

			if p == nil {
				err := ErrNoPoolsAvailable
				b.trySendError(task, err)

				continue
			}

			var wr PoolItem
			var err error
			if taskCtx != nil {
				wr, err = p.GetWithContext(taskCtx)
			} else {
				wr, err = p.Get()
			}

			if err != nil {
				if taskCtx != nil && errors.Is(err, taskCtx.Err()) {
					continue
				}
				b.trySendError(task, fmt.Errorf("failed to get connection: %w", err))

				continue
			}

			err = b.handleConnection(task, wr)
			if err != nil {
				p.Release(wr)

				continue
			}

			p.Put(wr)
		}
	}
}

// handleConnection manages a single connection operation.
func (b *broker) handleConnection(task *Task, wr PoolItem) error {
	netConn, ok := wr.(net.Conn)
	if !ok {
		err := errors.New("internal error: pool item is not net.Conn")
		b.trySendError(task, err)
		return err
	}

	b.connMu.Lock()
	b.activeConns.Store(string(task.taskID), netConn)
	b.connMu.Unlock()

	defer func() {
		b.connMu.Lock()
		b.activeConns.Delete(string(task.taskID))
		b.connMu.Unlock()
	}()

	cmd := b.addTask(task)

	if b.closing.Load() {
		err := ErrClosingBroker
		b.trySendError(task, err)
		return err
	}

	taskCtx := task.Context()

	writeDeadline := time.Now().Add(5 * time.Second)
	if err := netConn.SetWriteDeadline(writeDeadline); err != nil {
		wrappedErr := fmt.Errorf("setting write deadline: %w", err)
		b.trySendError(task, wrappedErr)
		return wrappedErr
	}

	if err := Write(netConn, cmd); err != nil {
		wrappedErr := fmt.Errorf("writing to connection: %w", err)
		b.trySendError(task, wrappedErr)
		return wrappedErr
	}

	readDeadline := time.Now().Add(5 * time.Second)
	if err := netConn.SetReadDeadline(readDeadline); err != nil {
		wrappedErr := fmt.Errorf("setting read deadline: %w", err)
		b.trySendError(task, wrappedErr)
		return wrappedErr
	}

	if taskCtx != nil {
		done := make(chan struct{})
		var readErr error
		go func() {
			defer close(done)
			resp, err := Read(netConn)
			if err != nil {
				readErr = fmt.Errorf("reading from connection: %w", err)
				b.trySendError(task, readErr)
				return
			}
			b.respondPending(resp)
		}()

		select {
		case <-taskCtx.Done():
			err := taskCtx.Err()
			return err
		case <-done:
			if readErr != nil {
				return readErr
			}
		}
	} else {
		resp, err := Read(netConn)
		if err != nil {
			wrappedErr := fmt.Errorf("reading from connection: %w", err)
			b.trySendError(task, wrappedErr)
			return wrappedErr
		}
		b.respondPending(resp)
	}

	// Ignore error when clearing deadline as connection will be reused
	_ = netConn.SetDeadline(time.Time{})

	return nil
}

// trySendError attempts to send an error to the task's error channel.
func (b *broker) trySendError(task *Task, err error) {
	defer func() {
		_ = recover() // Explicitly ignore recover value
	}()

	select {
	case task.errCh <- err:
		b.failPending(task)
	default:
	}
}

// respondPending finds the pending task by response header and sends the response.
func (b *broker) respondPending(resp []byte) {
	if len(resp) < taskIDSize {
		return
	}
	taskID := string(resp[:taskIDSize])

	if value, ok := b.pending.Load(taskID); ok {
		task, castOK := value.(*Task)
		if !castOK {
			b.pending.Delete(taskID)
			return
		}

		sent := false
		func() {
			defer func() {
				_ = recover() // Explicitly ignore recover value
			}()
			select {
			case task.response <- resp[taskIDSize:]:
				sent = true
			default:
			}
		}()

		if sent {
			b.pending.Delete(taskID)
		}
	}
}

// failPending removes a task from the pending list without sending a response.
// Used when an error occurs before a response is generated or context is canceled.
func (b *broker) failPending(task *Task) {
	b.pending.Delete(string(task.taskID))
	func() {
		defer func() {
			_ = recover() // Explicitly ignore recover value
		}()
		close(task.response)
		close(task.errCh)
	}()
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
