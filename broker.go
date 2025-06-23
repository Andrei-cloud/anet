package anet

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

var (
	// ErrQuit indicates the broker is shutting down normally.
	ErrQuit = errors.New("broker is quitting")

	// ErrClosingBroker indicates the broker is in the process of closing.
	ErrClosingBroker = errors.New("broker is closing")

	// ErrNoPoolsAvailable indicates no connection pools are available.
	ErrNoPoolsAvailable = errors.New("no connection pools available")
)

// BrokerConfig contains configuration options for a broker.
type BrokerConfig struct {
	// WriteTimeout is the timeout for writing to connections. Default is 5s.
	WriteTimeout time.Duration
	// ReadTimeout is the timeout for reading from connections. Default is 5s.
	ReadTimeout time.Duration
	// QueueSize is the size of the request queue. Default is 1000.
	QueueSize int
	// OptimizeMemory enables memory optimization features like task ID pooling.
	// When enabled, reduces allocations and improves performance. Default is false.
	OptimizeMemory bool
}

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

// broker implements the Broker interface.
type broker struct {
	workers      int
	compool      []Pool
	requestQueue chan *Task // Use blocking channel instead of RingBuffer
	pending      sync.Map
	activeConns  sync.Map
	//nolint:containedctx // Necessary for task cancellation within broker queue.
	ctx      context.Context
	cancel   context.CancelFunc
	logger   Logger
	wg       sync.WaitGroup
	closing  atomic.Bool
	config   *BrokerConfig
	poolIdx  atomic.Uint32 // atomic pool selection index
	taskPool sync.Pool     // Pool for Task structs
	respPool sync.Pool     // Pool for response channels
	errPool  sync.Pool     // Pool for error channels
}

// NoopLogger provides a default no-op logger.
type NoopLogger struct{}

// DefaultBrokerConfig returns the default broker configuration.
func DefaultBrokerConfig() *BrokerConfig {
	return &BrokerConfig{
		WriteTimeout:   5 * time.Second,
		ReadTimeout:    5 * time.Second,
		QueueSize:      1000,
		OptimizeMemory: true, // Memory optimization enabled by default.
	}
}

func (l *NoopLogger) Print(_ ...any)            {}
func (l *NoopLogger) Printf(_ string, _ ...any) {}
func (l *NoopLogger) Infof(_ string, _ ...any)  {}
func (l *NoopLogger) Warnf(_ string, _ ...any)  {}
func (l *NoopLogger) Errorf(_ string, _ ...any) {}

// NewBroker creates a new message broker.
func NewBroker(p []Pool, n int, l Logger, config *BrokerConfig) Broker {
	if l == nil {
		l = &NoopLogger{}
	}
	if config == nil {
		config = DefaultBrokerConfig()
	}
	ctx, cancel := context.WithCancel(context.Background())

	b := &broker{
		workers:      n,
		compool:      p,
		requestQueue: make(chan *Task, config.QueueSize), // Buffered channel for efficient queuing
		ctx:          ctx,
		cancel:       cancel,
		logger:       l,
		config:       config,
	}

	// Initialize object pools for memory optimization
	b.taskPool = sync.Pool{
		New: func() any {
			return &Task{}
		},
	}
	b.respPool = sync.Pool{
		New: func() any {
			return make(chan []byte, 1)
		},
	}
	b.errPool = sync.Pool{
		New: func() any {
			return make(chan error, 1)
		},
	}

	return b
}

// Send sends a request and waits for the response.
func (b *broker) Send(req *[]byte) ([]byte, error) {
	allUsed := true
	for _, p := range b.compool {
		if p.Len() < p.Cap() {
			allUsed = false

			break
		}
	}
	if allUsed {
		return nil, ErrClosingBroker
	}
	// Use context.TODO for Send to avoid allocation overhead
	// Use context.TODO for Send to avoid allocation overhead - safe since Send doesn't use context
	task := b.newTask(context.TODO(), req)
	if b.closing.Load() {
		return nil, ErrClosingBroker
	}
	b.pending.Store(task.id, task)
	// Use non-blocking channel send - fail if queue is full or broker closing
	select {
	case b.requestQueue <- task:
		// Successfully queued
	default:
		// Queue full or broker closing
		b.failPending(task)

		return nil, ErrClosingBroker
	}
	select {
	case resp := <-task.response:
		return resp, nil
	case err := <-task.errCh:
		return nil, err
	}
}

// SendContext sends a request with context support.
func (b *broker) SendContext(ctx context.Context, req *[]byte) ([]byte, error) {
	task := b.newTask(ctx, req)
	if b.closing.Load() {
		return nil, ErrClosingBroker
	}
	b.pending.Store(task.id, task)
	// Use non-blocking channel send with context checking
	select {
	case b.requestQueue <- task:
		// Successfully queued
	case <-ctx.Done():
		b.failPending(task)

		return nil, ctx.Err()
	default:
		// Queue full or broker closing
		b.failPending(task)
		if b.ctx.Err() != nil {
			return nil, ErrClosingBroker
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		return nil, ErrClosingBroker
	}
	select {
	case resp := <-task.response:
		return resp, nil
	case err := <-task.errCh:
		return nil, err
	case <-ctx.Done():
		b.failPending(task)

		return nil, ctx.Err()
	}
}

// Start launches worker goroutines to process requests.
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

func (b *broker) loop(_ int) error {
	for {
		// Blocking receive from queue - this eliminates spinning!
		// Workers will efficiently block until work is available
		select {
		case task := <-b.requestQueue:
			if task == nil {
				b.logger.Errorf("broker: received nil task (possible bug)")

				continue
			}

			// Check closing status without context - faster atomic check
			if b.closing.Load() {
				b.trySendError(task, ErrClosingBroker)

				continue
			}

			// Get task context once and cache it
			taskCtx := task.Context()

			// Use lock-free pool selection
			p := b.pickConnPool()

			if p == nil {
				b.trySendError(task, ErrNoPoolsAvailable)

				continue
			}

			// Context-aware connection retrieval
			var wr PoolItem
			var err error
			if taskCtx != nil {
				wr, err = p.GetWithContext(taskCtx)
			} else {
				wr, err = p.Get()
			}

			if err != nil {
				// Only check context error after operation failure
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

		case <-b.ctx.Done():
			return ErrQuit
		}
	}
}

func (b *broker) handleConnection(task *Task, wr PoolItem) error {
	netConn, ok := wr.(net.Conn)
	if !ok {
		err := errors.New("internal error: pool item is not net.Conn")
		b.trySendError(task, err)

		return err
	}

	// sync.Map is already thread-safe, no additional locking needed
	b.activeConns.Store(task.id, netConn)

	defer func() {
		b.activeConns.Delete(task.id)
	}()

	cmd := b.addTask(task)

	if b.closing.Load() {
		b.trySendError(task, ErrClosingBroker)

		return ErrClosingBroker
	}

	writeDeadline := time.Now().Add(b.config.WriteTimeout)
	if err := netConn.SetWriteDeadline(writeDeadline); err != nil {
		b.trySendError(task, fmt.Errorf("setting write deadline: %w", err))

		return err
	}

	if err := Write(netConn, cmd); err != nil {
		b.trySendError(task, fmt.Errorf("writing to connection: %w", err))

		return err
	}

	readDeadline := time.Now().Add(b.config.ReadTimeout)
	if err := netConn.SetReadDeadline(readDeadline); err != nil {
		b.trySendError(task, fmt.Errorf("setting read deadline: %w", err))

		return err
	}

	taskCtx := task.Context()
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

	_ = netConn.SetDeadline(time.Time{})

	return nil
}

func (b *broker) pickConnPool() Pool {
	if len(b.compool) == 0 {
		return nil
	}
	if len(b.compool) == 1 {
		return b.compool[0]
	}
	// Atomic round-robin pool selection for better load distribution
	idx := b.poolIdx.Add(1) % uint32(len(b.compool))

	return b.compool[idx]
}

func (b *broker) trySendError(task *Task, err error) {
	defer func() { _ = recover() }()
	select {
	case task.errCh <- err:
		b.failPending(task)
	default:
	}
}

func (b *broker) respondPending(resp []byte) {
	if len(resp) < taskIDSize {
		return
	}
	taskID := binary.BigEndian.Uint32(resp[:taskIDSize])

	if value, ok := b.pending.Load(taskID); ok {
		task, castOK := value.(*Task)
		if !castOK {
			b.pending.Delete(taskID)
			return
		}

		sent := false
		func() {
			defer func() { _ = recover() }()
			select {
			case task.response <- resp[taskIDSize:]:
				sent = true
			default:
			}
		}()

		if sent {
			b.pending.Delete(taskID)
			if task.optimized {
				globalTaskIDPool.putTaskID(task.taskID)
			}
			// Return successful tasks to pool as well
			if task.pooled {
				// Don't close channels for successful response, just return to pool
				go func() {
					// Wait briefly to ensure response was read, then return to pool
					select {
					case <-task.response:
					case <-time.After(time.Millisecond):
					}
					b.returnTaskToPool(task)
				}()
			}
		}
	}
}

func (b *broker) failPending(task *Task) {
	b.pending.Delete(task.id)
	if task.optimized {
		globalTaskIDPool.putTaskID(task.taskID)
	}
	func() {
		defer func() { _ = recover() }()
		close(task.response)
		close(task.errCh)
	}()

	// Return pooled objects to reduce memory pressure
	if task.pooled {
		b.returnTaskToPool(task)
	}
}

// returnTaskToPool returns a Task and its channels back to the pools.
func (b *broker) returnTaskToPool(task *Task) {
	// Clear sensitive data
	respCh := task.response
	errCh := task.errCh

	// Drain channels before returning to pool
	select {
	case <-respCh:
	default:
	}
	select {
	case <-errCh:
	default:
	}

	// Return to pools
	b.respPool.Put(respCh)
	b.errPool.Put(errCh)
	b.taskPool.Put(task)
}

func (b *broker) newTask(ctx context.Context, r *[]byte) *Task {
	// assign unique integer ID and encode into 4-byte header
	id := atomic.AddUint32(&nextTaskID, 1)

	// Use optimized task ID allocation if available
	var taskIDBytes []byte
	if b.config != nil && b.config.OptimizeMemory {
		taskIDBytes = globalTaskIDPool.getTaskID()
	} else {
		taskIDBytes = make([]byte, taskIDSize)
	}

	binary.BigEndian.PutUint32(taskIDBytes, id)

	// Get pooled Task struct and channels to reduce allocations
	task, ok := b.taskPool.Get().(*Task)
	if !ok {
		task = &Task{}
	}
	respCh, ok := b.respPool.Get().(chan []byte)
	if !ok {
		respCh = make(chan []byte, 1)
	}
	errCh, ok := b.errPool.Get().(chan error)
	if !ok {
		errCh = make(chan error, 1)
	}

	// Reset and initialize the task
	*task = Task{
		ctx:       ctx,
		id:        id,
		taskID:    taskIDBytes,
		request:   r,
		response:  respCh,
		errCh:     errCh,
		optimized: b.config != nil && b.config.OptimizeMemory,
		pooled:    true, // Mark as using pools for cleanup
	}

	return task
}

func (b *broker) addTask(task *Task) []byte {
	totalSize := taskIDSize + len(*task.request)

	// Use buffer pool for command allocation to reduce GC pressure
	cmd := globalBufferPool.getBuffer(totalSize)
	if len(cmd) > totalSize {
		cmd = cmd[:totalSize] // Trim to exact size needed
	}

	copy(cmd[:taskIDSize], task.taskID)
	copy(cmd[taskIDSize:], *task.request)

	return cmd
}

// Close shuts down the broker, canceling context and waiting for workers to exit.
func (b *broker) Close() {
	// cancel broker context to stop workers
	b.cancel()
	// wait for all worker goroutines to finish
	b.wg.Wait()
}
