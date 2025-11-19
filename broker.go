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

const (
	// pendingShards is the number of shards for the pending task table.
	pendingShards = 64
)

var (
	// ErrQuit indicates the broker is shutting down normally.
	ErrQuit = errors.New("broker is quitting")

	// ErrClosingBroker indicates the broker is in the process of closing.
	ErrClosingBroker = errors.New("broker is closing")

	// ErrNoPoolsAvailable indicates no connection pools are available.
	ErrNoPoolsAvailable = errors.New("no connection pools available")

	// ErrQueueFull indicates the broker's request queue is full.
	ErrQueueFull = errors.New("broker queue full")
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
	//nolint:containedctx // Necessary for task cancellation within broker queue.
	ctx      context.Context
	cancel   context.CancelFunc
	logger   Logger
	wg       sync.WaitGroup
	closing  atomic.Bool
	config   *BrokerConfig
	poolIdx  atomic.Uint32 // atomic pool selection index
	taskPool sync.Pool     // Pool for Task structs
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
			return &Task{
				response: make(chan []byte, 1),
				errCh:    make(chan error, 1),
			}
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
	// Use non-blocking channel send - fail if queue is full or broker closing
	select {
	case b.requestQueue <- task:
		// Successfully queued
	default:
		// Queue full or broker closing
		if b.closing.Load() || b.ctx.Err() != nil {
			return nil, ErrClosingBroker
		}

		return nil, ErrQueueFull
	}
	select {
	case resp := <-task.response:
		// Success path: cleanup here to avoid racing with responder goroutines.
		if task.optimized {
			globalTaskIDPool.putTaskID(task.taskID)
		}
		// Return pooled objects after consumer has received the response.
		b.returnTaskToPool(task)

		return resp, nil
	case err := <-task.errCh:
		// Error path: perform cleanup symmetrically.
		if task.optimized {
			globalTaskIDPool.putTaskID(task.taskID)
		}
		b.returnTaskToPool(task)

		return nil, err
	}
}

// SendContext sends a request with context support.
func (b *broker) SendContext(ctx context.Context, req *[]byte) ([]byte, error) {
	task := b.newTask(ctx, req)
	if b.closing.Load() {
		return nil, ErrClosingBroker
	}
	// Use non-blocking channel send with context checking
	select {
	case b.requestQueue <- task:
		// Successfully queued
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Queue full or broker closing
		if b.closing.Load() || b.ctx.Err() != nil {
			return nil, ErrClosingBroker
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		return nil, ErrQueueFull
	}
	select {
	case resp := <-task.response:
		// Success path: cleanup here to avoid racing with responder goroutines.
		if task.optimized {
			globalTaskIDPool.putTaskID(task.taskID)
		}
		b.returnTaskToPool(task)

		return resp, nil
	case err := <-task.errCh:
		// Error path: perform cleanup symmetrically.
		if task.optimized {
			globalTaskIDPool.putTaskID(task.taskID)
		}
		b.returnTaskToPool(task)

		return nil, err
	case <-ctx.Done():
		// Context canceled: signal handled by worker or here; ensure cleanup.
		if task.optimized {
			globalTaskIDPool.putTaskID(task.taskID)
		}
		b.returnTaskToPool(task)

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

			b.processTask(task)

		case <-b.ctx.Done():
			return ErrQuit
		}
	}
}

// processTask handles a single task with proper reference counting for safe pooling.
func (b *broker) processTask(task *Task) {
	// Add reference for worker goroutine access
	task.addRef()
	defer b.returnTaskToPool(task) // Will safely handle reference counting

	// Check closing status without context - faster atomic check
	if b.closing.Load() {
		b.trySendError(task, ErrClosingBroker)
		return
	}

	// Get task context once and cache it
	taskCtx := task.Context()

	// Use lock-free pool selection
	p := b.pickConnPool()

	if p == nil {
		b.trySendError(task, ErrNoPoolsAvailable)
		// b.failPending(task) // No longer needed
		return
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
			b.trySendError(task, taskCtx.Err())
			// b.failPending(task) // No longer needed
			return
		}
		b.trySendError(task, fmt.Errorf("failed to get connection: %w", err))
		// b.failPending(task) // No longer needed

		return
	}

	err = b.handleConnection(task, wr)
	if err != nil {
		p.Release(wr)
		// b.failPending(task) // No longer needed
		return
	}

	p.Put(wr)
}

func (b *broker) handleConnection(task *Task, wr PoolItem) error {
	netConn, ok := wr.(net.Conn)
	if !ok {
		err := errors.New("internal error: pool item is not net.Conn")
		b.trySendError(task, err)
		// b.failPending(task) // No longer needed

		return err
	}

	// Capture task ID to avoid race condition with task reuse
	// taskID := task.id

	// activeConns was write-only and unused for logic, removed for performance.
	// b.activeConns.Store(taskID, netConn)
	// defer func() { b.activeConns.Delete(taskID) }()

	// Prepare header in task.cmdBuf to avoid allocation
	header := task.cmdBuf
	payloadLen := taskIDSize + len(*task.request)

	switch LENGTHSIZE {
	case 2:
		binary.BigEndian.PutUint16(header[0:2], uint16(payloadLen))
	case 4:
		binary.BigEndian.PutUint32(header[0:4], uint32(payloadLen))
	}
	copy(header[LENGTHSIZE:], task.taskID)

	if b.closing.Load() {
		b.trySendError(task, ErrClosingBroker)
		// b.failPending(task) // No longer needed

		return ErrClosingBroker
	}

	// Optimization: Only set write deadline if configured
	if b.config.WriteTimeout > 0 {
		writeDeadline := time.Now().Add(b.config.WriteTimeout)
		if err := netConn.SetWriteDeadline(writeDeadline); err != nil {
			b.trySendError(task, fmt.Errorf("setting write deadline: %w", err))
			// b.failPending(task) // No longer needed

			return err
		}
	}

	// Use net.Buffers for zero-copy write
	task.writeBufs[0] = header
	task.writeBufs[1] = *task.request
	bufs := net.Buffers(task.writeBufs)

	if _, err := bufs.WriteTo(netConn); err != nil {
		b.trySendError(task, fmt.Errorf("writing to connection: %w", err))
		// b.failPending(task) // No longer needed

		return err
	}

	// Compute read deadline as the earlier of broker ReadTimeout and task context deadline (if any).
	var readDeadline time.Time
	if b.config.ReadTimeout > 0 {
		readDeadline = time.Now().Add(b.config.ReadTimeout)
	}
	if taskCtx := task.Context(); taskCtx != nil {
		if dl, ok := taskCtx.Deadline(); ok {
			if readDeadline.IsZero() || dl.Before(readDeadline) {
				readDeadline = dl
			}
		}
	}
	// Optimization: Only set read deadline if it's not zero
	if !readDeadline.IsZero() {
		if err := netConn.SetReadDeadline(readDeadline); err != nil {
			b.trySendError(task, fmt.Errorf("setting read deadline: %w", err))
			// b.failPending(task) // No longer needed

			return err
		}
	}

	// Synchronous read avoids goroutine leaks and ensures connection isn't reused concurrently.
	resp, err := Read(netConn)
	if err != nil {
		wrappedErr := fmt.Errorf("reading from connection: %w", err)
		b.trySendError(task, wrappedErr)
		// b.failPending(task) // No longer needed

		return wrappedErr
	}

	// Verify Task ID matches
	if len(resp) < taskIDSize {
		err := errors.New("response too short")
		b.trySendError(task, err)
		return err
	}
	respTaskID := binary.BigEndian.Uint32(resp[:taskIDSize])
	if respTaskID != task.id {
		err := fmt.Errorf("task ID mismatch: expected %d, got %d", task.id, respTaskID)
		b.trySendError(task, err)
		return err
	}

	// Deliver response directly
	func() {
		defer func() { _ = recover() }()
		task.response <- resp[taskIDSize:]
	}()

	// Optimization: Do NOT reset the deadline here.
	// The next user of this connection will overwrite the deadline anyway.
	// This saves one syscall per request.
	// _ = netConn.SetDeadline(time.Time{})

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
	default:
	}
}

// func (b *broker) respondPending(resp []byte) {
// 	if len(resp) < taskIDSize {
// 		return
// 	}
// 	taskID := binary.BigEndian.Uint32(resp[:taskIDSize])

// 	if task, ok := b.pending.Load(taskID); ok {
// 		// Deliver the response. Block until the receiver reads it.
// 		// Do not delete pending or return task here; the waiting sender will
// 		// perform cleanup after receiving to avoid races.
// 		func() {
// 			defer func() { _ = recover() }()
// 			task.response <- resp[taskIDSize:]
// 		}()
// 	}
// }

// func (b *broker) failPending(task *Task) {
// 	// Pure logical cleanup: only remove from pending.
// 	// Do NOT close channels or return the task; the waiting sender will
// 	// receive an error (via trySendError) and perform cleanup safely.
// 	b.pending.Delete(task.id)
// }

// returnTaskToPool returns a Task and its channels back to the pools.
// This is now safe due to reference counting - task is only pooled when refCount reaches 0.
func (b *broker) returnTaskToPool(task *Task) {
	if !task.pooled {
		return // Task wasn't from pool originally
	}

	// Only return to pool if this is the last reference
	if task.release() {
		// Zero out fields to prevent memory leaks and return the Task struct
		task.ctx = nil
		task.request = nil
		// Keep channels for reuse
		// task.response = nil
		// task.errCh = nil
		task.optimized = false
		task.pooled = false
		task.refCount = 0
		b.taskPool.Put(task)
	}
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

	// Get pooled Task struct to reduce allocations (now with safe reference counting)
	task, ok := b.taskPool.Get().(*Task)
	if !ok {
		task = &Task{
			response:  make(chan []byte, 1),
			errCh:     make(chan error, 1),
			cmdBuf:    make([]byte, LENGTHSIZE+taskIDSize),
			writeBufs: make([][]byte, 2),
		}
	} else {
		if cap(task.cmdBuf) < LENGTHSIZE+taskIDSize {
			task.cmdBuf = make([]byte, LENGTHSIZE+taskIDSize)
		}
		if cap(task.writeBufs) < 2 {
			task.writeBufs = make([][]byte, 2)
		}
	}
	task.cmdBuf = task.cmdBuf[:LENGTHSIZE+taskIDSize]
	task.writeBufs = task.writeBufs[:2]

	// Drain channels to ensure they are empty before reuse
	select {
	case <-task.response:
	default:
	}
	select {
	case <-task.errCh:
	default:
	}

	// Initialize the task
	task.ctx = ctx
	task.id = id
	task.taskID = taskIDBytes
	// task.response and task.errCh are already set and drained
	task.request = r
	task.optimized = b.config != nil && b.config.OptimizeMemory
	task.pooled = true
	task.refCount = 1 // Initial reference for the caller

	return task
}

// Close shuts down the broker, canceling context and waiting for workers to exit.
func (b *broker) Close() {
	// Ensure idempotent shutdown
	if !b.closing.CompareAndSwap(false, true) {
		return
	}

	// Best-effort: fail any queued tasks immediately to unblock senders
	for {
		select {
		case task := <-b.requestQueue:
			if task != nil {
				b.trySendError(task, ErrClosingBroker)
			}
		default:
			goto drained
		}
	}
drained:

	// Inform all pending tasks that we're closing
	// b.pending.ForEachAndClear(func(_ uint32, task *Task) {
	// 	if task != nil {
	// 		b.trySendError(task, ErrClosingBroker)
	// 		b.failPending(task)
	// 	}
	// })

	// Cancel broker context to stop workers and wait for them to exit
	b.cancel()
	b.wg.Wait()
}
