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
	// When enabled, reduces allocations and improves performance. Default is true.
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
	Print(v ...any)
	Printf(format string, v ...any)
	Infof(format string, v ...any)
	Warnf(format string, v ...any)
	Errorf(format string, v ...any)
}

// broker implements the Broker interface.
type broker struct {
	workers      int
	compool      []Pool
	requestQueue chan *Task
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
		OptimizeMemory: true,
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
		requestQueue: make(chan *Task, config.QueueSize),
		ctx:          ctx,
		cancel:       cancel,
		logger:       l,
		config:       config,
	}

	b.taskPool = sync.Pool{
		New: func() any {
			return &Task{
				response:  make(chan []byte, 1),
				errCh:     make(chan error, 1),
				cmdBuf:    make([]byte, 512),
				writeBufs: make([][]byte, 2),
			}
		},
	}

	return b
}

// Send sends a request and waits for the response.
func (b *broker) Send(req *[]byte) ([]byte, error) {
	if b.closing.Load() {
		return nil, ErrClosingBroker
	}

	task := b.newTask(context.Background(), req)

	select {
	case b.requestQueue <- task:
	default:
		if b.closing.Load() || b.ctx.Err() != nil {
			b.returnTaskToPool(task)
			return nil, ErrClosingBroker
		}
		b.returnTaskToPool(task)
		return nil, ErrQueueFull
	}

	select {
	case resp := <-task.response:
		b.returnTaskToPool(task)
		return resp, nil
	case err := <-task.errCh:
		b.returnTaskToPool(task)
		return nil, err
	case <-b.ctx.Done():
		b.returnTaskToPool(task)
		return nil, ErrClosingBroker
	}
}

// SendContext sends a request with context support.
func (b *broker) SendContext(ctx context.Context, req *[]byte) ([]byte, error) {
	if b.closing.Load() {
		return nil, ErrClosingBroker
	}
	if ctx == nil {
		return b.Send(req)
	}

	task := b.newTask(ctx, req)

	select {
	case b.requestQueue <- task:
	case <-ctx.Done():
		b.returnTaskToPool(task)
		return nil, ctx.Err()
	default:
		if b.closing.Load() || b.ctx.Err() != nil {
			b.returnTaskToPool(task)
			return nil, ErrClosingBroker
		}
		if err := ctx.Err(); err != nil {
			b.returnTaskToPool(task)
			return nil, err
		}
		b.returnTaskToPool(task)
		return nil, ErrQueueFull
	}

	select {
	case resp := <-task.response:
		b.returnTaskToPool(task)
		return resp, nil
	case err := <-task.errCh:
		b.returnTaskToPool(task)
		return nil, err
	case <-ctx.Done():
		b.returnTaskToPool(task)
		return nil, ctx.Err()
	case <-b.ctx.Done():
		b.returnTaskToPool(task)
		return nil, ErrClosingBroker
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
			return b.loop(workerID)
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
		select {
		case task := <-b.requestQueue:
			if task == nil {
				continue
			}
			b.processTask(task)
		case <-b.ctx.Done():
			return ErrQuit
		}
	}
}

func (b *broker) processTask(task *Task) {
	task.addRef()
	defer b.returnTaskToPool(task)

	if b.closing.Load() {
		b.trySendError(task, ErrClosingBroker)
		return
	}

	taskCtx := task.Context()
	if taskCtx != nil {
		if err := taskCtx.Err(); err != nil {
			b.trySendError(task, err)
			return
		}
	}

	p := b.pickConnPool()
	if p == nil {
		b.trySendError(task, ErrNoPoolsAvailable)
		return
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
			b.trySendError(task, taskCtx.Err())
			return
		}
		b.trySendError(task, fmt.Errorf("failed to get connection: %w", err))
		return
	}

	err = b.handleConnection(task, wr)
	if err != nil {
		p.Release(wr)
		return
	}

	p.Put(wr)
}

func (b *broker) handleConnection(task *Task, wr PoolItem) error {
	netConn, ok := wr.(net.Conn)
	if !ok {
		err := errors.New("internal error: pool item is not net.Conn")
		b.trySendError(task, err)
		return err
	}

	reqPayloadLen := 0
	if task.request != nil {
		reqPayloadLen = len(*task.request)
	}
	payloadLen := taskIDSize + reqPayloadLen
	totalFrameLen := LENGTHSIZE + payloadLen

	// Optimization: If total frame fits in task.cmdBuf, do a single contiguous write.
	if totalFrameLen <= cap(task.cmdBuf) {
		task.cmdBuf = task.cmdBuf[:totalFrameLen]
		switch LENGTHSIZE {
		case 2:
			binary.BigEndian.PutUint16(task.cmdBuf[0:2], uint16(payloadLen))
		case 4:
			binary.BigEndian.PutUint32(task.cmdBuf[0:4], uint32(payloadLen))
		}
		copy(task.cmdBuf[LENGTHSIZE:], task.taskID)
		if reqPayloadLen > 0 {
			copy(task.cmdBuf[LENGTHSIZE+taskIDSize:], *task.request)
		}

		if b.config.WriteTimeout > 0 {
			_ = netConn.SetWriteDeadline(time.Now().Add(b.config.WriteTimeout))
		}

		if _, err := netConn.Write(task.cmdBuf); err != nil {
			b.trySendError(task, fmt.Errorf("writing to connection: %w", err))
			return err
		}
	} else {
		// Larger frame: format header in small buffer and use net.Buffers
		header := task.cmdBuf[:LENGTHSIZE+taskIDSize]
		switch LENGTHSIZE {
		case 2:
			binary.BigEndian.PutUint16(header[0:2], uint16(payloadLen))
		case 4:
			binary.BigEndian.PutUint32(header[0:4], uint32(payloadLen))
		}
		copy(header[LENGTHSIZE:], task.taskID)

		if b.config.WriteTimeout > 0 {
			_ = netConn.SetWriteDeadline(time.Now().Add(b.config.WriteTimeout))
		}

		task.writeBufs[0] = header
		if task.request != nil {
			task.writeBufs[1] = *task.request
		} else {
			task.writeBufs[1] = nil
		}
		bufs := net.Buffers(task.writeBufs)
		if _, err := bufs.WriteTo(netConn); err != nil {
			b.trySendError(task, fmt.Errorf("writing to connection: %w", err))
			return err
		}
	}

	// Compute read deadline
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
	if !readDeadline.IsZero() {
		_ = netConn.SetReadDeadline(readDeadline)
	}

	// Synchronous read
	resp, err := Read(netConn)
	if err != nil {
		wrappedErr := fmt.Errorf("reading from connection: %w", err)
		b.trySendError(task, wrappedErr)
		return wrappedErr
	}

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
		select {
		case task.response <- resp[taskIDSize:]:
		default:
		}
	}()

	return nil
}

func (b *broker) pickConnPool() Pool {
	poolsLen := len(b.compool)
	if poolsLen == 0 {
		return nil
	}
	if poolsLen == 1 {
		return b.compool[0]
	}
	idx := b.poolIdx.Add(1) % uint32(poolsLen)
	return b.compool[idx]
}

func (b *broker) trySendError(task *Task, err error) {
	defer func() { _ = recover() }()
	select {
	case task.errCh <- err:
	default:
	}
}

func (b *broker) returnTaskToPool(task *Task) {
	if !task.pooled {
		return
	}

	if task.release() {
		if task.optimized && len(task.taskID) == taskIDSize {
			globalTaskIDPool.putTaskID(task.taskID)
		}
		task.ctx = nil
		task.request = nil
		task.taskID = nil
		task.optimized = false
		task.pooled = false
		task.refCount = 0
		b.taskPool.Put(task)
	}
}

func (b *broker) newTask(ctx context.Context, r *[]byte) *Task {
	id := atomic.AddUint32(&nextTaskID, 1)

	var taskIDBytes []byte
	optimizeMemory := b.config != nil && b.config.OptimizeMemory
	if optimizeMemory {
		taskIDBytes = globalTaskIDPool.getTaskID()
	} else {
		taskIDBytes = make([]byte, taskIDSize)
	}

	binary.BigEndian.PutUint32(taskIDBytes, id)

	task, ok := b.taskPool.Get().(*Task)
	if !ok {
		task = &Task{
			response:  make(chan []byte, 1),
			errCh:     make(chan error, 1),
			cmdBuf:    make([]byte, 512),
			writeBufs: make([][]byte, 2),
		}
	} else {
		if cap(task.cmdBuf) < 512 {
			task.cmdBuf = make([]byte, 512)
		}
		if cap(task.writeBufs) < 2 {
			task.writeBufs = make([][]byte, 2)
		}
	}

	// Drain stale channel entries if any
	select {
	case <-task.response:
	default:
	}
	select {
	case <-task.errCh:
	default:
	}

	task.ctx = ctx
	task.id = id
	task.taskID = taskIDBytes
	task.request = r
	task.optimized = optimizeMemory
	task.pooled = true
	task.refCount = 1

	return task
}

// Close shuts down the broker, canceling context and waiting for workers to exit.
func (b *broker) Close() {
	if !b.closing.CompareAndSwap(false, true) {
		return
	}

	// Fail any queued tasks immediately
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

	b.cancel()
	b.wg.Wait()
}
