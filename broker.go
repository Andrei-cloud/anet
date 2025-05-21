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
	mu           sync.Mutex
	connMu       sync.RWMutex
	workers      int
	compool      []Pool
	requestQueue *RingBuffer[*Task]
	pending      sync.Map
	activeConns  sync.Map
	//nolint:containedctx // Necessary for task cancellation within broker queue.
	ctx     context.Context
	cancel  context.CancelFunc
	logger  Logger
	rng     *rand.Rand
	wg      sync.WaitGroup
	closing atomic.Bool
	started atomic.Bool
	config  *BrokerConfig
}

// NoopLogger provides a default no-op logger.
type NoopLogger struct{}

// DefaultBrokerConfig returns the default broker configuration.
func DefaultBrokerConfig() *BrokerConfig {
	return &BrokerConfig{
		WriteTimeout: 5 * time.Second,
		ReadTimeout:  5 * time.Second,
		QueueSize:    1000,
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
	rngSource := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(rngSource)
	ctx, cancel := context.WithCancel(context.Background())

	return &broker{
		workers:      n,
		compool:      p,
		requestQueue: NewRingBuffer[*Task](uint64(config.QueueSize)),
		ctx:          ctx,
		cancel:       cancel,
		logger:       l,
		rng:          rng,
		config:       config,
	}
}

// Send sends a request and waits for the response.
func (b *broker) Send(req *[]byte) ([]byte, error) {
	// Return error if no pool connections are available.
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

	task := b.newTask(context.Background(), req)

	b.mu.Lock()
	if b.closing.Load() {
		b.mu.Unlock()

		return nil, ErrClosingBroker
	}
	b.pending.Store(string(task.taskID), task)
	b.mu.Unlock()

	// enqueue without blocking; fail if broker closing or queue full
	if b.ctx.Err() != nil || !b.requestQueue.Enqueue(task) {
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

	b.mu.Lock()
	if b.closing.Load() {
		b.mu.Unlock()

		return nil, ErrClosingBroker
	}
	b.pending.Store(string(task.taskID), task)
	b.mu.Unlock()

	// enqueue without blocking; handle context and broker closing
	if b.ctx.Err() != nil {
		b.failPending(task)

		return nil, ErrClosingBroker
	}
	if ctx.Err() != nil {
		b.failPending(task)

		return nil, ctx.Err()
	}
	if !b.requestQueue.Enqueue(task) {
		b.failPending(task)

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
		// exit if broker is closing
		if b.ctx.Err() != nil {
			return ErrQuit
		}
		// attempt to dequeue next task
		task, ok := b.requestQueue.Dequeue()
		if !ok {
			continue // no task available
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
			b.trySendError(task, ErrNoPoolsAvailable)

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
	taskID := string(resp[:taskIDSize])

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
		}
	}
}

func (b *broker) failPending(task *Task) {
	b.pending.Delete(string(task.taskID))
	func() {
		defer func() { _ = recover() }()
		close(task.response)
		close(task.errCh)
	}()
}

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
		created:  time.Now(),
	}
}

func (b *broker) addTask(task *Task) []byte {
	cmd := make([]byte, taskIDSize+len(*task.request))
	copy(cmd[:taskIDSize], task.taskID)
	copy(cmd[taskIDSize:], *task.request)

	return cmd
}

// Close shuts down the broker, cancelling context and waiting for workers to exit.
func (b *broker) Close() {
	// cancel broker context to stop workers
	b.cancel()
	// wait for all worker goroutines to finish
	b.wg.Wait()
}
