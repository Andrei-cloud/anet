package anet

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
	// WriteTimeout is the timeout for writing to connections.
	// Zero means the default (5s); a negative value disables the deadline.
	WriteTimeout time.Duration
	// ReadTimeout is the timeout for reading from connections.
	// Zero means the default (5s); a negative value disables the deadline.
	// In Multiplex mode it bounds each individual response read while
	// requests are outstanding.
	ReadTimeout time.Duration
	// QueueSize is the size of the request queue. Default is 1000.
	// Ignored in Multiplex mode, which has no queue.
	QueueSize int
	// OptimizeMemory enables task and frame-buffer reuse through sync.Pool.
	// When disabled, a fresh Task is allocated per request and recycled
	// buffers are released to the GC. Default is true.
	OptimizeMemory bool
	// Multiplex enables asynchronous pipelined transport: each pooled
	// connection gets a background writer and reader goroutine, and
	// requests are correlated by task ID instead of strict ordering. A
	// connection then carries up to MaxInflightPerConn outstanding requests
	// and worker goroutines are not involved at all (pass workers = 0 to
	// NewBroker). Requires factories that return live net.Conn values;
	// anet.NewTCPFactory is recommended so TCP keepalive detects dead peers
	// (multiplex reads cannot detect them by themselves without traffic).
	// Default is false.
	Multiplex bool
	// MaxInflightPerConn bounds outstanding requests on one connection in
	// Multiplex mode; submits beyond the bound shed with ErrQueueFull.
	// Default is 256.
	MaxInflightPerConn int
	// CloseTimeout bounds how long Close waits for in-flight work after
	// canceling, so a wedged connection can never hang shutdown.
	// Zero means the default (10s); a negative value waits indefinitely.
	CloseTimeout time.Duration
}

// Broker sends framed requests over pooled connections and matches
// responses by task ID.
type Broker interface {
	// Send submits a request and blocks until the response, an error, or
	// broker shutdown.
	Send(req *[]byte) ([]byte, error)
	// SendContext is Send with context cancellation and deadlines.
	SendContext(ctx context.Context, req *[]byte) ([]byte, error)
	// SendAsync submits a request without blocking and returns the channel
	// on which exactly one Response will be delivered: the payload on
	// success, or the failure cause. The returned channel is buffered, so
	// a caller that never receives from it leaks nothing beyond GC-able
	// memory once the request settles.
	SendAsync(req *[]byte) (<-chan Response, error)
	// SendAsyncContext is SendAsync with context cancellation: cancellation
	// abandons the wait; a late Response is buffered and dropped.
	SendAsyncContext(ctx context.Context, req *[]byte) (<-chan Response, error)
	// Start launches worker goroutines to process queued requests.
	// It blocks until the broker stops; pass workers = 0 in Multiplex mode.
	Start() error
	// Close shuts down the broker. It never blocks longer than CloseTimeout.
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
	closing  atomic.Bool
	config   *BrokerConfig
	poolIdx  atomic.Uint32 // atomic pool selection index
	nextID   atomic.Uint32 // per-broker task ID source (was a package-global: false sharing across brokers)
	taskPool sync.Pool     // Pool for Task structs

	// Worker lifecycle accounting. sync.WaitGroup is deliberately NOT used
	// here: the documented API shape (`go broker.Start()` ... `broker.Close()`)
	// lets Start's wg.Add run while Close's wg.Wait observes counter==0, which
	// is WaitGroup misuse the race detector flags (verified by reproduction:
	// Add-at-Start concurrent with Wait-at-Close races even in a 20-line
	// stdlib microtest). The mutex below admits all accounting before the
	// goroutines launch and lets Close arm its waiter channel under the same
	// lock, so no interleaving can miss or double-count a worker. Cold path
	// only: zero mutex operations on the request path.
	wmu         sync.Mutex
	liveWorkers int
	stoppingWg  bool
	idleWait    chan struct{} // created by Close; closed once liveWorkers hits 0
}

// NoopLogger provides a default no-op logger.
type NoopLogger struct{}

// DefaultBrokerConfig returns the default broker configuration.
func DefaultBrokerConfig() *BrokerConfig {
	return &BrokerConfig{
		WriteTimeout:       5 * time.Second,
		ReadTimeout:        5 * time.Second,
		QueueSize:          1000,
		OptimizeMemory:     true,
		MaxInflightPerConn: 256,
		CloseTimeout:       10 * time.Second,
	}
}

// applyDefaults normalizes a user-supplied configuration. Zero values take
// defaults (previously a zero ReadTimeout silently meant "no deadline", the
// main cause of unbounded broker.Close); negative values disable the
// corresponding deadline.
func (c *BrokerConfig) applyDefaults() {
	if c.WriteTimeout == 0 {
		c.WriteTimeout = 5 * time.Second
	} else if c.WriteTimeout < 0 {
		c.WriteTimeout = 0
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = 5 * time.Second
	} else if c.ReadTimeout < 0 {
		c.ReadTimeout = 0
	}
	if c.QueueSize == 0 {
		c.QueueSize = 1000
	}
	if c.MaxInflightPerConn == 0 {
		c.MaxInflightPerConn = 256
	} else if c.MaxInflightPerConn < 0 {
		c.MaxInflightPerConn = 0
	}
	if c.CloseTimeout == 0 {
		c.CloseTimeout = 10 * time.Second
	} else if c.CloseTimeout < 0 {
		c.CloseTimeout = 0
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
	} else {
		// Normalize a copy so caller structs stay immutable to us.
		cfg := *config
		cfg.applyDefaults()
		config = &cfg
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
			return &Task{} // res is allocated per request in newTask, never pooled
		},
	}

	return b
}

// Send sends a request and waits for the response.
func (b *broker) Send(req *[]byte) ([]byte, error) {
	res, err := b.submit(nil, req)
	if err != nil {
		return nil, err
	}
	return b.wait(res, nil)
}

// SendContext sends a request with context support.
func (b *broker) SendContext(ctx context.Context, req *[]byte) ([]byte, error) {
	if ctx == nil {
		return b.Send(req)
	}
	res, err := b.submit(ctx, req)
	if err != nil {
		return nil, err
	}
	return b.wait(res, ctx)
}

// SendAsync submits a request without blocking. See Broker.SendAsync.
func (b *broker) SendAsync(req *[]byte) (<-chan Response, error) {
	res, err := b.submit(nil, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// SendAsyncContext is SendAsync with context support. The channel is
// buffered: cancelling abandons the wait and the eventual Response is
// garbage-collected with the channel.
func (b *broker) SendAsyncContext(ctx context.Context, req *[]byte) (<-chan Response, error) {
	if ctx == nil {
		return b.SendAsync(req)
	}
	res, err := b.submit(ctx, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// submit stages a task and hands it to its consumer. On success the task
// belongs to a queue consumer (worker queue or multiplexed pipeline) and the
// caller holds only the result channel; on error the task was never enqueued
// and is recycled here.
func (b *broker) submit(ctx context.Context, req *[]byte) (<-chan Response, error) {
	if b.closing.Load() {
		return nil, ErrClosingBroker
	}

	task, err := b.newTask(ctx, req)
	if err != nil {
		return nil, err
	}
	res := task.res

	if b.config.Multiplex {
		if err := b.submitMuxed(task); err != nil {
			b.recycleTask(task)
			return nil, err
		}
		return res, nil
	}

	select {
	case b.requestQueue <- task:
		return res, nil
	case <-b.ctx.Done():
		// Never enqueued: the submitter still owns the task exclusively.
		b.recycleTask(task)
		return nil, ErrClosingBroker
	case <-ctxDone(ctx):
		b.recycleTask(task)
		return nil, ctx.Err()
	default:
		if b.closing.Load() || b.ctx.Err() != nil {
			b.recycleTask(task)
			return nil, ErrClosingBroker
		}
		if err := ctxErr(ctx); err != nil {
			b.recycleTask(task)
			return nil, err
		}
		b.recycleTask(task)
		return nil, ErrQueueFull
	}
}

// wait receives the single Response promised for a submitted request.
func (b *broker) wait(res <-chan Response, ctx context.Context) ([]byte, error) {
	if ctx == nil {
		select {
		case r := <-res:
			return r.Payload, r.Err
		case <-b.ctx.Done():
			return nil, ErrClosingBroker
		}
	}
	select {
	case r := <-res:
		return r.Payload, r.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.ctx.Done():
		return nil, ErrClosingBroker
	}
}

// Start launches worker goroutines to process requests.
func (b *broker) Start() error {
	eg := &errgroup.Group{}
	b.logger.Infof("Broker starting with %d workers...", b.workers)

	// Account for every worker before any of them launches (see broker.wmu).
	b.wmu.Lock()
	if b.stoppingWg {
		b.wmu.Unlock()
		b.logger.Warnf("Broker already closing; Start does nothing")
		return ErrQuit
	}
	b.liveWorkers = b.workers
	b.wmu.Unlock()

	for i := 0; i < b.workers; i++ {
		workerID := i
		eg.Go(func() error {
			defer b.endWorker()
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

// endWorker retires one worker and wakes Close when the last one exits.
func (b *broker) endWorker() {
	b.wmu.Lock()
	b.liveWorkers--
	if b.liveWorkers == 0 && b.idleWait != nil {
		close(b.idleWait)
		b.idleWait = nil
	}
	b.wmu.Unlock()
}

func (b *broker) loop(_ int) error {
	for {
		select {
		case task := <-b.requestQueue:
			b.processTask(task)
		case <-b.ctx.Done():
			return ErrQuit
		}
	}
}

// processTask executes one queued task. The worker inherited queue
// ownership of the task, so it recycles it exactly once on every path.
func (b *broker) processTask(task *Task) {
	defer b.recycleTask(task)

	if b.closing.Load() {
		b.deliver(task, Response{Err: ErrClosingBroker})
		return
	}

	taskCtx := task.Context()
	if taskCtx != nil {
		if err := taskCtx.Err(); err != nil {
			b.deliver(task, Response{Err: err})
			return
		}
	}

	p := b.pickConnPool()
	if p == nil {
		b.deliver(task, Response{Err: ErrNoPoolsAvailable})
		return
	}

	// A task without a caller context is cancelled by the broker context,
	// so broker Close wakes a worker parked on an exhausted pool.
	getCtx := taskCtx
	if getCtx == nil {
		getCtx = b.ctx
	}
	wr, err := p.GetWithContext(getCtx)
	if err != nil {
		if taskCtx != nil && errors.Is(err, taskCtx.Err()) {
			b.deliver(task, Response{Err: taskCtx.Err()})
			return
		}
		if errors.Is(err, ErrClosing) || errors.Is(err, b.ctx.Err()) {
			b.deliver(task, Response{Err: ErrClosingBroker})
			return
		}
		b.deliver(task, Response{Err: fmt.Errorf("failed to get connection: %w", err)})
		return
	}

	if err := b.handleConnection(task, wr); err != nil {
		// The connection's stream state is unknown after any error;
		// Release (close) it instead of returning it to the idle pool.
		p.Release(wr)
		return
	}

	p.Put(wr)
}

// handleConnection performs one synchronous request/response exchange.
// The frame was fully staged by the submitter into task-owned storage, so
// the worker never dereferences caller memory, and a single contiguous
// write replaces the old header+payload writev (which silently truncated
// payloadLen at 65532 bytes).
func (b *broker) handleConnection(task *Task, wr PoolItem) error {
	netConn, ok := wr.(net.Conn)
	if !ok {
		err := errors.New("internal error: pool item is not net.Conn")
		b.deliver(task, Response{Err: err})
		return err
	}

	if b.config.WriteTimeout > 0 {
		_ = netConn.SetWriteDeadline(time.Now().Add(b.config.WriteTimeout))
	}
	if _, err := netConn.Write(task.frame[:task.frameLen]); err != nil {
		err = fmt.Errorf("writing to connection: %w", err)
		b.deliver(task, Response{Err: err})
		return err
	}

	// Read deadline: configured timeout, intersected with the caller deadline.
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

	if err := b.readResponse(task, netConn); err != nil {
		b.deliver(task, Response{Err: err})
		return err
	}

	return nil
}

// readResponse reads one framed response using the task-owned header
// scratch (no per-read header escape) and delivers it with correlation.
func (b *broker) readResponse(task *Task, conn net.Conn) error {
	if _, err := io.ReadFull(conn, task.hdr[:]); err != nil {
		return fmt.Errorf("reading from connection: %w", err)
	}

	var length uint64
	switch LENGTHSIZE {
	case 2:
		length = uint64(binary.BigEndian.Uint16(task.hdr[:]))
	case 4:
		length = uint64(binary.BigEndian.Uint32(task.hdr[:]))
	default:
		return fmt.Errorf("unsupported header size: %d", LENGTHSIZE)
	}

	if length < taskIDSize {
		return errors.New("response too short")
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return fmt.Errorf("reading from connection: %w", err)
	}

	respTaskID := binary.BigEndian.Uint32(payload[:taskIDSize])
	if respTaskID != task.id {
		return fmt.Errorf("task ID mismatch: expected %d, got %d", task.id, respTaskID)
	}

	b.deliver(task, Response{Payload: payload[taskIDSize:]})
	return nil
}

// deliver hands the Response to the submitter. The channel is buffered with
// room for the single delivery guaranteed per task life, so the send never
// blocks; the default branch only covers an abandoned (GC'd) waiter.
func (b *broker) deliver(task *Task, r Response) {
	select {
	case task.res <- r:
	default:
	}
}

// recycleTask returns a task to the pool after its queue consumer is done.
// The delivery channel is dropped, not recycled: channels are allocated per
// request (see Task.res), so an abandoned waiter's buffered Response is
// simply garbage with its channel once the task moves on.
func (b *broker) recycleTask(task *Task) {
	if !b.config.OptimizeMemory {
		return // fresh per-request tasks: drop to the GC
	}

	if task.transient {
		PutBuffer(task.frame)
		task.frame = nil
		task.transient = false
	}

	task.ctx = nil
	task.id = 0
	task.frameLen = 0
	task.res = nil
	b.taskPool.Put(task)
}

// maxPayloadLen is the largest request payload the 2-byte length header can
// describe once the task ID is included in the frame.
const maxPayloadLen = (1 << (8 * LENGTHSIZE)) - 1 - taskIDSize

// newTask stages a complete frame ([len][id][payload]) into task-owned
// storage. The payload is copied once, here, in the submitter's own
// goroutine: after submit neither workers nor pipelines ever read caller
// memory, which closes the caller-reuse-after-cancel data race that the old
// net.Buffers path exposed.
func (b *broker) newTask(ctx context.Context, r *[]byte) (*Task, error) {
	payloadLen := 0
	if r != nil {
		payloadLen = len(*r)
	}
	// Bound the frame before writing the header: the old writev path
	// truncated uint16(payloadLen) at 65532 and desynchronized the stream.
	if payloadLen > maxPayloadLen {
		return nil, ErrMaxLenExceeded
	}
	frameLen := LENGTHSIZE + taskIDSize + payloadLen

	optimize := b.config.OptimizeMemory
	var task *Task
	if optimize {
		task, _ = b.taskPool.Get().(*Task)
	} else {
		task = &Task{}
	}
	// Fresh delivery channel per request (see Task.res): a pooled channel
	// would let a drainer swallow a live waiter's response.
	task.res = make(chan Response, 1)

	id := b.nextID.Add(1)

	// Grow or restage the frame buffer as needed.
	if cap(task.frame) < frameLen {
		if task.transient {
			PutBuffer(task.frame)
			task.frame = nil
			task.transient = false
		}
		if frameLen <= maxFrameRetain && optimize {
			task.frame = make([]byte, frameLen)
		} else {
			task.frame = GetBuffer(frameLen)
			task.transient = true
		}
	} else if task.transient {
		// The borrowed buffer is big enough to keep: adopt it.
		task.transient = false
	}
	frame := task.frame[:frameLen]

	payloadLenAfterID := taskIDSize + payloadLen
	switch LENGTHSIZE {
	case 2:
		binary.BigEndian.PutUint16(frame[0:2], uint16(payloadLenAfterID))
	case 4:
		binary.BigEndian.PutUint32(frame[0:4], uint32(payloadLenAfterID))
	}
	binary.BigEndian.PutUint32(frame[LENGTHSIZE:LENGTHSIZE+taskIDSize], id)
	if payloadLen > 0 {
		copy(frame[LENGTHSIZE+taskIDSize:], *r)
	}

	task.frameLen = frameLen
	task.id = id
	task.ctx = ctx

	return task, nil
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

// Close shuts down the broker, canceling context and waiting for workers to
// exit, bounded by CloseTimeout so no wedged connection can hang shutdown.
func (b *broker) Close() {
	if !b.closing.CompareAndSwap(false, true) {
		return
	}

	// Fail queued tasks immediately. Close is their queue consumer, so it
	// recycles each one exactly once.
	for {
		select {
		case task := <-b.requestQueue:
			b.deliver(task, Response{Err: ErrClosingBroker})
			b.recycleTask(task)
		default:
			goto drained
		}
	}
drained:

	b.cancel()

	// Arm the worker-exit waiter under wmu (see broker.wmu for why this is
	// not a WaitGroup): whoever drops liveWorkers to zero closes the channel;
	// if it is already zero, Close proceeds without waiting.
	b.wmu.Lock()
	b.stoppingWg = true
	var done chan struct{}
	if b.liveWorkers > 0 {
		done = make(chan struct{})
		b.idleWait = done
	}
	b.wmu.Unlock()

	if done != nil {
		if b.config.CloseTimeout > 0 {
			select {
			case <-done:
			case <-time.After(b.config.CloseTimeout):
				b.logger.Warnf("broker close: workers still draining after %v", b.config.CloseTimeout)
			}
		} else {
			<-done
		}
	}
}

// ctxDone/ctxErr nil-safe helpers: nil context means "only the broker can
// cancel", keeping the hot Send path out of select branches it cannot fire.
func ctxDone(ctx context.Context) <-chan struct{} {
	if ctx == nil {
		return nil // a nil channel never fires in select
	}
	return ctx.Done()
}

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}
