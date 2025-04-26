package anet

import (
	"context"
	"encoding/hex"
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

	// ErrMaxLenExceeded indicates a message exceeds the maximum allowed length.
	ErrMaxLenExceeded = errors.New("max length exceeded")

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
	Debugf(format string, v ...any) // Debug level.
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

// Debugf does nothing.
func (l *NoopLogger) Debugf(_ string, _ ...any) {}

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
	b.logger.Infof("Broker starting with %d workers...", b.workers) // Changed to Infof
	b.logger.Debugf(
		"Broker configuration: workers=%d, pools=%d",
		b.workers,
		len(b.compool),
	) // Add debug log for config.

	for i := 0; i < b.workers; i++ {
		workerID := i
		b.wg.Add(1)
		eg.Go(func() error {
			defer b.wg.Done()
			b.logger.Infof("Worker %d starting loop", workerID) // Changed to Infof
			err := b.loop(workerID)
			// Use Debugf for potentially noisy loop exit logging.
			b.logger.Debugf("Worker %d finished loop: %v", workerID, err)

			return err
		})
	}

	err := eg.Wait()
	if err != nil && !errors.Is(err, ErrQuit) { // Log only actual errors, not ErrQuit
		b.logger.Errorf("Broker stopped with error: %v", err) // Changed to Errorf
	} else {
		b.logger.Infof("Broker stopped gracefully.") // Changed to Infof
	}

	return err
}

// Close shuts down the broker and associated connection pools.
func (b *broker) Close() {
	b.mu.Lock()
	if b.closing.Load() {
		b.logger.Debugf("Broker Close called but already closing.") // Add debug log.
		b.mu.Unlock()

		return
	}
	b.logger.Debugf("Broker Close initiated.") // Add debug log.
	b.closing.Store(true)
	b.cancel() // Signal loop via context cancel.
	b.mu.Unlock()

	// Close all active connections immediately.
	b.connMu.Lock()
	closedCount := 0 // Count closed connections for debugging.
	b.activeConns.Range(func(key, value any) bool {
		if conn, ok := value.(net.Conn); ok {
			b.logger.Debugf("Closing active connection for task %s", key.(string)) // Add debug log.
			_ = conn.Close()
			closedCount++
		}
		b.activeConns.Delete(key)

		return true
	})
	b.connMu.Unlock()
	b.logger.Debugf("Closed %d active connections.", closedCount) // Add debug log.

	// Wait for workers to finish with a timeout.
	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		b.logger.Debugf("All workers finished gracefully.") // Add debug log.
		// Workers finished normally.
	case <-time.After(5 * time.Second):
		// Timeout waiting for workers, force close.
		b.logger.Warnf("Timeout waiting for workers to finish, forcing close.") // Changed to Warnf
	}

	// Now that workers are stopped, safely close the request channel.
	b.logger.Debugf("Closing request queue.") // Add debug log.
	close(b.requestQueue)

	// Fail any remaining pending tasks.
	failedCount := 0 // Count failed tasks for debugging.
	b.pending.Range(func(key, value any) bool {
		task, ok := value.(*Task)
		if !ok {
			return true
		}
		b.logger.Debugf(
			"Failing pending task %s due to broker closing.",
			hex.EncodeToString(task.taskID),
		) // Add debug log.
		select {
		case task.errCh <- ErrClosingBroker:
		default:
		}
		close(task.response)
		close(task.errCh)
		b.pending.Delete(key)
		failedCount++

		return true
	})
	b.logger.Debugf("Failed %d pending tasks.", failedCount) // Add debug log.

	// Close the underlying connection pools.
	b.logger.Debugf("Closing underlying connection pools.") // Add debug log.
	for i, p := range b.compool {
		p.Close()
		b.logger.Debugf("Closed pool %d.", i) // Add debug log.
	}

	b.logger.Print("Broker closed.")
}

func (b *broker) Send(req *[]byte) ([]byte, error) {
	var (
		resp []byte
		err  error
	)
	task := b.newTask(context.Background(), req)
	b.logger.Debugf("Send: Created task %s", hex.EncodeToString(task.taskID)) // Add debug log.

	// Lock to check closing status and potentially send.
	b.mu.Lock()
	if b.closing.Load() {
		b.mu.Unlock()
		b.logger.Debugf(
			"Send: Broker closing, failing task %s",
			hex.EncodeToString(task.taskID),
		) // Add debug log.
		// Use the specific error for closing broker.

		return nil, ErrClosingBroker
	}

	// Add task to pending list *before* sending to queue, under lock.
	b.pending.Store(string(task.taskID), task)
	b.logger.Debugf(
		"Send: Added task %s to pending list.",
		hex.EncodeToString(task.taskID),
	) // Add debug log.

	// Use select to handle potential blocking or closed channel during shutdown.
	select {
	case b.requestQueue <- task:
		b.logger.Debugf(
			"Send: Task %s sent to request queue.",
			hex.EncodeToString(task.taskID),
		) // Add debug log.
		// Successfully sent task.
		b.mu.Unlock() // Unlock *after* successful send.
	case <-b.ctx.Done():
		// Broker quit while trying to send.
		b.mu.Unlock() // Unlock before failing.
		b.failPending(
			task,
		) // Clean up the task.
		b.logger.Debugf(
			"Send: Broker quit while sending task %s.",
			hex.EncodeToString(task.taskID),
		) // Add debug log.

		return nil, ErrClosingBroker
	default:
		// This case might happen if the queue is full and we don't want to block indefinitely.
		// Or if the channel was closed between the `if b.closing` check and now.
		b.mu.Unlock()
		b.failPending(
			task,
		) // Clean up the task.
		b.logger.Debugf(
			"Send: Failed to send task %s to queue (full or closed).",
			hex.EncodeToString(task.taskID),
		) // Add debug log.

		return nil, ErrClosingBroker // Treat as if broker is closing.
	}

	// Wait for response or error outside the lock.
	b.logger.Debugf(
		"Send: Waiting for response for task %s.",
		hex.EncodeToString(task.taskID),
	) // Add debug log.
	select {
	case resp = <-task.response:
		b.logger.Debugf(
			"Send: Received response for task %s.",
			hex.EncodeToString(task.taskID),
		) // Add debug log.

		return resp, nil
	case err = <-task.errCh:
		b.logger.Debugf(
			"Send: Received error for task %s: %v",
			hex.EncodeToString(task.taskID),
			err,
		) // Add debug log.

		return nil, err
	}
}

func (b *broker) SendContext(ctx context.Context, req *[]byte) ([]byte, error) {
	var (
		resp []byte
		err  error
	)

	task := b.newTask(ctx, req)
	b.logger.Debugf(
		"SendContext: Created task %s",
		hex.EncodeToString(task.taskID),
	) // Add debug log.

	// Lock to check closing status and potentially send.
	b.mu.Lock()
	if b.closing.Load() {
		b.mu.Unlock()
		b.logger.Debugf(
			"SendContext: Broker closing, failing task %s",
			hex.EncodeToString(task.taskID),
		) // Add debug log.
		// Use the specific error for closing broker.

		return nil, ErrClosingBroker
	}

	// Add task to pending list *before* sending to queue, under lock.
	b.pending.Store(string(task.taskID), task)
	b.logger.Debugf(
		"SendContext: Added task %s to pending list.",
		hex.EncodeToString(task.taskID),
	) // Add debug log.

	// Use select to handle potential blocking or closed channel during shutdown.
	select {
	case b.requestQueue <- task:
		b.logger.Debugf(
			"SendContext: Task %s sent to request queue.",
			hex.EncodeToString(task.taskID),
		) // Add debug log.
		// Successfully sent task.
		b.mu.Unlock() // Unlock *after* successful send.
	case <-b.ctx.Done():
		// Broker quit while trying to send.
		b.mu.Unlock() // Unlock before failing.
		b.failPending(
			task,
		) // Clean up the task.
		b.logger.Debugf(
			"SendContext: Broker quit while sending task %s.",
			hex.EncodeToString(task.taskID),
		) // Add debug log.

		return nil, ErrClosingBroker
	case <-ctx.Done():
		// Context was canceled while trying to send.
		b.mu.Unlock() // Unlock before failing.
		b.failPending(
			task,
		) // Clean up the task.
		b.logger.Debugf(
			"SendContext: Context canceled while sending task %s: %v",
			hex.EncodeToString(task.taskID),
			ctx.Err(),
		) // Add debug log.

		return nil, ctx.Err()
	default:
		// This case might happen if the queue is full and we don't want to block indefinitely.
		// Or if the channel was closed between the `if b.closing` check and now.
		b.mu.Unlock()
		b.failPending(
			task,
		) // Clean up the task.
		b.logger.Debugf(
			"SendContext: Failed to send task %s to queue (full or closed).",
			hex.EncodeToString(task.taskID),
		) // Add debug log.

		return nil, ErrClosingBroker // Treat as if broker is closing.
	}

	// Wait for response or error outside the lock.
	b.logger.Debugf(
		"SendContext: Waiting for response/error for task %s.",
		hex.EncodeToString(task.taskID),
	) // Add debug log.
	select {
	case resp = <-task.response:
		b.logger.Debugf(
			"SendContext: Received response for task %s.",
			hex.EncodeToString(task.taskID),
		) // Add debug log.

		return resp, nil
	case err = <-task.errCh:
		b.logger.Debugf(
			"SendContext: Received error for task %s: %v",
			hex.EncodeToString(task.taskID),
			err,
		) // Add debug log.

		return nil, err
	case <-ctx.Done():
		b.logger.Debugf(
			"SendContext: Context canceled while waiting for task %s: %v",
			hex.EncodeToString(task.taskID),
			ctx.Err(),
		) // Add debug log.
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
		b.logger.Debugf("Worker %d waiting for task or quit signal.", workerID) // Add debug log.
		select {
		case <-b.ctx.Done():
			b.logger.Printf("Worker %d received quit signal", workerID)

			return ErrQuit
		case task, ok := <-b.requestQueue:
			if !ok {
				b.logger.Printf("Worker %d: requestQueue closed, exiting loop.", workerID)

				return nil
			}

			taskIDStr := hex.EncodeToString(
				task.taskID,
			) // Cache task ID string.
			b.logger.Debugf(
				"Worker %d picked up task %s from queue.",
				workerID,
				taskIDStr,
			) // Add debug log.

			if b.closing.Load() {
				b.logger.Debugf(
					"Worker %d: Broker closing, failing task %s.",
					workerID,
					taskIDStr,
				) // Add debug log.
				b.trySendError(task, ErrClosingBroker)

				continue
			}

			taskCtx := task.Context()
			// Use Printf for INFO level, Debugf for more detail if needed.
			b.logger.Printf("Worker %d received task %s", workerID, taskIDStr)

			b.connMu.RLock()
			p := b.pickConnPool()
			b.connMu.RUnlock()

			if p == nil {
				err := ErrNoPoolsAvailable
				b.logger.Printf(
					"Worker %d: Error picking pool for task %s: %v",
					workerID,
					taskIDStr,
					err,
				)
				b.trySendError(task, err)
				b.logger.Debugf(
					"Worker %d: Failed task %s due to no available pools.",
					workerID,
					taskIDStr,
				) // Add debug log.

				continue
			}
			b.logger.Debugf(
				"Worker %d: Picked pool for task %s.",
				workerID,
				taskIDStr,
			) // Add debug log.

			// Get connection with context awareness.
			var wr PoolItem
			var err error
			b.logger.Debugf(
				"Worker %d: Attempting to get connection for task %s.",
				workerID,
				taskIDStr,
			) // Add debug log.
			startTime := time.Now() // Measure connection acquisition time.
			if taskCtx != nil {
				wr, err = p.GetWithContext(taskCtx)
			} else {
				wr, err = p.Get()
			}
			acquisitionDuration := time.Since(startTime) // Calculate duration.

			if err != nil {
				b.logger.Debugf(
					"Worker %d: Failed to get connection for task %s after %s. Error: %v",
					workerID,
					taskIDStr,
					acquisitionDuration,
					err,
				) // Add debug log with duration.
				if taskCtx != nil && errors.Is(err, taskCtx.Err()) {
					b.logger.Printf(
						"Task %s context done while getting connection: %v",
						taskIDStr,
						err,
					)
					// No need to send error, SendContext handles context cancellation.
				} else {
					b.logger.Printf(
						"Worker %d: Error getting connection for task %s from pool: %v",
						workerID,
						taskIDStr,
						err,
					)
					b.trySendError(task, fmt.Errorf("failed to get connection: %w", err))
				}
				b.logger.Debugf(
					"Worker %d: Failed task %s due to connection error.",
					workerID,
					taskIDStr,
				) // Add debug log.

				continue
			}

			// Use Printf for INFO level, Debugf for more detail.
			b.logger.Printf(
				"Worker %d: Acquired connection for task %s",
				workerID,
				taskIDStr,
			)
			b.logger.Debugf(
				"Worker %d: Acquired connection for task %s in %s.",
				workerID,
				taskIDStr,
				acquisitionDuration,
			) // Add debug log with duration.

			// Handle connection with proper cleanup.
			b.logger.Debugf(
				"Worker %d: Handling connection for task %s.",
				workerID,
				taskIDStr,
			) // Add debug log.
			err = b.handleConnection(task, wr)
			if err != nil {
				// Use Debugf for potentially noisy connection handling errors.
				b.logger.Debugf(
					"Worker %d: Connection handling error for task %s: %v. Releasing connection.",
					workerID,
					taskIDStr,
					err,
				)
				p.Release(
					wr,
				) // Release potentially bad connection.
				b.logger.Debugf(
					"Worker %d: Released potentially bad connection after error for task %s.",
					workerID,
					taskIDStr,
				) // Add debug log.
				// Error already sent by handleConnection or respondPending.

				continue
			}

			// Connection is healthy, return it to pool.
			p.Put(wr)
			b.logger.Debugf(
				"Worker %d: Returned healthy connection to pool for task %s.",
				workerID,
				taskIDStr,
			) // Add debug log.

			// Use Printf for INFO level.
			b.logger.Printf(
				"Worker %d: Completed task %s",
				workerID,
				taskIDStr,
			)
		}
	}
}

// handleConnection manages a single connection operation.
func (b *broker) handleConnection(task *Task, wr PoolItem) error {
	taskIDStr := hex.EncodeToString(task.taskID) // Cache task ID string.
	netConn, ok := wr.(net.Conn)
	if !ok {
		err := errors.New("internal error: pool item is not net.Conn")
		b.logger.Debugf("handleConnection task %s: %v", taskIDStr, err) // Add debug log.
		b.trySendError(
			task,
			err,
		) // Send error back to caller.

		return err
	}
	connInfo := fmt.Sprintf(
		"%s -> %s",
		netConn.LocalAddr(),
		netConn.RemoteAddr(),
	) // Get conn info for logs.

	// Store active connection for shutdown tracking.
	b.connMu.Lock()
	b.activeConns.Store(string(task.taskID), netConn)
	b.connMu.Unlock()
	b.logger.Debugf(
		"handleConnection task %s: Stored active connection %s.",
		taskIDStr,
		connInfo,
	) // Add debug log.

	// Ensure connection is removed from active list.
	defer func() {
		b.connMu.Lock()
		b.activeConns.Delete(string(task.taskID))
		b.connMu.Unlock()
		b.logger.Debugf(
			"handleConnection task %s: Removed active connection %s.",
			taskIDStr,
			connInfo,
		) // Add debug log.
	}()

	cmd := b.addTask(task)
	b.logger.Debugf(
		"handleConnection task %s: Prepared command (len=%d).",
		taskIDStr,
		len(cmd),
	) // Add debug log.

	// Check if broker is closing before proceeding.
	if b.closing.Load() {
		err := ErrClosingBroker
		b.logger.Debugf(
			"handleConnection task %s: Broker closing before write.",
			taskIDStr,
		) // Add debug log.
		b.trySendError(
			task,
			err,
		) // Send error back to caller.

		return err
	}

	taskCtx := task.Context()

	// Set write deadline.
	writeDeadline := time.Now().Add(5 * time.Second)
	b.logger.Debugf(
		"handleConnection task %s: Setting write deadline to %s on %s.",
		taskIDStr,
		writeDeadline,
		connInfo,
	) // Add debug log.
	if err := netConn.SetWriteDeadline(writeDeadline); err != nil {
		b.logger.Debugf(
			"handleConnection task %s: Error setting write deadline on %s: %v",
			taskIDStr,
			connInfo,
			err,
		) // Add debug log.
		wrappedErr := fmt.Errorf("setting write deadline: %w", err)
		b.trySendError(task, wrappedErr) // Send error back to caller.

		return wrappedErr
	}

	b.logger.Debugf(
		"handleConnection task %s: Writing %d bytes to %s.",
		taskIDStr,
		len(cmd),
		connInfo,
	) // Add debug log.
	if err := Write(netConn, cmd); err != nil {
		b.logger.Debugf(
			"handleConnection task %s: Error writing to %s: %v",
			taskIDStr,
			connInfo,
			err,
		) // Add debug log.
		wrappedErr := fmt.Errorf("writing to connection: %w", err)
		b.trySendError(task, wrappedErr) // Send error back to caller.

		return wrappedErr
	}
	b.logger.Debugf(
		"handleConnection task %s: Successfully wrote %d bytes to %s.",
		taskIDStr,
		len(cmd),
		connInfo,
	) // Add debug log.

	// Set read deadline.
	readDeadline := time.Now().Add(5 * time.Second)
	b.logger.Debugf(
		"handleConnection task %s: Setting read deadline to %s on %s.",
		taskIDStr,
		readDeadline,
		connInfo,
	) // Add debug log.
	if err := netConn.SetReadDeadline(readDeadline); err != nil {
		b.logger.Debugf(
			"handleConnection task %s: Error setting read deadline on %s: %v",
			taskIDStr,
			connInfo,
			err,
		) // Add debug log.
		wrappedErr := fmt.Errorf("setting read deadline: %w", err)
		b.trySendError(task, wrappedErr) // Send error back to caller.

		return wrappedErr
	}

	// If task has context, use it to check for cancellation.
	if taskCtx != nil {
		done := make(chan struct{})
		var readErr error // Store potential read error.
		b.logger.Debugf(
			"handleConnection task %s: Starting read goroutine with context for %s.",
			taskIDStr,
			connInfo,
		) // Add debug log.
		go func() {
			defer close(done)
			b.logger.Debugf(
				"handleConnection task %s: Goroutine reading from %s.",
				taskIDStr,
				connInfo,
			) // Add debug log.
			resp, err := Read(netConn)
			if err != nil {
				readErr = fmt.Errorf("reading from connection: %w", err)
				b.logger.Debugf(
					"handleConnection task %s: Goroutine read error from %s: %v",
					taskIDStr,
					connInfo,
					readErr,
				) // Add debug log.
				b.trySendError(task, readErr)

				return
			}
			b.logger.Debugf(
				"handleConnection task %s: Goroutine read %d bytes from %s.",
				taskIDStr,
				len(resp),
				connInfo,
			) // Add debug log.
			b.respondPending(resp)
		}()

		select {
		case <-taskCtx.Done():
			err := taskCtx.Err()
			b.logger.Debugf(
				"handleConnection task %s: Context canceled while reading from %s: %v",
				taskIDStr,
				connInfo,
				err,
			) // Add debug log.
			// Error already sent by trySendError or SendContext will handle it.
			// We still return the context error to signal the loop.

			return err
		case <-done:
			b.logger.Debugf(
				"handleConnection task %s: Read goroutine completed for %s.",
				taskIDStr,
				connInfo,
			) // Add debug log.
			// Response received successfully or error handled by goroutine.
			if readErr != nil {
				return readErr // Propagate read error if it occurred.
			}
		}
	} else {
		b.logger.Debugf("handleConnection task %s: Reading directly (no context) from %s.", taskIDStr, connInfo) // Add debug log.
		resp, err := Read(netConn)
		if err != nil {
			b.logger.Debugf("handleConnection task %s: Direct read error from %s: %v", taskIDStr, connInfo, err) // Add debug log.
			wrappedErr := fmt.Errorf("reading from connection: %w", err)
			b.trySendError(task, wrappedErr) // Send error back to caller.

			return wrappedErr
		}
		b.logger.Debugf("handleConnection task %s: Direct read %d bytes from %s.", taskIDStr, len(resp), connInfo) // Add debug log.
		b.respondPending(resp)
	}

	// Clear connection deadlines.
	b.logger.Debugf(
		"handleConnection task %s: Clearing deadlines on %s.",
		taskIDStr,
		connInfo,
	) // Add debug log.
	if err := netConn.SetDeadline(time.Time{}); err != nil {
		// Log error but don't fail the operation just for deadline clearing.
		b.logger.Debugf(
			"handleConnection task %s: Error clearing deadline on %s: %v",
			taskIDStr,
			connInfo,
			err,
		) // Add debug log.
		// return fmt.Errorf("clearing deadline: %w", err) // Decided not to return error here.
	}

	b.logger.Debugf(
		"handleConnection task %s: Successfully handled connection %s.",
		taskIDStr,
		connInfo,
	) // Add debug log.

	return nil
}

// trySendError attempts to send an error to the task's error channel.
func (b *broker) trySendError(task *Task, err error) {
	taskIDStr := hex.EncodeToString(task.taskID)
	b.logger.Debugf("trySendError: Attempting to send error for task %s: %v", taskIDStr, err)
	defer func() {
		if r := recover(); r != nil {
			b.logger.Printf("Recovered panic sending error for task %s: %v", taskIDStr, r)
		}
	}()

	select {
	case task.errCh <- err:
		b.logger.Debugf("trySendError: Successfully sent error for task %s.", taskIDStr)
		// Only fail pending task if we successfully sent the error
		b.failPending(task)
	default:
		// Channel might be full or closed (e.g., context canceled concurrently)
		b.logger.Printf(
			"trySendError: Failed to send error for task %s (channel full or closed).",
			taskIDStr,
		)
	}
}

// respondPending finds the pending task by response header and sends the response.
func (b *broker) respondPending(resp []byte) {
	if len(resp) < taskIDSize {
		b.logger.Errorf(
			"respondPending: Response too short: %d bytes",
			len(resp),
		) // Changed to Errorf

		return
	}
	taskID := string(resp[:taskIDSize])
	taskIDStr := hex.EncodeToString(resp[:taskIDSize]) // Cache hex string.

	b.logger.Debugf("respondPending: Received response for task %s.", taskIDStr) // Add debug log.

	if value, ok := b.pending.Load(taskID); ok {
		task, castOK := value.(*Task)
		if !castOK {
			b.logger.Errorf( // Changed to Errorf
				"respondPending: Invalid type found in pending map for task %s.",
				taskIDStr,
			)
			b.pending.Delete(taskID) // Clean up invalid entry.

			return
		}

		// Safely attempt to send response, recovering if channel is closed.
		sent := false
		func() {
			defer func() {
				if r := recover(); r != nil {
					b.logger.Errorf( // Changed to Errorf
						"Recovered panic sending response for task %s: %v",
						taskIDStr,
						r,
					)
				}
			}()
			select {
			case task.response <- resp[taskIDSize:]:
				sent = true
				b.logger.Debugf(
					"respondPending: Sent %d byte response to task %s.",
					len(resp)-taskIDSize,
					taskIDStr,
				) // Add debug log.
			default:
				b.logger.Warnf( // Changed to Warnf
					"respondPending: Failed to send response to task %s (channel full or closed).",
					taskIDStr,
				)
			}
		}()

		// Only delete if successfully sent, otherwise let Close handle cleanup.
		if sent {
			b.pending.Delete(taskID)
			b.logger.Debugf(
				"respondPending: Removed task %s from pending list.",
				taskIDStr,
			) // Add debug log.
		}
	} else {
		b.logger.Warnf("respondPending: Task %s not found in pending list.", taskIDStr) // Changed to Warnf
	}
}

// failPending removes a task from the pending list without sending a response.
// Used when an error occurs before a response is generated or context is canceled.
func (b *broker) failPending(task *Task) {
	taskIDStr := hex.EncodeToString(task.taskID)                // Cache hex string.
	b.logger.Debugf("failPending: Failing task %s.", taskIDStr) // Add debug log.
	b.pending.Delete(string(task.taskID))
	// Close channels to signal completion/failure to the waiting Send/SendContext goroutine.
	// Use recover for safety, although closing already closed channels is a no-op.
	func() {
		defer func() {
			if r := recover(); r != nil {
				b.logger.Errorf(
					"Recovered panic closing channels for task %s: %v",
					taskIDStr,
					r,
				) // Changed to Errorf
			}
		}()
		close(task.response)
		close(task.errCh)
		b.logger.Debugf("failPending: Closed channels for task %s.", taskIDStr) // Add debug log.
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
