package anet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// errPipelineDead (internal) marks a connection whose stream state is lost.
	errPipelineDead = errors.New("multiplexed connection is dead")
	// errReleasedByPool (internal) fails a pipeline released by its pool.
	errReleasedByPool = errors.New("multiplexed connection released by pool")
)

// pipeline is the Multiplex-mode transport for one connection: a background
// writer goroutine frames outstanding requests onto the wire and a reader
// goroutine matches responses by task ID, so one connection carries up to
// MaxInflightPerConn concurrent requests and the broker runs no queue
// workers at all. The protocol is unchanged: [len][taskID][payload].
//
// Task ownership: pl.submit takes queue ownership of a task and exactly one
// of {reader on response, writer on write outcome, failAll on stream error,
// submitter-side shed} delivers its Response and recycles it, once.
//
// Synchronization: pending and writing are guarded by pmu. The reader may
// fail the pipeline while the writer is inside conn.Write, so the in-flight
// write's task is excluded from failAll (it would otherwise be recycled and
// restaged under the writer's bytes); the writer resolves it after Write
// returns.
type pipeline struct {
	conn    net.Conn
	b       *broker
	wch     chan *Task       // staged frames awaiting write (cap = maxInflight)
	pmu     sync.Mutex       // guards pending, writing
	pending map[uint32]*Task // outstanding requests by task ID (reader correlation)
	writing *Task            // task whose frame is inside conn.Write (failAll skips it)
	dead    atomic.Bool      // stream state lost; no further submits accepted
	quit    chan struct{}    // closed once by failAll: exits both goroutines
	wg      sync.WaitGroup   // tracks writer and reader goroutines (tests)
}

// newPipeline starts the writer and reader goroutines for conn.
func newPipeline(b *broker, conn net.Conn) *pipeline {
	pl := &pipeline{
		conn:    conn,
		b:       b,
		wch:     make(chan *Task, b.config.MaxInflightPerConn),
		pending: make(map[uint32]*Task, 16),
		quit:    make(chan struct{}),
	}
	pl.wg.Add(2)
	go pl.writeLoop()
	go pl.readLoop()
	return pl
}

// submit takes ownership of task and arranges exactly one eventual Response.
// It returns ErrQueueFull when this connection is at its outstanding bound
// (the caller tries another pool) or the pipeline is already dead.
func (pl *pipeline) submit(task *Task) error {
	if pl.dead.Load() {
		return errPipelineDead
	}

	maxInflight := pl.b.config.MaxInflightPerConn
	if maxInflight <= 0 {
		return ErrQueueFull // Multiplex mode needs MaxInflightPerConn > 0
	}

	pl.pmu.Lock()
	if pl.dead.Load() {
		pl.pmu.Unlock()
		return errPipelineDead
	}
	if len(pl.pending) >= maxInflight {
		pl.pmu.Unlock()
		return ErrQueueFull
	}
	if _, dup := pl.pending[task.id]; dup {
		// 32-bit ID wrap while the ID is still outstanding: shed; never
		// misdeliver one response to two requests.
		pl.pmu.Unlock()
		return ErrQueueFull
	}
	pl.pending[task.id] = task
	select {
	case pl.wch <- task:
		pl.pmu.Unlock()
		return nil
	default:
		delete(pl.pending, task.id)
		pl.pmu.Unlock()
		return ErrQueueFull
	}
}

// Close implements PoolItem. Closing is idempotent and fails every
// outstanding request exactly once.
func (pl *pipeline) Close() error {
	return pl.fail(errReleasedByPool)
}

// Validate implements the pool's self-validation hook. A pipeline owns its
// connection's stream state, so the pool must not probe it with raw reads
// (that would race the reader goroutine); liveness is simply "not dead",
// and TCP keepalive (see NewTCPFactory) is what surfaces silent peers.
func (pl *pipeline) Validate() bool {
	return !pl.dead.Load()
}

// writeLoop frames submitted tasks onto the connection.
func (pl *pipeline) writeLoop() {
	defer pl.wg.Done()
	b := pl.b

	for {
		var task *Task
		select {
		case task = <-pl.wch:
		case <-pl.quit:
			return
		case <-b.ctx.Done():
			pl.fail(ErrClosingBroker)
			return
		}

		// Load shedding: requests whose caller is already gone fail before
		// spending a write; nobody can receive their late response anyway.
		if taskCtx := task.Context(); taskCtx != nil {
			if err := taskCtx.Err(); err != nil {
				pl.pmu.Lock()
				delete(pl.pending, task.id)
				pl.pmu.Unlock()
				b.deliver(task, Response{Err: err})
				b.recycleTask(task)
				continue
			}
		}

		pl.pmu.Lock()
		pl.writing = task
		pl.pmu.Unlock()

		if b.config.WriteTimeout > 0 {
			_ = pl.conn.SetWriteDeadline(time.Now().Add(b.config.WriteTimeout))
		}
		_, err := pl.conn.Write(task.frame[:task.frameLen])

		pl.pmu.Lock()
		pl.writing = nil
		if err != nil {
			delete(pl.pending, task.id) // failAll must not double-own it
		}
		pl.pmu.Unlock()

		if err != nil {
			b.deliver(task, Response{Err: fmt.Errorf("writing to connection: %w", err)})
			b.recycleTask(task)
			pl.fail(errPipelineDead)
			return
		}

		if pl.dead.Load() {
			// The reader failed the stream while this frame was in Write and
			// skipped it; no response can arrive. Resolve it here.
			b.deliver(task, Response{Err: errPipelineDead})
			b.recycleTask(task)
			return
		}
	}
}

// readLoop receives frames and correlates them to pending tasks by ID.
func (pl *pipeline) readLoop() {
	defer pl.wg.Done()
	b := pl.b

	// Header scratch allocated once per connection: a stack array flowing
	// into conn.Read would escape on every response.
	hdr := make([]byte, LENGTHSIZE)

	for {
		// Bound response reads while requests are outstanding; leave idle
		// connections undated (TCP keepalive surfaces death instead).
		if b.config.ReadTimeout > 0 {
			if pl.inflight() > 0 {
				_ = pl.conn.SetReadDeadline(time.Now().Add(b.config.ReadTimeout))
			} else {
				_ = pl.conn.SetReadDeadline(time.Time{})
			}
		}

		if _, err := io.ReadFull(pl.conn, hdr); err != nil {
			pl.fail(errPipelineDead)
			return
		}

		var length uint64
		switch LENGTHSIZE {
		case 2:
			length = uint64(binary.BigEndian.Uint16(hdr))
		case 4:
			length = uint64(binary.BigEndian.Uint32(hdr))
		default:
			pl.fail(fmt.Errorf("unsupported header size: %d", LENGTHSIZE))
			return
		}
		if length < taskIDSize {
			pl.fail(errors.New("response too short"))
			return
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(pl.conn, payload); err != nil {
			pl.fail(errPipelineDead)
			return
		}

		id := binary.BigEndian.Uint32(payload[:taskIDSize])
		pl.pmu.Lock()
		task := pl.pending[id]
		if task != nil {
			delete(pl.pending, id)
		}
		pl.pmu.Unlock()

		if task == nil {
			// A late response for a request that was failed or shed. Drop it.
			b.logger.Warnf("multiplex: dropping response for unknown task ID %d", id)
			continue
		}

		b.deliver(task, Response{Payload: payload[taskIDSize:]})
		b.recycleTask(task)
	}
}

// inflight counts outstanding requests (map reads are lock-guarded).
func (pl *pipeline) inflight() int {
	pl.pmu.Lock()
	n := len(pl.pending)
	pl.pmu.Unlock()
	return n
}

// fail kills the pipeline exactly once: every pending request receives an
// error Response and is recycled, the connection is closed, and both
// goroutines exit. Reachable from the writer, the reader, or a pool
// Release; the task currently inside conn.Write is left to the writer to
// resolve, because recycling it here would restage it under the writer's
// in-progress bytes.
func (pl *pipeline) fail(cause error) error {
	first := !pl.dead.Swap(true)

	pl.pmu.Lock()
	failed := pl.pending
	if w := pl.writing; w != nil && failed[w.id] == w {
		delete(failed, w.id) // the writer owns that one
	}
	pl.pending = nil
	pl.pmu.Unlock()

	for _, task := range failed {
		pl.b.deliver(task, Response{Err: cause})
		pl.b.recycleTask(task)
	}

	if first {
		select {
		case <-pl.quit:
		default:
			close(pl.quit)
		}
	}

	return pl.conn.Close()
}

// Wait joins the writer and reader goroutines (used by tests).
func (pl *pipeline) Wait() { pl.wg.Wait() }

// submitMuxed routes a task through the multiplex transport, wrapping pool
// items in a pipeline on first use. It sheds with ErrQueueFull when every
// connection is at its outstanding bound and surfaces dial errors otherwise.
func (b *broker) submitMuxed(task *Task) error {
	n := len(b.compool)
	if n == 0 {
		return ErrNoPoolsAvailable
	}

	getCtx := task.ctx
	if getCtx == nil {
		getCtx = b.ctx
	}

	var lastErr error = ErrQueueFull
	for range n {
		p := b.pickConnPool()
		if p == nil {
			return ErrNoPoolsAvailable
		}

		wr, err := p.GetWithContext(getCtx)
		if err != nil {
			lastErr = fmt.Errorf("failed to get connection: %w", err)
			continue
		}

		pl, ok := wr.(*pipeline)
		if !ok {
			conn, isConn := wr.(net.Conn)
			if !isConn {
				p.Release(wr)
				return errors.New("multiplex mode requires pool items that are net.Conn")
			}
			pl = newPipeline(b, conn)
		}

		if err := pl.submit(task); err != nil {
			if pl.dead.Load() {
				p.Release(pl) // replace the corpse with a fresh dial next pass
			} else {
				p.Put(pl) // at its outstanding bound; other pools may have room
			}
			lastErr = err
			continue
		}

		// Checked back in immediately: a pipeline keeps working while pooled,
		// which is the whole point of Multiplex mode.
		p.Put(pl)
		return nil
	}

	return lastErr
}
