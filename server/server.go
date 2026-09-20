package server

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

	"github.com/andrei-cloud/anet"
)

// taskIDSize mirrors the protocol constant in package anet: every request
// frame carries a 4-byte big-endian task ID that the response must echo.
const taskIDSize = 4

// burstFlushCap bounds how many bytes one writer flush accumulates before a
// single syscall. Buffered, batched writes are the measured dominant lever
// at high connection counts (goperf.dev: ~50x send-side gain at 10K
// connections); latency is protected because an idle connection flushes each
// frame immediately and every flush arms a write deadline.
const burstFlushCap = 32 * 1024

// Server is an embeddable TCP server speaking the anet framed protocol:
// [len][taskID][request] in, [len][taskID][response] out.
type Server struct {
	address         string         // network address to listen on.
	listener        net.Listener   // TCP listener for incoming connections.
	config          *ServerConfig  // server configuration options.
	handler         Handler        // handler to process incoming messages.
	connMu          sync.RWMutex   // admits connections against Stop (see handleNewConnection).
	activeConns     sync.Map       // registry of active connections.
	activeConnCount atomic.Int32   // atomic counter for active connections
	connWG          sync.WaitGroup // tracks per-connection loops and writers.
	handlerWG       sync.WaitGroup // tracks per-message handler goroutines.
	stopChan        chan struct{}  // signals server shutdown.
	stopping        atomic.Bool    // atomic flag for idempotent shutdown.
	handlerSem      chan struct{}  // semaphore to limit concurrent handlers.
	framePool       sync.Pool      // reuse of response frame wrappers.
}

// respFrame is one queued response: the writer goroutine stages it onto the
// connection and then returns orig (the pooled request buffer) to the pool.
// resp itself is handler-owned, immutable after HandleMessage returned, so
// the writer may stage it at its own pace.
type respFrame struct {
	id   [taskIDSize]byte // task ID echoed from the request
	resp []byte           // response bytes, handler-owned, read by the writer
	orig []byte           // pooled request buffer to PutBuffer after staging
}

func NewServer(address string, handler Handler, config *ServerConfig) (*Server, error) {
	if handler == nil {
		return nil, errors.New("handler is required")
	}
	if config == nil {
		config = &ServerConfig{}
	}
	cfg := *config // never mutate the caller's struct
	cfg.applyDefaults()

	s := &Server{
		address:  address,
		config:   &cfg,
		handler:  handler,
		stopChan: make(chan struct{}),
	}
	s.framePool.New = func() any { return &respFrame{} }

	// <= 0, not == 0: a negative MaxConcurrentHandlers used to fall through
	// applyDefaults untouched and leave handlerSem nil, i.e. exactly the
	// unbounded per-message goroutine growth the semaphore exists to stop.
	if cfg.MaxConcurrentHandlers > 0 {
		s.handlerSem = make(chan struct{}, cfg.MaxConcurrentHandlers)
	}

	return s, nil
}

func (s *Server) Start() error {
	lc := net.ListenConfig{}
	if s.config.ReusePort {
		lc.Control = setReusePort
	}
	ln, err := lc.Listen(context.Background(), "tcp", s.address)
	if err != nil {
		return err
	}
	s.listener = ln

	go s.acceptLoop()

	return nil
}

func (s *Server) Stop() error {
	if !s.stopping.CompareAndSwap(false, true) {
		return nil
	}
	close(s.stopChan)

	// Barrier: every admission that had already passed the stopping check
	// under RLock completes here, so no activeConns.Store can follow the
	// Range below, and no connWG.Add can race the Wait below.
	s.connMu.Lock()
	s.connMu.Unlock()

	var err error
	if s.listener != nil {
		err = s.listener.Close()
	}

	s.closeActiveConns()

	done := make(chan struct{})
	go func() {
		s.connWG.Wait()
		// Every handlerWG.Add happens in a connectionLoop, which is
		// connWG-tracked, so all Adds are done once connWG is.
		s.handlerWG.Wait()
		close(done)
	}()

	if s.config.ShutdownTimeout > 0 {
		select {
		case <-done:
		case <-time.After(s.config.ShutdownTimeout):
			// Force again: conns accepted after the first Range, plus any
			// still wedged. This is what makes Stop self-healing instead of
			// leaking a connection plus goroutine pair per Stop race.
			s.logf("timeout waiting for connections to close; forcing")
			s.closeActiveConns()
			select {
			case <-done:
			case <-time.After(s.config.WriteTimeout + time.Second):
				s.logf("connections still running after forced close (wedged handler?)")
			}
		}
	} else {
		<-done
	}

	return err
}

func (s *Server) closeActiveConns() {
	s.activeConns.Range(func(_, val any) bool {
		if c, ok := val.(*ServerConn); ok {
			if closeErr := c.Conn.Close(); closeErr != nil {
				s.logf("connection close error: %v", closeErr)
			}
		}
		return true
	})
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			select {
			case <-s.stopChan:
				return
			default:
			}
			// No accept deadline is ever armed, so the old net.Error
			// Timeout() backoff branch was unreachable; anything reaching
			// here after Stop is a listener we no longer own.
			s.logf("accept error: %v", err)
			return
		}

		if s.config.MaxConns > 0 {
			if int(s.activeConnCount.Load()) >= s.config.MaxConns {
				_ = conn.Close()
				continue
			}
		}

		s.handleNewConnection(conn)
	}
}

// handleNewConnection admits a connection against Stop. Admission holds
// connMu.RLock across the stopping check and the bookkeeping, so a
// connection accepted across Stop is closed on the spot (it used to be
// stored after Stop's Range and never closed: a leaked conn plus a leaked
// goroutine), and connWG.Add can never be observed "concurrent with Wait".
func (s *Server) handleNewConnection(conn net.Conn) {
	s.connMu.RLock()
	if s.stopping.Load() {
		s.connMu.RUnlock()
		_ = conn.Close()
		return
	}

	sc := &ServerConn{
		Conn:    conn,
		server:  s,
		writeCh: make(chan *respFrame, s.config.writeQueueDepth()),
		done:    make(chan struct{}),
	}
	sc.init()

	s.activeConnCount.Add(1)
	s.activeConns.Store(sc, sc)

	s.connWG.Add(2) // connectionLoop + writer
	go s.connectionLoop(sc)
	go s.writeLoop(sc)

	s.connMu.RUnlock()
}

func (s *Server) removeConnection(sc *ServerConn) {
	s.activeConns.Delete(sc)
	s.activeConnCount.Add(-1)
}

// connectionLoop reads framed requests until the connection dies.
func (s *Server) connectionLoop(sc *ServerConn) {
	defer func() {
		s.removeConnection(sc)
		_ = sc.Conn.Close()
		s.connWG.Done()
	}()

	for {
		// Two-phase read deadline. The header wait uses IdleTimeout (0 by
		// default = quiet long-lived clients stay connected); once a frame
		// has started, its body must follow within ReadTimeout, which is
		// what previously-dead ReadTimeout field now actually enforces
		// (cures stalled mid-frame reads; IdleTimeout governs quiet time).
		if d := s.config.IdleTimeout; d > 0 {
			_ = sc.Conn.SetReadDeadline(time.Now().Add(d))
		}

		if _, err := io.ReadFull(sc.Conn, sc.hdr[:]); err != nil {
			return
		}

		var length uint64
		switch anet.LENGTHSIZE {
		case 2:
			length = uint64(binary.BigEndian.Uint16(sc.hdr[:]))
		case 4:
			length = uint64(binary.BigEndian.Uint32(sc.hdr[:]))
		}

		if length < taskIDSize {
			s.logf("protocol error: message too short")
			return
		}

		// Body read window: armed only when opted in (each SetDeadline is a
		// kernel deadline operation; arming it per frame cost ~25% latency
		// on the sequential benchmark). Cleared right after the body so it
		// never bleeds into the next (possibly long) header wait.
		armed := false
		if d := s.config.ReadTimeout; d > 0 {
			_ = sc.Conn.SetReadDeadline(time.Now().Add(d))
			armed = true
		}

		msg := anet.GetBuffer(int(length))
		if _, err := io.ReadFull(sc.Conn, msg); err != nil {
			anet.PutBuffer(msg)
			return
		}
		if armed {
			_ = sc.Conn.SetReadDeadline(time.Time{})
		}

		if err := s.dispatch(sc, msg); err != nil {
			anet.PutBuffer(msg)
			return
		}
	}
}

// dispatch hands one request frame to a handler goroutine, bounded by
// handlerSem. Acquiring the semaphore on the reader goroutine (not inside
// the new goroutine) is the backpressure: a slow pool parks this connection
// instead of spawning unbounded goroutines.
func (s *Server) dispatch(sc *ServerConn, msg []byte) error {
	if s.handlerSem != nil {
		select {
		case s.handlerSem <- struct{}{}:
		case <-s.stopChan:
			return errors.New("server stopping")
		case <-sc.done:
			return errors.New("connection closed")
		}
	}

	s.handlerWG.Add(1)
	go s.handleMessage(sc, msg)
	return nil
}

// handleMessage runs the user handler, converts a panic into an error
// (a panicking handler used to take the process down), and queues exactly
// one frame or frees the request buffer. frame is [taskID][request] as it
// arrived; the handler sees only the request part, and the bytes are valid
// until HandleMessage returns (the writer recycles them after that).
func (s *Server) handleMessage(sc *ServerConn, frame []byte) {
	defer s.handlerWG.Done()
	if s.handlerSem != nil {
		defer func() { <-s.handlerSem }()
	}

	resp, err := s.invoke(sc, frame[taskIDSize:])
	if err != nil {
		s.logf("handler error: %v", err)
	}

	if resp == nil {
		anet.PutBuffer(frame)
		return
	}

	f := s.framePool.Get().(*respFrame)
	copy(f.id[:], frame[:taskIDSize])
	f.resp = resp
	f.orig = frame

	select {
	case sc.writeCh <- f:
	case <-sc.done:
		s.releaseFrame(f)
	case <-s.stopChan:
		s.releaseFrame(f)
	}
}

// invoke calls the handler with panic protection.
func (s *Server) invoke(sc *ServerConn, req []byte) (resp []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			resp = nil
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return s.handler.HandleMessage(sc, req)
}

// writeLoop is the single writer for one connection. Single ownership
// removes the old per-response goroutine and its write mutex, lets bursts
// batch into one syscall, and keeps one write deadline per flush.
func (s *Server) writeLoop(sc *ServerConn) {
	defer func() {
		// Unblock readers/handlers parked on writeCh and signal the loop.
		for {
			select {
			case f := <-sc.writeCh:
				s.releaseFrame(f)
			default:
				close(sc.done)
				s.connWG.Done()
				return
			}
		}
	}()

	var stage []byte // writer-owned staging: no per-frame allocation
	for {
		var f *respFrame
		select {
		case f = <-sc.writeCh:
		case <-s.stopChan:
			return
		}

		// One staging buffer per writer: reset once per batch, so queued
		// frames accumulate into a single write instead of one syscall each.
		stage = stage[:0]
		for f != nil {
			payloadLen := taskIDSize + len(f.resp)
			stage = append(stage, byte(payloadLen>>8), byte(payloadLen))
			stage = append(stage, f.id[:]...)
			stage = append(stage, f.resp...)
			s.releaseFrame(f) // resp bytes now copied into stage

			if len(stage) >= burstFlushCap {
				break
			}
			select {
			case f = <-sc.writeCh:
			default:
				f = nil
			}
		}

		if len(stage) > 0 {
			if s.config.WriteTimeout > 0 {
				_ = sc.Conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))
			}
			if _, err := sc.Conn.Write(stage); err != nil {
				s.logf("write error: %v", err)
				_ = sc.Conn.Close()
				return
			}
		}

		// Freed every frame we drained before the flush that errored?
		// Yes: releaseFrame ran during staging, before Write.
	}
}

func (s *Server) releaseFrame(f *respFrame) {
	f.resp = nil
	if f.orig != nil {
		anet.PutBuffer(f.orig)
		f.orig = nil
	}
	s.framePool.Put(f)
}

func (s *Server) logf(format string, v ...any) {
	if s.config.Logger != nil {
		s.config.Logger.Printf(format, v...)
	}
}
