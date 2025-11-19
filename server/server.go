package server

import (
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/andrei-cloud/anet"
)

type Server struct {
	address         string         // network address to listen on.
	listener        net.Listener   // TCP listener for incoming connections.
	config          *ServerConfig  // server configuration options.
	handler         Handler        // handler to process incoming messages.
	activeConns     sync.Map       // registry of active connections.
	activeConnCount atomic.Int32   // atomic counter for active connections
	connWG          sync.WaitGroup // tracks active connection goroutines.
	stopChan        chan struct{}  // signals server shutdown.
	handlerSem      chan struct{}  // semaphore to limit concurrent handlers.
}

func NewServer(address string, handler Handler, config *ServerConfig) (*Server, error) {
	if handler == nil {
		return nil, errors.New("handler is required")
	}
	if config == nil {
		config = &ServerConfig{}
	}
	config.applyDefaults()

	s := &Server{
		address:  address,
		config:   config,
		handler:  handler,
		stopChan: make(chan struct{}),
	}

	if config.MaxConcurrentHandlers > 0 {
		s.handlerSem = make(chan struct{}, config.MaxConcurrentHandlers)
	}

	return s, nil
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	s.listener = ln

	go s.acceptLoop()

	return nil
}

func (s *Server) Stop() error {
	close(s.stopChan)

	err := s.listener.Close()
	if err != nil {
		return err
	}

	s.activeConns.Range(func(_, val any) bool {
		if c, ok := val.(*ServerConn); ok {
			if err := c.Conn.Close(); err != nil {
				s.logf("connection close error: %v", err)
			}
		}

		return true
	})

	done := make(chan struct{})
	go func() {
		s.connWG.Wait()

		close(done)
	}()

	if s.config.ShutdownTimeout > 0 {
		select {
		case <-done:
		case <-time.After(s.config.ShutdownTimeout):
			s.logf("timeout waiting for connections to close")
		}
	} else {
		<-done
	}

	return nil
}

func (s *Server) acceptLoop() {
	for {
		select {
		case <-s.stopChan:
			return
		default:
		}

		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				time.Sleep(100 * time.Millisecond)

				continue
			}
			s.logf("accept error: %v", err)

			return
		}

		if s.config.MaxConns > 0 {
			if int(s.activeConnCount.Load()) >= s.config.MaxConns {
				if err := conn.Close(); err != nil {
					s.logf("connection close error: %v", err)
				}

				continue
			}
		}

		s.handleNewConnection(conn)
	}
}

func (s *Server) handleNewConnection(conn net.Conn) {
	s.activeConnCount.Add(1)
	sc := &ServerConn{Conn: conn, server: s}
	sc.init()

	s.activeConns.Store(sc, sc)

	s.connWG.Add(1)
	go s.connectionLoop(sc)
}

func (s *Server) removeConnection(sc *ServerConn) {
	s.activeConns.Delete(sc)
	s.activeConnCount.Add(-1)
}

func (s *Server) connectionLoop(sc *ServerConn) {
	defer func() {
		s.removeConnection(sc)

		if err := sc.Conn.Close(); err != nil {
			s.logf("connection close error: %v", err)
		}

		s.connWG.Done()
	}()

	for {
		if s.config.IdleTimeout > 0 {
			if err := sc.Conn.SetReadDeadline(time.Now().Add(s.config.IdleTimeout)); err != nil {
				s.logf("set read deadline error: %v", err)
			}
		}

		msg, err := anet.ReadPooled(sc.Conn)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				s.logf("closing idle connection: %v", sc.Conn.RemoteAddr())
			}

			return
		}

		if len(msg) < 4 {
			s.logf("protocol error: message too short")
			anet.PutBuffer(msg) // Return buffer if invalid
			return
		}

		taskID := msg[:4]
		payload := msg[4:]

		s.dispatchMessage(sc, taskID, payload, msg)
	}
}

func (s *Server) dispatchMessage(sc *ServerConn, taskID, request, originalBuf []byte) {
	// Acquire semaphore if configured
	if s.handlerSem != nil {
		// Blocking here provides backpressure to the connection reader loop.
		s.handlerSem <- struct{}{}
	}

	go func() {
		defer func() {
			if s.handlerSem != nil {
				<-s.handlerSem
			}
			anet.PutBuffer(originalBuf)
		}()

		resp, err := s.handler.HandleMessage(sc, request)
		if err != nil {
			s.logf("handler error: %v", err)
		}

		if resp == nil {
			return
		}

		// reuse buffer for taskID+resp to reduce allocations.
		required := anet.LENGTHSIZE + len(taskID) + len(resp)
		buf := anet.GetBuffer(required)

		// Ensure buffer has enough capacity (GetBuffer guarantees at least required size)
		// But GetBuffer returns a slice with len=required if it allocated new,
		// or len=poolSize if from pool.
		// We need to reslice to required length.
		if cap(buf) < required {
			// Should not happen if GetBuffer works as documented
			buf = make([]byte, required)
		}
		buf = buf[:required]

		// Write header directly to avoid net.Buffers allocation in Write
		switch anet.LENGTHSIZE {
		case 2:
			binary.BigEndian.PutUint16(buf[0:2], uint16(len(taskID)+len(resp)))
		case 4:
			binary.BigEndian.PutUint32(buf[0:4], uint32(len(taskID)+len(resp)))
		}

		copy(buf[anet.LENGTHSIZE:anet.LENGTHSIZE+4], taskID)
		copy(buf[anet.LENGTHSIZE+4:], resp)

		sc.writeMu.Lock()
		if s.config.WriteTimeout > 0 {
			if err := sc.Conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout)); err != nil {
				s.logf("set write deadline error: %v", err)
			}
		}

		_, writeErr := sc.Conn.Write(buf)
		sc.writeMu.Unlock()

		// return buffer to pool.
		anet.PutBuffer(buf)

		if writeErr != nil {
			s.logf("write error: %v", writeErr)

			if err := sc.Conn.Close(); err != nil {
				s.logf("connection close error: %v", err)
			}
		}
	}()
}

func (s *Server) logf(format string, v ...any) {
	if s.config.Logger != nil {
		s.config.Logger.Printf(format, v...)
	}
}
