package server

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/andrei-cloud/anet"
)

var frameBufferPool = sync.Pool{
	New: func() any { return make([]byte, 0, 4096) },
} // pool for message frames.

type Server struct {
	address     string         // network address to listen on.
	listener    net.Listener   // TCP listener for incoming connections.
	config      *ServerConfig  // server configuration options.
	handler     Handler        // handler to process incoming messages.
	activeConns sync.Map       // registry of active connections.
	connWG      sync.WaitGroup // tracks active connection goroutines.
	stopChan    chan struct{}  // signals server shutdown.
	mu          sync.Mutex     // protects server internal state.
}

func NewServer(address string, handler Handler, config *ServerConfig) (*Server, error) {
	if handler == nil {
		return nil, errors.New("handler is required")
	}
	if config == nil {
		config = &ServerConfig{}
	}
	config.applyDefaults()

	return &Server{
		address:  address,
		config:   config,
		handler:  handler,
		stopChan: make(chan struct{}),
	}, nil
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
			count := 0
			s.activeConns.Range(func(_, _ any) bool {
				count++

				return true
			})
			if count >= s.config.MaxConns {
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
	sc := &ServerConn{Conn: conn, server: s}
	sc.init()

	s.activeConns.Store(sc, sc)

	s.connWG.Add(1)
	go s.connectionLoop(sc)
}

func (s *Server) removeConnection(sc *ServerConn) {
	s.activeConns.Delete(sc)
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

		msg, err := anet.Read(sc.Conn)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				s.logf("closing idle connection: %v", sc.Conn.RemoteAddr())
			}

			return
		}

		if len(msg) < 4 {
			s.logf("protocol error: message too short")

			return
		}

		taskID := msg[:4]
		payload := msg[4:]

		s.dispatchMessage(sc, taskID, payload)
	}
}

func (s *Server) dispatchMessage(sc *ServerConn, taskID []byte, request []byte) {
	go func() {
		resp, err := s.handler.HandleMessage(sc, request)
		if err != nil {
			s.logf("handler error: %v", err)
		}

		if resp == nil {
			return
		}

		// reuse buffer for taskID+resp to reduce allocations.
		buf := frameBufferPool.Get().([]byte)
		required := len(taskID) + len(resp)
		if cap(buf) < required {
			buf = make([]byte, required)
		}
		buf = buf[:required]
		copy(buf[:4], taskID)
		copy(buf[4:], resp)

		sc.writeMu.Lock()
		if s.config.WriteTimeout > 0 {
			if err := sc.Conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout)); err != nil {
				s.logf("set write deadline error: %v", err)
			}
		}

		writeErr := anet.Write(sc.Conn, buf)
		sc.writeMu.Unlock()

		// return buffer to pool.
		frameBufferPool.Put(buf[:0])

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
