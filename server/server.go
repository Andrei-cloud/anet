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
	stopping        atomic.Bool    // atomic flag for idempotent shutdown.
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
	if !s.stopping.CompareAndSwap(false, true) {
		return nil
	}
	close(s.stopChan)

	var err error
	if s.listener != nil {
		err = s.listener.Close()
	}

	s.activeConns.Range(func(_, val any) bool {
		if c, ok := val.(*ServerConn); ok {
			if closeErr := c.Conn.Close(); closeErr != nil {
				s.logf("connection close error: %v", closeErr)
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

	return err
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
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				time.Sleep(5 * time.Millisecond)
				continue
			}
			select {
			case <-s.stopChan:
				return
			default:
				s.logf("accept error: %v", err)
				return
			}
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
		_ = sc.Conn.Close()
		s.connWG.Done()
	}()

	for {
		if s.config.IdleTimeout > 0 {
			_ = sc.Conn.SetReadDeadline(time.Now().Add(s.config.IdleTimeout))
		}

		msg, err := anet.ReadPooled(sc.Conn)
		if err != nil {
			return
		}

		if len(msg) < 4 {
			s.logf("protocol error: message too short")
			anet.PutBuffer(msg)
			return
		}

		taskID := msg[:4]
		payload := msg[4:]

		s.dispatchMessage(sc, taskID, payload, msg)
	}
}

func (s *Server) dispatchMessage(sc *ServerConn, taskID, request, originalBuf []byte) {
	if s.handlerSem != nil {
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

		payloadLen := len(taskID) + len(resp)
		totalLen := anet.LENGTHSIZE + payloadLen

		if totalLen <= 512 {
			var stackBuf [512 + anet.LENGTHSIZE]byte
			switch anet.LENGTHSIZE {
			case 2:
				binary.BigEndian.PutUint16(stackBuf[0:2], uint16(payloadLen))
			case 4:
				binary.BigEndian.PutUint32(stackBuf[0:4], uint32(payloadLen))
			}
			copy(stackBuf[anet.LENGTHSIZE:anet.LENGTHSIZE+4], taskID)
			copy(stackBuf[anet.LENGTHSIZE+4:], resp)

			sc.writeMu.Lock()
			if s.config.WriteTimeout > 0 {
				_ = sc.Conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))
			}
			_, writeErr := sc.Conn.Write(stackBuf[:totalLen])
			sc.writeMu.Unlock()

			if writeErr != nil {
				s.logf("write error: %v", writeErr)
				_ = sc.Conn.Close()
			}
			return
		}

		buf := anet.GetBuffer(totalLen)
		switch anet.LENGTHSIZE {
		case 2:
			binary.BigEndian.PutUint16(buf[0:2], uint16(payloadLen))
		case 4:
			binary.BigEndian.PutUint32(buf[0:4], uint32(payloadLen))
		}
		copy(buf[anet.LENGTHSIZE:anet.LENGTHSIZE+4], taskID)
		copy(buf[anet.LENGTHSIZE+4:], resp)

		sc.writeMu.Lock()
		if s.config.WriteTimeout > 0 {
			_ = sc.Conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout))
		}
		_, writeErr := sc.Conn.Write(buf)
		sc.writeMu.Unlock()

		anet.PutBuffer(buf)

		if writeErr != nil {
			s.logf("write error: %v", writeErr)
			_ = sc.Conn.Close()
		}
	}()
}

func (s *Server) logf(format string, v ...any) {
	if s.config.Logger != nil {
		s.config.Logger.Printf(format, v...)
	}
}
