package server

import (
	"net"
	"sync"
)

// ServerConn represents a client connection on the server side.
type ServerConn struct {
	Conn    net.Conn   // underlying network connection.
	server  *Server    // reference to parent server.
	writeMu sync.Mutex // serializes concurrent writes.
}

// init configures TCP keepalive settings on the connection.
func (sc *ServerConn) init() {
	if tcpConn, ok := sc.Conn.(*net.TCPConn); ok {
		if sc.server.config.KeepAliveInterval > 0 {
			tcpConn.SetKeepAlive(true)

			tcpConn.SetKeepAlivePeriod(sc.server.config.KeepAliveInterval)
		}
	}
}
