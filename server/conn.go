package server

import (
	"net"

	"github.com/andrei-cloud/anet"
)

// ServerConn represents a client connection on the server side.
type ServerConn struct {
	Conn    net.Conn        // underlying network connection.
	server  *Server         // reference to parent server.
	writeCh chan *respFrame // responses queued for the single writer goroutine.
	done    chan struct{}   // closed once when the writer goroutine exits.
	// hdr is the read-header scratch. As a field of the heap-allocated
	// ServerConn it never escapes through io.ReadFull, replacing the old
	// per-message escaping stack array (~8 B/op measured).
	hdr [anet.LENGTHSIZE]byte
}

// init configures TCP keepalive and NoDelay settings on the connection.
func (sc *ServerConn) init() {
	if tcpConn, ok := sc.Conn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
		if sc.server.config.KeepAliveInterval > 0 {
			if err := tcpConn.SetKeepAlive(true); err != nil {
				sc.server.logf("set keepalive error: %v", err)
			}

			if err := tcpConn.SetKeepAlivePeriod(sc.server.config.KeepAliveInterval); err != nil {
				sc.server.logf("set keepalive period error: %v", err)
			}
		}
	}
}
