// Package anet_test provides tests for the anet package.
//
//nolint:all
package anet_test

import (
	"errors"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/andrei-cloud/anet"
)

// waitGroupWithTimeout attempts to wait for a WaitGroup with a timeout.
// Returns true if the WaitGroup completed before timeout, false otherwise.
// This is useful for preventing tests from hanging indefinitely.
func waitGroupWithTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// StartTestServer creates a TCP server for testing that echoes back any received messages.
// It implements the anet message framing protocol with proper error handling and graceful shutdown.
//
// Returns:
//   - The server's address as a string
//   - A cleanup function that should be called to stop the server
//   - Any error that occurred during server setup
//
// The server:
//   - Uses proper connection deadlines to prevent hanging
//   - Configures TCP keepalive for connection health
//   - Handles connection errors gracefully
//   - Tracks and waits for active connections during shutdown
//   - Implements clean shutdown with timeout
func StartTestServer() (string, func() error, error) {
	quit := make(chan struct{})
	ready := make(chan struct{})

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}

	// Track active connections for clean shutdown
	var activeConnections sync.WaitGroup
	shutdownTimeout := 5 * time.Second

	// Create a mutex to protect the listener during close
	var listenerMu sync.Mutex
	var listenerClosed bool

	go func() {
		// Signal that the server is ready to accept connections
		close(ready)

		for {
			// Use a timeout on Accept to avoid blocking forever on shutdown
			listenerMu.Lock()
			if listenerClosed {
				listenerMu.Unlock()
				return
			}
			if tcpListener, ok := l.(*net.TCPListener); ok {
				_ = tcpListener.SetDeadline(time.Now().Add(500 * time.Millisecond))
			}
			listenerMu.Unlock()

			select {
			case <-quit:
				return
			default:
				conn, err := l.Accept()
				if err != nil {
					if errors.Is(err, net.ErrClosed) {
						log.Printf("Test server listener closed.")
						return
					} else if os.IsTimeout(err) {
						continue
					}
					log.Printf("Test server accept error: %v", err)
					continue
				}

				activeConnections.Add(1)
				go func(conn net.Conn) {
					defer func() {
						_ = conn.Close()
						activeConnections.Done()
					}()

					// Configure TCP keepalive
					if tcpConn, ok := conn.(*net.TCPConn); ok {
						_ = tcpConn.SetKeepAlive(true)
						_ = tcpConn.SetKeepAlivePeriod(2 * time.Second)
					}

					for {
						// Reset read deadline for each message
						if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
							if !errors.Is(err, net.ErrClosed) {
								log.Printf("Test server set read deadline error: %v", err)
							}
							return
						}

						// Read the incoming message
						requestMsg, err := anet.Read(conn)
						if err != nil {
							if err != io.EOF && !errors.Is(err, net.ErrClosed) {
								log.Printf("Test server read error: %v", err)
							}
							return
						}

						// Reset write deadline for response
						if err := conn.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
							if !errors.Is(err, net.ErrClosed) {
								log.Printf("Test server set write deadline error: %v", err)
							}
							return
						}

						// Echo the message back
						err = anet.Write(conn, requestMsg)
						if err != nil {
							if !errors.Is(err, net.ErrClosed) {
								log.Printf("Test server write error: %v", err)
							}
							return
						}

						// Clear deadlines after response
						if err := conn.SetDeadline(time.Time{}); err != nil {
							if !errors.Is(err, net.ErrClosed) {
								log.Printf("Test server clear deadline error: %v", err)
							}
							return
						}

						// Check if server is shutting down
						select {
						case <-quit:
							return
						default:
							continue
						}
					}
				}(conn)
			}
		}
	}()

	// Wait for server to be ready
	<-ready

	return l.Addr().String(), func() error {
		// Signal all accept loops to quit
		close(quit)

		// Close the listener with proper mutex protection
		listenerMu.Lock()
		listenerClosed = true
		err := l.Close()
		listenerMu.Unlock()

		// Wait for all active connections to complete
		if !waitGroupWithTimeout(&activeConnections, shutdownTimeout) {
			log.Printf("Timed out waiting for test server connections to close")
		}

		return err
	}, nil
}
