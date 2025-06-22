// Package anet_test provides tests for the anet package.

package anet_test

import (
	"errors"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/andrei-cloud/anet"
)

// waitGroupWithTimeout waits for the wait group or times out.
func waitGroupWithTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return true
	case <-time.After(timeout):
		return false
	}
}

// StartTestServer creates a TCP server for testing that echoes back any received messages.
// It implements the anet message framing protocol with proper error handling and graceful shutdown.
// Enhanced for robustness under heavy benchmark load.
func StartTestServer() (string, func() error, error) {
	quit := make(chan struct{})
	ready := make(chan struct{})

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}

	// Configurable parameters via environment variables
	maxConns := 200 // default
	if v := os.Getenv("ANET_TESTSERVER_MAX_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxConns = n
		}
	}
	readDeadline := 30 * time.Second // increased for heavy load
	if v := os.Getenv("ANET_TESTSERVER_READ_DEADLINE"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			readDeadline = d
		}
	}
	writeDeadline := 30 * time.Second // increased for heavy load
	if v := os.Getenv("ANET_TESTSERVER_WRITE_DEADLINE"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			writeDeadline = d
		}
	}
	logVerbose := os.Getenv("ANET_TESTSERVER_VERBOSE") == "1"
	shutdownTimeout := 15 * time.Second // increased for heavy load

	var listenerMu sync.Mutex
	var listenerClosed bool
	listenerCond := sync.NewCond(&listenerMu)

	if tcpListener, ok := l.(*net.TCPListener); ok {
		_ = tcpListener.SetDeadline(time.Now().Add(30 * time.Second)) // longer deadline
	}

	connSem := make(chan struct{}, maxConns)
	var activeConnections sync.WaitGroup

	go func() {
		close(ready)
		acceptBackoff := 5 * time.Millisecond
		maxBackoff := 200 * time.Millisecond
		for {
			listenerMu.Lock()
			closed := listenerClosed
			if closed {
				listenerMu.Unlock()
				return
			}
			listenerMu.Unlock()

			if tcpListener, ok := l.(*net.TCPListener); ok {
				_ = tcpListener.SetDeadline(time.Now().Add(30 * time.Second))
			}

			select {
			case <-quit:
				return
			default:
				conn, err := l.Accept()
				if err != nil {
					if errors.Is(err, net.ErrClosed) {
						if logVerbose {
							log.Printf("Test server listener closed.")
						}
						listenerCond.Broadcast()
						return
					} else if os.IsTimeout(err) {
						acceptBackoff *= 2
						if acceptBackoff > maxBackoff {
							acceptBackoff = maxBackoff
						}
						listenerMu.Lock()
						listenerCond.Wait()
						listenerMu.Unlock()
						continue
					}
					if ne, ok := err.(net.Error); ok && ne.Temporary() {
						if logVerbose {
							log.Printf("Test server temporary accept error: %v", err)
						}
						acceptBackoff *= 2
						if acceptBackoff > maxBackoff {
							acceptBackoff = maxBackoff
						}
						listenerMu.Lock()
						listenerCond.Wait()
						listenerMu.Unlock()
						continue
					}
					if logVerbose {
						log.Printf("Test server accept error: %v", err)
					}
					listenerMu.Lock()
					listenerCond.Wait()
					listenerMu.Unlock()
					continue
				}
				acceptBackoff = 5 * time.Millisecond // reset on success

				// Acquire connection semaphore
				select {
				case connSem <- struct{}{}:
					// proceed
				case <-quit:
					_ = conn.Close()
					return
				}

				activeConnections.Add(1)
				go func(conn net.Conn) {
					var shouldBroadcast bool
					defer func() {
						_ = conn.Close()
						<-connSem
						activeConnections.Done()
						// Only broadcast if we were at maxConns
						if len(connSem) == maxConns-1 {
							shouldBroadcast = true
						}
						if shouldBroadcast {
							listenerCond.Broadcast()
						}
					}()

					if tcpConn, ok := conn.(*net.TCPConn); ok {
						_ = tcpConn.SetKeepAlive(true)
						_ = tcpConn.SetKeepAlivePeriod(1 * time.Second)
					}

					for {
						if err := conn.SetReadDeadline(time.Now().Add(readDeadline)); err != nil {
							if logVerbose && !errors.Is(err, net.ErrClosed) {
								log.Printf("Test server set read deadline error: %v", err)
							}
							return
						}
						requestMsg, err := anet.Read(conn)
						if err != nil {
							if err != io.EOF && !errors.Is(err, net.ErrClosed) {
								if ne, ok := err.(net.Error); ok && ne.Temporary() {
									if logVerbose {
										log.Printf("Test server temporary read error: %v", err)
									}
									continue
								}
								if logVerbose {
									log.Printf("Test server read error: %v", err)
								}
							}
							return
						}
						if err := conn.SetWriteDeadline(time.Now().Add(writeDeadline)); err != nil {
							if logVerbose && !errors.Is(err, net.ErrClosed) {
								log.Printf("Test server set write deadline error: %v", err)
							}
							return
						}
						err = anet.Write(conn, requestMsg)
						if err != nil {
							if !errors.Is(err, net.ErrClosed) {
								if ne, ok := err.(net.Error); ok && ne.Temporary() {
									if logVerbose {
										log.Printf("Test server temporary write error: %v", err)
									}
									continue
								}
								if logVerbose {
									log.Printf("Test server write error: %v", err)
								}
							}
							return
						}
						_ = conn.SetDeadline(time.Time{})
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

	<-ready

	return l.Addr().String(), func() error {
		close(quit)
		listenerMu.Lock()
		listenerClosed = true
		err := l.Close()
		listenerMu.Unlock()
		// Wait for all connections to close (waitgroup version)
		if !waitGroupWithTimeout(&activeConnections, shutdownTimeout) {
			log.Printf("Timed out waiting for test server connections to close")
		}
		return err
	}, nil
}
