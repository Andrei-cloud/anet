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
func StartTestServer() (string, func() error, error) {
	quit := make(chan struct{})
	ready := make(chan struct{})

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}

	maxConns := 200
	if v := os.Getenv("ANET_TESTSERVER_MAX_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxConns = n
		}
	}
	readDeadline := 5 * time.Second
	if v := os.Getenv("ANET_TESTSERVER_READ_DEADLINE"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			readDeadline = d
		}
	}
	writeDeadline := 5 * time.Second
	if v := os.Getenv("ANET_TESTSERVER_WRITE_DEADLINE"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			writeDeadline = d
		}
	}
	logVerbose := os.Getenv("ANET_TESTSERVER_VERBOSE") == "1"
	shutdownTimeout := 2 * time.Second

	var listenerMu sync.Mutex
	var listenerClosed bool
	listenerCond := sync.NewCond(&listenerMu)

	connSem := make(chan struct{}, maxConns)
	var activeConnections sync.WaitGroup
	var liveConns sync.Map

	handleConn := func(conn net.Conn) {
		liveConns.Store(conn, conn)
		var shouldBroadcast bool
		defer func() {
			liveConns.Delete(conn)
			_ = conn.Close()
			<-connSem
			activeConnections.Done()
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
			_ = tcpConn.SetNoDelay(true)
		}

		for {
			if err := conn.SetReadDeadline(time.Now().Add(readDeadline)); err != nil {
				if logVerbose && !errors.Is(err, net.ErrClosed) {
					log.Printf("Test server set read deadline error: %v", err)
				}
				return
			}
			requestMsg, err := anet.ReadPooled(conn)
			if err != nil {
				if err != io.EOF && !errors.Is(err, net.ErrClosed) {
					if ne, ok := err.(net.Error); ok && ne.Timeout() {
						if logVerbose {
							log.Printf("Test server temporary read timeout: %v", err)
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
				anet.PutBuffer(requestMsg)
				return
			}
			err = anet.Write(conn, requestMsg)
			anet.PutBuffer(requestMsg)
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					if ne, ok := err.(net.Error); ok && ne.Timeout() {
						if logVerbose {
							log.Printf("Test server temporary write timeout: %v", err)
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
	}

	go func() {
		close(ready)
		for {
			listenerMu.Lock()
			closed := listenerClosed
			if closed {
				listenerMu.Unlock()
				return
			}
			listenerMu.Unlock()

			conn, err := l.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					listenerCond.Broadcast()
					return
				}
				select {
				case <-quit:
					return
				default:
					continue
				}
			}

			select {
			case connSem <- struct{}{}:
			case <-quit:
				_ = conn.Close()
				return
			}

			activeConnections.Add(1)
			go handleConn(conn)
		}
	}()

	<-ready

	return l.Addr().String(), func() error {
		close(quit)
		listenerMu.Lock()
		listenerClosed = true
		err := l.Close()
		listenerMu.Unlock()

		liveConns.Range(func(key, value any) bool {
			if c, ok := value.(net.Conn); ok {
				_ = c.Close()
			}
			return true
		})

		if !waitGroupWithTimeout(&activeConnections, shutdownTimeout) {
			log.Printf("Timed out waiting for test server connections to close")
		}

		return err
	}, nil
}
