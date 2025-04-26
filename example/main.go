// Package main provides an example of using the anet library.
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/andrei-cloud/anet"
)

// taskIDSize is the size in bytes of the task ID header used by the broker.
const taskIDSize = 4

// loggerWrapper adapts the standard log.Logger to satisfy anet.Logger interface.
type loggerWrapper struct {
	*log.Logger
}

// Logger interface implementation.
func (lw *loggerWrapper) Printf(format string, v ...any) {
	lw.Logger.Printf(format, v...)
}

func (lw *loggerWrapper) Print(v ...any) {
	lw.Logger.Print(v...)
}

func (lw *loggerWrapper) Debugf(format string, v ...any) {
	lw.Printf("[DEBUG] "+format, v...)
}

func (lw *loggerWrapper) Infof(format string, v ...any) {
	lw.Printf(format, v...)
}

func (lw *loggerWrapper) Warnf(format string, v ...any) {
	lw.Printf("[WARN] "+format, v...)
}

func (lw *loggerWrapper) Errorf(format string, v ...any) {
	lw.Printf("[ERROR] "+format, v...)
}

// startEchoServer creates a TCP server that demonstrates the message framing protocol.
// It reads messages with a length header and task ID, then echoes them back.
func startEchoServer(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to start echo server: %w", err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			log.Printf("Error closing listener: %v", err)
		}
	}()
	log.Printf("Echo server listening on %s", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Server accept error: %v", err)
			continue
		}
		go handleEchoConnection(conn)
	}
}

// handleEchoConnection processes a single client connection using the message framing protocol.
func handleEchoConnection(conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()
	log.Printf("Server accepted connection from %s", conn.RemoteAddr())

	for {
		// Read length header (uint16).
		lenHeader := make([]byte, 2)
		if _, err := io.ReadFull(conn, lenHeader); err != nil {
			if err != io.EOF {
				log.Printf("Server read length error: %v", err)
			}

			return
		}
		msgLen := binary.BigEndian.Uint16(lenHeader)

		// Read message body (including Task ID).
		msg := make([]byte, msgLen)
		if _, err := io.ReadFull(conn, msg); err != nil {
			log.Printf("Server read body error: %v", err)
			return
		}

		log.Printf(
			"Server received %d bytes: TaskID=%x Data=%s",
			len(msg),
			msg[:taskIDSize],
			string(msg[taskIDSize:]),
		)

		// Echo back (write length header + original message).
		if err := anet.Write(conn, msg); err != nil {
			log.Printf("Server write error: %v", err)
			return
		}
		log.Printf("Server echoed %d bytes", len(msg))
	}
}

// tcpConnectionFactory creates new TCP connections for the connection pool.
// It implements proper timeouts and TCP keepalive settings.
func tcpConnectionFactory(addr string) (anet.PoolItem, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", addr, err)
	}

	poolItem, ok := conn.(anet.PoolItem)
	if !ok {
		return nil, errors.New("failed to assert net.Conn as anet.PoolItem")
	}

	return poolItem, nil
}

func main() {
	// Configure logging with microsecond precision.
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Start echo server.
	serverAddr := "localhost:3000"
	go func() {
		if err := startEchoServer(serverAddr); err != nil {
			log.Fatalf("Error starting echo server: %v", err)
		}
	}()
	time.Sleep(100 * time.Millisecond) // Give server time to start.

	// Initialize connection pool.
	poolCap := uint32(5)
	pools := anet.NewPoolList(poolCap, tcpConnectionFactory, []string{serverAddr}, nil)

	// Configure and create broker.
	numWorkers := 3
	logger := &loggerWrapper{
		Logger: log.New(os.Stdout, "BROKER: ", log.LstdFlags|log.Lmicroseconds),
	}
	broker := anet.NewBroker(pools, numWorkers, logger, nil)

	// Start broker workers and ensure cleanup.
	go func() {
		if err := broker.Start(); err != nil && err != anet.ErrQuit {
			log.Fatalf("Broker failed: %v", err)
		}
	}()
	defer broker.Close()

	// Send concurrent requests.
	requests := []string{"hello", "world", "anet test", "concurrent", "request"}
	var wg sync.WaitGroup

	for _, reqStr := range requests {
		wg.Add(1)
		go func(requestPayload string) {
			defer wg.Done()

			reqData := []byte(requestPayload)
			log.Printf("Client sending: %s", requestPayload)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			respData, err := broker.SendContext(ctx, &reqData)
			if err != nil {
				log.Printf("Client error sending '%s': %v", requestPayload, err)
			} else {
				log.Printf("Client received response for '%s': %s",
					requestPayload, string(respData))
			}
		}(reqStr)
	}

	log.Println("Client launched all requests.")
	wg.Wait()
	log.Println("Client finished processing all responses.")

	time.Sleep(200 * time.Millisecond) // Allow logs to flush.
}
