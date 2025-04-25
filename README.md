# anet - Asynchronous Network Broker & Pool

`anet` is a Go library providing components for efficient, asynchronous communication with network services, primarily featuring a connection pool and a message broker.

## Features

*   **Connection Pooling (`pool.go`)**: Manages a pool of reusable network connections (`PoolItem`) to specified addresses.
    *   Uses a factory function (`Factory`) to create new connections.
    *   Limits the number of concurrent connections (`Cap`).
    *   Provides methods to `Get`, `Put` (return), and `Release` (close) connections.
    *   Supports context-aware connection retrieval (`GetWithContext`).
*   **Asynchronous Broker (`broker.go`)**: Coordinates sending requests and receiving responses over pooled connections.
    *   Uses multiple worker goroutines for concurrent processing.
    *   Accepts requests via `Send` (blocking) or `SendContext` (supports cancellation/timeouts).
    *   Automatically prepends a unique Task ID header to outgoing messages.
    *   Matches incoming responses to pending requests using the Task ID header.
    *   Handles connection acquisition, writing requests, reading responses, and error management.
    *   Includes basic logging capabilities (accepts a `Logger` interface).
*   **Message Framing (`utils.go`)**: Implements simple message framing:
    *   Prepends a **`uint16`** (2 bytes, BigEndian) header indicating the length of the following message body.
    *   The broker adds a Task ID (default 4 bytes) *before* the user's request data but *after* the length header. The server is expected to return this Task ID in its response for matching.

## Basic Usage Example

```go
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/andrei-cloud/anet" // Assuming this is the correct import path
)

// Simple TCP Echo Server (for testing purposes)
func startEchoServer(addr string) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to start echo server: %v", err)
	}
	defer listener.Close()
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

func handleEchoConnection(conn net.Conn) {
	defer conn.Close()
	log.Printf("Server accepted connection from %s", conn.RemoteAddr())
	for {
		// Read length header (uint16)
		lenHeader := make([]byte, 2)
		if _, err := io.ReadFull(conn, lenHeader); err != nil {
			if err != io.EOF {
				log.Printf("Server read length error: %v", err)
			}
			return
		}
		msgLen := binary.BigEndian.Uint16(lenHeader)

		// Read message body (including Task ID)
		msg := make([]byte, msgLen)
		if _, err := io.ReadFull(conn, msg); err != nil {
			log.Printf("Server read body error: %v", err)
			return
		}

		log.Printf("Server received %d bytes: TaskID=%x Data=%s", len(msg), msg[:anet.taskIDSize], string(msg[anet.taskIDSize:]))

		// Echo back (write length header + original message)
		if err := anet.Write(conn, msg); err != nil {
			log.Printf("Server write error: %v", err)
			return
		}
		log.Printf("Server echoed %d bytes", len(msg))
	}
}


// Client Factory Function
func tcpConnectionFactory(addr string) (anet.PoolItem, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second) // Add timeout
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", addr, err)
	}
	return conn.(anet.PoolItem), nil // net.Conn implements PoolItem (Close())
}

func main() {
	serverAddr := "localhost:8080"
	go startEchoServer(serverAddr)
	time.Sleep(100 * time.Millisecond) // Give server time to start

	// --- Pool Setup ---
	poolCap := uint32(5)
	pools := anet.NewPoolList(poolCap, tcpConnectionFactory, []string{serverAddr})

	// --- Broker Setup ---
	numWorkers := 3
	// Use standard log package as the logger
	logger := log.New(os.Stdout, "BROKER: ", log.LstdFlags|log.Lmicroseconds)
	broker := anet.NewBroker(*pools, numWorkers, logger)

	// Start broker workers in background
	go func() {
		if err := broker.Start(); err != nil && err != anet.ErrQuit {
			log.Fatalf("Broker failed: %v", err)
		}
	}()
	defer broker.Close() // Ensure broker is closed on exit

	// --- Send Requests ---
	requests := []string{"hello", "world", "anet test"}
	for _, reqStr := range requests {
		reqData := []byte(reqStr)
		log.Printf("Client sending: %s", reqStr)

		// Use SendContext for timeout
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		respData, err := broker.SendContext(ctx, &reqData)
		cancel() // Release context resources

		if err != nil {
			log.Printf("Client error sending '%s': %v", reqStr, err)
		} else {
			log.Printf("Client received response for '%s': %s", reqStr, string(respData))
		}
		time.Sleep(50 * time.Millisecond) // Small delay between requests
	}

	log.Println("Client finished sending requests.")
	// Give time for logs to flush, etc.
	time.Sleep(200 * time.Millisecond)
}

```

## Notes

*   The server you connect to *must* read the `uint16` length header and then read the specified number of bytes.
*   The server *must* include the received Task ID header (first 4 bytes after the length) at the beginning of its response message (after the response length header) for the broker to match the response correctly.
*   Error handling, particularly around connection failures and timeouts, is crucial for robust applications.
*   The provided logger is basic; consider using a more structured logging library for production use.
