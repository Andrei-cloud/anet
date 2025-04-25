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
	"time"

	"github.com/andrei-cloud/anet"
)

const taskIDSize = 4 // Define taskIDSize locally

// Simple TCP Echo Server (for testing purposes).
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

func handleEchoConnection(conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}()
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

		log.Printf(
			"Server received %d bytes: TaskID=%x Data=%s",
			len(msg),
			msg[:taskIDSize],         // Use local taskIDSize
			string(msg[taskIDSize:]), // Use local taskIDSize
		)

		// Echo back (write length header + original message)
		if err := anet.Write(conn, msg); err != nil {
			log.Printf("Server write error: %v", err)
			return
		}
		log.Printf("Server echoed %d bytes", len(msg))
	}
}

// Client Factory Function.
func tcpConnectionFactory(addr string) (anet.PoolItem, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second) // Add timeout
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", addr, err)
	}

	poolItem, ok := conn.(anet.PoolItem)
	if !ok {
		return nil, errors.New("failed to assert net.Conn as anet.PoolItem")
	}

	return poolItem, nil // net.Conn implements PoolItem (Close())
}

func main() {
	serverAddr := "localhost:3000"
	// Run the server in a goroutine so it doesn't block
	go func() {
		if err := startEchoServer(serverAddr); err != nil {
			log.Fatalf("Error starting echo server: %v", err)
		}
	}()
	time.Sleep(100 * time.Millisecond) // Give server time to start

	// --- Pool Setup ---
	poolCap := uint32(5)
	pools := anet.NewPoolList(poolCap, tcpConnectionFactory, []string{serverAddr})

	// --- Broker Setup ---
	numWorkers := 3
	// Use standard log package as the logger
	logger := log.New(os.Stdout, "BROKER: ", log.LstdFlags|log.Lmicroseconds)
	broker := anet.NewBroker(pools, numWorkers, logger) // Pass pools directly

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
