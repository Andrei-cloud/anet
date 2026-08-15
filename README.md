# anet - High-Performance Asynchronous Network Broker & Connection Pool

[![Go Reference](https://pkg.go.dev/badge/github.com/andrei-cloud/anet.svg)](https://pkg.go.dev/github.com/andrei-cloud/anet)
[![Go Report Card](https://goreportcard.com/badge/github.com/andrei-cloud/anet)](https://goreportcard.com/report/github.com/andrei-cloud/anet)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

`anet` is a high-throughput, low-latency Go networking module designed for asynchronous RPC communication over TCP. It provides lock-free connection pooling, an asynchronous multiplexed message broker, zero-allocation buffer pooling, and an embeddable production-ready TCP server.

---

## Key Highlights

- ⚡ **Ultra-Low Latency & High Throughput**: Sub-10 microsecond request/response round-trips over TCP sockets.
- 🚀 **Zero-Allocation Hot Paths**: Fast-path stack framing ($\le 512$ bytes) and $O(1)$ bitwise-indexed multi-class buffer pooling (`24 ns/op`, `0 B/op`, `0 allocs/op`).
- 🔒 **100% Thread-Safe & Race-Free**: Comprehensive lifecycle synchronization across pools, brokers, and servers with zero data races under the Go race detector (`-race`).
- 🛡️ **Production Network Resilience**: Built-in TCP socket hygiene (`TCP_NODELAY`, TCP KeepAlives), automatic dead-connection pruning, backpressure handling, and graceful shutdowns.
- 🔄 **Multiplexed Message Correlation**: Automatic 4-byte Task ID prepending to match incoming asynchronous responses with pending requests over shared connections.

---

## Message Framing Protocol

`anet` uses a lightweight, binary framing protocol for all communication:

```
+-------------------+--------------------+------------------------+
| Length (2 Bytes)  | Task ID (4 Bytes)  | Payload Data (N Bytes) |
| BigEndian uint16  | BigEndian uint32   | Application Message    |
+-------------------+--------------------+------------------------+
```

1. **Length Header (2 bytes)**: A `uint16` in Big-Endian encoding indicating the total byte size of `Task ID + Payload`.
2. **Task ID (4 bytes)**: A `uint32` assigned by the broker to asynchronously correlate requests with responses.
3. **Payload (N bytes)**: Raw application data sent between client and server.

---

## Installation

```bash
go get github.com/andrei-cloud/anet@latest
```

Requirements: Go 1.22 or higher.

---

## Architecture & Components

### 1. Connection Pool (`pool.go`)
Manages reusable network resources (`PoolItem`, e.g., `net.Conn`) to target endpoints.
- **Lock-Free Fast Path**: Non-blocking channel acquisitions for warm connections.
- **Context-Aware**: `GetWithContext(ctx)` unblocks immediately upon context cancellation or timeout.
- **Health Validation**: Periodic background and on-demand validation strategies (`ValidationRead`, `ValidationPing`, or `ValidationNone`).
- **Safe Teardown**: Gracefully drains idle connections and prevents panics on concurrent closure.

### 2. Message Broker (`broker.go`)
Coordinates asynchronous request/response dispatch across worker goroutines and connection pools.
- **Asynchronous Workers**: Dispatches requests from an internal queue to pooled connections.
- **Task ID Correlation**: Automatically handles Task ID lifecycle and recycling via atomic reference counting.
- **Multi-Pool Round-Robin**: Automatically balances requests across multiple backend endpoints.
- **Backpressure**: Returns `ErrQueueFull` when the request queue is saturated rather than blocking indefinitely.

### 3. Buffer Pool (`bufferpool.go` & `utils.go`)
High-performance global byte buffer management.
- **$O(1)$ Bitwise Class Lookup**: Calculates size classes in constant time using `math/bits.Len32` across 12 power-of-two classes (32B to 64KB).
- **Pointer-Recycled Pools**: Eliminates Go `runtime.convTslice` heap boxing overhead on `sync.Pool.Put`.
- **Zero-Allocation Stack Framing**: Messages $\le 512$ bytes avoid heap allocation entirely during serialization and socket transmission.

### 4. Embeddable TCP Server (`server/`)
Production-ready TCP server built on top of the `anet` framing protocol.
- **Handler Interface**: Simple `HandleMessage(conn *ServerConn, req []byte) ([]byte, error)` API.
- **Concurrent Request Multiplexing**: Multiple workers can process pipelined requests concurrently on the same connection while response writes are serialized per socket.
- **Graceful Shutdown**: Stops accepting new connections, drains in-flight requests, and cleans up active connections within a configurable `ShutdownTimeout`.

---

## Quick Start Example

### Complete Server & Client

```go
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/andrei-cloud/anet"
	"github.com/andrei-cloud/anet/server"
)

func main() {
	addr := "127.0.0.1:9000"

	// 1. Start Echo Server
	handler := server.HandlerFunc(func(_ *server.ServerConn, req []byte) ([]byte, error) {
		// Echo back the request
		return req, nil
	})

	srv, err := server.NewServer(addr, handler, &server.ServerConfig{
		ShutdownTimeout: 2 * time.Second,
	})
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	if err := srv.Start(); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
	defer srv.Stop()

	// 2. Configure Client Connection Pool
	factory := func(targetAddr string) (anet.PoolItem, error) {
		conn, err := net.DialTimeout("tcp", targetAddr, 3*time.Second)
		if err != nil {
			return nil, err
		}
		return conn, nil
	}

	pool := anet.NewPool(10, factory, addr, &anet.PoolConfig{
		DialTimeout: 3 * time.Second,
		IdleTimeout: 60 * time.Second,
	})
	defer pool.Close()

	// 3. Create and Start Broker
	brokerCfg := &anet.BrokerConfig{
		WriteTimeout:   3 * time.Second,
		ReadTimeout:    3 * time.Second,
		QueueSize:      1000,
		OptimizeMemory: true,
	}

	broker := anet.NewBroker([]anet.Pool{pool}, 4, nil, brokerCfg)
	go func() {
		if err := broker.Start(); err != nil && err != anet.ErrQuit {
			log.Printf("broker error: %v", err)
		}
	}()
	defer broker.Close()

	// 4. Send Requests Synchronously or with Context
	req := []byte("Hello, anet!")
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	resp, err := broker.SendContext(ctx, &req)
	if err != nil {
		log.Fatalf("send failed: %v", err)
	}

	fmt.Printf("Received response: %s\n", string(resp))
}
```

---

## Configuration Reference

### Pool Configuration (`PoolConfig`)

```go
type PoolConfig struct {
	DialTimeout           time.Duration      // Timeout for creating new connections (default: 5s)
	IdleTimeout           time.Duration      // Max idle duration before closing (default: 60s)
	ValidationInterval    time.Duration      // Interval for periodic idle connection checks (default: 30s)
	KeepAliveInterval     time.Duration      // TCP keepalive probe interval (default: 30s)
	ValidationStrategy    ValidationStrategy // Validation strategy: ValidationRead, ValidationPing, ValidationNone (default: ValidationRead)
	ValidationTimeout     time.Duration      // Timeout for connection validation checks (default: 1s)
	MaxValidationAttempts int                // Max validation attempts before discarding a dead socket (default: 3)
}
```

### Broker Configuration (`BrokerConfig`)

```go
type BrokerConfig struct {
	WriteTimeout   time.Duration // Timeout for writing requests to network connections (default: 5s)
	ReadTimeout    time.Duration // Timeout for reading responses from network connections (default: 5s)
	QueueSize      int           // Inbound request queue buffer capacity (default: 1000)
	OptimizeMemory bool          // Enable zero-allocation task pooling and buffer recycling (default: true)
}
```

### Server Configuration (`ServerConfig`)

```go
type ServerConfig struct {
	ReadTimeout           time.Duration // Maximum duration for reading request frames (default: 5s)
	WriteTimeout          time.Duration // Maximum duration for writing response frames (default: 5s)
	IdleTimeout           time.Duration // Maximum idle time before closing idle client connections (default: 60s)
	KeepAliveInterval     time.Duration // TCP Keepalive interval on accepted connections (default: 30s)
	MaxConns              int           // Maximum concurrent client connections (default: 10000; 0 = unlimited)
	MaxConcurrentHandlers int           // Maximum concurrent handler executions (default: 0 = unlimited)
	ShutdownTimeout       time.Duration // Graceful shutdown period for active connections to drain (default: 5s)
}
```

---

## Multi-Server Load Balancing

When passing multiple connection pools to `NewBroker`, requests are automatically distributed evenly across pools using lock-free round-robin selection:

```go
pools := anet.NewPoolList(
	10, // Capacity per pool
	factory,
	[]string{
		"backend-1:9000",
		"backend-2:9000",
		"backend-3:9000",
	},
	&anet.PoolConfig{
		ValidationStrategy: anet.ValidationRead,
	},
)

broker := anet.NewBroker(pools, 8, nil, nil)
go broker.Start()
```

---

## Performance & Benchmarks

Run benchmarks locally:
```bash
go test -run=^$ -bench=. -benchmem ./...
```

*Environment: Apple M1 Pro (ARM64), macOS, Go 1.26*

| Benchmark | Latency | Memory Overhead | Allocations |
| :--- | :--- | :--- | :--- |
| **`BenchmarkBufferPool_GetPut`** | **23.9 ns/op** | **0 B/op** | **0 allocs/op** |
| **`BenchmarkNewPool_GetPut`** | **37.2 ns/op** | **0 B/op** | **0 allocs/op** |
| **`BenchmarkPool/Workers_1`** | **36.5 ns/op** | **0 B/op** | **0 allocs/op** |
| **`BenchmarkBroker_Pipe_Parallel`** | **4.90 µs/op** | 300 B/op | 7 allocs/op |
| **`BenchmarkBrokerSend/Workers_100`** | **9.08 µs/op** | 706 B/op | 7 allocs/op |
| **`BenchmarkServer_Echo_Parallel`** | **13.99 µs/op** | 1,279 B/op | 6 allocs/op |

---

## Error Handling

`anet` exposes standard sentinel errors for deterministic error checking:

| Error Variable | Description |
| :--- | :--- |
| `ErrTimeout` | Response was not received within the configured `ReadTimeout` or context deadline. |
| `ErrClosingBroker` | Request was rejected because the broker is currently shutting down. |
| `ErrQueueFull` | Request was rejected due to backpressure (the broker request queue is full). |
| `ErrNoPoolsAvailable` | No connection pools are configured or active for request routing. |
| `ErrClosing` | The connection pool is closed or shutting down. |
| `ErrInvalidMsgLength` | Frame length header was invalid or 0. |
| `ErrMaxLenExceeded` | Message payload exceeded the 64KB protocol limit. |

---

## License

MIT License. See [LICENSE](LICENSE) for full details.
