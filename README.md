# anet - High-Performance Asynchronous Network Broker & Connection Pool

[![Go Reference](https://pkg.go.dev/badge/github.com/andrei-cloud/anet.svg)](https://pkg.go.dev/github.com/andrei-cloud/anet)
[![Go Report Card](https://goreportcard.com/badge/github.com/andrei-cloud/anet)](https://goreportcard.com/report/github.com/andrei-cloud/anet)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

`anet` is a high-throughput, low-latency Go networking module for asynchronous RPC over TCP: connection pooling with health management, a request/response broker with task-ID correlation, an opt-in multiplexed transport that puts thousands of outstanding requests on a handful of connections, and an embeddable TCP server.

---

## Key Highlights

- ⚡ **Low Latency & High Throughput**: Single-digit-microsecond request/response round-trips over TCP; Multiplex mode measured **1.6x faster** than the queue-worker transport using 50x fewer connections.
- 🚀 **Allocation-Minded Hot Paths**: Framing `Write` is `0 B/op, 0 allocs/op`; server responses are `3 allocs/op` (was 6); buffer pool round-trip `~12 ns/op`.
- 🔒 **Race-Tested Lifecycle**: Pool, broker, and server shutdowns are admission-safe: every connection is closed exactly once, `Close`/`Stop` are bounded by their timeouts, and cancel-while-queued cannot recycle live tasks.
- 🛡️ **Production Network Hygiene**: `TCP_NODELAY` + keepalive via `anet.NewTCPFactory`, idle-connection eviction (actually enforced now), background validation, load shedding past outstanding-request bounds, and handler panic isolation.
- 🔄 **Asynchronous by Design**: `SendAsync`/`SendAsyncContext` never park a goroutine per request; Multiplex mode correlates responses by task ID so connections stay pooled while still in flight.

---

## Message Framing Protocol

```
+-------------------+--------------------+------------------------+
| Length (2 Bytes)  | Task ID (4 Bytes)  | Payload Data (N Bytes) |
| BigEndian uint16  | BigEndian uint32   | Application Message    |
+-------------------+--------------------+------------------------+
```

1. **Length Header (2 bytes)**: `uint16`, Big-Endian: the size of `Task ID + Payload`. Maximum payload is therefore 65531 bytes (`ErrMaxLenExceeded` beyond that).
2. **Task ID (4 bytes)**: `uint32` assigned per broker, used to correlate responses with requests (in-order *and* out-of-order).
3. **Payload (N bytes)**: Raw application data.

---

## Installation

```bash
go get github.com/andrei-cloud/anet@latest
```

Requirements: Go 1.23 or higher.

---

## Architecture & Components

### 1. Connection Pool (`pool.go`)

Reusable network resources (`PoolItem`, e.g. `net.Conn`) per endpoint.

- **Lock-Free Hot Path**: channel-based fast path with a lock-free `Put`/`Close` handshake (no mutex on Get/Put).
- **Context-Aware**: `GetWithContext(ctx)` unblocks immediately on context cancellation or pool close; a broker context wakes workers parked on an exhausted pool.
- **Idle Eviction**: `IdleTimeout` is stamped on Put and enforced by the background sweeper (dead idle connections actually get closed now); a connection that expired between sweeps fails its next write and is replaced.
- **Health Validation**: `ValidationRead` / `ValidationPing` / `ValidationNone`, plus a `Validate()` self-validation hook for transport-aware items (the multiplexed pipeline uses it, so raw probes never race its reader goroutine).
- **Safe Teardown**: every item that ever entered the queue is closed exactly once even with concurrent Puts during `Close`.

### 2. Message Broker (`broker.go`)

Request dispatch and response correlation.

- **Submit-Time Frame Staging**: the frame is built once into task-owned storage in the caller's goroutine; workers issue a single contiguous write and never dereference caller memory (no use-after-cancel races, no length truncation at the 64KB boundary).
- **Asynchronous API**: `SendAsync` returns a channel that receives exactly one `Response` — payload or error — without the caller blocking.
- **Queue Mode**: bounded request queue; saturation sheds with `ErrQueueFull` (load shedding) rather than blocking unboundedly.
- **Bounded Shutdown**: `Close` fails queued tasks, wakes parked waiters, and never blocks longer than `CloseTimeout`.
- **Multi-Pool Round-Robin** across backend endpoints.

### 3. Multiplexed Transport (`pipeline.go`)

`BrokerConfig.Multiplex = true` replaces the worker queue entirely:

- Each pooled connection gets a background **writer goroutine** and **reader goroutine**; responses are matched by task ID, so responses may arrive out of order.
- A connection is checked back into the pool *immediately after submit*, so N connections carry up to `N × MaxInflightPerConn` outstanding requests.
- Submits past the outstanding bound shed with `ErrQueueFull`; stream errors fail every outstanding request exactly once.
- Recommended pairing: dial with `anet.NewTCPFactory` so TCP keepalive surfaces silent peers (multiplex reads are deliberately undated while a connection is idle).

### 4. Buffer Pool (`bufferpool.go`)

- **O(1) Bitwise Class Lookup** over 12 power-of-two classes (32 B – 64 KB).
- **Pointer-Recycled Pools** (`*[]byte` recycling; no interface boxing on `Put`).

### 5. Embeddable TCP Server (`server/`)

- **Handler Interface**: `HandleMessage(conn *ServerConn, req []byte) ([]byte, error)`; a panicking handler is logged, not fatal, and the connection keeps serving.
- **Concurrent Requests, Batched Writes**: handlers run concurrently while a single per-connection writer goroutine batches queued responses into one `write` syscall per burst (no per-response goroutine or write mutex).
- **Admission-Safe Shutdown**: connections accepted across `Stop` are closed on the spot; `Stop` re-forces closure after its grace period and never leaks a conn/goroutine pair.
- **Optional `SO_REUSEPORT`** (`ServerConfig.ReusePort`, unix only) for kernel load-balanced scale-out.

---

## Quick Start Example

```go
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/andrei-cloud/anet"
	"github.com/andrei-cloud/anet/server"
)

func main() {
	addr := "127.0.0.1:9000"

	// 1. Start Echo Server
	handler := server.HandlerFunc(func(_ *server.ServerConn, req []byte) ([]byte, error) {
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

	// 2. Connection pool: NewTCPFactory dials with DialTimeout and arms TCP
	// keepalive + TCP_NODELAY from the same config.
	poolCfg := anet.DefaultPoolConfig()
	poolCfg.DialTimeout = 3 * time.Second
	pool := anet.NewPool(10, anet.NewTCPFactory(poolCfg), addr, poolCfg)
	defer pool.Close()

	// 3. Broker with 4 queue workers.
	broker := anet.NewBroker([]anet.Pool{pool}, 4, nil, nil)
	go func() {
		if err := broker.Start(); err != nil && err != anet.ErrQuit {
			log.Printf("broker error: %v", err)
		}
	}()
	defer broker.Close()

	// 4. Synchronous request.
	req := []byte("Hello, anet!")
	resp, err := broker.Send(&req)
	if err != nil {
		log.Fatalf("send failed: %v", err)
	}
	fmt.Printf("Received response: %s\n", string(resp))
}
```

## Asynchronous Fan-Out

Submit thousands of requests without parking a goroutine per response; the caller never blocks until it chooses to:

```go
var inflight []<-chan anet.Response
for _, req := range requests {
	req := req
	ch, err := broker.SendAsync(&req) // returns immediately
	if err != nil {
		return err // ErrQueueFull: shed by backpressure
	}
	inflight = append(inflight, ch)
}

for _, ch := range inflight {
	r := <-ch // exactly one Response per submitted request
	if r.Err != nil {
		return r.Err
	}
	use(r.Payload)
}
```

## Multiplex Mode (few connections, thousands outstanding)

```go
poolCfg := anet.DefaultPoolConfig()
pools := anet.NewPoolList(2, anet.NewTCPFactory(poolCfg), addrs, poolCfg)

cfg := anet.DefaultBrokerConfig()
cfg.Multiplex = true
cfg.MaxInflightPerConn = 512

broker := anet.NewBroker(pools, 0 /* Multiplex mode needs no workers */, nil, cfg)
// No Start() needed. Send / SendAsync / SendAsyncContext work as usual:
// up to 2 x 512 requests are in flight per pair of connections at once.
```

Measured (`BenchmarkTransport_RoundTrip`, 100 concurrent goroutines, Apple M5 Max):

| Transport | ns/op | Conns + workers |
| :-- | --: | :-- |
| Sync queue | 9,263 | 100 conns, 100 workers |
| Async submit + await | 9,546 | 100 conns, 100 workers |
| **Multiplex** | **5,891** | **2 conns, 0 workers** |

---

## Configuration Reference

### Pool Configuration (`PoolConfig`)

```go
type PoolConfig struct {
	DialTimeout           time.Duration      // Dial timeout covering DNS+dial (default 5s; applied by NewTCPFactory)
	IdleTimeout           time.Duration      // Max idle time before eviction (default 60s; negative disables)
	ValidationInterval    time.Duration      // Background sweep cadence (default 30s)
	KeepAliveInterval     time.Duration      // TCP keepalive period for NewTCPFactory dials (default 30s)
	ValidationStrategy    ValidationStrategy // ValidationRead / ValidationPing / ValidationNone (default ValidationRead)
	ValidationTimeout     time.Duration      // Per validation probe (default 1s)
	MaxValidationAttempts int                // Attempts before discarding (default 3)
	DisableNoDelay        bool               // Keep Nagle enabled on NewTCPFactory dials (default false: TCP_NODELAY)
	Logger                Logger             // Pool diagnostics (default &NoopLogger{})
}
```

### Broker Configuration (`BrokerConfig`)

```go
type BrokerConfig struct {
	WriteTimeout       time.Duration // Write deadline per request frame (default 5s; negative disables)
	ReadTimeout        time.Duration // Read deadline per response frame (default 5s; negative disables)
	QueueSize          int           // Request queue capacity; ignored in Multiplex mode (default 1000)
	OptimizeMemory     bool          // Reuse Task structs and frame buffers via sync.Pool (default true)
	Multiplex          bool          // Per-connection pipelined transport; workers unused (default false)
	MaxInflightPerConn int           // Outstanding-request bound per connection in Multiplex mode; sheds past it (default 256)
	CloseTimeout       time.Duration // Upper bound for broker Close to wait for workers (default 10s; negative waits forever)
}
```

### Server Configuration (`ServerConfig`)

```go
type ServerConfig struct {
	ReadTimeout           time.Duration // Body-read deadline for a started frame; opt-in, cleared between frames (default 0 = off)
	WriteTimeout          time.Duration // Deadline per writer flush (default 5s)
	IdleTimeout           time.Duration // Quiet-connection deadline: bounds the wait for the next frame header (default 0 = quiet clients stay)
	MaxConns              int           // Maximum concurrent connections (default 0 = unlimited)
	MaxConcurrentHandlers int           // Maximum concurrent handler executions (default 10000)
	ShutdownTimeout       time.Duration // Grace period before Stop force-closes (default 5s)
	KeepAliveInterval     time.Duration // TCP keepalive on accepted connections (default 30s)
	Logger                anet.Logger   // Server events (default nil = silent)
	ReusePort             bool          // SO_REUSEPORT on the listener (unix only, default false)
	WriteQueueDepth       int           // Queued responses per connection before handlers block (default 64)
}
```

---

## Multi-Server Load Balancing

Multiple pools passed to `NewBroker` are balanced with an atomic round-robin:

```go
pools := anet.NewPoolList(
	10, // Capacity per pool
	anet.NewTCPFactory(nil),
	[]string{"backend-1:9000", "backend-2:9000", "backend-3:9000"},
	nil,
)

broker := anet.NewBroker(pools, 8, nil, nil)
go broker.Start()
```

---

## Shutdown Guarantees

- `broker.Close`: fails queued and parked requests, wakes pool waiters through the broker context, bounded by `CloseTimeout`; `Start`/`Close` in any order never races.
- `pool.Close`: every connection that ever went idle is closed exactly once, even with concurrent `Put`s mid-close.
- `server.Stop`: stops admitting, closes the listener and every accepted connection (including one accepted mid-`Stop`), waits `ShutdownTimeout`, then force-closes and logs wedged handlers.

---

## Performance & Benchmarks

Run locally:

```bash
go test -run=^$ -bench=. -benchmem ./...
```

*Environment: Apple M5 Max (ARM64), macOS, Go 1.27.1.* Before = the pre-async-build, After = this build.

| Benchmark | Before | After | Memory Before → After | Allocs Before → After |
| :-- | --: | --: | :-- | :-- |
| `Write_Small` | 717 ns/op | **510 ns/op** | 576 → **0 B/op** | 1 → **0** |
| `BufferPool_GetPut` | 11.5 ns/op | 12.0 ns/op | 0 B/op | 0 |
| `Pool_GetPut` (idle reuse) | 17.6 ns/op | 47.2 ns/op\* | 0 B/op | 0 |
| `Broker_Pipe_Parallel` | 2.94 µs/op | 3.41 µs/op\*\* | 301 → 435 B/op\*\* | 7 → 7 |
| `BrokerSend/Workers_100` | 6.99 µs/op | **6.60 µs/op** | 703 → **259 B/op** | 7 → **6** |
| `Server_Echo_Parallel` | 9.56 µs/op | 9.21 µs/op | 1,283 → **75 B/op** | 6 → **3** |
| `Transport_RoundTrip/Multiplex_2conn` | — (new) | **5.89 µs/op** | 178 B/op | 4 |

\* The pool now stamps idle time on Put and enforces `IdleTimeout`; the eviction plumbing costs one clock read per Put. On the µs-scale RPC path this is below measurement noise.
\*\* The broker's result delivery is a fresh `chan Response` per request (~96 B): a pooled channel could swallow a live waiter's response (drain-vs-parked-select race, reproduced); correctness bought one small allocation. On the real TCP transport (`BrokerSend`) bytes per request still fell ~63% and latency improved; the in-memory `net.Pipe` bench, which is dominated by allocation rather than I/O, shows the channel cost.

---

## Error Handling

| Error Variable | Description |
| :--- | :--- |
| `ErrClosingBroker` | Request rejected: broker is shutting down. |
| `ErrQueueFull` | Request shed by backpressure (queue saturated or Multiplex outstanding bound hit). |
| `ErrNoPoolsAvailable` | No connection pools configured for routing. |
| `ErrQuit` | Returned by `Start` when the broker stopped due to `Close`. |
| `ErrClosing` | The connection pool is closed or shutting down. |
| `ErrInvalidMsgLength` | Frame length header was invalid. |
| `ErrMaxLenExceeded` | Payload exceeded the 65531-byte protocol limit. |

---

## License

MIT License. See [LICENSE](LICENSE) for full details.
