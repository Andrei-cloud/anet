# anet - Asynchronous Network Broker & Pool
[![Go Reference](https://pkg.go.dev/badge/github.com/andrei-cloud/anet.svg)](https://pkg.go.dev/github.com/andrei-cloud/anet)
[![Go Report Card](https://goreportcard.com/badge/github.com/andrei-cloud/anet)](https://goreportcard.com/report/github.com/andrei-cloud/anet)

 `anet` is a Go module providing components for efficient, asynchronous communication with network services, primarily featuring a connection pool and a message broker.

 ## Installation

 ```bash
go get github.com/andrei-cloud/anet@latest
 ```
 
## Features

* **Connection Pooling (`pool.go`)**: Manages a pool of reusable network connections (`PoolItem`) to specified addresses.
    * Uses a factory function (`Factory`) to create new connections.
    * Limits the number of concurrent connections (`Cap`).
    * Provides methods to `Get`, `Put` (return), and `Release` (close) connections.
    * Supports context-aware connection retrieval (`GetWithContext`).
    * Configurable connection validation and health checks.
    * Automatic connection cleanup and resource management.

* **Asynchronous Broker (`broker.go`)**: Coordinates sending requests and receiving responses over pooled connections.
    * Uses multiple worker goroutines for concurrent processing.
    * Accepts requests via `Send` (blocking) or `SendContext` (supports cancellation/timeouts).
    * Automatically prepends a unique Task ID header to outgoing messages.
    * Matches incoming responses to pending requests using the Task ID header.
    * Handles connection acquisition, writing requests, reading responses, and error management.
    * Includes structured logging capabilities (accepts a `Logger` interface).

* **Message Framing (`utils.go`)**: Implements simple message framing protocol.
    * Prepends a `uint16` (2 bytes, BigEndian) length header indicating message size.
    * The broker adds a Task ID (4 bytes) before the user's request data but after the length header.
    * Built-in buffer pooling for efficient memory usage.
    * Comprehensive error handling for invalid lengths and size limits.

* **TCP Server (`server/server.go`, `server_config.go`, `handler.go`)**: Embeddable framework to accept and process framed messages over TCP using the anet protocol.
    * ServerConfig allows tuning ReadTimeout, WriteTimeout, IdleTimeout, KeepAliveInterval, MaxConns, and ShutdownTimeout.
    * Handler interface and HandlerFunc adapter define application message processor.
    * Server struct manages listener, active connections, and graceful shutdown via Start and Stop.
    * Reuses anet.Read and anet.Write for consistent framing and Task ID correlation.

## Configuration

### Pool Configuration
```go
type PoolConfig struct {
    DialTimeout         time.Duration // Timeout for creating new connections (default: 5s)
    IdleTimeout        time.Duration // How long connections can remain idle (default: 60s)
    ValidationInterval time.Duration // How often to validate idle connections (default: 30s)
    KeepAliveInterval time.Duration // Interval for TCP keepalive (default: 30s)
}
```

### Broker Configuration
```go
// BrokerConfig holds settings for broker behavior and queue sizing.
type BrokerConfig struct {
    WriteTimeout   time.Duration // timeout for write operations (default: 5s).
    ReadTimeout    time.Duration // timeout for read operations (default: 5s).
    QueueSize      int           // request queue capacity (default: 1000).
    OptimizeMemory bool          // enable memory optimizations like task ID pooling (default: true).
}
```

By default, `OptimizeMemory` is now enabled. This reduces memory allocations and GC pressure by pooling task ID buffers for all brokers unless explicitly set to false.

## Advanced Configuration

### Connection Pool Tuning

The connection pool can be tuned for different workload patterns:

```go
config := &anet.PoolConfig{
    // Shorter dial timeout for latency-sensitive applications
    DialTimeout: 2 * time.Second,
    
    // Longer idle timeout for sporadic workloads
    IdleTimeout: 5 * time.Minute,
    
    // More frequent validation for unstable networks
    ValidationInterval: 15 * time.Second,
    
    // Aggressive keepalive for flaky networks
    KeepAliveInterval: 15 * time.Second,
}

pool := anet.NewPool(poolCap, factory, addr, config)
```

### Memory Optimizations

The anet broker includes optional memory optimizations that can significantly improve performance in high-throughput scenarios:

```go
config := &anet.BrokerConfig{
    WriteTimeout:   5 * time.Second,
    ReadTimeout:    5 * time.Second,
    QueueSize:      1000,
    OptimizeMemory: true, // Enable memory optimizations
}

broker := anet.NewBroker(pools, workers, logger, config)
```

**Memory Optimization Features:**
- **Task ID Pooling**: Reuses pre-allocated task ID buffers instead of creating new ones for each request
- **Reduced Allocations**: Minimizes memory allocations in critical paths
- **Cache-Line Optimization**: Uses cache-line padding to reduce false sharing in concurrent scenarios

**When to Enable:**
- High-throughput applications (>1000 requests/second)
- Latency-sensitive scenarios where GC pressure matters
- Applications with sustained concurrent load

**Performance Impact:**
- Reduces memory allocations by up to 50% for task management
- Improves GC performance in high-load scenarios
- Minimal overhead when enabled

### Broker Performance Tuning

The broker can be optimized for different throughput and reliability requirements:

```go
config := &anet.BrokerConfig{
    WriteTimeout:   2 * time.Second,  // Shorter timeout for real-time applications
    ReadTimeout:    2 * time.Second,  // Shorter read timeout  
    QueueSize:      5000,             // Larger queue for high throughput
    OptimizeMemory: true,             // Enable memory optimizations for performance
}

broker := anet.NewBroker(pools, workers, logger, config)
```

### Load Balancing

When using multiple connection pools, requests are distributed across pools using a random selection algorithm. This provides basic load balancing and failover:

```go
// Create pools for multiple backend servers
pools := anet.NewPoolList(
    poolCap,
    factory,
    []string{
        "server1:8080",
        "server2:8080",
        "server3:8080",
    },
    config,
)
```

### Production Best Practices

1. Connection Management:
   - Monitor pool size and connection age
   - Configure appropriate timeouts for your network
   - Use TCP keepalive to detect stale connections
   - Set proper validation intervals

2. Error Handling:
   - Handle temporary network errors with retries
   - Use context timeouts for deadlines
   - Log and monitor error rates
   - Implement circuit breakers if needed

3. Performance:
   - Size pools based on expected load
   - Adjust worker count for concurrency
   - Monitor response times and latency
   - Use buffer pooling for large messages
   - Enable memory optimizations (`OptimizeMemory: true`) for high-throughput scenarios

4. Operations:
   - Implement proper metrics collection
   - Use structured logging in production
   - Plan for graceful shutdowns
   - Monitor resource usage

## Basic Usage Example

See the [example/main.go](example/main.go) file for a complete working example including both server and client code.

```go
// client side:
factory := func(addr string) (anet.PoolItem, error) { /* ... */ }
pools := anet.NewPoolList(5, factory, []string{"localhost:9000"}, nil)
brokerCfg := &anet.BrokerConfig{
    WriteTimeout:   5*time.Second, 
    ReadTimeout:    5*time.Second, 
    QueueSize:      1000,
    OptimizeMemory: true, // Enable memory optimizations
}
broker := anet.NewBroker(pools, 3, nil, brokerCfg)
go broker.Start()
resp, err := broker.Send(&[]byte("hello"))
```

## Notes

* The server must implement the message framing protocol:
    * Read the `uint16` length header first.
    * Then read the specified number of bytes.
    * Include the received Task ID (first 4 bytes) in responses.
* Error handling is crucial for robust applications:
    * Handle connection failures and timeouts appropriately.
    * Use context deadlines for timeouts and cancellation.
    * Check error types for proper error handling.
* For production use:
    * Consider using a structured logging library.
    * Configure appropriate timeouts and retry settings.
    * Monitor connection pool usage and health.
    * Implement proper shutdown handling.

## Error Types

* `ErrTimeout`: Response not received within deadline.
* `ErrQuit`: Broker is shutting down normally.
* `ErrClosingBroker`: Broker is in process of closing.
* `ErrNoPoolsAvailable`: No connection pools are available.
* `ErrClosing`: Pool is shutting down.
* `ErrInvalidMsgLength`: Message length header is invalid.
* `ErrMaxLenExceeded`: Message exceeds maximum allowed size.
