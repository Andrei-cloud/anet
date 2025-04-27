// Package anet provides components for efficient, asynchronous
// network communication with framed messages, connection pooling,
// and a high-throughput broker.
//
// Key Components:
//
// 1. Message Framing (utils.go):
//   - Encodes messages with 2-byte big-endian length header.
//   - Write and Read functions handle framing automatically.
//   - Uses sync.Pool for efficient buffer management.
//   - Error handling for invalid length (ErrInvalidMsgLength) and size limits (ErrMaxLenExceeded).
//
// 2. Connection Pool (pool.go):
//   - Manages reusable network connections with safe concurrent access.
//   - Interface-based design with PoolItem and Factory abstractions.
//   - Context-aware connection retrieval with GetWithContext.
//   - Built-in connection validation and health checks.
//   - Configurable timeouts and keep-alive settings.
//   - Graceful shutdown with proper resource cleanup.
//
// 3. Asynchronous Broker (broker.go):
//   - Coordinates request/response over pooled connections.
//   - Multiple worker goroutines for high throughput.
//   - Automatic task ID generation and correlation.
//   - Context support for timeouts and cancellation.
//   - Request retries with configurable limits.
//   - Structured logging interface for observability.
//   - Graceful shutdown handling.
//
// 4. TCP Server (server/server.go, server_config.go, handler.go):
//   - Embeddable framework to accept and process framed messages over TCP using the anet protocol.
//   - ServerConfig allows tuning of ReadTimeout, WriteTimeout, IdleTimeout, MaxConns, KeepAliveInterval, ShutdownTimeout, and optional Logger.
//   - Handler and HandlerFunc types define the application message processor interface.
//   - Server struct manages listener, active connections, and graceful shutdown.
//   - Start begins accepting connections and dispatches messages to Handler.
//   - Stop signals shutdown, closes listener and active connections, and waits for handlers to finish.
//
// Basic Server Usage Example:
//
//	handler := HandlerFunc(func(conn *ServerConn, req []byte) ([]byte, error) {
//	    // process request and return response.
//	    return req, nil.
//	})
//	srv, err := NewServer(":9000", handler, nil)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	go srv.Start()  // run in background.
//	defer srv.Stop()
//
// Note: Server reuses anet.Read and anet.Write for framing and supports concurrent requests via Task ID correlation.
//
// Configuration:
//
// The library provides configuration structures for both pool and broker:
//
//	type PoolConfig struct {
//	    DialTimeout         time.Duration // Connection creation timeout (default: 5s)
//	    IdleTimeout        time.Duration // Max idle time (default: 60s)
//	    ValidationInterval time.Duration // Health check interval (default: 30s)
//	    KeepAliveInterval time.Duration // TCP keepalive interval (default: 30s)
//	}
//
//	type BrokerConfig struct {
//	    RequestTimeout    time.Duration // Response timeout (default: 30s)
//	    ShutdownTimeout  time.Duration // Graceful shutdown timeout (default: 5s)
//	    MaxRetries       int          // Max request retries (default: 3)
//	}
//
// Error Handling:
//
// The package defines several error types for specific conditions:
//   - ErrTimeout: Response not received within deadline.
//   - ErrQuit: Broker is shutting down normally.
//   - ErrClosingBroker: Broker is in process of closing.
//   - ErrNoPoolsAvailable: No connection pools are available.
//   - ErrClosing: Pool is shutting down.
//   - ErrInvalidMsgLength: Message length header is invalid.
//   - ErrMaxLenExceeded: Message exceeds maximum allowed size.
//
// Basic Usage Example:
//
//	// Create connection factory
//	factory := func(addr string) (anet.PoolItem, error) {
//	    conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
//	    if err != nil {
//	        return nil, err
//	    }
//	    return conn, nil
//	}
//
//	// Initialize pool with capacity
//	pool := anet.NewPool(5, factory, "localhost:8080", nil)
//	defer pool.Close()
//
//	// Create broker with workers
//	broker := anet.NewBroker([]anet.Pool{pool}, 3, logger, nil)
//	defer broker.Close()
//
//	// Start broker in background
//	go broker.Start()
//
//	// Send request with timeout
//	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
//	defer cancel()
//
//	req := []byte("hello")
//	resp, err := broker.SendContext(ctx, &req)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	fmt.Printf("Response: %s\n", resp)
package anet
