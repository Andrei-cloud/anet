// Package anet provides components for efficient, asynchronous
// network communication with framed messages, connection pooling,
// and a high-throughput broker.
//
// Features:
//
// 1. Message Framing (utils.go):
//   - Write and Read functions to encode and decode messages
//     with a 2-byte big-endian length header.
//   - ErrInvalidMsgLength and ErrMaxLenExceeded for error reporting.
//
// 2. Connection Pool (pool.go):
//   - Pool interface for managing reusable network connections.
//   - Factory function type to create new PoolItem instances.
//   - NewPool and NewPoolList to create pools with context-aware
//     connection retrieval (GetWithContext) and lifecycle control.
//   - Support for safe concurrent access, capacity limits, and graceful
//     shutdown via Close().
//   - ErrClosing indicates operations on a closed pool.
//
// 3. Asynchronous Broker (broker.go):
//   - Broker interface for sending requests and receiving
//     responses over pooled connections.
//   - Send (blocking) and SendContext (with cancellation/timeouts)
//     methods for submitting requests.
//   - NewBroker to create a broker with multiple workers and
//     optional Logger for structured logging.
//   - Correlates requests and responses using a Task ID header.
//   - ErrTimeout, ErrQuit, ErrClosingBroker, ErrNoPoolsAvailable
//     for robust error handling.
//
// Basic Usage:
//
//	// Create a connection factory
//	factory := func(addr string) (anet.PoolItem, error) {
//	    conn, err := net.Dial("tcp", addr)
//	    if err != nil {
//	        return nil, err
//	    }
//	    return conn, nil
//	}
//
//	// Initialize a pool
//	pool := anet.NewPool(5, factory, "localhost:8080")
//	defer pool.Close()
//
//	// Create and start a broker with 3 workers
//	broker := anet.NewBroker(pool, 3, nil)
//	defer broker.Close()
//	go broker.Start()
//
//	// Send a request and wait for a response
//	req := []byte("hello world")
//	resp, err := broker.Send(&req)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Response: %s\n", resp)
package anet
