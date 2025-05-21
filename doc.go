// Package anet provides asynchronous, framed message delivery over TCP,
// connection pooling, and a high-throughput request/response broker.
//
// Features:
//   - Message framing: Read and Write automatically handle a 2-byte length header
//     and payload framing for byte slices.
//   - Connection Pool: NewPool creates a pool of reusable connections with
//     context-aware GetWithContext, health checks, and graceful shutdown.
//   - Broker: NewBroker coordinates concurrent request dispatch and response
//     matching by automatically prepending a 4-byte Task ID header.
//   - TCP Server: NewServer starts an embeddable TCP server that processes
//     framed messages via a Handler interface and supports graceful shutdown.
//
// Basic Client Example:
//
//	factory := func(addr string) (anet.PoolItem, error) {
//	    // create and configure net.Conn
//	}
//	pool := anet.NewPool(5, factory, "localhost:9000", nil)
//	broker := anet.NewBroker([]anet.Pool{pool}, 3, nil, nil)
//	go broker.Start()
//	defer broker.Close()
//	req := []byte("hello")
//	resp, err := broker.Send(&req)
//	if err != nil {
//	    // handle error
//	}
//
// Basic Server Example:
//
//	handler := server.HandlerFunc(func(c *anet.ServerConn, req []byte) ([]byte, error) {
//	    return req, nil
//	})
//	srv, err := anet.NewServer(":9000", handler, nil)
//	if err != nil {
//	    // handle error
//	}
//	go srv.Start()
//	defer srv.Stop()
//
// For more details and configuration options, see the README.
package anet
