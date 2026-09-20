// Package anet provides asynchronous, framed message delivery over TCP,
// connection pooling, and a high-throughput request/response broker.
//
// # Features
//
//   - Message framing: Read and Write handle a big-endian LENGTHSIZE-byte
//     length header and payload framing for byte slices.
//   - Connection pool: NewPool creates pools of reusable connections with
//     context-aware GetWithContext, idle-connection eviction (IdleTimeout,
//     now actually enforced), background validation, and a lock-free
//     Put/Close handshake. NewTCPFactory dials connections with the
//     config's DialTimeout and arms TCP keepalive/NODELAY from it.
//   - Broker: NewBroker coordinates request dispatch and response matching
//     with a 4-byte task ID header. Requests are staged into task-owned
//     frame buffers at submit time, so a single contiguous write goes to
//     the wire and the caller's payload is never touched after submit.
//   - Asynchronous API: SendAsync and SendAsyncContext submit without
//     blocking and return a channel on which exactly one Response arrives —
//     the caller never parks a goroutine waiting for the round-trip.
//   - Multiplex mode: BrokerConfig.Multiplex gives each pooled connection a
//     background writer and reader goroutine and correlates responses by
//     task ID, so a handful of connections carry thousands of outstanding
//     requests (bounded by MaxInflightPerConn, which sheds with
//     ErrQueueFull) without any broker worker goroutines.
//   - TCP Server: server.NewServer starts an embeddable TCP server that
//     processes framed messages via a Handler interface, with per-connection
//     writer batching, handler panic isolation, admission-safe shutdown,
//     and optional SO_REUSEPORT.
//
// # Shutdown guarantees
//
// broker.Close never blocks longer than BrokerConfig.CloseTimeout and
// wakes callers parked on an exhausted pool. server.Stop closes every
// accepted connection, including ones accepted mid-shutdown. pool.Close
// closes every connection it handed back exactly once.
//
// # Basic Client Example
//
//	poolCfg := anet.DefaultPoolConfig()
//	pool := anet.NewPool(5, anet.NewTCPFactory(poolCfg), "localhost:9000", poolCfg)
//	broker := anet.NewBroker([]anet.Pool{pool}, 3, nil, nil)
//	go broker.Start()
//	defer broker.Close()
//	req := []byte("hello")
//	resp, err := broker.Send(&req)
//
// # Asynchronous Fan-Out Example
//
//	var inflight []<-chan anet.Response
//	for _, req := range requests {
//	    req := req
//	    ch, err := broker.SendAsync(&req) // returns immediately
//	    if err != nil {
//	        return err
//	    }
//	    inflight = append(inflight, ch)
//	}
//	for _, ch := range inflight {
//	    r := <-ch // exactly one Response per submitted request
//	    if r.Err != nil {
//	        return r.Err
//	    }
//	    use(r.Payload)
//	}
//
// # Multiplex Example (thousands of outstanding requests, few connections)
//
//	cfg := anet.DefaultBrokerConfig()
//	cfg.Multiplex = true
//	cfg.MaxInflightPerConn = 512
//	broker := anet.NewBroker(pools, 0 /* no workers */, nil, cfg)
//	// no Start needed; use SendAsync / Send as usual
//
// # Basic Server Example
//
//	handler := server.HandlerFunc(func(c *server.ServerConn, req []byte) ([]byte, error) {
//	    return req, nil
//	})
//	srv, err := server.NewServer(":9000", handler, nil)
//	if err != nil {
//	    return err
//	}
//	go srv.Start()
//	defer srv.Stop()
//
// For more details and configuration options, see the README.
package anet
