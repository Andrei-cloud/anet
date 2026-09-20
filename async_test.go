package anet_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/andrei-cloud/anet"
	"github.com/stretchr/testify/require"
)

// quietListener accepts connections and never speaks: connections dials to it
// stall in read until their deadlines, the worst case for shutdown paths.
func quietListener(t *testing.T) (string, func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			_ = c // held open, silent
		}
	}()
	return l.Addr().String(), func() { _ = l.Close(); wg.Wait() }
}

// TestSendContextCancelWhileQueued is the regression for the use-after-recycle
// class: callers whose contexts expire while their task sits in the request
// queue used to return the task to the pool while its pointer was still
// queued, letting a worker process a zombie (and, when the object was
// resubmitted, cross-deliver responses). Run under -race.
func TestSendContextCancelWhileQueued(t *testing.T) {
	t.Parallel()

	addr, stopLn := quietListener(t)
	defer stopLn()

	factory := func(a string) (anet.PoolItem, error) {
		c, err := net.DialTimeout("tcp", a, 2*time.Second)
		if err != nil {
			return nil, err
		}
		return c, nil
	}

	poolCfg := anet.DefaultPoolConfig()
	poolCfg.ValidationStrategy = anet.ValidationNone
	pool := anet.NewPool(2, factory, addr, poolCfg)
	defer pool.Close()

	bcfg := anet.DefaultBrokerConfig()
	bcfg.QueueSize = 2 // tiny: forces queue-full + backlog
	bcfg.ReadTimeout = 30 * time.Second
	bcfg.CloseTimeout = 200 * time.Millisecond
	broker := anet.NewBroker([]anet.Pool{pool}, 1, nil, bcfg)
	go func() { _ = broker.Start() }()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			req := []byte("payload")
			// The peer never speaks, so every legitimate outcome is an error
			// (shed, cancel, close, or I/O timeout). The test's real teeth
			// are -race on the cancel-while-queued path and the bounded
			// Close below; only a successful send would be a bug.
			if _, err := broker.SendContext(ctx, &req); err == nil {
				t.Error("quiet peer should never answer")
			}
		}()
	}
	wg.Wait()

	done := make(chan struct{})
	go func() { broker.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("broker.Close hung after mass cancellation")
	}
}

// TestCloseWithPoolExhausted is the regression for the Close hang: workers
// parked on an exhausted pool must observe broker shutdown, and Close must
// be bounded by CloseTimeout even with a worker wedged in a stalled read.
func TestCloseWithPoolExhausted(t *testing.T) {
	t.Parallel()

	addr, stopLn := quietListener(t)
	defer stopLn()

	factory := func(a string) (anet.PoolItem, error) {
		c, err := net.DialTimeout("tcp", a, 2*time.Second)
		if err != nil {
			return nil, err
		}
		return c, nil
	}

	poolCfg := anet.DefaultPoolConfig()
	poolCfg.ValidationStrategy = anet.ValidationNone
	pool := anet.NewPool(1, factory, addr, poolCfg) // single connection
	defer pool.Close()

	bcfg := anet.DefaultBrokerConfig()
	bcfg.ReadTimeout = 500 * time.Millisecond // worker stalls in Read, then errors out
	bcfg.CloseTimeout = 300 * time.Millisecond
	broker := anet.NewBroker([]anet.Pool{pool}, 2, nil, bcfg)
	go func() { _ = broker.Start() }()

	// Occupy the connection in a stalled read.
	req := []byte("occupy")
	first, aerr := broker.SendAsync(&req)
	require.NoError(t, aerr)
	time.Sleep(100 * time.Millisecond)

	// Second send parks waiting for the (only) connection.
	blocked := make(chan error, 1)
	go func() {
		r := []byte("parked")
		_, err := broker.Send(&r)
		blocked <- err
	}()
	time.Sleep(100 * time.Millisecond)

	start := time.Now()
	broker.Close()
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Close took %v, expected bounded by CloseTimeout", d)
	}

	// The parked send must be released by Close; the exact error depends on
	// whether it woke from the pool wait or was mid-read on a dying socket.
	select {
	case err := <-blocked:
		if err == nil {
			t.Error("quiet peer should never answer")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("send parked on pool did not observe broker close")
	}

	// Closing the pool releases the stalled connection; the worker's wedged
	// read then fails and the delivery guarantee puts a Response on `first`.
	pool.Close()
	select {
	case r := <-first:
		require.Error(t, r.Err)
	case <-time.After(3 * time.Second):
		t.Fatal("no Response delivered for the stalled request after pool close")
	}
}

// TestPoolConcurrentPutClose closes the lock-free handshake: every created
// item must end up closed exactly once, whoever drained it.
func TestPoolConcurrentPutClose(t *testing.T) {
	t.Parallel()

	var (
		mu      sync.Mutex
		created []*trackItem
	)
	factory := func(_ string) (anet.PoolItem, error) {
		it := &trackItem{}
		mu.Lock()
		created = append(created, it)
		mu.Unlock()
		return it, nil
	}

	pool := anet.NewPool(4, factory, "addr", nil)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 2000; j++ {
				it, err := pool.Get()
				if err != nil {
					return // closing
				}
				pool.Put(it)
			}
		}()
	}
	time.Sleep(30 * time.Millisecond)
	pool.Close()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, created)
	for i, it := range created {
		if n := it.closes.Load(); n != 1 {
			t.Fatalf("item %d closed %d times, want exactly 1", i, n)
		}
	}
	require.Equal(t, 0, pool.Len())
}

type trackItem struct{ closes atomic.Int32 }

func (it *trackItem) Close() error { it.closes.Add(1); return nil }

// TestSendAsyncEcho exercises the async API for correctness: every submitted
// request receives exactly one Response with its own payload.
func TestSendAsyncEcho(t *testing.T) {
	t.Parallel()

	addr, stop, err := StartTestServer()
	require.NoError(t, err)
	defer func() { _ = stop() }()

	factory := func(a string) (anet.PoolItem, error) {
		return net.DialTimeout("tcp", a, 2*time.Second)
	}
	poolCfg := anet.DefaultPoolConfig()
	poolCfg.ValidationStrategy = anet.ValidationNone
	pools := anet.NewPoolList(4, factory, []string{addr}, poolCfg)
	defer func() {
		for _, p := range pools {
			p.Close()
		}
	}()

	bcfg := anet.DefaultBrokerConfig()
	broker := anet.NewBroker(pools, 8, nil, bcfg)
	go func() { _ = broker.Start() }()
	defer broker.Close()

	const n = 200
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := []byte(fmt.Sprintf("payload-%d", i))
			res, serr := broker.SendAsync(&req)
			if serr != nil {
				errCh <- serr
				return
			}
			r := <-res
			if r.Err != nil {
				errCh <- r.Err
				return
			}
			if string(r.Payload) != string(req) {
				errCh <- fmt.Errorf("payload mismatch: got %q want %q", r.Payload, req)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		t.Errorf("async send: %v", e)
	}
}

// TestSendAsyncContextClose: waiters (sync and async) must be released by
// Close with an error, never left hanging.
func TestSendAsyncContextClose(t *testing.T) {
	t.Parallel()

	addr, stop, err := StartTestServer()
	require.NoError(t, err)
	defer func() { _ = stop() }()

	factory := func(a string) (anet.PoolItem, error) {
		return net.DialTimeout("tcp", a, 2*time.Second)
	}
	poolCfg := anet.DefaultPoolConfig()
	poolCfg.ValidationStrategy = anet.ValidationNone
	pool := anet.NewPool(2, factory, addr, poolCfg)
	defer pool.Close()

	bcfg := anet.DefaultBrokerConfig()
	broker := anet.NewBroker([]anet.Pool{pool}, 2, nil, bcfg)
	go func() { _ = broker.Start() }()

	res, err := broker.SendAsyncContext(context.Background(), nil)
	require.NoError(t, err)
	broker.Close()

	// Delivery guarantee: a Response (payload or error) must arrive; never hang.
	select {
	case r := <-res:
		_ = r
	case <-time.After(3 * time.Second):
		t.Fatal("abandoned async response never arrived after Close")
	}
}

// TestMultiplexEcho proves the pipelined transport: with two connections and
// 200 concurrent requests, every response correlates with its request, and
// in-flight far exceeds the connection count.
func TestMultiplexEcho(t *testing.T) {
	t.Parallel()

	addr, stop, err := StartTestServer()
	require.NoError(t, err)
	defer func() { _ = stop() }()

	poolCfg := anet.DefaultPoolConfig() // NewTCPFactory-dialect config
	factory := anet.NewTCPFactory(poolCfg)
	pools := anet.NewPoolList(2, factory, []string{addr}, poolCfg)
	defer func() {
		for _, p := range pools {
			p.Close()
		}
	}()

	bcfg := anet.DefaultBrokerConfig()
	bcfg.Multiplex = true
	broker := anet.NewBroker(pools, 0, nil, bcfg) // no workers: the point of Multiplex
	defer broker.Close()

	const n = 200
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := []byte(fmt.Sprintf("mux-%d", i))
			res, serr := broker.SendAsync(&req)
			if serr != nil {
				errs <- serr
				return
			}
			r := <-res
			if r.Err != nil {
				errs <- r.Err
				return
			}
			if string(r.Payload) != string(req) {
				errs <- fmt.Errorf("mux payload mismatch: got %q want %q", r.Payload, req)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Errorf("multiplex: %v", e)
	}

	// Synchronous Send must work identically on the multiplex transport.
	req := []byte("sync-over-mux")
	resp, err := broker.Send(&req)
	require.NoError(t, err)
	require.Equal(t, req, resp)
}

// TestMultiplexInflightBound: submits beyond MaxInflightPerConn shed with
// ErrQueueFull instead of queueing without bound (load shedding).
func TestMultiplexInflightBound(t *testing.T) {
	t.Parallel()

	// A server whose handler stalls 300ms per message.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = l.Close() }()
	var srvWG sync.WaitGroup
	srvWG.Add(1)
	go func() {
		defer srvWG.Done()
		for {
			c, aerr := l.Accept()
			if aerr != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				for {
					msg, rerr := anet.Read(c)
					if rerr != nil {
						return
					}
					time.Sleep(300 * time.Millisecond)
					if werr := anet.Write(c, msg); werr != nil { // framed [id][payload] echo
						return
					}
				}
			}(c)
		}
	}()

	factory := anet.NewTCPFactory(nil)
	pool := anet.NewPool(1, factory, l.Addr().String(), nil)
	defer pool.Close()

	bcfg := anet.DefaultBrokerConfig()
	bcfg.Multiplex = true
	bcfg.MaxInflightPerConn = 1
	broker := anet.NewBroker([]anet.Pool{pool}, 0, nil, bcfg)
	defer broker.Close()

	req := []byte("first")
	res1, err := broker.SendAsync(&req)
	require.NoError(t, err)

	// Enough time for the first submit to be registered outstanding.
	time.Sleep(50 * time.Millisecond)

	shed := 0
	for i := 0; i < 4; i++ {
		r := []byte("burst")
		_, serr := broker.SendAsync(&r)
		if errors.Is(serr, anet.ErrQueueFull) {
			shed++
		}
	}
	require.NotZero(t, shed, "expected at least one ErrQueueFull shed past the bound")

	r1 := <-res1
	require.NoError(t, r1.Err)
	require.Equal(t, req, r1.Payload)
	_ = l.Close()
	srvWG.Wait()
}
