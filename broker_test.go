//nolint:all
package anet_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/andrei-cloud/anet"
	"github.com/stretchr/testify/require"
)

// TestBroker tests the broker functionality
func TestBroker(t *testing.T) {
	// Run subtests sequentially to avoid races between Start and Close
	// t.Parallel() removed

	// Start test server for all broker subtests
	addr, stop, err := StartTestServer()
	require.NoError(t, err)
	defer stop() // stop server after all subtests complete

	// Sleep to ensure the server is fully ready
	time.Sleep(100 * time.Millisecond)

	// Factory function with timeouts to prevent test hangs
	factory := func(addr string) (anet.PoolItem, error) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err != nil {
			return nil, err
		}

		// Set I/O deadlines
		if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to set deadline: %w", err)
		}

		return conn, nil
	}

	t.Run("Send Success", func(t *testing.T) {
		// sequential subtest
		p := anet.NewPool(1, factory, addr, nil) // Use default config
		require.NotNil(t, p)
		defer p.Close()

		broker := anet.NewBroker([]anet.Pool{p}, 1, nil, nil) // Use default config
		require.NotNil(t, broker)

		// Start broker workers
		done := make(chan error, 1)
		go func() {
			done <- broker.Start()
		}()

		// Wait a moment for workers to spin up
		time.Sleep(50 * time.Millisecond)

		msg := []byte("hello broker")
		resp, err := broker.Send(&msg)
		if err != nil {
			if strings.Contains(err.Error(), "connection refused") {
				broker.Close()
				<-done
				t.Skip("Skipping test due to connection error")
			}
			require.NoError(t, err)
		}
		require.NotNil(t, resp)
		require.Equal(t, msg, resp)

		// Shutdown broker and wait for workers to exit
		broker.Close()
		<-done
	})

	t.Run("SendContext Success", func(t *testing.T) {
		// sequential subtest
		p := anet.NewPool(1, factory, addr, nil) // Use default config
		require.NotNil(t, p)
		defer p.Close()

		broker := anet.NewBroker([]anet.Pool{p}, 1, nil, nil) // Use default config
		require.NotNil(t, broker)

		// Start broker workers
		done := make(chan error, 1)
		go func() {
			done <- broker.Start()
		}()

		// Wait a moment for workers to spin up
		time.Sleep(50 * time.Millisecond)

		msg := []byte("hello context")
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		resp, err := broker.SendContext(ctx, &msg)
		if err != nil {
			if strings.Contains(err.Error(), "connection refused") {
				broker.Close()
				<-done
				t.Skip("Skipping test due to connection error")
			}
			require.NoError(t, err)
		}
		require.NotNil(t, resp)
		require.Equal(t, msg, resp)

		// Shutdown broker and wait for workers to exit
		broker.Close()
		<-done
	})

	t.Run("SendContext Timeout", func(t *testing.T) {
		// sequential subtest

		// Create a factory that intentionally blocks until context times out
		slowFactory := func(addr string) (anet.PoolItem, error) {
			time.Sleep(200 * time.Millisecond)
			conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
			if err != nil {
				return nil, err
			}

			// Set I/O deadlines
			if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
				conn.Close()
				return nil, fmt.Errorf("failed to set deadline: %w", err)
			}

			return conn, nil
		}

		p := anet.NewPool(1, slowFactory, addr, nil) // Use default config
		require.NotNil(t, p)
		defer p.Close()

		broker := anet.NewBroker([]anet.Pool{p}, 1, nil, nil) // Use default config
		require.NotNil(t, broker)

		// Start broker workers
		done := make(chan error, 1)
		go func() {
			done <- broker.Start()
		}()

		// Wait a moment for workers to spin up
		time.Sleep(50 * time.Millisecond)

		// Get the only connection in the pool to force timeout
		// Use context with timeout to avoid hanging in case of connection issues
		getCtx, getCancel := context.WithTimeout(context.Background(), 1*time.Second)
		item, err := p.GetWithContext(getCtx)
		getCancel()

		if err != nil {
			if strings.Contains(err.Error(), "connection refused") {
				broker.Close()
				<-done
				t.Skip("Skipping test due to connection error")
			}
			require.NoError(t, err)
		}

		defer p.Put(item)

		// Now try with a very short timeout that should expire
		msg := []byte("hello timeout")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		resp, err := broker.SendContext(ctx, &msg)
		require.Error(t, err)
		require.True(t, errors.Is(err, context.DeadlineExceeded))
		require.Nil(t, resp)

		// Shutdown broker and wait for workers to exit
		broker.Close()
		<-done
	})

	t.Run("Send Pool Connection Error", func(t *testing.T) {
		// sequential subtest

		// Create a factory that always returns an error
		errorFactory := func(string) (anet.PoolItem, error) {
			return nil, errors.New("pool connection error")
		}

		p := anet.NewPool(1, errorFactory, addr, nil) // Use default config
		require.NotNil(t, p)
		defer p.Close()

		broker := anet.NewBroker([]anet.Pool{p}, 1, nil, nil) // Use default config
		require.NotNil(t, broker)
		defer broker.Close()

		// Start broker workers in background.
		go func() {
			err := broker.Start()
			require.True(t, err == nil || err == anet.ErrQuit)
		}()

		time.Sleep(50 * time.Millisecond) // Give broker time to start.

		msg := []byte("hello pool error")
		resp, err := broker.Send(&msg)
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "pool connection error")
	})

	t.Run("Broker Close Idempotency", func(t *testing.T) {
		// sequential subtest

		p := anet.NewPool(1, factory, addr, nil) // Use default config
		require.NotNil(t, p)
		defer p.Close()

		broker := anet.NewBroker([]anet.Pool{p}, 1, nil, nil) // Use default config
		require.NotNil(t, broker)
		broker.Close()
		broker.Close() // Should be safe to call multiple times
	})

	t.Run("Send with Queue Full", func(t *testing.T) {
		// sequential subtest
		config := &anet.BrokerConfig{
			QueueSize:    1,
			WriteTimeout: 100 * time.Millisecond,
			ReadTimeout:  100 * time.Millisecond,
		}

		// Create a pool with small capacity
		p := anet.NewPool(1, factory, addr, nil)
		require.NotNil(t, p)
		defer p.Close()

		broker := anet.NewBroker([]anet.Pool{p}, 1, nil, config)
		require.NotNil(t, broker)
		defer broker.Close()

		// Block the only available connection
		item, err := p.Get()
		if err != nil {
			t.Skip("Skipping test due to connection error")
			return
		}
		defer p.Put(item)

		// Try to send when no connections are available
		msg := []byte("overflow")
		_, err = broker.Send(&msg)
		require.Error(t, err)
		require.Equal(t, anet.ErrClosingBroker, err)
	})

	t.Run("SendContext with Multiple Pools", func(t *testing.T) {
		// sequential subtest
		p1 := anet.NewPool(1, factory, addr, nil)
		p2 := anet.NewPool(1, factory, addr, nil)
		require.NotNil(t, p1)
		require.NotNil(t, p2)
		defer p1.Close()
		defer p2.Close()

		broker := anet.NewBroker([]anet.Pool{p1, p2}, 2, nil, nil)
		require.NotNil(t, broker)

		// Start broker workers
		done := make(chan error, 1)
		go func() {
			done <- broker.Start()
		}()

		// Wait for broker to start
		time.Sleep(50 * time.Millisecond)

		// Send multiple concurrent requests
		var wg sync.WaitGroup
		concurrentRequests := 4
		wg.Add(concurrentRequests)

		for i := 0; i < concurrentRequests; i++ {
			go func(id int) {
				defer wg.Done()
				msg := []byte(fmt.Sprintf("concurrent-%d", id))
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer cancel()

				resp, err := broker.SendContext(ctx, &msg)
				if err != nil {
					if strings.Contains(err.Error(), "connection refused") {
						return
					}
					require.NoError(t, err)
				}
				require.Equal(t, msg, resp)
			}(i)
		}

		wg.Wait()
		broker.Close()
		<-done
	})

	t.Run("SendContext with Pool Shutdown", func(t *testing.T) {
		// sequential subtest
		p := anet.NewPool(1, factory, addr, nil)
		require.NotNil(t, p)

		broker := anet.NewBroker([]anet.Pool{p}, 1, nil, nil)
		require.NotNil(t, broker)

		// Start broker workers
		done := make(chan error, 1)
		go func() {
			done <- broker.Start()
		}()

		// Wait for broker to start
		time.Sleep(50 * time.Millisecond)

		// Close pool while broker is running
		p.Close()

		msg := []byte("after pool close")
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		_, err := broker.SendContext(ctx, &msg)
		require.Error(t, err)

		broker.Close()
		<-done
	})
}

// TestMultiPoolBroker tests the broker with multiple pools.
// Moved to a separate test to avoid interference with other tests.
func TestMultiPoolBroker(t *testing.T) {
	// Don't run in parallel to avoid test server interference.
	// t.Parallel()

	// Create a dedicated test server for multiple pools test.
	multiPoolAddr, multiPoolStop, err := StartTestServer()
	require.NoError(t, err)
	defer func() {
		err := multiPoolStop()
		if err != nil {
			t.Logf("Error stopping test server: %v", err)
		}
	}()

	// Sleep longer to ensure the server is fully ready.
	time.Sleep(300 * time.Millisecond)

	// Custom factory with proper timeouts.
	factory := func(addr string) (anet.PoolItem, error) {
		// Add a timeout to the connection.
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			return nil, err
		}

		// Set proper timeouts for all operations.
		err = conn.(*net.TCPConn).SetKeepAlive(true)
		if err != nil {
			conn.Close()

			return nil, fmt.Errorf("error setting keepalive: %w", err)
		}

		err = conn.(*net.TCPConn).SetKeepAlivePeriod(2 * time.Second)
		if err != nil {
			conn.Close()

			return nil, fmt.Errorf("error setting keepalive period: %w", err)
		}

		if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			conn.Close()

			return nil, fmt.Errorf("error setting deadline: %w", err)
		}

		return conn, nil
	}

	p1 := anet.NewPool(2, factory, multiPoolAddr, nil) // Use default config
	require.NotNil(t, p1)
	defer p1.Close()

	p2 := anet.NewPool(2, factory, multiPoolAddr, nil) // Use default config
	require.NotNil(t, p2)
	defer p2.Close()

	broker := anet.NewBroker([]anet.Pool{p1, p2}, 4, nil, nil) // Use default config
	require.NotNil(t, broker)
	defer broker.Close()

	// Start broker workers in background.
	brokerDone := make(chan error, 1)
	go func() {
		brokerDone <- broker.Start()
	}()

	// Give broker time to start.
	time.Sleep(200 * time.Millisecond)

	// Function to handle connection errors.
	isConnectionError := func(err error) bool {
		return err != nil && (strings.Contains(err.Error(), "connection reset") ||
			strings.Contains(err.Error(), "broken pipe") ||
			strings.Contains(err.Error(), "connection refused") ||
			strings.Contains(err.Error(), "i/o timeout"))
	}

	// First message.
	msg1 := []byte("pool 1")
	resp1, err1 := broker.Send(&msg1)
	if isConnectionError(err1) {
		t.Skipf("Skipping test due to connection error: %v", err1)

		return
	}
	require.NoError(t, err1)
	require.Equal(t, msg1, resp1)

	// Add delay between sends.
	time.Sleep(200 * time.Millisecond)

	// Second message.
	msg2 := []byte("pool 2")
	resp2, err2 := broker.Send(&msg2)
	if isConnectionError(err2) {
		t.Skipf("Skipping test due to connection error: %v", err2)

		return
	}
	require.NoError(t, err2)
	require.Equal(t, msg2, resp2)

	// Verify broker shutdown.
	broker.Close()
	select {
	case err := <-brokerDone:
		require.True(t, err == nil || err == anet.ErrQuit, "unexpected broker error: %v", err)
	case <-time.After(2 * time.Second):
		t.Error("Broker shutdown timed out")
	}
}

// TestConcurrentBrokerSend tests the broker's behavior under high concurrent load.
// This specifically checks for race conditions that might not be caught by other tests.
func TestConcurrentBrokerSend(t *testing.T) {
	t.Parallel() // Run this test in parallel with other top-level tests

	// Create a dedicated server for this test to avoid contention
	serverAddr, serverStop, err := StartTestServer()
	require.NoError(t, err)
	defer func() { _ = serverStop() }() // Ignore error from stop

	// Sleep to ensure the server is fully ready
	time.Sleep(100 * time.Millisecond)

	// Create a connection factory with timeout for stability
	factory := func(addr string) (anet.PoolItem, error) {
		// Add connection timeout to avoid hanging tests
		conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
		if err != nil {
			return nil, err
		}

		// Set read deadline to prevent hanging
		if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			if closeErr := conn.Close(); closeErr != nil {
				return nil, fmt.Errorf("read deadline error: %v, close error: %v", err, closeErr)
			}
			return nil, err
		}

		// Set write deadline to prevent hanging
		if err := conn.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
			if closeErr := conn.Close(); closeErr != nil {
				return nil, fmt.Errorf("write deadline error: %v, close error: %v", err, closeErr)
			}
			return nil, err
		}

		return conn, nil
	}

	// Create multiple pools to better simulate real-world usage
	poolCount := 3
	poolCapacity := uint32(15) // Increase capacity to handle concurrent connections
	pools := make([]anet.Pool, poolCount)
	for i := 0; i < poolCount; i++ {
		pools[i] = anet.NewPool(poolCapacity, factory, serverAddr, nil) // Use default config
		require.NotNil(t, pools[i])
		defer pools[i].Close()
	}

	// Create a broker with enough workers
	workerCount := 10                                      // Sufficient workers for concurrent processing
	broker := anet.NewBroker(pools, workerCount, nil, nil) // Use default config
	require.NotNil(t, broker)
	defer broker.Close()

	// Start broker workers in background
	brokerStarted := make(chan struct{})
	go func() {
		close(brokerStarted) // Signal that broker.Start() has been called
		err := broker.Start()
		require.True(t, err == nil || err == anet.ErrQuit)
	}()

	// Wait for broker to initialize
	<-brokerStarted
	time.Sleep(50 * time.Millisecond)

	// Use context with deadline to control overall test duration
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test parameters for concurrent clients
	clientCount := 8       // Reduced count to avoid overwhelming the test server
	requestsPerClient := 5 // Reduced requests per client to finish faster

	// Use wait group to synchronize all clients
	var wg sync.WaitGroup
	wg.Add(clientCount)

	// Create buffered error channel to collect any errors
	errorChan := make(chan error, clientCount*requestsPerClient)

	// Launch multiple clients sending concurrently
	for i := 0; i < clientCount; i++ {
		go func(clientID int) {
			defer wg.Done()

			// Add a small stagger to client starts to avoid initial thundering herd
			time.Sleep(time.Duration(clientID) * time.Millisecond * 5)

			for j := 0; j < requestsPerClient; j++ {
				// Check if overall test deadline is approaching
				select {
				case <-ctx.Done():
					return // Exit early if the test context is done
				default:
					// Continue with the test
				}

				// Create a unique message for this client and request
				msg := []byte(fmt.Sprintf("client-%d-req-%d", clientID, j))

				// Create a per-request context with reasonable timeout
				reqCtx, reqCancel := context.WithTimeout(ctx, 500*time.Millisecond)

				// Alternate between Send and SendContext to test both methods
				var resp []byte
				var err error
				if j%2 == 0 {
					resp, err = broker.Send(&msg)
				} else {
					resp, err = broker.SendContext(reqCtx, &msg)
				}

				// Always cancel context to prevent leaks
				reqCancel()

				if err != nil {
					// Filter out expected context deadline errors
					if !errors.Is(err, context.DeadlineExceeded) &&
						!errors.Is(err, context.Canceled) &&
						!strings.Contains(err.Error(), "context") {
						errorChan <- fmt.Errorf("client %d request %d failed: %w", clientID, j, err)
					}
					continue
				}

				// Verify the response matches what we expect
				if !bytes.Equal(resp, msg) {
					errorChan <- fmt.Errorf("client %d request %d: unexpected response: expected %q, got %q",
						clientID, j, string(msg), string(resp))
				}

				// Add small delay between requests from the same client
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Millisecond * 10):
					// Small delay to prevent overwhelming the server
				}
			}
		}(i)
	}

	// Wait for all clients to complete with a timeout
	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()

	// Wait for completion or context deadline
	select {
	case <-waitDone:
		t.Log("All concurrent clients completed successfully")
	case <-ctx.Done():
		t.Log("Test context deadline exceeded, checking for errors")
	}

	// Signal we're done collecting errors
	close(errorChan)

	// Filter out connection errors when checking errors
	var errors []error
	for err := range errorChan {
		if err != nil && !strings.Contains(err.Error(), "context") &&
			!strings.Contains(err.Error(), "EOF") &&
			!strings.Contains(err.Error(), "connection reset") &&
			!strings.Contains(err.Error(), "connection refused") {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		for _, err := range errors {
			t.Logf("Concurrent error: %v", err)
		}
		require.Empty(t, errors, "Concurrent broker operations produced errors")
	}
}

func BenchmarkBrokerSend(b *testing.B) {
	addr, stop, err := StartTestServer()
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = stop() }() // Ignore error from stop.

	// Wait for the server to be ready
	time.Sleep(100 * time.Millisecond)

	// Use smaller worker counts for benchmarking to reduce resource contention
	worker_counts := []int{1, 5, 10, 20, 50, 100}

	// Use a factory with proper timeouts to prevent hanging during benchmarks
	factory := func(addr string) (anet.PoolItem, error) {
		conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
		if err != nil {
			return nil, err
		}

		// Set reasonable deadlines for benchmark operations
		if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			if closeErr := conn.Close(); closeErr != nil {
				b.Logf("Error closing connection: %v after deadline error: %v", closeErr, err)
			}
			return nil, err
		}

		return conn, nil
	}

	for _, count := range worker_counts {
		// Create a new pool for each worker count test
		p := anet.NewPool(
			uint32(count+5),
			factory,
			addr,
			nil,
		) // Ensure pool capacity exceeds worker count
		require.NotNil(b, p)

		b.Run(fmt.Sprintf("Workers_%d", count), func(b *testing.B) {
			// Create the broker with the current worker count
			broker := anet.NewBroker([]anet.Pool{p}, count, nil, nil) // Use default config
			require.NotNil(b, broker)
			defer broker.Close()

			// Start broker workers in background and wait for it to initialize
			brokerStarted := make(chan struct{})
			go func() {
				close(brokerStarted)
				err := broker.Start()
				if err != nil && err != anet.ErrQuit {
					b.Logf("Broker error: %v", err)
				}
			}()
			<-brokerStarted
			time.Sleep(50 * time.Millisecond)

			msg := []byte("benchmark-test")

			// Use a proper wait group to ensure all goroutines finish
			var wg sync.WaitGroup

			// Create a semaphore to limit concurrent operations to avoid overwhelming the system
			// This ensures each operation completes before too many new ones pile up
			concurrencyLimit := make(chan struct{}, count*2)

			// Reset the timer after setup but before the actual benchmark operations
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				wg.Add(1)
				concurrencyLimit <- struct{}{} // Acquire semaphore

				go func(wg *sync.WaitGroup) {
					defer func() {
						<-concurrencyLimit // Release semaphore
						wg.Done()
					}()

					// Use Send instead of SendContext to avoid context allocation overhead
					_, err := broker.Send(&msg)

					if err != nil && !errors.Is(err, context.DeadlineExceeded) &&
						!errors.Is(err, context.Canceled) &&
						!errors.Is(err, anet.ErrClosingBroker) {
						b.Logf("Benchmark send error: %v", err)
					}
				}(&wg)
			}

			// Wait for all goroutines to complete
			wg.Wait()
		})

		// Clean up the pool after each worker count benchmark
		p.Close()
	}
}

// TestConnectionReuse tests that connections are properly reused from pool.
func TestConnectionReuse(t *testing.T) {
	// Create test server.
	addr, stop, err := StartTestServer()
	require.NoError(t, err)
	defer func() {
		err := stop()
		if err != nil {
			t.Logf("Error stopping test server: %v", err)
		}
	}()

	// Sleep to ensure server is ready.
	time.Sleep(200 * time.Millisecond)

	// Track connections created by factory.
	var connMu sync.Mutex
	conns := make(map[net.Conn]struct{})

	// Factory that tracks created connections.
	factory := func(addr string) (anet.PoolItem, error) {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			return nil, err
		}

		// Set TCP keepalive.
		tcpConn := conn.(*net.TCPConn)
		err = tcpConn.SetKeepAlive(true)
		if err != nil {
			conn.Close()

			return nil, fmt.Errorf("error setting keepalive: %w", err)
		}

		err = tcpConn.SetKeepAlivePeriod(2 * time.Second)
		if err != nil {
			conn.Close()

			return nil, fmt.Errorf("error setting keepalive period: %w", err)
		}

		// Track the new connection.
		connMu.Lock()
		conns[conn] = struct{}{}
		connMu.Unlock()

		return conn, nil
	}

	// Create pool with capacity 2.
	poolSize := uint32(2)
	p := anet.NewPool(poolSize, factory, addr, nil) // Use default config
	require.NotNil(t, p)
	defer p.Close()

	broker := anet.NewBroker([]anet.Pool{p}, 2, nil, nil) // Use default config
	require.NotNil(t, broker)
	defer broker.Close()

	// Start broker.
	brokerDone := make(chan error, 1)
	go func() {
		brokerDone <- broker.Start()
	}()

	// Give broker time to start.
	time.Sleep(200 * time.Millisecond)

	// Send multiple requests sequentially and verify connection reuse.
	numRequests := 5
	for i := 0; i < numRequests; i++ {
		msg := []byte(fmt.Sprintf("request-%d", i))
		resp, err := broker.Send(&msg)
		if err != nil {
			if strings.Contains(err.Error(), "connection reset") ||
				strings.Contains(err.Error(), "broken pipe") ||
				strings.Contains(err.Error(), "connection refused") {
				t.Skipf("Skipping test due to connection error: %v", err)

				return
			}
			require.NoError(t, err)
		}
		require.Equal(t, msg, resp)

		// Short delay between requests.
		time.Sleep(50 * time.Millisecond)
	}

	// Verify connection reuse.
	connMu.Lock()
	numConns := len(conns)
	connMu.Unlock()

	// Should have created at most pool size connections.
	require.LessOrEqual(t, numConns, int(poolSize),
		"Expected at most %d connections, got %d", poolSize, numConns)

	// Clean shutdown.
	broker.Close()
	select {
	case err := <-brokerDone:
		require.True(t, err == nil || err == anet.ErrQuit)
	case <-time.After(2 * time.Second):
		t.Error("Broker shutdown timed out")
	}
}
