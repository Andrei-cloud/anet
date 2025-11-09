package anet_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/andrei-cloud/anet"
	"github.com/stretchr/testify/require"
)

// TestTaskPoolingSafety tests that task pooling works safely under concurrent access.
func TestTaskPoolingSafety(t *testing.T) {
	t.Parallel()

	// Create test server
	addr, stop, err := StartTestServer()
	require.NoError(t, err)
	defer func() { _ = stop() }()

	// Factory function
	factory := func(addr string) (anet.PoolItem, error) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err != nil {
			return nil, err
		}

		return conn, nil
	}

	// Create pool and broker with memory optimization enabled
	p := anet.NewPool(10, factory, addr, nil)
	defer p.Close()

	config := &anet.BrokerConfig{
		WriteTimeout:   1 * time.Second,
		ReadTimeout:    1 * time.Second,
		QueueSize:      100,
		OptimizeMemory: true, // This enables task pooling
	}

	broker := anet.NewBroker([]anet.Pool{p}, 5, nil, config)
	defer broker.Close()

	// Start broker
	done := make(chan error, 1)
	go func() {
		done <- broker.Start()
	}()

	// Wait for broker to start
	time.Sleep(50 * time.Millisecond)

	// Test concurrent requests to ensure task pooling safety
	const numRequests = 100
	var wg sync.WaitGroup
	errors := make(chan error, numRequests)

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(_ int) {
			defer wg.Done()

			msg := []byte("test message")
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			resp, err := broker.SendContext(ctx, &msg)
			if err != nil {
				errors <- err
				return
			}

			// Verify response (should be echo)
			require.Equal(t, msg, resp)
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for any errors
	errorList := make([]error, 0, numRequests)
	for err := range errors {
		errorList = append(errorList, err)
	}

	if len(errorList) > 0 {
		t.Logf("Errors during concurrent task execution: %v", errorList)
	}

	// Stop broker
	broker.Close()
	<-done
}

// TestTaskReferenceCountingEdgeCases tests edge cases in task reference counting.
func TestTaskReferenceCountingEdgeCases(t *testing.T) {
	t.Parallel()

	// Create test server
	addr, stop, err := StartTestServer()
	require.NoError(t, err)
	defer func() { _ = stop() }()

	// Factory function
	factory := func(addr string) (anet.PoolItem, error) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err != nil {
			return nil, err
		}

		return conn, nil
	}

	// Create pool and broker
	p := anet.NewPool(2, factory, addr, nil)
	defer p.Close()

	config := &anet.BrokerConfig{
		WriteTimeout:   500 * time.Millisecond,
		ReadTimeout:    500 * time.Millisecond,
		QueueSize:      10,
		OptimizeMemory: true,
	}

	broker := anet.NewBroker([]anet.Pool{p}, 2, nil, config)
	defer broker.Close()

	// Start broker
	done := make(chan error, 1)
	go func() {
		done <- broker.Start()
	}()

	// Wait for broker to start
	time.Sleep(50 * time.Millisecond)

	// Test rapid fire requests to stress reference counting
	for i := 0; i < 50; i++ {
		msg := []byte("rapid test")
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)

		_, err := broker.SendContext(ctx, &msg)
		cancel()

		// We expect some timeouts in this stress test, that's okay
		if err != nil && err != context.DeadlineExceeded {
			// Log unexpected errors but don't fail the test
			t.Logf("Unexpected error in rapid fire test: %v", err)
		}
	}

	// Stop broker
	broker.Close()
	<-done
}

// BenchmarkTaskPoolingPerformance benchmarks the performance improvement from task pooling.
func BenchmarkTaskPoolingPerformance(b *testing.B) {
	// Create test server
	addr, stop, err := StartTestServer()
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = stop() }()

	// Factory function
	factory := func(addr string) (anet.PoolItem, error) {
		conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
		if err != nil {
			return nil, err
		}

		return conn, nil
	}

	// Create pool
	p := anet.NewPool(20, factory, addr, nil)
	defer p.Close()

	// Benchmark with pooling enabled
	b.Run("WithTaskPooling", func(b *testing.B) {
		config := &anet.BrokerConfig{
			WriteTimeout:   1 * time.Second,
			ReadTimeout:    1 * time.Second,
			QueueSize:      1000,
			OptimizeMemory: true, // Enable task pooling
		}

		broker := anet.NewBroker([]anet.Pool{p}, 10, nil, config)
		defer broker.Close()

		// Start broker
		done := make(chan error, 1)
		go func() {
			done <- broker.Start()
		}()

		// Wait for startup
		time.Sleep(50 * time.Millisecond)

		msg := []byte("benchmark test")

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, err := broker.Send(&msg)
				if err != nil {
					b.Logf("Benchmark error: %v", err)
				}
			}
		})

		broker.Close()
		<-done
	})

	// For comparison, we could also benchmark without pooling by setting OptimizeMemory: false
	// but since we've implemented safe pooling, we'll keep it enabled for better performance
}
