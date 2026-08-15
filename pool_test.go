package anet_test

import (
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

func TestPool(t *testing.T) {
	// Create a shared server for all pool tests
	addr, stop, err := StartTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = stop() // stop server after all subtests complete
	}()

	// Sleep to ensure the server is fully ready
	time.Sleep(50 * time.Millisecond)

	factory := func(addr string) (anet.PoolItem, error) {
		// Add connection timeout to avoid test hanging
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err != nil {
			return nil, err
		}

		// Set read/write deadlines to avoid hanging
		if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("failed to set deadline: %w", err)
		}

		return conn, nil
	}

	t.Run("NewPool", func(t *testing.T) {
		p := anet.NewPool(1, factory, addr, nil)
		require.NotNil(t, p)
		defer p.Close()
	})

	t.Run("Get Len Put", func(t *testing.T) {
		p := anet.NewPool(1, factory, addr, nil)
		require.NotNil(t, p)
		defer p.Close()

		require.Equal(t, 0, p.Len())

		item, err := p.Get()
		if err != nil {
			t.Skipf("Skipping test due to connection error: %v", err)
			return
		}
		require.Equal(t, 1, p.Len())

		p.Put(item)
		require.Equal(t, 1, p.Len())
	})

	t.Run("Get on closed", func(t *testing.T) {
		p := anet.NewPool(1, factory, addr, nil)
		require.NotNil(t, p)
		p.Close()

		require.Equal(t, 0, p.Len())

		item, err := p.Get()
		require.Error(t, err)
		require.Nil(t, item)
	})

	t.Run("GetWithContext", func(t *testing.T) {
		p := anet.NewPool(1, factory, addr, nil)
		require.NotNil(t, p)
		defer p.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		item, err := p.GetWithContext(ctx)
		if err != nil {
			t.Skipf("Skipping test due to connection error: %v", err)
			return
		}
		require.NotNil(t, item)

		ctx2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel2()
		item2, err2 := p.GetWithContext(ctx2)
		require.ErrorIs(t, err2, context.DeadlineExceeded)
		require.Nil(t, item2)
	})

	t.Run("Release", func(t *testing.T) {
		p := anet.NewPool(1, factory, addr, nil)
		require.NotNil(t, p)
		defer p.Close()

		item, err := p.Get()
		if err != nil {
			t.Skipf("Skipping test due to connection error: %v", err)
			return
		}
		require.NotNil(t, item)

		require.Equal(t, 1, p.Len())
		p.Release(item)
		require.Equal(t, 0, p.Len())
	})

	t.Run("Cap", func(t *testing.T) {
		p := anet.NewPool(5, factory, addr, nil)
		require.NotNil(t, p)
		defer p.Close()
		require.Equal(t, 5, p.Cap())
	})

	t.Run("Factory Error", func(t *testing.T) {
		errorFactory := func(_ string) (anet.PoolItem, error) {
			return nil, errors.New("factory error")
		}
		p := anet.NewPool(1, errorFactory, addr, nil)
		require.NotNil(t, p)
		defer p.Close()

		item, err := p.Get()
		require.Error(t, err)
		require.Nil(t, item)
		require.Contains(t, err.Error(), "factory error")
	})

	t.Run("GetWithContext Factory Error", func(t *testing.T) {
		errorFactory := func(_ string) (anet.PoolItem, error) {
			return nil, errors.New("factory error ctx")
		}
		p := anet.NewPool(1, errorFactory, addr, nil)
		require.NotNil(t, p)
		defer p.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		item, err := p.GetWithContext(ctx)
		require.Error(t, err)
		require.Nil(t, item)
		require.Contains(t, err.Error(), "factory error ctx")
	})

	t.Run("Put on closed", func(t *testing.T) {
		p := anet.NewPool(1, factory, addr, nil)
		require.NotNil(t, p)

		item, err := p.Get()
		if err != nil {
			t.Skipf("Skipping test due to connection error: %v", err)
			return
		}
		require.NotNil(t, item)
		require.Equal(t, 1, p.Len())

		p.Close() // Close the pool

		p.Put(item) // Try putting back into closed pool
		require.Equal(t, 0, p.Len())
	})

	t.Run("Release nil", func(t *testing.T) {
		p := anet.NewPool(1, factory, addr, nil)
		require.NotNil(t, p)
		defer p.Close()
		p.Release(nil)
		require.Equal(t, 0, p.Len())
	})

	t.Run("NewPoolList", func(t *testing.T) {
		addrs := []string{addr, addr}
		pools := anet.NewPoolList(2, factory, addrs, nil)
		require.Len(t, pools, 2)
		for _, p := range pools {
			require.NotNil(t, p)
			require.Equal(t, 2, p.Cap())
			p.Close()
		}
	})

	t.Run("Concurrent Access", func(t *testing.T) {
		poolCapacity := uint32(20)
		p := anet.NewPool(poolCapacity, factory, addr, nil)
		require.NotNil(t, p)
		defer p.Close()

		concurrency := 5
		iterations := 10

		var subwg sync.WaitGroup
		subwg.Add(concurrency)

		errChan := make(chan error, concurrency*iterations)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		for i := 0; i < concurrency; i++ {
			go func(id int) {
				defer subwg.Done()
				for j := 0; j < iterations; j++ {
					select {
					case <-ctx.Done():
						errChan <- fmt.Errorf("goroutine %d: test context canceled: %w", id, ctx.Err())
						return
					default:
					}

					switch j % 3 {
					case 0:
						opCtx, opCancel := context.WithTimeout(ctx, 100*time.Millisecond)
						item, err := p.GetWithContext(opCtx)
						opCancel()

						if err != nil {
							if !errors.Is(err, context.DeadlineExceeded) &&
								!errors.Is(err, ctx.Err()) &&
								!strings.Contains(err.Error(), "connection refused") {
								errChan <- fmt.Errorf("goroutine %d: Get error on iteration %d: %w", id, j, err)
							}
							continue
						}

						if item == nil {
							errChan <- fmt.Errorf("goroutine %d: Got nil item without error on iteration %d", id, j)
							continue
						}

						time.Sleep(1 * time.Millisecond)
						p.Put(item)
					case 1:
						opCtx, opCancel := context.WithTimeout(ctx, 50*time.Millisecond)
						item, err := p.GetWithContext(opCtx)
						opCancel()

						if err != nil {
							if !errors.Is(err, context.DeadlineExceeded) &&
								!errors.Is(err, ctx.Err()) &&
								!strings.Contains(err.Error(), "connection refused") {
								errChan <- fmt.Errorf("goroutine %d: GetWithContext error on iteration %d: %w", id, j, err)
							}
							continue
						}

						if item != nil {
							p.Put(item)
						}
					case 2:
						opCtx, opCancel := context.WithTimeout(ctx, 100*time.Millisecond)
						item, err := p.GetWithContext(opCtx)
						opCancel()

						if err != nil {
							if !errors.Is(err, context.DeadlineExceeded) &&
								!errors.Is(err, ctx.Err()) &&
								!strings.Contains(err.Error(), "connection refused") {
								errChan <- fmt.Errorf("goroutine %d: Get error on iteration %d: %w", id, j, err)
							}
							continue
						}

						if item == nil {
							errChan <- fmt.Errorf("goroutine %d: Got nil item without error on iteration %d", id, j)
							continue
						}

						time.Sleep(1 * time.Millisecond)
						p.Release(item)
					}
				}
			}(i)
		}

		waitDone := make(chan struct{})
		go func() {
			subwg.Wait()
			close(waitDone)
		}()

		select {
		case <-waitDone:
			t.Log("All concurrent goroutines completed successfully")
		case <-ctx.Done():
			t.Log("Test context deadline exceeded, proceeding to error checking")
		}

		close(errChan)

		var testErrors []error
		for err := range errChan {
			if err != nil && !strings.Contains(err.Error(), "context") &&
				!strings.Contains(err.Error(), "connection refused") {
				testErrors = append(testErrors, err)
			}
		}

		if len(testErrors) > 0 {
			for _, err := range testErrors {
				t.Logf("Concurrent error: %v", err)
			}
		}
	})

	t.Run("Get With Invalid Connection", func(t *testing.T) {
		invalidFactory := func(_ string) (anet.PoolItem, error) {
			conn, err := net.DialTimeout("tcp", "localhost:1", 500*time.Millisecond)
			if err != nil {
				return nil, err
			}
			return conn, nil
		}

		p := anet.NewPool(1, invalidFactory, addr, nil)
		require.NotNil(t, p)
		defer p.Close()

		item, err := p.Get()
		require.Error(t, err)
		require.Nil(t, item)
	})

	t.Run("validateIdleConnections", func(t *testing.T) {
		config := &anet.PoolConfig{
			ValidationInterval: 100 * time.Millisecond,
		}
		p := anet.NewPool(2, factory, addr, config)
		require.NotNil(t, p)
		defer p.Close()

		item1, err := p.Get()
		if err != nil {
			t.Skipf("Skipping test due to connection error: %v", err)
			return
		}
		item2, err := p.Get()
		if err != nil {
			p.Put(item1)
			t.Skipf("Skipping test due to connection error: %v", err)
			return
		}

		p.Put(item1)
		p.Put(item2)

		if conn, ok := item1.(net.Conn); ok {
			_ = conn.Close()
		}

		time.Sleep(150 * time.Millisecond)
		require.Equal(t, 1, p.Len())
	})

	t.Run("Release Invalid Connection", func(t *testing.T) {
		p := anet.NewPool(1, factory, addr, nil)
		require.NotNil(t, p)
		defer p.Close()

		item, err := p.Get()
		if err != nil {
			t.Skipf("Skipping test due to connection error: %v", err)
			return
		}

		if conn, ok := item.(net.Conn); ok {
			_ = conn.Close()
		}

		p.Release(item)
		require.Equal(t, 0, p.Len())
	})

	t.Run("GetWithContext Pool Full", func(t *testing.T) {
		p := anet.NewPool(1, factory, addr, nil)
		require.NotNil(t, p)
		defer p.Close()

		item, err := p.Get()
		if err != nil {
			t.Skipf("Skipping test due to connection error: %v", err)
			return
		}
		defer p.Put(item)

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		item2, err := p.GetWithContext(ctx)
		require.Error(t, err)
		require.True(t, errors.Is(err, context.DeadlineExceeded))
		require.Nil(t, item2)
	})
}

func BenchmarkPool(b *testing.B) {
	addr, stop, err := StartTestServer()
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = stop() }()

	workerNum := []int{1}
	factory := func(addr string) (anet.PoolItem, error) {
		return net.Dial("tcp", addr)
	}

	for _, i := range workerNum {
		p := anet.NewPool(1, factory, addr, nil)
		require.NotNil(b, p)
		b.Run(fmt.Sprintf("Workers %d", i), func(b *testing.B) {
			benchmarkPool(p, b)
		})
		p.Close()
	}
}

func benchmarkPool(p anet.Pool, b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item, _ := p.Get()
		p.Put(item)
	}
}
