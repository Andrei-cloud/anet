package anet_test

import (
	"context"
	"errors" // Add import for errors package
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/andrei-cloud/anet"
	"github.com/stretchr/testify/require"
)

func StartTestServer() (string, func() error, error) {
	quit := make(chan struct{})
	ready := make(chan struct{}) // Signal channel to indicate server is ready
	l, err := net.Listen("tcp", ":")
	if err != nil {
		return "", nil, err
	}

	go func() {
		// Signal that the server is ready to accept connections
		close(ready)

		for {
			select {
			case <-quit:
				_ = l.Close() // Ignore error on close during shutdown
				return
			default:
				conn, err := l.Accept()
				if err != nil {
					// Check if the error is due to the listener being closed.
					if errors.Is(err, net.ErrClosed) {
						log.Printf("Test server listener closed.")

						return // Exit goroutine when listener is closed
					}
					log.Printf("Test server accept error: %v", err)

					return // Exit goroutine on other accept errors
				}
				// Handle one echo request per connection using anet framing
				go func(conn net.Conn) {
					defer func() { _ = conn.Close() }() // Ignore close error in defer

					// Read the incoming message (includes TaskID)
					requestMsg, err := anet.Read(conn)
					if err != nil {
						if err != io.EOF && !errors.Is(err, net.ErrClosed) {
							log.Printf("Test server read error: %v", err)
						}

						return
					}

					// Simulate processing: Echo the message back (preserving TaskID)
					// anet.Write will add the length prefix automatically.
					err = anet.Write(conn, requestMsg)
					if err != nil {
						if !errors.Is(err, net.ErrClosed) {
							log.Printf("Test server write error: %v", err)
						}
					}
				}(conn)
			}
		}
	}()

	// Wait for server to be ready
	<-ready

	return l.Addr().String(), func() error {
		close(quit)

		return l.Close()
	}, err
}

func TestPool(t *testing.T) {
	// Don't run the entire test in parallel
	addr, stop, err := StartTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stop() }() // Ignore error from stop

	// Sleep to ensure the server is fully ready
	time.Sleep(100 * time.Millisecond)

	factory := func(addr string) (anet.PoolItem, error) {
		return net.Dial("tcp", addr)
	}

	t.Run("NewPool", func(t *testing.T) {
		p := anet.NewPool(1, factory, addr)
		require.NotNil(t, p)
		defer p.Close()
	})

	t.Run("Get Len Put", func(t *testing.T) {
		p := anet.NewPool(1, factory, addr)
		require.NotNil(t, p)
		defer p.Close()

		require.Equal(t, 0, p.Len())

		item, err := p.Get()
		require.NoError(t, err)
		require.Equal(t, 1, p.Len())

		p.Put(item)
		require.Equal(t, 1, p.Len())
	})

	t.Run("Get on closed", func(t *testing.T) {
		p := anet.NewPool(1, factory, addr)
		require.NotNil(t, p)
		p.Close()

		require.Equal(t, 0, p.Len())

		item, err := p.Get()
		require.Error(t, err)
		require.Nil(t, item)
	})

	t.Run("GetWithContext", func(t *testing.T) {
		p := anet.NewPool(1, factory, addr)
		require.NotNil(t, p)
		defer p.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		item, err := p.GetWithContext(ctx)
		require.NoError(t, err)
		require.NotNil(t, item)

		ctx2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel2()
		item2, err2 := p.GetWithContext(ctx2)
		require.ErrorIs(t, err2, context.DeadlineExceeded)
		require.Nil(t, item2)
	})

	t.Run("Release", func(t *testing.T) {
		p := anet.NewPool(1, factory, addr)
		require.NotNil(t, p)
		defer p.Close()

		item, err := p.Get()
		require.NoError(t, err)
		require.NotNil(t, item)

		require.Equal(t, 1, p.Len())
		p.Release(item)
		require.Equal(t, 0, p.Len())
	})

	t.Run("Cap", func(t *testing.T) {
		p := anet.NewPool(5, factory, addr)
		require.NotNil(t, p)
		defer p.Close()
		require.Equal(t, 5, p.Cap())
	})

	t.Run("Factory Error", func(t *testing.T) {
		// Rename unused parameter addr to _
		errorFactory := func(_ string) (anet.PoolItem, error) {
			// Use errors.New for static error message
			return nil, errors.New("factory error")
		}
		p := anet.NewPool(1, errorFactory, addr)
		require.NotNil(t, p)
		defer p.Close()

		item, err := p.Get()
		require.Error(t, err)
		require.Nil(t, item)
		require.Contains(t, err.Error(), "factory error")
	})

	t.Run("GetWithContext Factory Error", func(t *testing.T) {
		// Rename unused parameter addr to _
		errorFactory := func(_ string) (anet.PoolItem, error) {
			// Use errors.New for static error message
			return nil, errors.New("factory error ctx")
		}
		p := anet.NewPool(1, errorFactory, addr)
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
		p := anet.NewPool(1, factory, addr)
		require.NotNil(t, p)

		item, err := p.Get()
		require.NoError(t, err)
		require.NotNil(t, item)
		require.Equal(t, 1, p.Len())

		p.Close() // Close the pool

		p.Put(item) // Try putting back into closed pool
		require.Equal(t, 0, p.Len())
	})

	t.Run("Release nil", func(t *testing.T) {
		p := anet.NewPool(1, factory, addr)
		require.NotNil(t, p)
		defer p.Close()
		p.Release(nil)
		require.Equal(t, 0, p.Len())
	})

	t.Run("NewPoolList", func(t *testing.T) {
		addrs := []string{addr, addr}
		pools := anet.NewPoolList(2, factory, addrs)
		require.Len(t, pools, 2)
		for _, p := range pools {
			require.NotNil(t, p)
			require.Equal(t, 2, p.Cap())
			p.Close()
		}
	})
}

func TestBroker(t *testing.T) {
	// Don't run in parallel at the top level
	addr, stop, err := StartTestServer()
	require.NoError(t, err)
	defer func() { _ = stop() }() // Ignore error from stop

	// Sleep to ensure the server is fully ready
	time.Sleep(100 * time.Millisecond)

	factory := func(addr string) (anet.PoolItem, error) {
		return net.Dial("tcp", addr)
	}

	t.Run("NewBroker", func(t *testing.T) {
		p := anet.NewPool(1, factory, addr)
		require.NotNil(t, p)
		broker := anet.NewBroker([]anet.Pool{p}, 1, nil)
		require.NotNil(t, broker)
		broker.Close()
		p.Close()
	})

	t.Run("Send and Receive", func(t *testing.T) {
		p := anet.NewPool(2, factory, addr)
		require.NotNil(t, p)
		defer p.Close()

		broker := anet.NewBroker([]anet.Pool{p}, 2, nil)
		require.NotNil(t, broker)
		defer broker.Close()

		// Start broker workers in background
		go func() {
			err := broker.Start()
			require.True(t, err == nil || err == anet.ErrQuit)
		}()

		time.Sleep(50 * time.Millisecond) // Give broker time to start

		msg := []byte("hello broker")
		resp, err := broker.Send(&msg)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, msg, resp)
	})

	t.Run("SendContext Success", func(t *testing.T) {
		p := anet.NewPool(1, factory, addr)
		require.NotNil(t, p)
		defer p.Close()

		broker := anet.NewBroker([]anet.Pool{p}, 1, nil)
		require.NotNil(t, broker)
		defer broker.Close()

		// Start broker workers in background
		go func() {
			err := broker.Start()
			require.True(t, err == nil || err == anet.ErrQuit)
		}()

		time.Sleep(50 * time.Millisecond) // Give broker time to start

		msg := []byte("hello context")
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		resp, err := broker.SendContext(ctx, &msg)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, msg, resp)
	})

	t.Run("SendContext Timeout", func(t *testing.T) {
		// Create a factory that intentionally blocks until context times out
		slowFactory := func(addr string) (anet.PoolItem, error) {
			time.Sleep(200 * time.Millisecond)
			return net.Dial("tcp", addr)
		}

		p := anet.NewPool(1, slowFactory, addr)
		require.NotNil(t, p)
		defer p.Close()

		broker := anet.NewBroker([]anet.Pool{p}, 1, nil)
		require.NotNil(t, broker)
		defer broker.Close()

		// Start broker workers in background
		go func() {
			err := broker.Start()
			require.True(t, err == nil || err == anet.ErrQuit)
		}()

		time.Sleep(50 * time.Millisecond) // Give broker time to start

		// Get the only connection in the pool to force timeout
		item, err := p.Get()
		require.NoError(t, err)
		defer p.Put(item)

		// Now try with a very short timeout that should expire
		msg := []byte("hello timeout")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		resp, err := broker.SendContext(ctx, &msg)
		require.Error(t, err)
		require.True(t, errors.Is(err, context.DeadlineExceeded))
		require.Nil(t, resp)
	})

	t.Run("Send on Closed Broker", func(t *testing.T) {
		p := anet.NewPool(1, factory, addr)
		require.NotNil(t, p)
		defer p.Close()

		broker := anet.NewBroker([]anet.Pool{p}, 1, nil)
		require.NotNil(t, broker)
		broker.Close()

		msg := []byte("hello closed")
		resp, err := broker.Send(&msg)
		require.ErrorIs(t, err, anet.ErrClosingBroker)
		require.Nil(t, resp)
	})

	t.Run("Send with Pool Error", func(t *testing.T) {
		// Rename unused parameter addr to _
		errorFactory := func(_ string) (anet.PoolItem, error) {
			// Use errors.New for static error message
			return nil, errors.New("pool connection error")
		}
		p := anet.NewPool(1, errorFactory, addr)
		require.NotNil(t, p)
		defer p.Close()

		broker := anet.NewBroker([]anet.Pool{p}, 1, nil)
		require.NotNil(t, broker)
		defer broker.Close()

		// Start broker workers in background
		go func() {
			err := broker.Start()
			require.True(t, err == nil || err == anet.ErrQuit)
		}()

		time.Sleep(50 * time.Millisecond) // Give broker time to start

		msg := []byte("hello pool error")
		resp, err := broker.Send(&msg)
		require.Error(t, err)
		require.Nil(t, resp)
		require.Contains(t, err.Error(), "pool connection error")
	})

	t.Run("Send with Multiple Pools", func(t *testing.T) {
		p1 := anet.NewPool(1, factory, addr)
		require.NotNil(t, p1)
		defer p1.Close()
		p2 := anet.NewPool(1, factory, addr)
		require.NotNil(t, p2)
		defer p2.Close()

		broker := anet.NewBroker([]anet.Pool{p1, p2}, 2, nil)
		require.NotNil(t, broker)
		defer broker.Close()

		// Start broker workers in background
		go func() {
			err := broker.Start()
			require.True(t, err == nil || err == anet.ErrQuit)
		}()

		time.Sleep(50 * time.Millisecond) // Give broker time to start

		msg1 := []byte("pool 1")
		resp1, err1 := broker.Send(&msg1)
		require.NoError(t, err1)
		require.Equal(t, msg1, resp1)

		msg2 := []byte("pool 2")
		resp2, err2 := broker.Send(&msg2)
		require.NoError(t, err2)
		require.Equal(t, msg2, resp2)
	})

	t.Run("Broker Close Idempotency", func(t *testing.T) {
		p := anet.NewPool(1, factory, addr)
		require.NotNil(t, p)
		defer p.Close()

		broker := anet.NewBroker([]anet.Pool{p}, 1, nil)
		require.NotNil(t, broker)
		broker.Close()
		broker.Close() // Should be safe to call multiple times
	})
}

func TestUtils(t *testing.T) {
	// Don't run in parallel at the top level
	addr, stop, err := StartTestServer()
	require.NoError(t, err)
	defer func() { _ = stop() }() // Ignore error from stop

	t.Run("Write Success", func(t *testing.T) {
		// Create a fresh connection for this test
		conn, err := net.Dial("tcp", addr)
		require.NoError(t, err)
		defer func() { _ = conn.Close() }() // Ignore close error in defer

		msg := []byte("hello utils write")
		err = anet.Write(conn, msg)
		require.NoError(t, err)

		// Read response from the echo server
		responseMsg, err := anet.Read(conn)
		require.NoError(t, err)
		require.Equal(t, msg, responseMsg)
	})

	t.Run("Read Success", func(t *testing.T) {
		// Create a fresh connection for this test
		conn, err := net.Dial("tcp", addr)
		require.NoError(t, err)
		defer func() { _ = conn.Close() }() // Ignore close error in defer

		msg := []byte("hello utils read")
		err = anet.Write(conn, msg)
		require.NoError(t, err)

		readMsg, err := anet.Read(conn)
		require.NoError(t, err)
		require.Equal(t, msg, readMsg)
	})

	t.Run("Write Error (Closed Conn)", func(t *testing.T) {
		// Create a fresh connection just to close it
		conn, err := net.Dial("tcp", addr)
		require.NoError(t, err)
		err = conn.Close()
		require.NoError(t, err)

		msg := []byte("write error")
		err = anet.Write(conn, msg)
		require.Error(t, err)
	})

	t.Run("Read Error (Closed Conn)", func(t *testing.T) {
		// Create a fresh connection just to close it
		conn, err := net.Dial("tcp", addr)
		require.NoError(t, err)
		err = conn.Close()
		require.NoError(t, err)

		_, err = anet.Read(conn)
		require.Error(t, err)
	})
}

func BenchmarkBrokerSend(b *testing.B) {
	addr, stop, err := StartTestServer()
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = stop() }() // Ignore error from stop

	worker_num := []int{1, 10, 50, 100, 500, 1000}
	factory := func(addr string) (anet.PoolItem, error) {
		return net.Dial("tcp", addr)
	}

	p := anet.NewPool(1, factory, addr)
	require.NotNil(b, p)
	defer p.Close()

	for _, i := range worker_num {
		b.Run(fmt.Sprintf("Workers %d", i), func(b *testing.B) {
			broker := anet.NewBroker([]anet.Pool{p}, i, nil)
			require.NotNil(b, p)

			msg := []byte("test")
			wg := &sync.WaitGroup{}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				wg.Add(1)
				go func(wg *sync.WaitGroup) {
					defer wg.Done()
					_, err := broker.Send(&msg)
					require.NoError(b, err)
				}(wg)
			}
			wg.Wait()
		})
	}
}

func BenchmarkPool(b *testing.B) {
	addr, stop, err := StartTestServer()
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = stop() }() // Ignore error from stop

	worker_num := []int{1}
	factory := func(addr string) (anet.PoolItem, error) {
		return net.Dial("tcp", addr)
	}

	for _, i := range worker_num {
		p := anet.NewPool(1, factory, addr)
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
