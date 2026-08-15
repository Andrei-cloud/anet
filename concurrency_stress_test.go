package anet_test

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/andrei-cloud/anet"
	"github.com/stretchr/testify/require"
)

// TestConcurrency_BufferPool hammers the global buffer pool across all size classes.
func TestConcurrency_BufferPool(t *testing.T) {
	t.Parallel()

	const numGoroutines = 50
	const iterations = 500

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(gID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(gID + 1)))

			for i := 0; i < iterations; i++ {
				// Random size from 1B to 70KB
				sz := rng.Intn(70*1024) + 1
				buf := anet.GetBuffer(sz)
				if len(buf) != sz {
					t.Errorf("GetBuffer(%d) returned slice of len %d", sz, len(buf))
					return
				}
				// Write pattern
				if sz > 0 {
					buf[0] = 0xAA
					buf[sz-1] = 0xBB
				}
				anet.PutBuffer(buf)
			}
		}(g)
	}

	wg.Wait()
}

// TestConcurrency_PoolStress tests connection pool concurrent operations and close races.
func TestConcurrency_PoolStress(t *testing.T) {
	t.Parallel()

	addr, stop, err := StartTestServer()
	require.NoError(t, err)
	defer func() { _ = stop() }()

	factory := func(a string) (anet.PoolItem, error) {
		conn, err := net.DialTimeout("tcp", a, 500*time.Millisecond)
		if err != nil {
			return nil, err
		}
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		return conn, nil
	}

	p := anet.NewPool(20, factory, addr, &anet.PoolConfig{
		ValidationStrategy: anet.ValidationNone,
	})
	require.NotNil(t, p)

	const numWorkers = 30
	const opsPerWorker = 50

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for w := 0; w < numWorkers; w++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				item, err := p.GetWithContext(ctx)
				cancel()

				if err != nil {
					continue
				}
				if item == nil {
					continue
				}

				if i%5 == 0 {
					p.Release(item)
				} else {
					p.Put(item)
				}
			}
		}(w)
	}

	// Close the pool concurrently while workers are operating
	time.Sleep(20 * time.Millisecond)
	p.Close()
	p.Close() // Idempotent close check

	wg.Wait()
}

// TestConcurrency_BrokerStress tests broker under concurrent request load, timeouts, and cancellations.
func TestConcurrency_BrokerStress(t *testing.T) {
	t.Parallel()

	addr, stop, err := StartTestServer()
	require.NoError(t, err)
	defer func() { _ = stop() }()

	factory := func(a string) (anet.PoolItem, error) {
		conn, err := net.DialTimeout("tcp", a, 500*time.Millisecond)
		if err != nil {
			return nil, err
		}
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		return conn, nil
	}

	p := anet.NewPool(20, factory, addr, nil)
	require.NotNil(t, p)
	defer p.Close()

	brokerConfig := &anet.BrokerConfig{
		WriteTimeout:   500 * time.Millisecond,
		ReadTimeout:    500 * time.Millisecond,
		QueueSize:      500,
		OptimizeMemory: true,
	}

	broker := anet.NewBroker([]anet.Pool{p}, 10, nil, brokerConfig)
	require.NotNil(t, broker)

	go func() { _ = broker.Start() }()
	defer broker.Close()

	const clients = 30
	const requestsPerClient = 30

	var successCount atomic.Uint64
	var errorCount atomic.Uint64
	var wg sync.WaitGroup
	wg.Add(clients)

	for c := 0; c < clients; c++ {
		go func(clientID int) {
			defer wg.Done()
			for r := 0; r < requestsPerClient; r++ {
				req := []byte(fmt.Sprintf("client-%d-req-%d", clientID, r))

				var resp []byte
				var err error

				if r%2 == 0 {
					ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
					resp, err = broker.SendContext(ctx, &req)
					cancel()
				} else {
					resp, err = broker.Send(&req)
				}

				if err != nil {
					errorCount.Add(1)
				} else {
					if string(resp) != string(req) {
						t.Errorf("mismatched response: got %s, want %s", string(resp), string(req))
					}
					successCount.Add(1)
				}
			}
		}(c)
	}

	wg.Wait()
	require.True(t, successCount.Load() > 0, "expected at least some successful broker sends")
}
