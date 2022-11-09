package anet_test

import (
	"context"
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
	l, err := net.Listen("tcp", ":")
	if err != nil {
		return "", nil, err
	}
	go func() {
		for {
			select {
			case <-quit:
				l.Close()
				return
			default:
				conn, err := l.Accept()
				if err != nil {
					return
				}
				go func(conn net.Conn) {
					defer conn.Close()

					select {
					case <-quit:
						return
					default:
						_, err := io.Copy(conn, conn)
						if err != nil {
							log.Fatal(err)
							return
						}
					}
				}(conn)
			}
		}
	}()
	return l.Addr().String(), func() error {
		close(quit)
		return l.Close()
	}, err
}

func TestPool(t *testing.T) {
	addr, stop, err := StartTestServer()
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	factory := func(addr string) anet.Factory {
		return func() (anet.PoolItem, error) {
			return net.Dial("tcp", addr)
		}
	}

	t.Run("NewPool", func(t *testing.T) {
		p := anet.NewPool(1, factory(addr))
		require.NotNil(t, p)
		defer p.Close()
	})

	t.Run("Get Len Put", func(t *testing.T) {
		p := anet.NewPool(1, factory(addr))
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
		p := anet.NewPool(1, factory(addr))
		require.NotNil(t, p)
		p.Close()

		require.Equal(t, 0, p.Len())

		item, err := p.Get()
		require.Error(t, err)
		require.Nil(t, item)
	})

	t.Run("GetWithContext", func(t *testing.T) {
		p := anet.NewPool(1, factory(addr))
		require.NotNil(t, p)
		defer p.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		item, err := p.GetWithContext(ctx)
		require.NoError(t, err)
		require.NotNil(t, item)

		item, err = p.GetWithContext(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, item)
	})

	t.Run("Release", func(t *testing.T) {
		p := anet.NewPool(1, factory(addr))
		require.NotNil(t, p)
		defer p.Close()

		item, err := p.Get()
		require.NoError(t, err)
		require.NotNil(t, item)

		require.Equal(t, 1, p.Len())
		p.Release(item)
		require.Equal(t, 0, p.Len())
	})

}

func BenchmarkBrokerSend(b *testing.B) {
	addr, stop, err := StartTestServer()
	if err != nil {
		b.Fatal(err)
	}
	defer stop()

	worker_num := []int{1, 10, 50, 100, 500, 1000}
	factory := func(addr string) anet.Factory {
		return func() (anet.PoolItem, error) {
			return net.Dial("tcp", addr)
		}
	}

	p := anet.NewPool(1, factory(addr))
	require.NotNil(b, p)
	defer p.Close()

	for _, i := range worker_num {
		b.Run(fmt.Sprintf("Workers %d", i), func(b *testing.B) {
			broker := anet.NewBroker(p, i, nil)
			require.NotNil(b, p)

			go broker.Start()

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
	defer stop()

	worker_num := []int{1}
	factory := func(addr string) anet.Factory {
		return func() (anet.PoolItem, error) {
			return net.Dial("tcp", addr)
		}
	}

	for _, i := range worker_num {
		p := anet.NewPool(1, factory(addr))
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
