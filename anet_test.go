package anet

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPool(t *testing.T) {
	factory := func(addr string) Factory {
		return func() (PoolItem, error) {
			return net.Dial("tcp", addr)
		}
	}

	addr, stop := SpinTestServer()
	defer stop()

	t.Run("NewPool", func(t *testing.T) {
		p := NewPool(1, factory(addr))
		require.NotNil(t, p)
		defer p.Close()
	})

	t.Run("Get Len Put", func(t *testing.T) {
		p := NewPool(1, factory(addr))
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
		p := NewPool(1, factory(addr))
		require.NotNil(t, p)
		p.Close()

		require.Equal(t, 0, p.Len())

		item, err := p.Get()
		require.Error(t, err)
		require.Nil(t, item)
	})

	t.Run("GetWithContext", func(t *testing.T) {
		p := NewPool(1, factory(addr))
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
		p := NewPool(1, factory(addr))
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
	worker_num := []int{1}
	factory := func(addr string) Factory {
		return func() (PoolItem, error) {
			return net.Dial("tcp", addr)
		}
	}

	addr, stop := SpinTestServer()
	defer stop()

	p := NewPool(3, factory(addr))
	require.NotNil(b, p)
	defer p.Close()

	for _, i := range worker_num {
		b.Run(fmt.Sprintf("Workers %d", i), func(b *testing.B) {
			broker := NewBroker(p, i, nil)
			require.NotNil(b, p)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go broker.Start(ctx)

			msg := []byte("test")
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				broker.Send(&msg)
			}
		})
	}
}

func BenchmarkPool(b *testing.B) {
	worker_num := []int{1, 50, 100, 1000, 5000}
	factory := func(addr string) Factory {
		return func() (PoolItem, error) {
			return net.Dial("tcp", addr)
		}
	}

	addr, stop := SpinTestServer()
	defer stop()

	for _, i := range worker_num {
		p := NewPool(1, factory(addr))
		require.NotNil(b, p)
		b.Run(fmt.Sprintf("Workers %d", i), func(b *testing.B) {
			benchmarkPool(p, b)
		})
		p.Close()
	}
}

func benchmarkPool(p *pool, b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item, _ := p.Get()
		p.Put(item)
	}
}
