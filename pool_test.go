package anet

import (
	"context"
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

	addr, stop := spinTestServer()
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
