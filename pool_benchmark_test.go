package anet_test

import (
	"testing"

	"github.com/andrei-cloud/anet"
)

// dummyItem implements PoolItem for benchmark tests.
type dummyItem struct{}

// Close implements PoolItem.Close.
func (d *dummyItem) Close() error {
	return nil
}

// BenchmarkNewPool_GetPut measures performance of pool Get and Put operations.
func BenchmarkNewPool_GetPut(b *testing.B) {
	factory := func(_ string) (anet.PoolItem, error) {
		return &dummyItem{}, nil
	}
	p := anet.NewPool(64, factory, "addr", nil)
	defer p.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item, err := p.Get()
		if err != nil {
			b.Fatalf("get error: %v", err)
		}

		p.Put(item)
	}
}
