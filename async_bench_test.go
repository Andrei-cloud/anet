package anet_test

import (
	"fmt"
	"net"
	"sync/atomic"
	"testing"

	"github.com/andrei-cloud/anet"
)

// newAsyncBenchBroker builds a broker over the shared test echo server with
// the given transport shape: nconns pooled connections, workers queue
// workers, and multiplex on/off.
func newAsyncBenchBroker(b *testing.B, nconns, workers int, multiplex bool) anet.Broker {
	b.Helper()
	addr, stop, err := StartTestServer()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = stop() })

	poolCfg := anet.DefaultPoolConfig()
	poolCfg.ValidationStrategy = anet.ValidationNone
	pools := anet.NewPoolList(uint32(nconns), func(a string) (anet.PoolItem, error) {
		return net.DialTimeout("tcp", a, 2e9)
	}, []string{addr}, poolCfg)
	b.Cleanup(func() {
		for _, p := range pools {
			p.Close()
		}
	})

	bcfg := anet.DefaultBrokerConfig()
	bcfg.Multiplex = multiplex
	broker := anet.NewBroker(pools, workers, nil, bcfg)
	if !multiplex {
		go func() { _ = broker.Start() }()
	}
	b.Cleanup(broker.Close)
	return broker
}

// BenchmarkTransport_RoundTrip measures full request/response throughput
// (send + await echo) per transport shape: synchronous queue workers vs the
// async API on queue workers vs Multiplex pipelining with the same or fewer
// connections. Same 12-byte payload everywhere.
func BenchmarkTransport_RoundTrip(b *testing.B) {
	payload := []byte("roundtrip-pr")

	b.Run("Sync", func(b *testing.B) {
		broker := newAsyncBenchBroker(b, 100, 100, false)
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			req := payload
			for pb.Next() {
				if _, err := broker.Send(&req); err != nil {
					b.Fatal(err)
				}
			}
		})
	})

	b.Run("AsyncAwait", func(b *testing.B) {
		broker := newAsyncBenchBroker(b, 100, 100, false)
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			req := payload
			for pb.Next() {
				res, err := broker.SendAsync(&req)
				if err != nil {
					b.Fatal(err)
				}
				if r := <-res; r.Err != nil {
					b.Fatal(r.Err)
				}
			}
		})
	})

	for _, nconns := range []int{2, 8, 32} {
		b.Run(fmt.Sprintf("Multiplex_%dconn", nconns), func(b *testing.B) {
			broker := newAsyncBenchBroker(b, nconns, 0, true)
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				req := payload
				for pb.Next() {
					res, err := broker.SendAsync(&req)
					if err != nil {
						b.Fatal(err)
					}
					if r := <-res; r.Err != nil {
						b.Fatal(r.Err)
					}
				}
			})
		})
	}
}

// BenchmarkSendAsync_FireHundred measures burst submit + drain: each
// iteration submits 100 requests before awaiting any, the workload where
// "the caller never parks a goroutine per request" pays. Submits past the
// outstanding bound shed with ErrQueueFull (by design); sheds are reported
// as a metric and the iteration completes with what was accepted.
func BenchmarkSendAsync_FireHundred(b *testing.B) {
	req := []byte("firehundred-")

	var sheds atomic.Int64

	fire := func(b *testing.B, broker anet.Broker) func(pb *testing.PB) {
		return func(pb *testing.PB) {
			for pb.Next() {
				var chans [100]<-chan anet.Response
				fired := 0
				for range chans {
					res, err := broker.SendAsync(&req)
					if err != nil {
						if err == anet.ErrQueueFull {
							sheds.Add(1)
							continue // shed by backpressure, by design
						}
						b.Fatal(err)
					}
					chans[fired] = res
					fired++
				}
				for i := 0; i < fired; i++ {
					if r := <-chans[i]; r.Err != nil {
						b.Fatal(r.Err)
					}
				}
			}
		}
	}

	b.Run("Multiplex_8conn", func(b *testing.B) {
		broker := newAsyncBenchBroker(b, 8, 0, true)
		b.ReportAllocs()
		b.RunParallel(fire(b, broker))
		b.ReportMetric(float64(sheds.Load())/float64(b.N), "sheds/op")
	})

	b.Run("SyncQueue_100workers", func(b *testing.B) {
		broker := newAsyncBenchBroker(b, 100, 100, false)
		b.ReportAllocs()
		b.RunParallel(fire(b, broker))
		b.ReportMetric(float64(sheds.Load())/float64(b.N), "sheds/op")
	})
}
