package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"runtime"
	"time"

	"github.com/andrei-cloud/anet/broker"
	"github.com/andrei-cloud/anet/pool"
	"golang.org/x/sync/errgroup"
)

const workers = 2

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	fmt.Println("pooler")

	factory := func(addr string) pool.Factory {
		return func() (pool.PoolItem, error) {
			return net.Dial("tcp", addr)
		}
	}

	log.Println("initializing pool")
	p := pool.NewPool(workers, factory(":3456"))

	log.Println("initializing broker")
	broker := broker.NewBroker(p, workers)
	defer broker.Close()

	brokerCtx, stopBroker := context.WithCancel(context.Background())
	defer stopBroker()

	go broker.Start(brokerCtx)

	wg := errgroup.Group{}
	wg.SetLimit(workers)
	start := time.Now()
	for i := 0; i < 10000; i++ {
		i := i
		wg.Go(func() error {
			return func(i int) error {
				request := []byte(fmt.Sprintf("hello_%d", i))
				log.Println("sending request to broker")
				start := time.Now()
				resp, err := broker.Send(request)
				if err != nil {
					return err
				}

				log.Printf("response in %v\n", time.Since(start))
				log.Println(string(resp))
				return nil
			}(i)
		})
	}

	wg.Wait()
	log.Printf("finished in %v\n", time.Since(start))
}
