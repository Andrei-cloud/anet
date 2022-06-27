package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"runtime"
	"time"

	"github.com/andrei-cloud/anet"
	log "github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

const workers = 2

type loggerWrapper struct {
	l log.Logger
}

func (l *loggerWrapper) Log(keyvals ...interface{}) error {
	l.l.Print(keyvals...)
	return nil
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	logger := loggerWrapper{
		l: log.New(os.Stdout).With().Timestamp().Logger(),
	}

	factory := func(addr string) anet.Factory {
		return func() (anet.PoolItem, error) {
			return net.Dial("tcp", addr)
		}
	}

	logger.Log("info", "initializing pool")
	p := anet.NewPool(workers, factory(":3456"))

	logger.Log("info", "initializing broker")
	broker := anet.NewBroker(p, workers, &logger)
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
				logger.Log("info", "sending request to broker")
				start := time.Now()
				resp, err := broker.Send(request)
				if err != nil {
					return err
				}

				logger.Log("latency", time.Since(start), "response", string(resp))
				return nil
			}(i)
		})
	}

	wg.Wait()
	logger.Log("finishedin", time.Since(start))
}
