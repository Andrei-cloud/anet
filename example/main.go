// Package main provides an example of using the anet library.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/andrei-cloud/anet"
	"github.com/andrei-cloud/anet/server"
)

// loggerWrapper adapts the standard log.Logger to satisfy anet.Logger interface.
type loggerWrapper struct {
	*log.Logger
}

// Logger interface implementation.
func (lw *loggerWrapper) Printf(format string, v ...any) {
	lw.Logger.Printf(format, v...)
}

func (lw *loggerWrapper) Print(v ...any) {
	lw.Logger.Print(v...)
}

func (lw *loggerWrapper) Debugf(format string, v ...any) {
	lw.Printf("[DEBUG] "+format, v...)
}

func (lw *loggerWrapper) Infof(format string, v ...any) {
	lw.Printf(format, v...)
}

func (lw *loggerWrapper) Warnf(format string, v ...any) {
	lw.Printf("[WARN] "+format, v...)
}

func (lw *loggerWrapper) Errorf(format string, v ...any) {
	lw.Printf("[ERROR] "+format, v...)
}

// poolFactory dials TCP connections with the config's DialTimeout plus TCP
// keepalive and TCP_NODELAY wired from the same PoolConfig, via the library
// helper (previously this example hand-rolled a dialer and the config's
// keepalive settings were dead fields).
var poolFactory = anet.NewTCPFactory(&anet.PoolConfig{
	DialTimeout:       5 * time.Second,
	KeepAliveInterval: 30 * time.Second,
})

// startServer initializes and starts the anet TCP server.
func startServer(addr string) (*server.Server, error) {
	handler := server.HandlerFunc(func(_ *server.ServerConn, req []byte) ([]byte, error) {
		// reverse request data.
		out := make([]byte, len(req))
		for i := range req {
			out[len(req)-1-i] = req[i]
		}

		return out, nil
	})
	srv, err := server.NewServer(addr, handler, nil)
	if err != nil {
		return nil, fmt.Errorf("server setup failed: %w", err)
	}

	if err := srv.Start(); err != nil {
		return nil, fmt.Errorf("server failed to start: %w", err)
	}

	return srv, nil
}

// newBroker configures and starts an anet broker for the given server address.
func newBroker(addr string) anet.Broker {
	poolCap := uint32(5)
	pools := anet.NewPoolList(poolCap, poolFactory, []string{addr}, nil)
	numWorkers := 3
	logger := &loggerWrapper{
		Logger: log.New(os.Stdout, "BROKER: ", log.LstdFlags|log.Lmicroseconds),
	}

	// Enable memory optimizations for better performance
	config := &anet.BrokerConfig{
		WriteTimeout:   5 * time.Second,
		ReadTimeout:    5 * time.Second,
		QueueSize:      1000,
		OptimizeMemory: true, // Enable memory optimizations
	}

	broker := anet.NewBroker(pools, numWorkers, logger, config)

	go func() {
		if err := broker.Start(); err != nil && err != anet.ErrQuit {
			log.Printf("broker failed: %v", err)
		}
	}()

	return broker
}

// sendRequests performs concurrent client requests through the broker.
func sendRequests(broker anet.Broker, requests []string) {
	var wg sync.WaitGroup
	for _, reqStr := range requests {
		wg.Add(1)
		go func(requestPayload string) {
			defer wg.Done()

			reqData := []byte(requestPayload)
			log.Printf("client sending: %s", requestPayload)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			respData, err := broker.SendContext(ctx, &reqData)
			if err != nil {
				log.Printf("client error sending '%s': %v", requestPayload, err)

				return
			}

			log.Printf("client received response for '%s': %s", requestPayload, string(respData))
		}(reqStr)
	}

	log.Println("client launched all requests.")
	wg.Wait()
	log.Println("client finished processing all responses.")
}

// sendAsyncRequests shows the asynchronous API: all requests are in flight
// without parking a goroutine per response wait; the caller collects the
// Responses in completion order.
func sendAsyncRequests(broker anet.Broker, requests []string) error {
	type pending struct {
		label string
		ch    <-chan anet.Response
	}

	var outstanding []pending
	for _, reqStr := range requests {
		reqData := []byte(reqStr)
		ch, err := broker.SendAsync(&reqData)
		if err != nil {
			return fmt.Errorf("send %s: %w", reqStr, err)
		}
		outstanding = append(outstanding, pending{label: reqStr, ch: ch})
	}
	log.Printf("client has %d requests in flight", len(outstanding))

	for _, p := range outstanding {
		r := <-p.ch
		if r.Err != nil {
			log.Printf("client error for '%s': %v", p.label, r.Err)
			continue
		}
		log.Printf("client received response for '%s': %s", p.label, string(r.Payload))
	}
	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	addr := "localhost:3000"

	srv, err := startServer(addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	broker := newBroker(addr)

	defer func() {
		if err := srv.Stop(); err != nil {
			log.Printf("error stopping server: %v", err)
		}
	}()
	defer broker.Close()

	sendRequests(broker, []string{"hello", "world", "anet test", "concurrent", "request"})

	if err := sendAsyncRequests(broker, []string{"async one", "async two"}); err != nil {
		fmt.Fprintf(os.Stderr, "async sends failed: %v\n", err)
	}

	time.Sleep(200 * time.Millisecond)
}
