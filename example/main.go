// Package main provides an example of using the anet library.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
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

// tcpConnectionFactory creates new TCP connections for the connection pool.
// It implements proper timeouts and TCP keepalive settings.
func tcpConnectionFactory(addr string) (anet.PoolItem, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", addr, err)
	}

	poolItem, ok := conn.(anet.PoolItem)
	if !ok {
		return nil, errors.New("failed to assert net.Conn as anet.PoolItem")
	}

	return poolItem, nil
}

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
	pools := anet.NewPoolList(poolCap, tcpConnectionFactory, []string{addr}, nil)
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

	time.Sleep(200 * time.Millisecond)
}
