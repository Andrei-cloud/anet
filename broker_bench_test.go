package anet

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
)

// mockPipeServer simulates a server that echoes messages back over a net.Pipe.
// It reads the length header, reads the payload, and then writes them back.
func mockPipeServer(c net.Conn) {
	defer c.Close()
	header := make([]byte, LENGTHSIZE)
	payload := make([]byte, 65535)

	for {
		// Read header
		if _, err := io.ReadFull(c, header); err != nil {
			return
		}

		var length int
		switch LENGTHSIZE {
		case 2:
			length = int(binary.BigEndian.Uint16(header))
		case 4:
			length = int(binary.BigEndian.Uint32(header))
		}

		// Read payload
		if _, err := io.ReadFull(c, payload[:length]); err != nil {
			return
		}

		// Echo back
		if _, err := c.Write(header); err != nil {
			return
		}
		if _, err := c.Write(payload[:length]); err != nil {
			return
		}
	}
}

// pipeFactory creates a client-server pipe pair and starts the mock server.
func pipeFactory(_ string) (PoolItem, error) {
	client, server := net.Pipe()
	go mockPipeServer(server)
	return client, nil
}

func BenchmarkBroker_Pipe_Sequential(b *testing.B) {
	poolConfig := DefaultPoolConfig()
	poolConfig.ValidationStrategy = ValidationNone

	p := NewPool(1, pipeFactory, "pipe", poolConfig)

	brokerConfig := DefaultBrokerConfig()
	brokerConfig.OptimizeMemory = true

	broker := NewBroker([]Pool{p}, 1, &NoopLogger{}, brokerConfig)
	go func() { _ = broker.Start() }()
	defer broker.Close()

	req := []byte("hello world")
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := broker.Send(&req)
		if err != nil {
			b.Fatalf("Send failed: %v", err)
		}
	}
}

func BenchmarkBroker_Pipe_Parallel(b *testing.B) {
	poolConfig := DefaultPoolConfig()
	poolConfig.ValidationStrategy = ValidationNone

	p := NewPool(100, pipeFactory, "pipe", poolConfig)

	brokerConfig := DefaultBrokerConfig()
	brokerConfig.OptimizeMemory = true
	brokerConfig.QueueSize = 10000

	broker := NewBroker([]Pool{p}, 100, &NoopLogger{}, brokerConfig)
	go func() { _ = broker.Start() }()
	defer broker.Close()

	req := []byte("hello world")
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := broker.Send(&req)
			if err != nil {
				if err != ErrQueueFull && err != ErrClosingBroker {
					b.Errorf("Send failed: %v", err)
				}
			}
		}
	})
}

func BenchmarkBroker_Pipe_SendContext(b *testing.B) {
	poolConfig := DefaultPoolConfig()
	poolConfig.ValidationStrategy = ValidationNone

	p := NewPool(1, pipeFactory, "pipe", poolConfig)

	brokerConfig := DefaultBrokerConfig()
	brokerConfig.OptimizeMemory = true

	broker := NewBroker([]Pool{p}, 1, &NoopLogger{}, brokerConfig)
	go func() { _ = broker.Start() }()
	defer broker.Close()

	req := []byte("hello world")
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := broker.SendContext(ctx, &req)
		if err != nil {
			b.Fatalf("Send failed: %v", err)
		}
	}
}

func BenchmarkWrite_Small(b *testing.B) {
	pipeR, pipeW := net.Pipe()
	defer pipeR.Close()
	defer pipeW.Close()

	go func() {
		buf := make([]byte, 1024)
		for {
			if _, err := pipeR.Read(buf); err != nil {
				return
			}
		}
	}()

	msg := []byte("benchmark test message payload")
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := Write(pipeW, msg); err != nil {
			b.Fatalf("Write failed: %v", err)
		}
	}
}

func BenchmarkBufferPool_GetPut(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf := GetBuffer(256)
		PutBuffer(buf)
	}
}
