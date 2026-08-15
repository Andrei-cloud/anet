package server

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/andrei-cloud/anet"
	"github.com/stretchr/testify/require"
)

func TestServerEcho(t *testing.T) {
	t.Parallel()

	handler := HandlerFunc(func(_ *ServerConn, req []byte) ([]byte, error) {
		return req, nil
	})
	srv, err := NewServer("127.0.0.1:0", handler, nil)
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	defer func() {
		_ = srv.Stop()
	}()

	addr := srv.listener.Addr().String()

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	payload := []byte("hello")
	taskID := [4]byte{0x01, 0x02, 0x03, 0x04}
	msg := append(taskID[:], payload...)

	require.NoError(t, anet.Write(conn, msg))
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))

	resp, err := anet.Read(conn)
	require.NoError(t, err)
	require.True(t, len(resp) >= 4)
	require.Equal(t, taskID[:], resp[:4])
	require.Equal(t, payload, resp[4:])
}

func TestServerLifecycle(t *testing.T) {
	handler := HandlerFunc(func(_ *ServerConn, req []byte) ([]byte, error) {
		return req, nil
	})
	srv, err := NewServer("127.0.0.1:0", handler, &ServerConfig{
		ShutdownTimeout: 100 * time.Millisecond,
	})
	require.NoError(t, err)
	require.NoError(t, srv.Start())

	addr := srv.listener.Addr().String()
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)

	require.NoError(t, srv.Stop())
	_ = conn.Close()
}

func TestServerHandlerErrors(t *testing.T) {
	handler := HandlerFunc(func(_ *ServerConn, req []byte) ([]byte, error) {
		if string(req) == "error" {
			return nil, errors.New("handler failure")
		}
		if string(req) == "nil" {
			return nil, nil
		}
		return req, nil
	})
	srv, err := NewServer("127.0.0.1:0", handler, nil)
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	defer func() { _ = srv.Stop() }()

	addr := srv.listener.Addr().String()
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// Send nil-response message
	taskID := [4]byte{0x01, 0x02, 0x03, 0x04}
	msg := append(taskID[:], []byte("nil")...)
	require.NoError(t, anet.Write(conn, msg))

	// Send error-response message
	msg = append(taskID[:], []byte("error")...)
	require.NoError(t, anet.Write(conn, msg))

	// Send valid echo to verify connection is still functioning
	msg = append(taskID[:], []byte("echo")...)
	require.NoError(t, anet.Write(conn, msg))

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
	resp, err := anet.Read(conn)
	require.NoError(t, err)
	require.Equal(t, []byte("echo"), resp[4:])
}

func BenchmarkServer_Echo_Sequential(b *testing.B) {
	handler := HandlerFunc(func(_ *ServerConn, req []byte) ([]byte, error) {
		return req, nil
	})
	srv, err := NewServer("127.0.0.1:0", handler, nil)
	if err != nil {
		b.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = srv.Stop() }()

	conn, err := net.Dial("tcp", srv.listener.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	taskID := [4]byte{0x01, 0x02, 0x03, 0x04}
	payload := []byte("bench message")
	msg := append(taskID[:], payload...)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := anet.Write(conn, msg); err != nil {
			b.Fatal(err)
		}
		resp, err := anet.Read(conn)
		if err != nil {
			b.Fatal(err)
		}
		if len(resp) < 4 {
			b.Fatal("invalid response")
		}
	}
}

func BenchmarkServer_Echo_Parallel(b *testing.B) {
	handler := HandlerFunc(func(_ *ServerConn, req []byte) ([]byte, error) {
		return req, nil
	})
	srv, err := NewServer("127.0.0.1:0", handler, nil)
	if err != nil {
		b.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		b.Fatal(err)
	}
	defer func() { _ = srv.Stop() }()

	addr := srv.listener.Addr().String()

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			b.Errorf("dial error: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

		taskID := [4]byte{0x01, 0x02, 0x03, 0x04}
		payload := []byte("bench message")
		msg := append(taskID[:], payload...)

		for pb.Next() {
			if err := anet.Write(conn, msg); err != nil {
				b.Errorf("write error: %v", err)
				return
			}
			resp, err := anet.Read(conn)
			if err != nil {
				b.Errorf("read error: %v", err)
				return
			}
			if len(resp) < 4 {
				b.Errorf("invalid response")
				return
			}
		}
	})
}
