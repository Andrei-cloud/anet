package server

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/andrei-cloud/anet"
	"github.com/stretchr/testify/require"
)

// panicEchoHandler panics on the "panic" request, otherwise echoes.
func panicEchoHandler() Handler {
	return HandlerFunc(func(_ *ServerConn, req []byte) ([]byte, error) {
		if string(req) == "panic" {
			panic(errors.New("handler exploded"))
		}
		return req, nil
	})
}

// TestServerHandlerPanicSurvives: a panicking handler must not take the
// process down nor wedge the connection for later messages.
func TestServerHandlerPanicSurvives(t *testing.T) {
	t.Parallel()

	srv, err := NewServer("127.0.0.1:0", panicEchoHandler(), nil)
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	defer func() { _ = srv.Stop() }()

	addr := srv.listener.Addr().String()
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// A request the handler panics on produces no response.
	bad := append([]byte{9, 9, 9, 9}, []byte("panic")...)
	require.NoError(t, anet.Write(conn, bad))

	// The connection must still serve an echo afterwards.
	good := append([]byte{1, 2, 3, 4}, []byte("alive")...)
	require.NoError(t, anet.Write(conn, good))
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	resp, err := anet.Read(conn)
	require.NoError(t, err)
	require.Equal(t, []byte("alive"), resp[4:])
}

// TestServerStopWhileAccepting hammers dials across Stop: every accepted
// connection must end closed and Stop must return promptly (regression for
// the store-after-Range leak and the Add-vs-Wait misuse window).
func TestServerStopWhileAccepting(t *testing.T) {
	for round := 0; round < 4; round++ {
		srv, err := NewServer("127.0.0.1:0", panicEchoHandler(), &ServerConfig{
			ShutdownTimeout: 500 * time.Millisecond,
		})
		require.NoError(t, err)
		require.NoError(t, srv.Start())
		addr := srv.listener.Addr().String()

		var dialers sync.WaitGroup
		for i := 0; i < 24; i++ {
			dialers.Add(1)
			go func() {
				defer dialers.Done()
				c, derr := net.Dial("tcp", addr)
				if derr != nil {
					return
				}
				req := append([]byte{1, 2, 3, 4}, []byte("hi")...)
				_ = anet.Write(c, req)
				time.Sleep(20 * time.Millisecond)
				_ = c.Close()
			}()
		}
		time.Sleep(15 * time.Millisecond)

		start := time.Now()
		require.NoError(t, srv.Stop())
		if d := time.Since(start); d > 4*time.Second {
			t.Fatalf("Stop took %v", d)
		}
		dialers.Wait()

		// Connections admitted across Stop must drain, not linger forever.
		deadline := time.Now().Add(3 * time.Second)
		var n int32 = -1
		for time.Now().Before(deadline) {
			n = srv.activeConnCount.Load()
			if n == 0 {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		require.Equal(t, int32(0), n, "connections outlived Stop")
	}
}

// TestServerWriterBurstBatching sends many concurrent requests on one
// connection and requires every response to arrive intact (the batching
// writer must not corrupt or drop frames).
func TestServerWriterBurstBatching(t *testing.T) {
	t.Parallel()

	srv, err := NewServer("127.0.0.1:0", HandlerFunc(
		func(_ *ServerConn, req []byte) ([]byte, error) { return req, nil }), nil)
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	defer func() { _ = srv.Stop() }()

	conn, err := net.Dial("tcp", srv.listener.Addr().String())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	const n = 300
	var sent atomic.Int32
	for i := 0; i < n; i++ {
		frame := make([]byte, 0, 8)
		frame = append(frame, byte(i>>8), byte(i), 'a', 'b', 'c', 'd')
		if werr := anet.Write(conn, frame); werr != nil {
			t.Fatalf("write %d: %v", i, werr)
		}
		sent.Add(1)
	}

	got := make(map[uint16]bool, n)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	for i := 0; i < n; i++ {
		resp, rerr := anet.Read(conn)
		require.NoError(t, rerr)
		require.Len(t, resp, 6)
		got[uint16(resp[0])<<8|uint16(resp[1])] = true
	}
	require.Len(t, got, n, "every burst response must arrive exactly once")
}
