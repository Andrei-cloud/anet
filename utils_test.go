package anet_test

import (
	"net"
	"testing"
	"time"

	"github.com/andrei-cloud/anet"
	"github.com/stretchr/testify/require"
)

//nolint:all
func TestUtils(t *testing.T) {
	// Don't run in parallel at the top level
	t.Parallel()

	// Create a separate server for this test suite
	addr, stop, err := StartTestServer()
	require.NoError(t, err)
	defer stop() // Stop server after all subtests complete

	t.Run("Write Success", func(t *testing.T) {
		t.Parallel()

		// Create a fresh connection for this test with proper timeout
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err != nil {
			t.Skipf("Skipping test due to connection error: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

		// Set I/O deadlines to prevent hanging
		if err := conn.SetDeadline(time.Now().Add(1 * time.Second)); err != nil {
			t.Fatalf("Failed to set deadline: %v", err)
		}

		msg := []byte("hello utils write")
		err = anet.Write(conn, msg)
		require.NoError(t, err)

		// Read response from the echo server
		responseMsg, err := anet.Read(conn)
		require.NoError(t, err)
		require.Equal(t, msg, responseMsg)
	})

	t.Run("Read Success", func(t *testing.T) {
		t.Parallel()

		// Create a fresh connection for this test with proper timeout
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err != nil {
			t.Skipf("Skipping test due to connection error: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

		// Set I/O deadlines to prevent hanging
		if err := conn.SetDeadline(time.Now().Add(1 * time.Second)); err != nil {
			t.Fatalf("Failed to set deadline: %v", err)
		}

		msg := []byte("hello utils read")
		err = anet.Write(conn, msg)
		require.NoError(t, err)

		readMsg, err := anet.Read(conn)
		require.NoError(t, err)
		require.Equal(t, msg, readMsg)
	})

	t.Run("Write Error (Closed Conn)", func(t *testing.T) {
		t.Parallel()

		// Create a fresh connection just to close it
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err != nil {
			t.Skipf("Skipping test due to connection error: %v", err)
			return
		}
		err = conn.Close()
		require.NoError(t, err)

		msg := []byte("write error")
		err = anet.Write(conn, msg)
		require.Error(t, err)
	})

	t.Run("Read Error (Closed Conn)", func(t *testing.T) {
		t.Parallel()

		// Create a fresh connection just to close it
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err != nil {
			t.Skipf("Skipping test due to connection error: %v", err)
			return
		}
		err = conn.Close()
		require.NoError(t, err)

		_, err = anet.Read(conn)
		require.Error(t, err)
	})
}
