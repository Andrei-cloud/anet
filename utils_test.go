// Package anet_test provides tests and examples for the anet package.
package anet_test

import (
	"net"
	"testing"
	"time"

	"github.com/andrei-cloud/anet"
	"github.com/stretchr/testify/require"
)

// TestUtils verifies the message framing protocol implementation.
// It uses a test server to validate the complete request/response cycle.
//
//nolint:all
func TestUtils(t *testing.T) {
	t.Parallel()

	// Create a test server for the entire test suite
	addr, stop, err := StartTestServer()
	require.NoError(t, err)
	defer stop() // Clean up server after all tests complete

	// Sub-tests for specific functionality
	t.Run("Write Success", func(t *testing.T) {
		t.Parallel()

		// Create a fresh connection with proper timeout
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

		// Test writing a message
		msg := []byte("hello utils write")
		err = anet.Write(conn, msg)
		require.NoError(t, err)

		// Verify echo response
		readMsg, err := anet.Read(conn)
		require.NoError(t, err)
		require.Equal(t, msg, readMsg)
	})

	t.Run("Read Success", func(t *testing.T) {
		t.Parallel()

		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err != nil {
			t.Skipf("Skipping test due to connection error: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

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

	t.Run("Maximum Message Size", func(t *testing.T) {
		t.Parallel()

		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err != nil {
			t.Skipf("Skipping test due to connection error: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

		// Create message larger than uint16 max
		msg := make([]byte, 70000)
		err = anet.Write(conn, msg)
		require.ErrorIs(t, err, anet.ErrMaxLenExceeded)
	})
}
