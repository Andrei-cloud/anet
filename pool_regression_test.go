package anet_test

import (
	"net"
	"testing"
	"time"

	"github.com/andrei-cloud/anet"
)

// TestPoolValidation_PoisonedConnection verifies that a connection with unexpected data
// is considered invalid and removed from the pool.
func TestPoolValidation_PoisonedConnection(t *testing.T) {
	// Start a real TCP server
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer l.Close()

	// Server that accepts connections and immediately sends garbage data
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				// Send unexpected data to the client
				c.Write([]byte("garbage"))
				// Keep connection open for a bit
				time.Sleep(2 * time.Second)
			}(conn)
		}
	}()

	factory := func(addr string) (anet.PoolItem, error) {
		return net.Dial("tcp", addr)
	}

	config := &anet.PoolConfig{
		DialTimeout:           1 * time.Second,
		IdleTimeout:           10 * time.Second,
		ValidationInterval:    100 * time.Millisecond,
		ValidationStrategy:    anet.ValidationRead, // Use read validation
		ValidationTimeout:     500 * time.Millisecond,
		MaxValidationAttempts: 1,
	}

	pool := anet.NewPool(1, factory, l.Addr().String(), config)
	defer pool.Close()

	// Get a connection (this creates it)
	item, err := pool.Get()
	if err != nil {
		t.Fatalf("failed to get connection: %v", err)
	}

	// Put it back. It is now idle.
	pool.Put(item)

	// Wait for the server to send garbage and for the pool to validate
	// The validation interval is 100ms.
	time.Sleep(500 * time.Millisecond)

	// The connection should have been discarded because of the garbage data.
	// However, with the CURRENT BUG, it will be considered valid.
	// We want to verify if it's still in the pool.

	// If we try to Get(), we should get a NEW connection or an error if the pool is empty/closed.
	// But since we only have 1 connection capacity, if the old one was discarded,
	// Get() will try to create a new one.
	// To verify it was discarded, we can check the pool length or check if the connection is the same.

	// Let's check internal state if possible, or infer from behavior.
	// If the connection was discarded, the pool count should be 0 (until we Get again).
	// But Get() will create a new one.

	// Let's use a trick: The server only accepts ONE connection.
	// If the pool discarded the connection and tries to create a new one, Dial will fail (or hang)
	// because the server loop above only accepted once.
	// Actually, let's make the server accept loop robust.

	// Better approach:
	// 1. Get conn1.
	// 2. Put conn1.
	// 3. Wait for validation.
	// 4. Get conn2.
	// 5. If validation worked (discarded garbage conn), conn2 should be a NEW connection.
	// 6. If validation failed (kept garbage conn), conn2 should be conn1.

	conn1, ok := item.(net.Conn)
	if !ok {
		t.Fatalf("pool item is not net.Conn")
	}
	addr1 := conn1.LocalAddr().String()

	// Wait for validation cycle
	time.Sleep(1 * time.Second)

	// Get connection again
	item2, err := pool.Get()
	if err != nil {
		// If the pool discarded it, it might try to dial again.
		// If our server isn't accepting, it might fail.
		// Let's make sure the server accepts more connections.
		t.Fatalf("failed to get second connection: %v", err)
	}
	defer pool.Release(item2)

	conn2, ok := item2.(net.Conn)
	if !ok {
		t.Fatalf("pool item 2 is not net.Conn")
	}
	addr2 := conn2.LocalAddr().String()

	// If the bug exists, the pool kept the connection despite it having garbage data.
	// So addr1 should equal addr2.
	// We WANT them to be different (indicating the old one was discarded).
	if addr1 == addr2 {
		t.Errorf(
			"Pool returned the same connection %s despite it receiving unexpected data. It should have been discarded.",
			addr1,
		)
	} else {
		t.Logf("Pool correctly discarded poisoned connection %s and created new one %s", addr1, addr2)
	}
}
