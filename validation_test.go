package anet

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// timeoutError implements net.Error with Timeout() returning true
type timeoutError struct{}

func (e *timeoutError) Error() string   { return "timeout" }
func (e *timeoutError) Timeout() bool   { return true }
func (e *timeoutError) Temporary() bool { return false }

// mockValidationConn is a test connection that can simulate various states
type mockValidationConn struct {
	closed          atomic.Bool
	readShouldError bool
	readError       error
	data            []byte
	readPos         int
}

func newMockValidationConn() *mockValidationConn {
	return &mockValidationConn{
		data: []byte("test data"),
	}
}

func (m *mockValidationConn) Read(b []byte) (int, error) {
	if m.closed.Load() {
		return 0, io.EOF
	}
	if m.readShouldError {
		return 0, m.readError
	}
	if m.readPos >= len(m.data) {
		// Simulate timeout for validation - create a proper timeout error
		return 0, &timeoutError{}
	}
	n := copy(b, m.data[m.readPos:])
	m.readPos += n
	return n, nil
}

func (m *mockValidationConn) Write(b []byte) (int, error) {
	if m.closed.Load() {
		return 0, io.EOF
	}
	return len(b), nil
}

func (m *mockValidationConn) Close() error {
	m.closed.Store(true)
	return nil
}

func (m *mockValidationConn) SetDeadline(t time.Time) error {
	return nil
}

func (m *mockValidationConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (m *mockValidationConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func (m *mockValidationConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0}
}

func (m *mockValidationConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8080}
}

// mockBasicItem is a PoolItem that doesn't support deadlines
type mockBasicItem struct {
	valid bool
}

func (m *mockBasicItem) Close() error {
	return nil
}

func TestConnectionValidation(t *testing.T) {
	tests := []struct {
		name     string
		strategy ValidationStrategy
		setup    func() PoolItem
		expected bool
	}{
		{
			name:     "ValidationNone should always pass",
			strategy: ValidationNone,
			setup: func() PoolItem {
				conn := newMockValidationConn()
				conn.closed.Store(true) // Even closed connections should pass
				return conn
			},
			expected: true,
		},
		{
			name:     "ValidationRead with healthy connection",
			strategy: ValidationRead,
			setup: func() PoolItem {
				return newMockValidationConn()
			},
			expected: true,
		},
		{
			name:     "ValidationRead with closed connection",
			strategy: ValidationRead,
			setup: func() PoolItem {
				conn := newMockValidationConn()
				conn.closed.Store(true)
				return conn
			},
			expected: false,
		},
		{
			name:     "ValidationRead with timeout (healthy)",
			strategy: ValidationRead,
			setup: func() PoolItem {
				conn := newMockValidationConn()
				conn.readPos = len(conn.data) // Force timeout
				return conn
			},
			expected: true, // Timeout is expected for healthy connections
		},
		{
			name:     "ValidationPing with healthy connection",
			strategy: ValidationPing,
			setup: func() PoolItem {
				return newMockValidationConn()
			},
			expected: true,
		},
		{
			name:     "Basic item with ValidationRead",
			strategy: ValidationRead,
			setup: func() PoolItem {
				return &mockBasicItem{valid: true}
			},
			expected: true, // Should fallback to basic validation
		},
		{
			name:     "Nil connection",
			strategy: ValidationRead,
			setup: func() PoolItem {
				return nil
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultPoolConfig()
			config.ValidationStrategy = tt.strategy
			config.ValidationTimeout = 100 * time.Millisecond
			config.MaxValidationAttempts = 2

			p := &pool{
				config: config,
			}

			item := tt.setup()
			result := p.validateConnection(item)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestValidationStrategies(t *testing.T) {
	t.Run("ValidationPing with non-TCP connection", func(t *testing.T) {
		config := DefaultPoolConfig()
		config.ValidationStrategy = ValidationPing

		p := &pool{config: config}

		// Use basic item that doesn't implement net.Conn
		item := &mockBasicItem{valid: true}
		err := p.validatePing(item)
		require.NoError(t, err) // Should return nil for non-TCP connections
	})

	t.Run("ValidationRead with retry logic", func(t *testing.T) {
		config := DefaultPoolConfig()
		config.ValidationStrategy = ValidationRead
		config.MaxValidationAttempts = 3

		p := &pool{config: config}

		// Test that validation respects MaxValidationAttempts
		// Use a connection that always fails to verify retry count
		conn := newMockValidationConn()
		conn.readShouldError = true
		conn.readError = io.ErrUnexpectedEOF

		result := p.validateConnectionWithStrategy(conn)
		require.False(t, result) // Should fail after all attempts

		// Test successful validation on healthy connection
		healthyConn := newMockValidationConn()
		result2 := p.validateConnectionWithStrategy(healthyConn)
		require.True(t, result2) // Should succeed immediately
	})
}

func TestPoolValidationIntegration(t *testing.T) {
	factory := func(addr string) (PoolItem, error) {
		return newMockValidationConn(), nil
	}

	config := DefaultPoolConfig()
	config.ValidationStrategy = ValidationRead
	config.ValidationInterval = 50 * time.Millisecond
	config.ValidationTimeout = 100 * time.Millisecond

	pool := NewPool(5, factory, "test:8080", config)
	defer pool.Close()

	// Get a connection and put it back
	item, err := pool.Get()
	require.NoError(t, err)
	require.NotNil(t, item)

	pool.Put(item)

	// Wait for validation cycle
	time.Sleep(100 * time.Millisecond)

	// Pool should still work after validation
	item2, err := pool.Get()
	require.NoError(t, err)
	require.NotNil(t, item2)
}

func TestValidationSubset(t *testing.T) {
	// Create mock connections with different states
	factory := func(addr string) (PoolItem, error) {
		return newMockValidationConn(), nil
	}

	config := DefaultPoolConfig()
	config.ValidationStrategy = ValidationRead
	config.ValidationTimeout = 50 * time.Millisecond

	pool := NewPool(10, factory, "test:8080", config).(*pool)
	defer pool.Close()

	// Fill pool with connections
	var items []PoolItem
	for i := 0; i < 5; i++ {
		item, err := pool.Get()
		require.NoError(t, err)
		items = append(items, item)
	}

	// Return all items
	for _, item := range items {
		pool.Put(item)
	}

	// Break one connection
	if len(items) > 0 {
		if mockConn, ok := items[0].(*mockValidationConn); ok {
			mockConn.closed.Store(true)
		}
	}

	// Run validation subset
	pool.validateConnectionSubset()

	// Pool should still be functional
	item, err := pool.Get()
	require.NoError(t, err)
	require.NotNil(t, item)
}

func TestValidationWithNilConfig(t *testing.T) {
	factory := func(addr string) (PoolItem, error) {
		return newMockValidationConn(), nil
	}

	// Pool should use default config when nil is passed
	poolInstance := NewPool(5, factory, "test:8080", nil)
	defer poolInstance.Close()

	item, err := poolInstance.Get()
	require.NoError(t, err)
	require.NotNil(t, item)

	poolInstance.Put(item)

	// Validate that default config is used
	poolImpl := poolInstance.(*pool)
	require.Equal(t, ValidationRead, poolImpl.config.ValidationStrategy)
	require.Equal(t, 1*time.Second, poolImpl.config.ValidationTimeout)
	require.Equal(t, 3, poolImpl.config.MaxValidationAttempts)
}

func BenchmarkConnectionValidation(b *testing.B) {
	config := DefaultPoolConfig()
	config.ValidationStrategy = ValidationRead
	config.ValidationTimeout = 10 * time.Millisecond

	p := &pool{config: config}

	benchmarks := []struct {
		name     string
		strategy ValidationStrategy
	}{
		{"ValidationNone", ValidationNone},
		{"ValidationPing", ValidationPing},
		{"ValidationRead", ValidationRead},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			p.config.ValidationStrategy = bm.strategy

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					conn := newMockValidationConn()
					p.validateConnection(conn)
					conn.Close()
				}
			})
		})
	}
}

func TestValidationTimeout(t *testing.T) {
	config := DefaultPoolConfig()
	config.ValidationStrategy = ValidationRead
	config.ValidationTimeout = 10 * time.Millisecond // Very short timeout

	p := &pool{config: config}

	// Create connection that will cause timeout
	conn := newMockValidationConn()
	conn.readPos = len(conn.data) // Force timeout behavior

	start := time.Now()
	result := p.validateConnection(conn)
	elapsed := time.Since(start)

	require.True(t, result)                        // Timeout should be considered valid
	require.Less(t, elapsed, 100*time.Millisecond) // Should timeout quickly
}
