package anet

import (
	"testing"
)

// nextPow2 returns the smallest power of two >= v, with a minimum of 32.
func nextPow2(v int) int {
	res := 32
	for res < v {
		res <<= 1
	}
	return res
}

func TestGetBufferBasic(t *testing.T) {
	cases := []struct {
		size        int
		expectedCap int
	}{
		{size: 1, expectedCap: 32},
		{size: 32, expectedCap: 32},
		{size: 33, expectedCap: 64},
		{size: 1000, expectedCap: nextPow2(1000)},
		{size: maxBufferSize, expectedCap: maxBufferSize},
	}

	for _, c := range cases {
		buf := globalBufferPool.getBuffer(c.size)
		if len(buf) < c.size {
			t.Errorf("getBuffer(%d) returned len %d, want >= %d", c.size, len(buf), c.size)
		}
		capBuf := cap(buf)
		if capBuf != c.expectedCap {
			t.Errorf("getBuffer(%d) returned cap %d, want %d", c.size, capBuf, c.expectedCap)
		}
		// return buffer to pool
		globalBufferPool.putBuffer(buf)
	}
}

func TestGetBufferLarge(t *testing.T) {
	// request size > maxBufferSize should allocate exact size
	large := maxBufferSize*2 + 1
	buf := globalBufferPool.getBuffer(large)
	if len(buf) != large {
		t.Errorf("getBuffer(large) returned len %d, want %d", len(buf), large)
	}
	if cap(buf) != large {
		t.Errorf("getBuffer(large) returned cap %d, want %d", cap(buf), large)
	}
	// putBuffer should not panic
	globalBufferPool.putBuffer(buf)
}
