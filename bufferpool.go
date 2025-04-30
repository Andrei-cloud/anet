// Package anet provides network communication components.
package anet

import (
	"sync"
)

// maxBufferSize is the maximum size of buffers that will be pooled.
// Larger buffers will be allocated but not pooled to prevent memory bloat.
const maxBufferSize = 2 * 1024 // 2KB

// Global buffer pool instance used by the message framing utilities.
var globalBufferPool = newBufferPool()

// bufferPool manages a set of sync.Pool instances for different buffer sizes.
// This helps reduce memory allocations and GC pressure by reusing buffers.
type bufferPool struct {
	pools []*sync.Pool // Array of pools for different size classes
}

// newBufferPool creates a new buffer pool with pre-allocated sync.Pool instances
// for common buffer sizes. This improves performance by reducing allocations
// for frequently used message sizes.
func newBufferPool() *bufferPool {
	bp := &bufferPool{
		pools: make([]*sync.Pool, 32), // Pool sizes from 32B to 64KB.
	}

	for i := range bp.pools {
		size := 32 << uint(i) // 32, 64, 128, ..., 64KB.
		if size > maxBufferSize {
			break
		}
		bp.pools[i] = &sync.Pool{
			New: func() any {
				return make([]byte, size)
			},
		}
	}

	return bp
}

// getBuffer retrieves a buffer from the pool that is at least size bytes.
// If no suitable buffer exists in the pool, a new one will be allocated.
// The returned buffer may be larger than requested but will be at least size bytes.
func (bp *bufferPool) getBuffer(size int) []byte {
	if size > maxBufferSize {
		return make([]byte, size)
	}

	// Find the smallest pool that fits the size.
	poolIdx := 0
	poolSize := 32
	for poolSize < size {
		poolSize *= 2
		poolIdx++
	}

	// retrieve buffer from pool and check type assertion.
	obj := bp.pools[poolIdx].Get()
	if buf, ok := obj.([]byte); ok {
		return buf
	}
	// fallback allocation if buffer type is not as expected.
	return make([]byte, poolSize)
}

// putBuffer returns a buffer to the pool for future reuse.
// Buffers larger than maxBufferSize are not pooled to prevent memory bloat.
// The buffer should not be accessed after being returned to the pool.
func (bp *bufferPool) putBuffer(buf []byte) {
	if len(buf) > maxBufferSize {
		return // Don't pool large buffers.
	}

	// Find the correct pool
	poolIdx := 0
	poolSize := 32
	for poolSize < len(buf) {
		poolSize *= 2
		poolIdx++
	}

	//nolint:staticcheck // passing slice which is pointer-like
	bp.pools[poolIdx].Put(buf)
}
