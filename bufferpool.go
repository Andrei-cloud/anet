package anet

import (
	"sync"
)

// maxBufferSize is the maximum size of buffers that will be pooled.
const maxBufferSize = 64 * 1024 // 64KB

// bufferPool is a pool of byte slices for reuse.
type bufferPool struct {
	pools []*sync.Pool
}

// Global buffer pool instance.
var globalBufferPool = newBufferPool()

// newBufferPool creates a new buffer pool.
func newBufferPool() *bufferPool {
	bp := &bufferPool{
		pools: make([]*sync.Pool, 32), // Pool sizes from 32B to 64KB
	}

	for i := range bp.pools {
		size := 32 << uint(i) // 32, 64, 128, ..., 64KB
		if size > maxBufferSize {
			break
		}
		bp.pools[i] = &sync.Pool{
			New: func() interface{} {
				return make([]byte, size)
			},
		}
	}

	return bp
}

// getBuffer retrieves a buffer from the pool that is at least size bytes.
func (bp *bufferPool) getBuffer(size int) []byte {
	if size > maxBufferSize {
		return make([]byte, size)
	}

	// Find the smallest pool that fits the size
	poolIdx := 0
	poolSize := 32
	for poolSize < size {
		poolSize *= 2
		poolIdx++
	}

	return bp.pools[poolIdx].Get().([]byte)
}

// putBuffer returns a buffer to the pool.
func (bp *bufferPool) putBuffer(buf []byte) {
	if len(buf) > maxBufferSize {
		return // Don't pool large buffers
	}

	// Find the correct pool
	poolIdx := 0
	poolSize := 32
	for poolSize < len(buf) {
		poolSize *= 2
		poolIdx++
	}

	bp.pools[poolIdx].Put(buf)
}
