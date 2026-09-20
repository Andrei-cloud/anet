// Package anet provides network communication components.
package anet

import (
	"math/bits"
	"sync"
)

// maxBufferSize is the maximum size of buffers that will be pooled.
// Larger buffers will be allocated directly and not pooled to prevent memory bloat.
const maxBufferSize = 64 * 1024 // 64KB

const (
	minClassShift = 5                                 // 1 << 5 = 32 bytes
	maxClassShift = 16                                // 1 << 16 = 65536 bytes (64KB)
	numClasses    = maxClassShift - minClassShift + 1 // 12 size classes
)

// globalBufferPool manages the buffer pools.
var globalBufferPool = newBufferPool()

// ptrPool reuses *[]byte pointers to avoid any heap allocation on Put.
var ptrPool = sync.Pool{
	New: func() any {
		return new([]byte)
	},
}

// bufferPool manages a set of sync.Pool instances for different power-of-two size classes (32B to 64KB).
type bufferPool struct {
	pools [numClasses]sync.Pool
}

// newBufferPool creates a new buffer pool with pre-allocated sync.Pool instances.
func newBufferPool() *bufferPool {
	bp := &bufferPool{}
	for i := 0; i < numClasses; i++ {
		size := 1 << (i + minClassShift)
		bp.pools[i] = sync.Pool{
			New: func() any {
				ptr := new([]byte)
				*ptr = make([]byte, size)
				return ptr
			},
		}
	}
	return bp
}

// GetBuffer retrieves a buffer from the pool that is at least size bytes.
// If size exceeds maxBufferSize, a fresh buffer is allocated directly.
// The returned buffer slice has length equal to size, with capacity >= size.
func GetBuffer(size int) []byte {
	return globalBufferPool.getBuffer(size)
}

// PutBuffer returns a buffer to the pool for future reuse with ZERO heap allocations.
func PutBuffer(buf []byte) {
	globalBufferPool.putBuffer(buf)
}

// getBuffer retrieves a buffer from the appropriate size class pool in O(1).
func (bp *bufferPool) getBuffer(size int) []byte {
	if size <= 0 {
		return []byte{}
	}
	if size > maxBufferSize {
		return make([]byte, size)
	}

	var classIdx int
	if size > (1 << minClassShift) {
		// size in (32, 65536] => bits.Len32(size-1) in [6, 16] => classIdx in [1, 11].
		classIdx = bits.Len32(uint32(size-1)) - minClassShift
	} else {
		classIdx = 0
	}

	poolSize := 1 << (classIdx + minClassShift)
	obj := bp.pools[classIdx].Get()
	if ptr, ok := obj.(*[]byte); ok && ptr != nil {
		buf := *ptr
		*ptr = nil
		ptrPool.Put(ptr)
		if cap(buf) >= poolSize {
			return buf[:size]
		}
	}

	buf := make([]byte, poolSize)
	return buf[:size]
}

// putBuffer returns a buffer to its appropriate size class pool in O(1) with 0 allocs.
func (bp *bufferPool) putBuffer(buf []byte) {
	c := cap(buf)
	if c > maxBufferSize || c < (1<<minClassShift) {
		return
	}

	classIdx := bits.Len32(uint32(c)) - 1 - minClassShift
	// c in [32, 65536] => bits.Len32(c) in [6, 17] => classIdx in [0, 11],
	// so the range guard below is unreachable; it stays as cheap insurance
	// because putBuffer is exported through PutBuffer to user buffers.
	if classIdx < 0 || classIdx >= numClasses {
		return
	}

	// poolSize = 2^floor(log2(c)) <= c for every c in [32, 65536], so a
	// c < poolSize check would be unreachable.
	poolSize := 1 << (classIdx + minClassShift)
	buf = buf[:poolSize]
	ptr := ptrPool.Get().(*[]byte)
	*ptr = buf
	bp.pools[classIdx].Put(ptr)
}
