package anet

import (
	"context"
)

// Constants for task management.
const (
	// taskIDSize is the size in bytes of the task ID header.
	// This must be consistent between broker and server implementations.
	taskIDSize = 4
)

// nextTaskID is the global atomic counter for assigning unique task IDs.
var nextTaskID uint32

// globalTaskIDPool is the pool of pre-allocated task IDs.
var globalTaskIDPool = newTaskIDPool(1024) // Pre-allocate 1024 task IDs.

// taskIDPool manages reusable task ID byte arrays to reduce allocations.
type taskIDPool struct {
	pool *RingBuffer[[4]byte] // Ring buffer of pre-allocated 4-byte arrays.
	size uint64               // Size of the pool.
}

// Task represents a single request/response operation managed by the broker.
// Each task has a unique ID that is prepended to the request message and
// must be included at the start of the response for proper correlation.
type Task struct {
	//nolint:containedctx // Necessary for task cancellation within broker queue.
	ctx       context.Context // Context for cancellation and timeouts
	id        uint32          // integer identifier for request/response correlation
	taskID    []byte          // Unique identifier bytes for framing (big-endian uint32)
	request   *[]byte         // Request payload to be sent
	response  chan []byte     // Channel for receiving the response
	errCh     chan error      // Channel for receiving errors
	optimized bool            // Tracks whether task uses pooled memory (for cleanup)
	pooled    bool            // Tracks whether task struct and channels are pooled
}

// newTaskIDPool creates a new task ID pool with the specified size.
func newTaskIDPool(size uint64) *taskIDPool {
	pool := NewRingBuffer[[4]byte](size)
	// Pre-fill the pool with task ID arrays.
	for i := uint64(0); i < size; i++ {
		var taskIDArray [4]byte
		pool.Enqueue(taskIDArray)
	}

	return &taskIDPool{
		pool: pool,
		size: size,
	}
}

// getTaskID retrieves a task ID byte array from the pool.
// If the pool is empty, it allocates a new one.
func (tp *taskIDPool) getTaskID() []byte {
	if taskIDArray, ok := tp.pool.Dequeue(); ok {
		return taskIDArray[:]
	}
	// Pool is empty, allocate new task ID.
	return make([]byte, taskIDSize)
}

// putTaskID returns a task ID byte array to the pool.
// Only returns arrays of the correct size to maintain pool integrity.
func (tp *taskIDPool) putTaskID(taskID []byte) {
	if len(taskID) != taskIDSize {
		return // Don't pool incorrectly sized arrays.
	}
	var taskIDArray [4]byte
	copy(taskIDArray[:], taskID)
	// Return to pool - use blocking enqueue since pool capacity should be sufficient.
	tp.pool.Enqueue(taskIDArray)
}

// Context returns the task's context, which can be used for cancellation
// and timeout control. The context is typically created with a timeout
// when using SendContext.
func (t *Task) Context() context.Context {
	return t.ctx
}
