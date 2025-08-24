package anet

import (
	"context"
	"sync"
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
var globalTaskIDPool = newTaskIDPool(1024) // Initial target size (advisory only).

// taskIDPool manages reusable task ID byte arrays using a sync.Pool for
// concurrency safety and reduced allocations.
type taskIDPool struct {
	pool sync.Pool
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
func newTaskIDPool(_ uint64) *taskIDPool {
	tp := &taskIDPool{}
	tp.pool = sync.Pool{
		New: func() any {
			// Always produce a fresh 4-byte slice when the pool is empty.
			return make([]byte, taskIDSize)
		},
	}

	return tp
}

// getTaskID retrieves a task ID byte array from the pool.
func (tp *taskIDPool) getTaskID() []byte {
	if v := tp.pool.Get(); v != nil {
		if b, ok := v.([]byte); ok && len(b) == taskIDSize {
			return b
		}
	}
	// Fallback if assertion fails
	return make([]byte, taskIDSize)
}

// putTaskID returns a task ID byte array to the pool.
func (tp *taskIDPool) putTaskID(taskID []byte) {
	if len(taskID) != taskIDSize {
		return // Don't pool incorrectly sized arrays.
	}
	// Optionally zero the slice to avoid retaining IDs in memory; not required for correctness.
	taskID[0], taskID[1], taskID[2], taskID[3] = 0, 0, 0, 0
	tp.pool.Put(taskID)
}

// Context returns the task's context, which can be used for cancellation
// and timeout control. The context is typically created with a timeout
// when using SendContext.
func (t *Task) Context() context.Context {
	return t.ctx
}
