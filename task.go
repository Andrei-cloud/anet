package anet

import (
	"context"
	"encoding/hex"
	"sync"
	"time"
)

// Constants for task management.
const (
	// taskIDSize is the size in bytes of the task ID header.
	// This must be consistent between broker and server implementations.
	taskIDSize = 4
)

// nextTaskID is the global atomic counter for assigning unique task IDs.
var nextTaskID uint32

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
	created   time.Time       // Creation timestamp for monitoring
	closeOnce sync.Once       // Ensures cleanup runs exactly once
}

// Context returns the task's context, which can be used for cancellation
// and timeout control. The context is typically created with a timeout
// when using SendContext.
func (t *Task) Context() context.Context {
	return t.ctx
}

// ID returns the task's unique ID as a hexadecimal string.
// This is useful for logging and debugging request/response flows.
func (t *Task) ID() string {
	return hex.EncodeToString(t.taskID)
}

// Created returns the task creation time.
// This can be used to monitor request latency and detect stale tasks.
func (t *Task) Created() time.Time {
	return t.created
}

// Request returns the request payload.
// The broker will prepend the task ID to this payload before sending.
func (t *Task) Request() *[]byte {
	return t.request
}
