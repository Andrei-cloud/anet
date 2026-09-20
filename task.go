package anet

import "context"

// Constants for task management.
const (
	// taskIDSize is the size in bytes of the task ID header.
	// This must be consistent between broker and server implementations.
	taskIDSize = 4
)

// maxFrameRetain is the largest frame-staging buffer a pooled Task keeps for
// reuse. Frames beyond this size are drawn from the global buffer pool per
// request and returned after the write, bounding pooled memory to
// maxBufferSize per idle task (same ceiling as the buffer pool classes).
const maxFrameRetain = maxBufferSize

// Response is the result of a single framed request, delivered exactly once
// on the channel returned by SendAsync and SendAsyncContext. Payload and Err
// are mutually exclusive: on success Payload holds the response bytes with
// the task ID header stripped (caller-owned, GC-freed; do not pass them to
// PutBuffer), and Err is nil. On failure Err is non-nil and Payload is nil.
type Response struct {
	Payload []byte // response payload on success, nil on failure
	Err     error  // failure cause, nil on success
}

// Task is the internal unit of work managed by a broker.
//
// Ownership model ("scheme Z"): the submitting goroutine owns the Task
// exclusively until it is enqueued (or handed to a multiplexed pipeline);
// from then on exactly one queue consumer — a worker or the Close drain —
// owns it and recycles it exactly once. The submitter holds a reference to
// task.res (the result channel), never to the Task itself, so a caller that
// times out or abandons a request can never recycle a Task that is still
// queued. This invariant is what prevents the historical class of
// use-after-recycle bugs (zombie processing of stale tasks, cross-response
// delivery, refCount underflow). There is deliberately no reference counter.
type Task struct {
	//nolint:containedctx // Task is queued for cancellation-aware dispatch.
	ctx      context.Context // caller context; nil means only the broker can cancel
	id       uint32          // integer identifier for request/response correlation
	frame    []byte          // fully staged frame [len][taskID][payload], task-owned
	frameLen int             // valid frame length (frame may be larger)
	res      chan Response   // per-request delivery channel (cap 1); the Task struct and frame
	// buffer are pooled, but the channel is allocated fresh per request and dropped on
	// recycle. A pooled channel would race a parked-but-unscheduled waiter against the
	// next submitter's stale-entry drain: delivery, recycle, and drain can all complete
	// before the waiter's select is scheduled, silently swallowing its response. A fresh
	// channel makes that interleaving impossible (one alloc, ~96 B/op, worth its price).
	hdr [LENGTHSIZE]byte // response header scratch: task-owned heap bytes, so
	// reading it through io.Reader never escapes to a fresh heap allocation (the old
	// `var hdr [LENGTHSIZE]byte; io.ReadFull(r, hdr[:])` pattern escaped, ~8 B/op).
	transient bool // frame too large to retain; return to the buffer pool after writing
}

// Context returns the task's context.
func (t *Task) Context() context.Context {
	return t.ctx
}
