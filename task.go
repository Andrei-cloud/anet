package anet

import (
	"context"
	"encoding/hex" // Import hex package
)

const taskIDSize = 4

// Task represents a single request/response operation.
// NOTE: The linter flags a "containedctx" warning because storing context in a struct
// is generally discouraged. However, in this specific case, it is necessary for our
// design to allow context-aware cancellation when a request is in the broker queue.
// Alternative approaches would require significant refactoring of the broker architecture.
// If this becomes problematic, consider redesigning to pass context explicitly.
type Task struct {
	ctx      context.Context //nolint:containedctx // Context for cancellation/timeout.
	taskID   []byte          // Unique ID for matching responses.
	request  *[]byte         // Request payload.
	response chan []byte     // Channel to send response back.
	errCh    chan error      // Channel to send errors back.
}

// Context returns the task's context.
func (t *Task) Context() context.Context {
	if t.ctx == nil {
		return context.Background()
	}

	return t.ctx
}

func (b *broker) failPending(task *Task) {
	// Attempt to load and delete first to avoid closing channels multiple times if called concurrently.
	// Log task ID as hex
	if _, loaded := b.pending.LoadAndDelete(string(task.taskID)); loaded {
		// Close channels only if we successfully removed the task.
		close(task.response)
		close(task.errCh)
		b.logger.Printf("Task %s failed and removed from pending.", hex.EncodeToString(task.taskID))
	} else {
		// Log task ID as hex
		b.logger.Printf("Task %s already removed from pending.", hex.EncodeToString(task.taskID))
	}
}

func (b *broker) respondPending(msg []byte) {
	if len(msg) < taskIDSize {
		b.logger.Printf("Received message too short to contain task ID: len=%d", len(msg))

		return // Ignore message if too short.
	}
	taskIDBytes := msg[:taskIDSize]
	taskIDKey := string(taskIDBytes) // Keep key as string for map lookup
	var (
		item any
		ok   bool
	)

	// Atomically load and delete the task from the pending map.
	if item, ok = b.pending.LoadAndDelete(taskIDKey); !ok {
		// Log task ID as hex
		b.logger.Printf(
			"Received response for unknown or already completed task ID: %s",
			hex.EncodeToString(taskIDBytes),
		)

		return // Task not found or already handled.
	}

	task, ok := item.(*Task)
	if !ok {
		// Log or handle the unexpected type if necessary
		b.logger.Printf(
			"Unexpected type in pending map for task ID %s",
			hex.EncodeToString(taskIDBytes),
		)
		// Attempt to close channels if possible, though the type is wrong
		if taskWithError, ok := item.(interface{ ErrorChannel() chan error }); ok {
			close(taskWithError.ErrorChannel())
		}
		if taskWithResponse, ok := item.(interface{ ResponseChannel() chan []byte }); ok {
			close(taskWithResponse.ResponseChannel())
		}

		return // Cannot proceed with wrong type
	}

	response := make([]byte, len(msg)-taskIDSize)
	copy(response, msg[taskIDSize:])
	select {
	case task.response <- response:
		// Log task ID as hex
		b.logger.Printf("Successfully sent response for task %s", hex.EncodeToString(taskIDBytes))
		// Close channels after successfully sending the response.
		close(task.response)
		close(task.errCh)
	default:
		// This case should ideally not happen with buffered channels,
		// but log it if it does. It might indicate a logic error.
		// Log task ID as hex
		b.logger.Printf(
			"Response channel for task %s blocked unexpectedly.",
			hex.EncodeToString(taskIDBytes),
		)
		// Close channels anyway to clean up.
		close(task.response)
		close(task.errCh)
	}
}
