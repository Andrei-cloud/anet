package anet

import "context"

const taskIDSize = 4

// Task represents a single request/response operation.
// Storing context here is generally discouraged (containedctx lint error),
// but necessary for GetWithContext in the broker loop without major refactoring.
// Consider passing context explicitly if this becomes problematic.
type Task struct {
	ctx      context.Context // Context for cancellation/timeout.
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

	return t.ctx // Added newline
}

// addTask prepares the command bytes including the task ID header.
func (b *broker) addTask(task *Task) []byte {
	b.pending.Store(string(task.taskID), task)
	cmd := make([]byte, taskIDSize+len(*task.request))
	copy(cmd[:taskIDSize], task.taskID)
	copy(cmd[taskIDSize:], *task.request)

	return cmd // Added newline
}

func (b *broker) failPending(task *Task) {
	// Attempt to load and delete first to avoid closing channels multiple times if called concurrently.
	if _, loaded := b.pending.LoadAndDelete(string(task.taskID)); loaded {
		// Close channels only if we successfully removed the task.
		close(task.response)
		close(task.errCh)
		b.logger.Printf("Task %s failed and removed from pending.", string(task.taskID))
	} else {
		b.logger.Printf("Task %s already removed from pending.", string(task.taskID))
	}
}

func (b *broker) respondPending(msg []byte) {
	if len(msg) < taskIDSize {
		b.logger.Printf("Received message too short to contain task ID: len=%d", len(msg))
		return // Ignore message if too short.
	}
	taskIDKey := string(msg[:taskIDSize])
	var (
		item any
		ok   bool
	)

	// Atomically load and delete the task from the pending map.
	if item, ok = b.pending.LoadAndDelete(taskIDKey); !ok {
		b.logger.Printf("Received response for unknown or already completed task ID: %s", taskIDKey)
		return // Task not found or already handled.
	}

	task := item.(*Task)
	response := make([]byte, len(msg)-taskIDSize)
	copy(response, msg[taskIDSize:])

	select {
	case task.response <- response:
		b.logger.Printf("Successfully sent response for task %s", taskIDKey)
		// Close channels after successfully sending the response.
		close(task.response)
		close(task.errCh)
	default:
		// This case should ideally not happen with buffered channels,
		// but log it if it does. It might indicate a logic error.
		b.logger.Printf("Response channel for task %s blocked unexpectedly.", taskIDKey)
		// Close channels anyway to clean up.
		close(task.response)
		close(task.errCh)
	}
}
