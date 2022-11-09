package anet

import "context"

const taskIDSize = 4

type Task struct {
	ctx      context.Context
	taskID   []byte
	request  *[]byte
	response chan []byte
	errCh    chan error
}

func (t *Task) Context() context.Context {
	if t.ctx == nil {
		t.ctx = context.Background()
	}
	return t.ctx
}

func (b *broker) newTask(r *[]byte) *Task {
	return &Task{
		taskID:   randString(taskIDSize),
		request:  r,
		response: make(chan []byte),
		errCh:    make(chan error),
	}
}

func (b *broker) addTask(task *Task) []byte {
	b.pending.Store(string(task.taskID), task)
	return append([]byte(task.taskID), *task.request...)
}

func (b *broker) failPending(task *Task) {
	close(task.errCh)
	close(task.response)
	b.pending.Delete(string(task.taskID))
}

func (b *broker) respondPending(msg []byte) {
	var (
		item any
		ok   bool
	)
	response := make([]byte, len((msg)[taskIDSize:]))
	copy(response, (msg)[taskIDSize:])

	if item, ok = b.pending.LoadAndDelete(string(msg[:taskIDSize])); !ok {
		return
	}

	select {
	case <-item.(*Task).response:
	default:
		item.(*Task).response <- response
	}
}
