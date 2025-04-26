package anet

import (
	"context"
	"encoding/hex"
	"sync"
	"time"
)

const (
	taskIDSize = 4
)

// Task represents a single request/response operation.
type Task struct {
	//nolint:containedctx // Necessary for task cancellation within broker queue.
	ctx       context.Context
	taskID    []byte
	request   *[]byte
	response  chan []byte
	errCh     chan error
	created   time.Time
	closeOnce sync.Once
}

// Context returns the task's context.
func (t *Task) Context() context.Context {
	return t.ctx
}

// ID returns the task's unique ID as a hex string.
func (t *Task) ID() string {
	return hex.EncodeToString(t.taskID)
}

// Created returns the time the task was created.
func (t *Task) Created() time.Time {
	return t.created
}

// Request returns the request payload.
func (t *Task) Request() *[]byte {
	return t.request
}
