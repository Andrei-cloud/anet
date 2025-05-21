// Package anet provides network communication components.
package anet

import "sync"

// nextPow2Uint64 returns the smallest power of two >= v with a minimum of 1.
func nextPow2Uint64(v uint64) uint64 {
	if v == 0 {
		return 1
	}
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	return v + 1
}

// RingBuffer is a fixed-size circular buffer for items of any type.
type RingBuffer[T any] struct {
	buf  []T        // underlying buffer array.
	mask uint64     // mask for index wrapping.
	head uint64     // next position to read from.
	tail uint64     // next position to write to.
	lock sync.Mutex // mutex to protect concurrent access.
}

// NewRingBuffer creates a new RingBuffer with capacity rounded up to a power of two.
func NewRingBuffer[T any](size uint64) *RingBuffer[T] {
	cap := nextPow2Uint64(size)
	return &RingBuffer[T]{
		buf:  make([]T, cap),
		mask: cap - 1,
	}
}

// Enqueue adds an item to the buffer. It returns false if the buffer is full.
func (r *RingBuffer[T]) Enqueue(item T) bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	if (r.tail - r.head) == uint64(len(r.buf)) {
		return false // buffer is full.
	}
	r.buf[r.tail&r.mask] = item
	r.tail++
	return true
}

// Dequeue removes and returns an item. It returns false if the buffer is empty.
func (r *RingBuffer[T]) Dequeue() (T, bool) {
	var zero T
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.tail == r.head {
		return zero, false // buffer is empty.
	}
	item := r.buf[r.head&r.mask]
	r.head++
	return item, true
}

// Len returns the number of items in the buffer.
func (r *RingBuffer[T]) Len() uint64 {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.tail - r.head
}

// Cap returns the capacity of the buffer.
func (r *RingBuffer[T]) Cap() uint64 {
	return uint64(len(r.buf))
}
