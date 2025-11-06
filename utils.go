package anet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
)

// Message framing constants.
const (
	// LENGTHSIZE defines the size of the length header in bytes.
	// The length header is a big-endian uint16 value.
	LENGTHSIZE = 2
)

// Common framing errors.
var (
	// ErrInvalidMsgLength indicates that a message length header is invalid.
	ErrInvalidMsgLength = errors.New("invalid message length")

	// ErrMaxLenExceeded indicates the message length exceeds the maximum allowed.
	// This is determined by the maximum value that can be stored in LENGTHSIZE bytes.
	ErrMaxLenExceeded = errors.New("maximum message length exceeded")
)

var writeBufPool = sync.Pool{New: func() any { return make([]byte, 0, 4096) }}

// RingBuffer is a fixed-size circular buffer for items of any type.
type RingBuffer[T any] struct {
	buf  []T    // underlying buffer array.
	mask uint64 // mask for index wrapping.
	head uint64 // next position to read from.
	tail uint64 // next position to write to.
}

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

// NewRingBuffer creates a new RingBuffer with capacity rounded up to a power of two.
func NewRingBuffer[T any](size uint64) *RingBuffer[T] {
	capacity := nextPow2Uint64(size)

	return &RingBuffer[T]{
		buf:  make([]T, capacity),
		mask: capacity - 1,
	}
}

// Enqueue adds an item to the buffer. It returns false if the buffer is full.
func (r *RingBuffer[T]) Enqueue(item T) bool {
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
	if r.tail == r.head {
		return zero, false // buffer is empty.
	}
	item := r.buf[r.head&r.mask]
	r.head++

	return item, true
}

// Len returns the number of items in the buffer.
func (r *RingBuffer[T]) Len() uint64 {
	return r.tail - r.head
}

// Cap returns the capacity of the buffer.
func (r *RingBuffer[T]) Cap() uint64 {
	return uint64(len(r.buf))
}

// Write sends data over a connection using the message framing protocol.
// It prepends a big-endian length header of LENGTHSIZE bytes to the data.
// The maximum message size is determined by LENGTHSIZE (65535 bytes for uint16).
//
// The complete frame format is:
//
//	[length header (2 bytes)][payload (length bytes)]
//
// Returns ErrMaxLenExceeded if the message is too large for the length header.
func Write(w io.Writer, in []byte) error {
	// Calculate maximum allowed length based on header size.
	maxLen := uint64(1<<(8*LENGTHSIZE)) - 1
	if uint64(len(in)) > maxLen {
		return ErrMaxLenExceeded
	}

	// Build header in a small stack buffer to avoid allocations.
	var hdr [LENGTHSIZE]byte
	switch LENGTHSIZE {
	case 2:
		binary.BigEndian.PutUint16(hdr[:], uint16(len(in)))
	case 4:
		binary.BigEndian.PutUint32(hdr[:], uint32(len(in)))
	default:
		return fmt.Errorf("unsupported header size: %d", LENGTHSIZE)
	}

	// Attempt a vectorized write using net.Buffers to avoid copying payload.
	// Falls back internally to sequential writes if writev is unavailable.
	bufs := net.Buffers{hdr[:], in}
	n, err := bufs.WriteTo(w)
	if err != nil {
		return err
	}
	expected := int64(len(hdr) + len(in))
	if n != expected {
		return io.ErrShortWrite
	}

	return nil
}

// Read receives data from a connection using the message framing protocol.
// It first reads a big-endian length header of LENGTHSIZE bytes, then reads
// the specified number of payload bytes.
//
// The complete frame format is:
//
//	[length header (2 bytes)][payload (length bytes)]
//
// Returns:
//   - The payload data.
//   - io.EOF if the connection was closed cleanly.
//   - Other errors for network or protocol issues.
func Read(r io.Reader) ([]byte, error) {
	// Read the length header into a small stack buffer.
	var hdr [LENGTHSIZE]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}

	// Parse the length value.
	var length uint64
	switch LENGTHSIZE {
	case 2:
		length = uint64(binary.BigEndian.Uint16(hdr[:]))
	case 4:
		length = uint64(binary.BigEndian.Uint32(hdr[:]))
	default:
		return nil, fmt.Errorf("unsupported header size: %d", LENGTHSIZE)
	}

	// Allocate the exact-sized payload buffer and read directly into it.
	if length == 0 {
		return []byte{}, nil
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}

	return payload, nil
}
