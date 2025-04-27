package anet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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

	// Get a buffer from the pool for the header.
	headerBuf := globalBufferPool.getBuffer(LENGTHSIZE)
	defer globalBufferPool.putBuffer(headerBuf)

	// Write length as big-endian value.
	switch LENGTHSIZE {
	case 2:
		binary.BigEndian.PutUint16(headerBuf, uint16(len(in)))
	case 4:
		binary.BigEndian.PutUint32(headerBuf, uint32(len(in)))
	default:

		return fmt.Errorf("unsupported header size: %d", LENGTHSIZE)
	}

	// Write the header.
	if _, err := w.Write(headerBuf[:LENGTHSIZE]); err != nil {
		return err
	}

	// Write the payload.
	if _, err := w.Write(in); err != nil {
		return err
	}

	// Return nil on success.
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
	// Get a buffer from the pool for the header.
	headerBuf := globalBufferPool.getBuffer(LENGTHSIZE)
	defer globalBufferPool.putBuffer(headerBuf)

	// Read the length header.
	if _, err := io.ReadFull(r, headerBuf[:LENGTHSIZE]); err != nil {
		return nil, err
	}

	// Parse the length value.
	var length uint64
	switch LENGTHSIZE {
	case 2:
		length = uint64(binary.BigEndian.Uint16(headerBuf))
	case 4:
		length = uint64(binary.BigEndian.Uint32(headerBuf))
	default:

		return nil, fmt.Errorf("unsupported header size: %d", LENGTHSIZE)
	}

	// Get a buffer from the pool for the message.
	message := globalBufferPool.getBuffer(int(length))
	defer globalBufferPool.putBuffer(message)

	// Read the payload.
	if _, err := io.ReadFull(r, message[:length]); err != nil {
		return nil, err
	}

	// Create a new buffer containing just the payload data.
	result := make([]byte, length)
	copy(result, message[:length])

	return result, nil
}
