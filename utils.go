package anet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	LENGTHSIZE = 2
)

// ErrInvalidMsgLength indicates a message length header is invalid.
var ErrInvalidMsgLength = errors.New("invalid message length")

// ErrMaxLenExceeded indicates the message length exceeds the maximum allowed.
var ErrMaxLenExceeded = errors.New("maximum message length exceeded")

// Write writes data prefixed with a big-endian length header.
func Write(w io.Writer, in []byte) error {
	maxLen := uint64(1<<(8*LENGTHSIZE)) - 1
	if uint64(len(in)) > maxLen {
		return ErrMaxLenExceeded
	}

	// Get a buffer from the pool for the header
	headerBuf := globalBufferPool.getBuffer(LENGTHSIZE)
	defer globalBufferPool.putBuffer(headerBuf)

	switch LENGTHSIZE {
	case 2:
		binary.BigEndian.PutUint16(headerBuf, uint16(len(in)))
	case 4:
		binary.BigEndian.PutUint32(headerBuf, uint32(len(in)))
	default:
		return fmt.Errorf("unsupported header size: %d", LENGTHSIZE)
	}

	// Write the header
	if _, err := w.Write(headerBuf[:LENGTHSIZE]); err != nil {
		return err
	}

	// Write the payload
	if _, err := w.Write(in); err != nil {
		return err
	}

	return nil
}

// Read reads data prefixed with a big-endian length header.
func Read(r io.Reader) ([]byte, error) {
	// Get a buffer from the pool for the header
	headerBuf := globalBufferPool.getBuffer(LENGTHSIZE)
	defer globalBufferPool.putBuffer(headerBuf)

	if _, err := io.ReadFull(r, headerBuf[:LENGTHSIZE]); err != nil {
		return nil, err
	}

	var length uint64
	switch LENGTHSIZE {
	case 2:
		length = uint64(binary.BigEndian.Uint16(headerBuf))
	case 4:
		length = uint64(binary.BigEndian.Uint32(headerBuf))
	default:
		return nil, fmt.Errorf("unsupported header size: %d", LENGTHSIZE)
	}

	// Get a buffer from the pool for the message
	message := globalBufferPool.getBuffer(int(length))
	defer globalBufferPool.putBuffer(message)

	if _, err := io.ReadFull(r, message[:length]); err != nil {
		return nil, err
	}

	// Create a new buffer with just the needed size for the result
	// Note: We have to copy here as the pooled buffer will be reused
	result := make([]byte, length)
	copy(result, message[:length])

	return result, nil
}
