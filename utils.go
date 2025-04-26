package anet

import (
	"bytes"
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

// Write writes data prefixed with a big-endian length header of configured size.
func Write(w io.Writer, in []byte) error {
	// Validate message length against header size.
	maxLen := uint64(1<<(8*LENGTHSIZE)) - 1
	if uint64(len(in)) > maxLen {
		return ErrMaxLenExceeded
	}

	out := &bytes.Buffer{}
	// Write length header based on configured size.
	switch LENGTHSIZE {
	case 2:
		if err := binary.Write(out, binary.BigEndian, uint16(len(in))); err != nil {
			return err
		}
	case 4:
		if err := binary.Write(out, binary.BigEndian, uint32(len(in))); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported header size: %d", LENGTHSIZE)
	}
	if _, err := out.Write(in); err != nil {
		return err
	}
	if _, err := out.WriteTo(w); err != nil {
		return err
	}

	return nil
}

// Read reads data prefixed with a big-endian length header of configured size.
func Read(r io.Reader) ([]byte, error) {
	var length uint64
	switch LENGTHSIZE {
	case 2:
		var l2 uint16
		if err := binary.Read(r, binary.BigEndian, &l2); err != nil {
			return nil, err
		}
		length = uint64(l2)
	case 4:
		var l4 uint32
		if err := binary.Read(r, binary.BigEndian, &l4); err != nil {
			return nil, err
		}
		length = uint64(l4)
	default:
		return nil, fmt.Errorf("unsupported header size: %d", LENGTHSIZE)
	}

	message := make([]byte, int(length))
	if _, err := io.ReadFull(r, message); err != nil {
		return nil, err
	}

	// If message contains nested length header, unwrap it.
	if len(message) > LENGTHSIZE {
		var innerLen uint64
		switch LENGTHSIZE {
		case 2:
			innerLen = uint64(binary.BigEndian.Uint16(message[:2]))
		case 4:
			innerLen = uint64(binary.BigEndian.Uint32(message[:4]))
		}
		if innerLen == uint64(len(message)-LENGTHSIZE) {
			message = message[LENGTHSIZE:]
		}
	}

	return message, nil
}
