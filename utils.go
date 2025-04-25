package anet

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
)

const (
	LENGTHSIZE = 2
)

// ErrInvalidMsgLength indicates a message length header is invalid.
var ErrInvalidMsgLength = errors.New("invalid message length")

// Write writes data prefixed with uint16 length header to the writer.
func Write(w io.Writer, in []byte) error {
	if len(in) > math.MaxUint16 {
		return ErrMaxLenExceeded
	}

	out := &bytes.Buffer{}
	err := binary.Write(out, binary.BigEndian, uint16(len(in)))
	if err != nil {
		return err
	}
	_, err = out.Write(in)
	if err != nil {
		return err
	}
	_, err = out.WriteTo(w)
	if err != nil {
		return err
	}

	return nil
}

// Read reads data prefixed with uint16 length header from the reader.
func Read(r io.Reader) ([]byte, error) {
	var length uint16
	err := binary.Read(r, binary.BigEndian, &length)
	if err != nil {
		return nil, err
	}

	message := make([]byte, int(length))
	_, err = io.ReadFull(r, message)
	if err != nil {
		return nil, err
	}

	return message, nil
}
