package anet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand"
	"time"
)

const letterBytes = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits

	LENGTHSIZE = 2
)

var src = rand.NewSource(time.Now().UnixNano())
var ErrInvalidMsgLength = fmt.Errorf("invalid message length")

func randString(n int) []byte {
	sb := bytes.Buffer{}
	sb.Grow(n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			sb.WriteByte(letterBytes[idx])
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return sb.Bytes()
}

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
