package broker

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"strings"
	"time"
)

const letterBytes = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits
)

var src = rand.NewSource(time.Now().UnixNano())
var ErrInvalidMsgLength = fmt.Errorf("invalid message length")

func randString(n int) string {
	sb := strings.Builder{}
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

	return sb.String()
}

func Encode(in []byte) ([]byte, error) {
	l := int16(len(in))
	out := new(bytes.Buffer)
	err := binary.Write(out, binary.BigEndian, l)
	if err != nil {
		return nil, err
	}
	err = binary.Write(out, binary.BigEndian, in)
	if err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func Decode(r *bufio.Reader) ([]byte, error) {
	var length int16
	l, err := r.Peek(2)
	if err != nil {
		return nil, err
	}
	lb := bytes.NewReader(l)
	err = binary.Read(lb, binary.BigEndian, &length)
	if err != nil {
		return nil, err
	}
	if int16(r.Buffered()) < length+2 {
		return nil, ErrInvalidMsgLength
	}
	pack := make([]byte, int(2+length))
	_, err = r.Read(pack)
	if err != nil {
		return nil, err
	}
	return pack[2:], nil
}
