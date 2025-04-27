package server

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/andrei-cloud/anet"
)

func TestServerEcho(t *testing.T) {
	handler := HandlerFunc(func(sc *ServerConn, req []byte) ([]byte, error) {
		return req, nil
	})
	srv, err := NewServer("127.0.0.1:0", handler, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	addr := srv.listener.Addr().String()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	payload := []byte("hello")
	taskID := [4]byte{0x01, 0x02, 0x03, 0x04}
	msg := append(taskID[:], payload...)

	if err := anet.Write(conn, msg); err != nil {
		t.Fatal(err)
	}

	conn.SetReadDeadline(time.Now().Add(time.Second))

	resp, err := anet.Read(conn)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp) < 4 {
		t.Fatalf("response too short")
	}
	if !bytes.Equal(resp[:4], taskID[:]) {
		t.Errorf("task ID mismatch")
	}
	if !bytes.Equal(resp[4:], payload) {
		t.Errorf("expected %s, got %s", payload, resp[4:])
	}
}
