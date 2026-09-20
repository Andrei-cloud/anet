//go:build unix

package server

import (
	"syscall"
)

// setReusePort arms SO_REUSEPORT so the kernel load-balances incoming
// connections across listeners bound to the same address and port
// (goperf.dev sockets guidance: one acceptor per core, no accept-queue
// drops on SYN bursts).
func setReusePort(_, _ string, c syscall.RawConn) error {
	var sockErr error
	if err := c.Control(func(fd uintptr) {
		sockErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEPORT, 1)
	}); err != nil {
		return err
	}
	return sockErr
}
