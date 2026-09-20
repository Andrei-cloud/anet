//go:build !unix

package server

import (
	"errors"
	"syscall"
)

// errReusePortUnsupported: SO_REUSEPORT is not available on this platform.
var errReusePortUnsupported = errors.New("ReusePort is only supported on unix")

func setReusePort(_, _ string, _ syscall.RawConn) error {
	return errReusePortUnsupported
}
