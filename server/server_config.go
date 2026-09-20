package server

import (
	"time"

	"github.com/andrei-cloud/anet"
)

// Default server configuration values.
const (
	DefaultReadTimeout           = 0 * time.Second  // default 0 leaves mid-frame reads unarmed (net/http ReadHeaderTimeout precedent: opt-in).
	DefaultWriteTimeout          = 5 * time.Second  // deadline per writer flush.
	DefaultIdleTimeout           = 0 * time.Second  // default 0 keeps quiet long-lived connections open.
	DefaultMaxConns              = 0                // default max connections means no limit.
	DefaultShutdownTimeout       = 5 * time.Second  // grace period for shutdown wait.
	DefaultKeepAliveInterval     = 30 * time.Second // default TCP keepalive period.
	DefaultMaxConcurrentHandlers = 10000            // default max concurrent handlers.
	DefaultWriteQueueDepth       = 64               // default queued responses per connection.
)

// ServerConfig holds server configuration.
//
// Read deadline accounting (both fields are now actually wired; the previous
// build declared ReadTimeout and never armed it):
//   - IdleTimeout bounds the wait for the next frame header. Zero (default)
//     keeps quiet connections open indefinitely; set it (or use heartbeats)
//     to reap idle peers.
//   - ReadTimeout bounds the body of a frame whose header has arrived, and
//     is cleared again once the body is complete, so it never cuts off a
//     connection that merely stays quiet between frames. Opt-in (default 0):
//     arming it costs a deadline operation per frame.
type ServerConfig struct {
	ReadTimeout           time.Duration // body-read deadline for a started frame.
	WriteTimeout          time.Duration // maximum duration for a writer flush.
	IdleTimeout           time.Duration // quiet time allowed before a connection dies.
	MaxConns              int           // maximum concurrent connections allowed.
	MaxConcurrentHandlers int           // maximum concurrent message handlers allowed.
	ShutdownTimeout       time.Duration // grace period for shutdown wait.
	KeepAliveInterval     time.Duration // interval for TCP keepalive probes.
	Logger                anet.Logger   // optional logger for server events.
	// ReusePort sets SO_REUSEPORT on the listener so the kernel spreads
	// incoming connections across listeners bound to the same address
	// (multi-process/multi-listener scale-out). Unix only.
	ReusePort bool
	// WriteQueueDepth bounds queued responses per connection; handlers
	// block past it (backpressure). Default is 64.
	WriteQueueDepth int
}

func (c *ServerConfig) applyDefaults() {
	if c.ReadTimeout < 0 {
		c.ReadTimeout = 0
	}

	if c.WriteTimeout == 0 {
		c.WriteTimeout = DefaultWriteTimeout
	} else if c.WriteTimeout < 0 {
		c.WriteTimeout = 0
	}

	if c.IdleTimeout < 0 {
		c.IdleTimeout = 0
	}

	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = DefaultShutdownTimeout
	} else if c.ShutdownTimeout < 0 {
		c.ShutdownTimeout = 0
	}

	if c.KeepAliveInterval == 0 {
		c.KeepAliveInterval = DefaultKeepAliveInterval
	} else if c.KeepAliveInterval < 0 {
		c.KeepAliveInterval = 0
	}

	// <= 0: a negative value used to survive applyDefaults and leave the
	// semaphore nil, i.e. unbounded goroutines per in-flight message.
	if c.MaxConcurrentHandlers <= 0 {
		c.MaxConcurrentHandlers = DefaultMaxConcurrentHandlers
	}

	if c.WriteQueueDepth <= 0 {
		c.WriteQueueDepth = DefaultWriteQueueDepth
	}
}

func (c *ServerConfig) writeQueueDepth() int {
	if c.WriteQueueDepth > 0 {
		return c.WriteQueueDepth
	}
	return DefaultWriteQueueDepth
}
