package server

import (
	"time"

	"github.com/andrei-cloud/anet"
)

const (
	DefaultReadTimeout       = 5 * time.Second  // default read timeout duration.
	DefaultWriteTimeout      = 5 * time.Second  // default write timeout duration.
	DefaultIdleTimeout       = 0 * time.Second  // default idle timeout disables idle closure.
	DefaultMaxConns          = 0                // default max connections means no limit.
	DefaultShutdownTimeout   = 5 * time.Second  // default shutdown timeout duration.
	DefaultKeepAliveInterval = 30 * time.Second // default TCP keepalive period.
)

type ServerConfig struct {
	ReadTimeout           time.Duration // maximum duration for read operations.
	WriteTimeout          time.Duration // maximum duration for write operations.
	IdleTimeout           time.Duration // duration a connection can remain idle.
	MaxConns              int           // maximum concurrent connections allowed.
	MaxConcurrentHandlers int           // maximum concurrent message handlers allowed.
	ShutdownTimeout       time.Duration // grace period for shutdown wait.
	KeepAliveInterval     time.Duration // interval for TCP keepalive probes.
	Logger                anet.Logger   // optional logger for server events.
}

func (c *ServerConfig) applyDefaults() {
	if c.ReadTimeout == 0 {
		c.ReadTimeout = DefaultReadTimeout
	}

	if c.WriteTimeout == 0 {
		c.WriteTimeout = DefaultWriteTimeout
	}

	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = DefaultShutdownTimeout
	}

	if c.KeepAliveInterval == 0 {
		c.KeepAliveInterval = DefaultKeepAliveInterval
	}
}
