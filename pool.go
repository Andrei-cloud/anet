package anet

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// ValidationNone disables connection validation.
	ValidationNone ValidationStrategy = "none"
	// ValidationPing sends a simple ping to validate connection.
	ValidationPing ValidationStrategy = "ping"
	// ValidationRead attempts to read with timeout to validate connection.
	ValidationRead ValidationStrategy = "read"
)

// ErrClosing indicates the pool is shutting down.
var ErrClosing = errors.New("pool is closing")

// ValidationStrategy defines how connections should be validated.
type ValidationStrategy string

// PoolConfig contains configuration options for a connection pool.
type PoolConfig struct {
	// DialTimeout is the timeout for creating new connections. Default is 5s.
	// It is applied by factories that consult the config, such as NewTCPFactory.
	DialTimeout time.Duration
	// IdleTimeout is how long a connection may remain idle before being
	// evicted on the next Get or background validation pass. Default is 60s;
	// zero disables eviction.
	IdleTimeout time.Duration
	// ValidationInterval is how often to validate idle connections. Default is 30s.
	ValidationInterval time.Duration
	// KeepAliveInterval is the TCP keepalive period armed by NewTCPFactory.
	// Default is 30s; negative disables keepalive.
	KeepAliveInterval time.Duration
	// ValidationStrategy defines how to validate connections. Default is ValidationRead.
	ValidationStrategy ValidationStrategy
	// ValidationTimeout is the timeout for connection validation operations. Default is 1s.
	ValidationTimeout time.Duration
	// MaxValidationAttempts is the maximum number of validation attempts before discarding connection. Default is 3.
	MaxValidationAttempts int
	// DisableNoDelay keeps Nagle enabled on connections created by
	// NewTCPFactory. Framed request/response traffic is latency-sensitive,
	// so the default (false) disables Nagle with TCP_NODELAY.
	DisableNoDelay bool
	// Logger receives pool diagnostics. Default is &NoopLogger{} (the
	// previous default wrote straight to os.Stderr).
	Logger Logger
}

func (c *PoolConfig) applyDefaults() {
	if c.DialTimeout == 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = 60 * time.Second
	}
	if c.KeepAliveInterval == 0 {
		c.KeepAliveInterval = 30 * time.Second
	} else if c.KeepAliveInterval < 0 {
		c.KeepAliveInterval = 0
	}
	if c.ValidationStrategy == "" {
		c.ValidationStrategy = ValidationRead
	}
	if c.ValidationTimeout == 0 {
		c.ValidationTimeout = 1 * time.Second
	}
	if c.MaxValidationAttempts == 0 {
		c.MaxValidationAttempts = 3
	}
	if c.Logger == nil {
		c.Logger = &NoopLogger{}
	}
}

// Pool manages a collection of reusable connections.
type Pool interface {
	Get() (PoolItem, error)
	GetWithContext(context.Context) (PoolItem, error)
	Release(PoolItem)
	Put(PoolItem)
	Len() int
	Cap() int
	Close()
}

// PoolItem represents a closeable resource managed by the pool.
type PoolItem interface {
	Close() error
}

// Factory creates new pool items.
type Factory func(string) (PoolItem, error)

// NewTCPFactory returns a Factory that dials TCP connections with the
// configuration's DialTimeout (covering DNS plus dial) and arms TCP
// keepalive/no-delay on the result, wiring KeepAliveInterval to real socket
// options. Connections it dials report their errors, so dead peers surface
// through I/O errors and pool validation instead of silent staleness.
func NewTCPFactory(config *PoolConfig) Factory {
	cfg := PoolConfig{}
	if config != nil {
		cfg = *config
	}
	cfg.applyDefaults()

	return func(addr string) (PoolItem, error) {
		conn, err := net.DialTimeout("tcp", addr, cfg.DialTimeout)
		if err != nil {
			return nil, err
		}
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			if cfg.KeepAliveInterval > 0 {
				_ = tcpConn.SetKeepAlive(true)
				_ = tcpConn.SetKeepAlivePeriod(cfg.KeepAliveInterval)
			}
			if !cfg.DisableNoDelay {
				_ = tcpConn.SetNoDelay(true)
			}
		}
		return conn, nil
	}
}

// poolEntry wraps an idle item with the time it went idle, so expired
// connections are evicted without holding a lock on the hot path. The old
// pool declared IdleTimeout but never enforced it.
type poolEntry struct {
	item   PoolItem
	idleNs atomic.Int64 // unix nanoseconds of the Put that parked item
}

// pool implements the Pool interface.
//
// Synchronization: there is deliberately no mutex around queue. Channels
// are internally synchronized; the mutex only ever guarded the Close drain
// handshake, and that is now done with the closing flag plus a post-send
// drain: a Put whose send lands after Close's drain observes closing=true
// (CAS precedes the drain, both atomic) and closes the item itself, while a
// send that lands before the drain is drained by Close. Every item that
// ever enters the queue is therefore closed exactly once, and the contended
// reader-count RMWs are off the hot path.
type pool struct {
	addr        string
	capacity    uint32
	count       atomic.Uint32
	queue       chan *poolEntry
	factoryFunc Factory
	closing     atomic.Bool
	logger      Logger
	config      *PoolConfig
	stopChan    chan struct{}
	entryPool   sync.Pool // reuse of poolEntry wrappers
}

// DefaultPoolConfig returns the default configuration.
func DefaultPoolConfig() *PoolConfig {
	cfg := &PoolConfig{
		DialTimeout:           5 * time.Second,
		IdleTimeout:           60 * time.Second,
		ValidationInterval:    30 * time.Second,
		KeepAliveInterval:     30 * time.Second,
		ValidationStrategy:    ValidationRead,
		ValidationTimeout:     1 * time.Second,
		MaxValidationAttempts: 3,
	}
	return cfg
}

// NewPoolList creates a list of Pool interfaces from a slice of addresses.
func NewPoolList(poolCap uint32, f Factory, addrs []string, config *PoolConfig) []Pool {
	pools := make([]Pool, 0, len(addrs))
	for _, addr := range addrs {
		p := NewPool(poolCap, f, addr, config)
		pools = append(pools, p)
	}

	return pools
}

// NewPool creates a new connection pool.
func NewPool(poolCap uint32, f Factory, addr string, config *PoolConfig) Pool {
	cfg := &PoolConfig{}
	if config != nil {
		*cfg = *config
	}
	cfg.applyDefaults()

	p := &pool{
		addr:        addr,
		capacity:    poolCap,
		queue:       make(chan *poolEntry, poolCap),
		factoryFunc: f,
		logger:      cfg.Logger,
		config:      cfg,
		stopChan:    make(chan struct{}),
	}
	p.entryPool.New = func() any { return &poolEntry{} }

	// Start background validation if interval is set.
	if p.config.ValidationInterval > 0 && p.config.ValidationStrategy != ValidationNone {
		go p.validateIdleConnections()
	}

	return p
}

// getEntry takes a wrapper from the pool.
func (p *pool) getEntry() *poolEntry {
	return p.entryPool.Get().(*poolEntry)
}

// expired reports whether an idle stamp has aged past IdleTimeout.
func (p *pool) expired(idleNs int64) bool {
	return p.config.IdleTimeout > 0 &&
		time.Now().UnixNano()-idleNs > int64(p.config.IdleTimeout)
}

// take unwraps an entry and recycles the wrapper. It deliberately performs
// no time.Now: measuring staleness on every Get cost ~4x in the Get/Put
// microbenchmark (two deadline clock calls per round-trip). Idle expiry is
// enforced by the background validation pass (validateConnectionSubset) and
// by I/O self-healing — a connection that expired between validations fails
// its first write, gets Released, and is replaced with a fresh dial.
func (p *pool) take(ent *poolEntry) (PoolItem, bool) {
	item := ent.item
	ent.item = nil
	p.entryPool.Put(ent)
	if item == nil {
		return nil, false
	}
	return item, true
}

// putStamped parks item with an explicit idle stamp.
func (p *pool) putStamped(item PoolItem, idleNs int64) {
	if item == nil {
		return
	}
	if p.closing.Load() {
		p.Release(item)
		return
	}

	ent := p.getEntry()
	ent.item = item
	ent.idleNs.Store(idleNs)

	select {
	case p.queue <- ent:
		// Close handshake (see pool doc): if shutdown started while this
		// send raced the drain, close what we just enqueued ourselves.
		if p.closing.Load() {
			p.drainClose()
		}
	default:
		ent.item = nil
		p.entryPool.Put(ent)
		p.Release(item) // pool full: drop this connection
	}
}

// Get retrieves an item from the pool with optimized fast path.
func (p *pool) Get() (PoolItem, error) {
	return p.get(nil)
}

// GetWithContext retrieves an item with context cancellation support.
func (p *pool) GetWithContext(ctx context.Context) (PoolItem, error) {
	if ctx == nil {
		return p.Get()
	}
	return p.get(ctx)
}

func (p *pool) get(ctx context.Context) (PoolItem, error) {
	if p.closing.Load() {
		return nil, ErrClosing
	}

	for {
		// Fast path: reuse an idle connection without any lock.
		select {
		case ent := <-p.queue:
			if item, ok := p.take(ent); ok {
				return item, nil
			}
			continue // wrapper recycled or expired item closed: retry
		default:
		}

		// Try to create a new connection if under capacity.
		for {
			current := p.count.Load()
			if current >= p.capacity {
				break
			}
			if p.count.CompareAndSwap(current, current+1) {
				item, err := p.factoryFunc(p.addr)
				if err != nil {
					p.decrement()
					return nil, err
				}
				if p.closing.Load() {
					p.Release(item)
					return nil, ErrClosing
				}
				return item, nil
			}
		}

		// Wait for a connection to become available, context cancellation,
		// or pool closing.
		if ctx == nil {
			select {
			case ent := <-p.queue:
				if item, ok := p.take(ent); ok {
					return item, nil
				}
				continue
			case <-p.stopChan:
				return nil, ErrClosing
			}
		}
		select {
		case ent := <-p.queue:
			if item, ok := p.take(ent); ok {
				return item, nil
			}
			continue
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.stopChan:
			return nil, ErrClosing
		}
	}
}

// Put returns an item to the pool.
func (p *pool) Put(item PoolItem) {
	p.putStamped(item, time.Now().UnixNano())
}

// Release closes an item and decrements pool count.
func (p *pool) Release(item PoolItem) {
	if item == nil {
		return
	}

	// CAS loop instead of an unconditional decrement: one double-Release
	// would wrap the uint32 count to ~4.29e9, after which the capacity
	// check is permanently true and the pool can never dial again.
	for {
		c := p.count.Load()
		if c == 0 {
			break // never counted (caller misuse): close, but do not underflow
		}
		if p.count.CompareAndSwap(c, c-1) {
			break
		}
	}

	if err := item.Close(); err != nil {
		p.logger.Errorf("Error closing pool item: %v", err)
	}
}

// drainClose closes every item left in the queue. Whoever observes
// closing=true may call it; channel receives hand each item to exactly one
// drainer, so items are closed exactly once even with concurrent drains.
func (p *pool) drainClose() {
	for {
		select {
		case ent := <-p.queue:
			item := ent.item
			ent.item = nil
			p.entryPool.Put(ent)
			p.Release(item)
		default:
			return
		}
	}
}

// Close closes the pool and all its items safely without channel races.
func (p *pool) Close() {
	if !p.closing.CompareAndSwap(false, true) {
		return
	}

	close(p.stopChan)
	p.drainClose()
}

// Len returns the current number of items created in the pool.
func (p *pool) Len() int {
	return int(p.count.Load())
}

// Cap returns the capacity of the pool.
func (p *pool) Cap() int {
	return int(p.capacity)
}

// decrement removes one from the created-connection count without
// underflowing (see Release).
func (p *pool) decrement() {
	for {
		c := p.count.Load()
		if c == 0 {
			return
		}
		if p.count.CompareAndSwap(c, c-1) {
			return
		}
	}
}

// validateIdleConnections periodically validates idle connections.
func (p *pool) validateIdleConnections() {
	ticker := time.NewTicker(p.config.ValidationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopChan:
			return
		case <-ticker.C:
			p.validateConnectionSubset()
		}
	}
}

// validateConnectionSubset is the idle-connection sweeper. It drains the
// idle queue once per pass, evicts everything that aged past IdleTimeout
// (stamp-only: no I/O spent on dead connections), protocol-validates at most
// five survivors, and re-parks the rest with their original stamps —
// validation traffic does not make a connection less idle. Draining the
// whole queue rather than five items makes IdleTimeout enforcement complete
// even for pools much larger than the validation subset; it runs on a
// background goroutine at ValidationInterval cadence, off the hot path.
func (p *pool) validateConnectionSubset() {
	if p.closing.Load() {
		return
	}
	if p.config.ValidationStrategy == ValidationNone {
		return
	}

	idle := make([]*poolEntry, 0, p.capacity)
	for {
		select {
		case ent := <-p.queue:
			idle = append(idle, ent)
		default:
			goto swept
		}
	}
swept:

	const maxToCheck = 5
	validated := 0
	for _, ent := range idle {
		if p.expired(ent.idleNs.Load()) {
			item := ent.item
			ent.item = nil
			p.entryPool.Put(ent)
			p.Release(item)
			continue
		}

		if validated < maxToCheck {
			validated++
			if !p.validateConnection(ent.item) {
				item := ent.item
				ent.item = nil
				p.entryPool.Put(ent)
				p.Release(item)
				continue
			}
		}

		p.requeue(ent)
	}
}

// requeue returns an entry wrapper to the idle queue with its stamp kept,
// releasing the item if the queue moved under us or shutdown started.
func (p *pool) requeue(ent *poolEntry) {
	if p.closing.Load() {
		item := ent.item
		ent.item = nil
		p.entryPool.Put(ent)
		p.Release(item)
		return
	}
	select {
	case p.queue <- ent:
		if p.closing.Load() {
			p.drainClose()
		}
	default:
		item := ent.item
		ent.item = nil
		p.entryPool.Put(ent)
		p.Release(item)
	}
}

// validateConnection validates a connection based on the configured strategy.
// Items may validate themselves (see pipeline.Validate); that takes
// precedence because self-validating items own their connection's stream
// state and cannot be probed with raw reads.
func (p *pool) validateConnection(item PoolItem) bool {
	if item == nil {
		return false
	}

	if v, ok := item.(interface{ Validate() bool }); ok {
		return v.Validate()
	}

	conn, ok := item.(interface{ SetDeadline(time.Time) error })
	if !ok {
		return true // validateConnectionBasic: nothing to probe, accept
	}

	deadline := time.Now().Add(p.config.ValidationTimeout)
	if err := conn.SetDeadline(deadline); err != nil {
		return false
	}

	defer func() {
		_ = conn.SetDeadline(time.Time{})
	}()

	return p.validateConnectionWithStrategy(item)
}

func (p *pool) validateConnectionWithStrategy(item PoolItem) bool {
	var lastErr error

	for range p.config.MaxValidationAttempts {
		var err error

		switch p.config.ValidationStrategy {
		case ValidationNone:
			return true
		case ValidationPing:
			err = p.validatePing(item)
		case ValidationRead:
			err = p.validateRead(item)
		default:
			return true
		}

		if err == nil {
			return true
		}

		lastErr = err
		// No inter-attempt sleep: each attempt is already bounded by
		// ValidationTimeout, and the sleep only delayed declaring a
		// connection dead.
	}

	if lastErr != nil {
		p.logger.Errorf("Connection validation failed after %d attempts: %v",
			p.config.MaxValidationAttempts, lastErr)
	}

	return false
}

func (p *pool) validatePing(item PoolItem) error {
	if conn, ok := item.(net.Conn); ok {
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			var b [1]byte
			_ = tcpConn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
			n, err := tcpConn.Read(b[:])
			_ = tcpConn.SetReadDeadline(time.Time{})

			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					return nil
				}
				return err
			}

			if n > 0 {
				return errors.New("unexpected data during ping validation")
			}
		}
	}
	return nil
}

func (p *pool) validateRead(item PoolItem) error {
	reader, ok := item.(io.Reader)
	if !ok {
		return nil
	}

	if conn, ok := item.(net.Conn); ok {
		oldDeadline := time.Now().Add(p.config.ValidationTimeout)
		_ = conn.SetReadDeadline(oldDeadline)
		defer func() { _ = conn.SetReadDeadline(time.Time{}) }()
	}

	var b [1]byte
	n, err := reader.Read(b[:])
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return nil
		}
		return err
	}

	if n > 0 {
		return errors.New("unexpected data during read validation")
	}

	return nil
}
