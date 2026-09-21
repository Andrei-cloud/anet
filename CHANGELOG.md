# Changelog

All notable changes to `github.com/andrei-cloud/anet` are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/); this
project adheres to [Semantic Versioning](https://semver.org/).

## [1.0.0] - 2026-09-21

The asynchronous build, released as v1.0.0. Landmarks: the `SendAsync` API,
a multiplexed per-connection transport, a rewritten task-ownership model,
bounded shutdowns, and measured allocation reductions across every hot
path. Reaching 1.0.0 freezes the exported API under Semantic Versioning:
additive changes bump minor, breaking changes major.
Introduced on branch `perf/async-networking` (commit `3a834b5`) on top of
`v0.3.0`. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the design.

### Added

- **`SendAsync` / `SendAsyncContext`** on the `Broker` interface: submit a
  request without blocking. The returned `<-chan Response` receives exactly
  one `Response{Payload, Err}` per accepted request. Cancelling the context
  abandons the wait; a late response is garbage-collected with its channel.
- **`BrokerConfig.Multiplex`**: a per-connection pipelined transport
  (`pipeline.go`). Each connection gets a background writer and reader
  goroutine and is correlated by task ID, so one connection carries up to
  `MaxInflightPerConn` outstanding requests. Connections return to the pool
  immediately after submit; no broker workers are involved (`Start` is not
  needed). Submits past the bound shed with `ErrQueueFull`.
- **`BrokerConfig.MaxInflightPerConn`** (default 256): outstanding-request
  bound per multiplexed connection.
- **`BrokerConfig.CloseTimeout`** (default 10 s): the upper bound on how long
  `Close` waits for workers. Negative waits forever, the previous behavior.
- **`anet.NewTCPFactory(cfg)`**: a `Factory` that dials with `DialTimeout`
  (covering DNS plus connect) and arms TCP keepalive and `TCP_NODELAY` from
  the same `PoolConfig`. These fields were previously declared and inert.
- **`PoolConfig.DisableNoDelay`** and **`PoolConfig.Logger`** (diagnostics
  now go to the configured `Logger`; the default is silent, previously they
  wrote to `os.Stderr`).
- **`ServerConfig.ReusePort`**: sets `SO_REUSEPORT` on the listener so the
  kernel spreads accepts across listeners on the same address (unix only).
- **`ServerConfig.WriteQueueDepth`** (default 64): queued responses per
  connection before handlers park (backpressure).
- Per-connection **response write batching**: a single writer goroutine per
  server connection batches queued responses into one `write` syscall per
  burst (up to 32 KB staging).
- **Handler panic isolation**: a panicking server handler is logged and
  skipped; the process survives and the connection keeps serving.
- **Pool self-validation hook**: items exposing `Validate() bool` validate
  themselves. Multiplexed pipelines use it so raw `ValidationRead` probes
  never race their reader goroutine.

### Changed (behavior and configuration semantics)

- **`BrokerConfig` timeouts: `0` now means the default, not "disabled".**
  A zero `WriteTimeout`/`ReadTimeout` previously armed *no* deadline, which
  is what made `broker.Close` able to hang forever. Pass a negative value to
  explicitly disable a deadline. `applyDefaults` normalizes a copy of the
  struct, so the caller's config is never mutated.
- **`ServerConfig.ReadTimeout` defaults to 0 (off), was 5 s in docs but never
  applied.** It is now actually wired — as the body-read deadline for a frame
  whose header has arrived, cleared between frames — and it is opt-in,
  following the `net/http` `ReadHeaderTimeout` precedent. Arming it costs one
  deadline operation per frame (~1–2 µs on macOS, measured as a ~25% latency
  regression when on by default).
- **`ServerConfig.MaxConcurrentHandlers` defaults to 10000** (the README's
  "0 = unlimited" claim was never the code's behavior; any non-positive value
  now gets the default).
- **`pool.Close` semantics completed**: every connection that ever entered
  the idle queue is now closed exactly once even with concurrent `Put`s
  racing the drain. Previously an item could slip in after the drain and be
  leaked unclosed.
- **Pool diagnostics are silent by default**: with no `Logger` configured,
  pool warnings no longer print to `os.Stderr`.
- **Oversized payloads are rejected, not truncated**: requests whose payload
  exceeds 65531 bytes now fail at submit with `ErrMaxLenExceeded`. The old
  scatter-gather path truncated the 2-byte length header at 65532 and
  desynchronized the connection.
- **`Start` after `Close` returns `ErrQuit` immediately** instead of racing
  shutdown; `Start` and `Close` are safe in any order.
- **`server.Stop` is self-healing**: connections accepted mid-shutdown are
  closed on the spot, and `Stop` re-forces closure of anything still up after
  `ShutdownTimeout` before logging.

### Migration notes for v0.3.0 users

1. You pass a bare `&BrokerConfig{}` today expecting "no deadlines": add
   negative timeouts. A bare struct now means 5 s write/read deadlines.
2. The `Broker` interface gained two methods; third-party implementations
   must add `SendAsync` and `SendAsyncContext` (or embed `anet.Broker`).
3. `NewPool` with `ValidationInterval > 0` now actually closes idle
   connections after `IdleTimeout` (it always claimed to; it did not). Set
   `IdleTimeout` negative to opt out.

### Fixed

- **Task use-after-recycle (critical).** A caller whose context expired while
  its task sat in the request queue returned the task to the pool while the
  queue still held its pointer. Workers then processed zombie tasks with
  stale fields — the true source of spurious `task ID mismatch` errors on
  healthy connections — and could run two workers on the same task struct at
  once, corrupting frames and cross-delivering responses between callers.
  Fixed structurally by the new ownership model (task staging at submit,
  single queue-consumer ownership). Regression: `TestSendContextCancelWhileQueued`.
- **`broker.Close` could hang forever.** Workers parked on an exhausted
  connection pool never observed broker shutdown (the pool wait watched only
  the caller context and the pool's own stop channel), and reads armed with
  no deadline never observed cancellation. Fixed: broker-context pool waits,
  always-present deadlines, bounded close. Regression: `TestCloseWithPoolExhausted`.
- **`Start` versus `Close` WaitGroup misuse.** `wg.Add` ran on the `Start`
  goroutine while `Close`'s `wg.Wait` observed counter zero — reproduced as a
  race report in a 20-line stdlib microtest, so it is fixed with mutex-based
  accounting rather than documentation.
- **Server accept-versus-`Stop` race.** A connection accepted after `Stop`
  began its close sweep was stored after `Stop` had enumerated connections:
  it was never closed, leaking the connection and its goroutines, and its
  `connWG.Add` could be observed "concurrent with Wait". Fixed by the
  `connMu` admission barrier plus re-forced closure. Regressions:
  `TestServerStopWhileAccepting`, `TestServerHandlerPanicSurvives`.
- **Pool `Put`-during-`Close` leak.** With the old mutex-based close
  handshake an item could still be accounted inconsistently; the new closing
  handshake guarantees exactly-once closure. Regression:
  `TestPoolConcurrentPutClose` (8 workers x 2000 operations under `-race`).
- **`pool.count` underflow.** One double-`Release` wrapped the `uint32` count
  to ~4.29e9 and permanently disabled dialing; decrements now floor at zero.
- **Frame-length truncation at the protocol boundary** (see Changed).
- **Dead configuration fields** now wired: `PoolConfig.IdleTimeout`
  (background eviction of idle connections), `ServerConfig.ReadTimeout`,
  `PoolConfig.KeepAliveInterval` and NoDelay (via `NewTCPFactory`),
  `ServerConfig.MaxConcurrentHandlers` bounds.
- **Dead code with a false claim**: the framing "fast path for small
  messages: format on stack without any heap allocations" allocated 576 B per
  message (the array escaped through the `io.Writer` interface). Replaced with
  zero-allocation buffer-pool staging.
- **Unreachable guards removed** (`sync.Pool` fallbacks that cannot fire,
  dead class-index clamps, an accept-timeout backoff for a deadline that is
  never armed, `time.Sleep` padding inside validation retries).
- **`nextPow2Uint64` overflow** for inputs above 2^63 (clamped; it previously
  returned 0 and silently built a reject-everything ring buffer).
- Documentation fixes: the README's error table listed `ErrTimeout`, which
  the library never exported; the `ServerConfig` reference table listed
  defaults that did not match `applyDefaults` (`MaxConns`,
  `MaxConcurrentHandlers`, `IdleTimeout`).

### Performance

Measured before/after on Apple M5 Max, darwin/arm64, Go 1.27.1, `count=3`,
via `benchstat`. Full methodology and per-allocation accounting:
[docs/ARCHITECTURE.md §13](docs/ARCHITECTURE.md).

| Benchmark | Before | After | Bytes Before → After | Allocs Before → After |
| :-- | --: | --: | :-- | :-- |
| `Write_Small` | 717 ns | 510 ns | 576 → **0 B** | 1 → **0** |
| `BrokerSend/Workers_1` | 20.1 µs | 22.5 µs | 700 → **258 B** | 7 → 6 |
| `BrokerSend/Workers_100` | 6.99 µs | **6.60 µs** | 703 → **259 B** | 7 → **6** |
| `Server_Echo_Sequential` | 19.8 µs | 21.7 µs\* | 1,278 → **74 B** | 6 → **3** |
| `Server_Echo_Parallel` | 9.56 µs | **9.21 µs** | 1,283 → **75 B** | 6 → **3** |
| `TaskPoolingPerformance` | 12.1 µs | 11.6 µs | 629 → **186 B** | 5 → 4 |
| `Pool/GetPut` | 17.6 ns | 47.2 ns\*\* | 0 B | 0 |
| `BufferPool/GetPut` | 11.5 ns | 12.0 ns | 0 B | 0 |

New transport comparison (`BenchmarkTransport_RoundTrip`; 18 concurrent
caller threads, loopback TCP; `Mmsg/s` is aggregate end-to-end messages per
second across all threads): the synchronous queue on 100 connections with
100 workers runs 8.53 µs/round-trip (**0.117 Mmsg/s**); **Multiplex on 2
connections with zero workers runs 4.20 µs (0.238 Mmsg/s)** — double the
throughput on 50x fewer connections. Burst submission
(`BenchmarkSendAsync_FireHundred`, 100 requests per iteration): the
synchronous queue sheds 36–103 submissions per iteration at saturation;
Multiplex sheds 0.13–0.58. Server side (`BenchmarkServer_Echo_Parallel`):
0.106 Mmsg/s aggregate across 18 client connections at 9.4 µs/op.

\* The sequential echo path pays one channel handoff to the batching writer
goroutine; the parallel throughput and 94% of the allocations improved.
\*\* The pool stamps idle time on `Put` so `IdleTimeout` is finally enforced;
one clock read per `Put`, negligible on microsecond-scale RPC paths.

Accepted costs, documented instead of hidden:

- One fresh ~96 B delivery channel per broker request. A pooled channel can
  swallow a parked waiter's response (the drain races the waiter's select
  before it is scheduled); this deadlock was reproduced during development.
  Correctness is worth 96 bytes.
- The per-request channel shows as +45% bytes on the allocation-bound
  in-memory `net.Pipe` benchmarks, while real-transport bytes fell 63%. The
  single-worker queue configuration (`Workers_1`, one goroutine per request
  in flight) pays ~12% latency for the new handoff; the multi-worker
  configurations that this library is tuned for improved.

### Testing and tooling

- Regression tests for every fixed bug above (see ARCHITECTURE.md §15 for the
  full map). Full suite green under `go test -race -count=2 ./...`.
- New benchmarks: `BenchmarkTransport_RoundTrip` (sync vs async vs multiplex
  at 2/8/32 connections), `BenchmarkSendAsync_FireHundred` (burst shedding).
- Examples: `example/main.go` demonstrates the async fan-out pattern and uses
  `NewTCPFactory`.

## v0.3.0 and earlier

See the git history (`git log --oneline --decorate`); no changelog was kept
before this revision.
