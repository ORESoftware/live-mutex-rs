# live-mutex-rs

[![CI: cargo test](https://img.shields.io/badge/test-cargo%20test-blue?style=flat-square)](#local-development)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat-square)](LICENSE)

A Rust port of the Node.js [`live-mutex`](https://github.com/ORESoftware/live-mutex)
networked-mutex library. The default deployment is a single-process broker that
synchronizes access to per-key lock state, plus a Rust client library that talks
to it over TCP, Unix domain sockets, or HTTP (for serverless / Lambda callers).
An experimental HTTP-only `BrokerRaft` backend adds a replicated Raft log,
leader election, quorum commit, and follower failover for three- or five-node
clusters.

This crate is what we run in production at ORE Software (internally as
`dd-rust-network-mutex`); the binary name still reflects that history. The
public crate is the same code, MIT-licensed, with no internal dependencies.

## Why a Rust port

The Node.js [`live-mutex`](https://github.com/ORESoftware/live-mutex) broker is
well-loved and battle-tested, but the JavaScript runtime puts a ceiling on
throughput and tail latency for a service whose entire job is to park, wake,
and dispatch correlation-ID frames as fast as possible. This crate is a
from-scratch Rust port. Several features landed in this port first and were
later mirrored back into upstream `live-mutex` (composite locking, fencing
tokens, a single TTL sweeper, an HTML status page); the issue numbers below
are kept for historical traceability.

Headline features:

- **Multi-key (composite) locking** — atomic acquisition of up to five keys
  in a single request, deadlock-free via global lexicographic ordering, with
  a per-key fencing token returned for each acquired key. See
  [Multi-key (composite) locking](#multi-key-composite-locking) below.
  (Originally tracked at [`live-mutex#105`](https://github.com/ORESoftware/live-mutex/issues/105),
  now also landed in upstream Node.js.)
- **Fencing tokens** — every successful `acquire` (single-key, semaphore
  slot, or composite member) returns a per-key strictly-increasing `u64`.
  Callers attach the token to whatever resource the lock protects so a
  stale leaseholder's eventual write can be rejected. See
  <https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html>.
- **Reader-writer locks** alongside the regular exclusive-lock client
  (`RwClient`). Reader/writer state is tracked in the same broker key, with
  fencing tokens emitted on both reader and writer grants.
- **Semaphore-style locks** — `max=N` admits up to N simultaneous holders for
  a key. Each holder gets its own fencing token. See
  [Semaphore-style locks](#semaphore-style-locks-concurrency--1) below.
- **HTTP transport** for callers that can't hold a long-lived TCP connection
  (Lambda, Cloudflare Workers, Vercel functions). Long-poll via `waitMs`.
- **Experimental BrokerRaft HA backend** — an HTTP-only three- or five-node
  Raft path with PreVote leader election, quorum commit, incremental log
  replication, chunked snapshot catch-up, and joint-consensus membership changes. See
  [High availability](#high-availability).
- **Single TTL sweeper** instead of a per-request timer. Originally tracked
  at [`live-mutex#13`](https://github.com/ORESoftware/live-mutex/issues/13);
  now also landed in upstream Node.js.
- **HTML operator status page**. Originally tracked at
  [`live-mutex#108`](https://github.com/ORESoftware/live-mutex/issues/108).
- **TCP\_NODELAY / TCP\_QUICKACK socket-tuning experiment** with Prometheus
  counters. Originally tracked at
  [`live-mutex#22`](https://github.com/ORESoftware/live-mutex/issues/22).
- **TLS** behind the optional `tls` cargo feature, although a load balancer or
  service mesh is usually a more capable terminator.

The internal queue is a doubly-linked arena-backed list with O(1) push/pop at
both ends and O(1) removal of any element by request UUID — same property
[`@oresoftware/linked-queue`](https://www.npmjs.com/package/@oresoftware/linked-queue)
gives the Node.js broker. See `src/queue.rs`.

## Crate layout

```
.
├── Cargo.toml
├── LICENSE
├── readme.md
├── PROTOCOL.md         # single source of truth for the JSON wire format
├── src/                # broker + Rust client
│   ├── main.rs         # binary entrypoint (env-driven config)
│   ├── lib.rs          # public re-exports
│   ├── protocol.rs     # serde-tagged Request / Response enums (canonical schema)
│   ├── queue.rs        # O(1) linked queue
│   ├── broker.rs       # transport-agnostic lock state machine
│   ├── server.rs       # TCP, UDS, HTTP listeners (+ TLS feature)
│   ├── metrics.rs      # /metrics text output
│   ├── status.rs       # HTML operator status page (live-mutex#108)
│   ├── sockopt.rs      # TCP_NODELAY + TCP_QUICKACK helpers (live-mutex#22)
│   └── client.rs       # Tokio-based Rust client (Client + RwClient)
├── tests/
│   └── integration.rs  # end-to-end TCP/UDS/HTTP smoke tests
├── examples/
│   └── wire_format_probe.rs  # prints canonical JSON for cross-runtime devs
├── clients/                  # cross-runtime clients (TS / Go / Dart / Gleam)
│   ├── README.md
│   ├── ts/                   # TypeScript: discriminated union + compare-vs-live-mutex
│   ├── go/                   # Go: typed const block + cmd/smoke
│   ├── dart/                 # Dart: sealed classes + bin/smoke.dart
│   └── gleam/                # Gleam: real ADT + Erlang gen_tcp FFI
└── scripts/
    ├── run-all-client-smokes.sh   # exercise every runtime in one command
    └── quickack-experiment.sh     # A/B latency probe with QUICKACK on/off (Linux)
```

## Cross-runtime clients

We ship clients in **five** runtimes — Rust, TypeScript, Go, Dart, and Gleam
— so the broker can be exercised from anywhere. Every client mirrors the
Rust `Request`/`Response` enum *as a sum type in its native idiom* (TS
discriminated union, Go typed const + switch, Dart sealed class, Gleam
custom type), so `if (data.type === '…')` magic-string handling — the
shape used by upstream `live-mutex`'s `broker.js` — is impossible across
the entire client surface. See `clients/README.md` for details.

### Head-to-head benchmark vs. `oresoftware/live-mutex`

`clients/ts/src/compare.ts` runs the same workload (configurable
`WORKERS`, `KEYS`, `DURATION_MS`) against both brokers in the same Node
process and prints a side-by-side ops/s, avg/max latency, and ratio:

```
[compare] workers=8 keys=4 duration=2000ms ours=127.0.0.1:6970 theirs=127.0.0.1:6971
ours      total= 102775  throughput=   51388 ops/s  avg=   0.16ms  max=   1.35ms  errors=0
theirs    total=  76411  throughput=   38206 ops/s  avg=   0.21ms  max=   3.69ms  errors=0
[compare] ratio (ours / theirs) = 1.35x
```

(Sample numbers from a single laptop M-class run; absolute throughput is
hardware-dependent, but the ratio is a useful first signal.)

### Redis / Broker / BrokerRaft benchmark

`examples/redis_vs_raft_bench.rs` provides a rough acquire/release comparison
between Redis locks, the regular Broker HTTP path, and the BrokerRaft HTTP/LB
path. It normalizes away each system's fencing-token format: Redis uses
`SET key token NX PX ttl` plus a compare-and-delete Lua unlock, while the Broker
targets use `/v1/lock` and `/v1/unlock`.

```bash
# Redis on 127.0.0.1:6379, Broker HTTP on 127.0.0.1:6971,
# and BrokerRaft LB/service on 127.0.0.1:6972.
BENCH_WORKERS=16 BENCH_KEYS=256 BENCH_DURATION_MS=10000 \
  BENCH_TARGET=all BENCH_BROKER=127.0.0.1:6971 BENCH_RAFT=127.0.0.1:6972 \
  cargo run --release --example redis_vs_raft_bench --no-default-features

# Run only one side.
BENCH_TARGET=redis cargo run --release --example redis_vs_raft_bench --no-default-features
BENCH_TARGET=broker BENCH_BROKER=127.0.0.1:6971 \
  cargo run --release --example redis_vs_raft_bench --no-default-features
BENCH_TARGET=raft BENCH_RAFT=127.0.0.1:6972 \
  cargo run --release --example redis_vs_raft_bench --no-default-features

# Directly compare Redis and BrokerRaft.
BENCH_TARGET=redis-raft BENCH_REDIS=127.0.0.1:6379 BENCH_RAFT=127.0.0.1:6972 \
  cargo run --release --example redis_vs_raft_bench --no-default-features

# Directly compare regular Broker and BrokerRaft.
BENCH_TARGET=broker-raft BENCH_BROKER=127.0.0.1:6971 BENCH_RAFT=127.0.0.1:6972 \
  cargo run --release --example redis_vs_raft_bench --no-default-features

# Simulate a round-robin LB across all BrokerRaft HTTP endpoints.
BENCH_TARGET=raft BENCH_RAFT=127.0.0.1:6972,127.0.0.1:6973,127.0.0.1:6974 \
  cargo run --release --example redis_vs_raft_bench --no-default-features

# Simulate a leader-aware LB by probing /raft/leaderz and sending all Raft
# benchmark traffic to the current ready leader.
BENCH_TARGET=raft BENCH_RAFT=127.0.0.1:6972,127.0.0.1:6973,127.0.0.1:6974 \
  BENCH_RAFT_ROUTE=leader \
  cargo run --release --example redis_vs_raft_bench --no-default-features

# Keep leader-aware traffic, but scrape all Raft nodes for the full-log guard.
BENCH_TARGET=raft BENCH_RAFT=127.0.0.1:6972,127.0.0.1:6973,127.0.0.1:6974 \
  BENCH_RAFT_ROUTE=leader BENCH_RAFT_METRICS=true \
  BENCH_RAFT_METRICS_ENDPOINTS=127.0.0.1:6972,127.0.0.1:6973,127.0.0.1:6974 \
  cargo run --release --example redis_vs_raft_bench --no-default-features
```

Set `BENCH_HTTP_AUTH_TOKEN` when the HTTP API requires auth. The standalone
benchmark opens one short-lived connection per request by default, which matches
the simple LB-facing API but means the number includes connection setup cost.
Set `BENCH_HTTP_KEEPALIVE=true` to reuse per-worker HTTP sockets per endpoint
when you want to reduce client-side TCP churn and isolate Broker versus
BrokerRaft server work more closely. Pass a single `BENCH_RAFT` endpoint for a
real LB service or leader-preferred profiling. Pass a comma-separated list to
round-robin benchmark traffic across node HTTP ports per HTTP request, so
acquire and release in one cycle may hit different nodes and include follower
proxy cost. Set `BENCH_RAFT_ROUTE=leader` with a comma-separated endpoint list
to model a leader-aware LB: the harness probes `/raft/leaderz` and collapses
traffic to the ready leader when one is found, falling back to round-robin if no
endpoint reports leader readiness. Set `BENCH_IO_TIMEOUT_MS` to cap each network
operation when an endpoint is missing or unhealthy.

For CPU profiles, use `scripts/profile-broker.sh` to launch and profile either
a regular Broker server or a local three-node BrokerRaft cluster under the same
benchmark. Set `PROFILE_TARGET=raft` to profile the ready BrokerRaft leader
process; the default `PROFILE_TARGET=broker` profiles the regular Broker
baseline. `scripts/profile-raft.sh` still profiles the Raft integration-test
workload. Both scripts build with the `profiling` profile and force frame
pointers so `perf`, `sample`, or flamegraph output has usable stacks.
`scripts/profile-broker.sh` defaults `BENCH_HTTP_KEEPALIVE=true` for cleaner
server-side CPU profiles; set `BENCH_HTTP_KEEPALIVE=false` there when you want
one-shot connection behavior.
`scripts/profile-broker.sh` also captures before/after `/metrics` and status
artifacts in `target/profiles` by default, including every local BrokerRaft node
when `PROFILE_TARGET=raft`; set `CAPTURE_METRICS=false` to disable that scrape.
The `flamegraph` mode uses the standalone `flamegraph` binary for
`SAMPLE_SECONDS` and stops it directly, so it does not require GNU `timeout(1)`.
When profiling a local BrokerRaft cluster, the benchmark full-log guard samples
all three local Raft HTTP endpoints by default, even if request traffic is routed
to the current leader.
Profiler failures still leave benchmark and after-metrics artifacts in the output
directory before the script returns the profiler status.
BrokerRaft also keeps small nonblocking leader role/term and leader peer hint
caches for hot forwarding and heartbeat checks; demotion paths clear the role
cache before slow hard-state writes, and write admission still uses the normal
leader-readiness checks. The election loop sleeps until the current election
deadline while follower/candidate instead of polling the runtime mutex every
20 ms. Durable hard state is cached after startup and successful writes, so
exact duplicate term/vote/commit writes are elided; commit-only advancement uses
a cached, pre-sized fixed two-slot sidecar instead of reopening, rewriting, and
fsyncing `raft-hard-state.json`.

Local loopback measurement on 2026-06-05 with 8 workers, 256 keys, a
leader-preferred 3-node BrokerRaft cluster, and 3-second runs:

| Target | Raft sync policy | Successful cycles/sec | p50 cycle latency |
| --- | --- | ---: | ---: |
| BrokerRaft | `sync_log=true`, `sync_commit=true` | 319 | 24.031 ms |
| BrokerRaft | `sync_log=false`, `sync_commit=true` | 1,072 | 6.832 ms |

`sync_log=false` is intentionally unsafe: it skips append-log fsyncs to expose
the disk-sync ceiling, but a process or host crash can lose the latest unsynced
Raft entries. `sync_commit=false` is a separate unsafe benchmark switch that
skips commit-slot fsyncs; acknowledged entries may recover only through the last
synced commit slot after a crash. Keep the defaults `sync_log=true` and
`sync_commit=true` for crash-safe failover tests.

## Wire protocol (TCP / UDS)

Each frame is one JSON object terminated by `\n`. A final valid JSON object
without a trailing newline is also accepted when the peer closes its write side,
matching common JSONL stream-parser flush behavior. Every request carries a
client-generated `uuid` correlation ID; the broker echoes it on the matching
response. **Both the `type` discriminator and every field are camelCase.**
The canonical schema is `src/protocol.rs` (a serde-tagged Rust enum); see
`PROTOCOL.md` for the cross-runtime contract.

### Client → broker requests

| `type`                          | Required fields                    | Optional fields                                                   | Notes                                                              |
| ------------------------------- | ---------------------------------- | ----------------------------------------------------------------- | ------------------------------------------------------------------ |
| `version`                       | `uuid`, `value`                    | —                                                                 | Recommended first frame.                                           |
| `auth`                          | `uuid`, `token`                    | —                                                                 | Required when `LMX_AUTH_TOKEN` is set.                             |
| `lock`                          | `uuid` plus (`key` OR `keys`)      | `pid`, `ttl`, `max`, `force`, `retryCount`, `keepLocksAfterDeath` | `keys` array (1..=5) is a composite lock.                          |
| `unlock`                        | `uuid` plus (`key` OR `keys`), `lockUuid` | `force`                                                    | `lockUuid` is the value returned on grant.                         |
| `registerRead`                  | `uuid`, `key`                      | —                                                                 | Reader-writer: reader.                                             |
| `registerWrite`                 | `uuid`, `key`                      | —                                                                 | Reader-writer: writer.                                             |
| `endRead` / `endWrite`          | `uuid`, `key`                      | —                                                                 | Reader-writer release.                                             |
| `lockInfo`                      | `uuid`, `key`                      | —                                                                 | Returns holders + queue depth.                                     |
| `ls`                            | `uuid`                             | —                                                                 | Returns all known keys.                                            |

### Broker → client responses

| `type`                                | Notable fields                                                                                                                  |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `version`                             | `brokerVersion`, `ok`                                                                                                           |
| `auth`                                | `ok`                                                                                                                            |
| `lock`                                | `acquired`, `key`, `lockRequestCount`, `lockUuid?`, `fencingToken?`, `readersCount?`                                            |
| `compositeLock`                       | `acquired`, `keys`, `lockUuid?`, `fencingTokens?` (`{key: token}`)                                                              |
| `unlock`                              | `unlocked`, `keys`, `lockRequestCount`                                                                                          |
| `registerReadResult`                  | `granted`, `key`, `readersCount`, `writerFlag`, `lockUuid?`, `fencingToken?`                                                    |
| `registerWriteResult`                 | same shape                                                                                                                      |
| `endReadResult` / `endWriteResult`    | `key`, `readersCount`, `writerFlag`                                                                                             |
| `lockInfo`                            | `key`, `isLocked`, `lockholderUuids`, `lockRequestCount`, `readersCount`, `writerFlag`                                          |
| `lsResult`                            | `keys`                                                                                                                          |
| `error`                               | `error`                                                                                                                         |

A `lock` request that can't be granted immediately receives **two** responses
sharing the same `uuid`: first an `acquired:false` notice with the queue depth,
then later — when the broker dequeues you — an `acquired:true` grant with
`lockUuid` and `fencingToken`. The Rust `Client` handles the multiplexing for
you; raw protocol implementers must keep an inflight table that allows
multiple responses per correlation UUID.

## HTTP API (serverless / Lambda)

| Method | Path                  | Body                                                                  | Notes                                                                       |
| ------ | --------------------- | --------------------------------------------------------------------- | --------------------------------------------------------------------------- |
| GET    | `/`, `/status`        | —                                                                     | HTML operator status page (originally `live-mutex#108`). Auto-refreshes 5s. |
| GET    | `/healthz`, `/readyz` | —                                                                     | Liveness/readiness.                                                         |
| GET    | `/metrics`            | —                                                                     | Prometheus text exposition (`dd_rust_network_mutex_*`).                      |
| POST   | `/v1/lock`            | `{ "key" \| "keys", "ttlMs?", "max?", "waitMs?" }`                    | Returns `{ acquired, lockUuid?, fencingTokens, queueDepth, keys, error? }`. Validation failures (missing `key`/`keys`, oversized composite, `key` and `keys` both set) come back as 400 with `error` populated. |
| POST   | `/v1/unlock`          | `{ "key" \| "keys", "lockUuid?", "force?" }`                          | Returns `{ unlocked, keys }`. `lockUuid` is required unless `force: true` (operator override that breaks any existing holder). |
| POST   | `/v1/rw/read`         | `{ "key", "waitMs?" }`                                                | Returns `{ granted, lockUuid?, fencingToken?, readersCount, writerFlag }`.  |
| POST   | `/v1/rw/read/end`     | `{ "key", "lockUuid" }`                                               | —                                                                           |
| POST   | `/v1/rw/write`        | same shape                                                            | —                                                                           |
| POST   | `/v1/rw/write/end`    | same shape                                                            | —                                                                           |
| GET    | `/v1/lock-info/:key`  | —                                                                     | —                                                                           |
| GET    | `/v1/locks`           | —                                                                     | List all known keys.                                                        |

When BrokerRaft is enabled, the HTTP listener also exposes `/raft/status`,
`/raft/progress`, `/raft/learners`, and `/raft/leaderz`. `/raft/progress`
reports per-peer `nextIndex`, `matchIndex`, lag, and staged-learner state;
impossible progress above the leader's local log tail remains visible for
debugging but reports conservative lag and not-caught-up status.
`GET/POST/DELETE /raft/learners` lets an operator inspect, consensus-stage, and
remove non-voting learners before promoting them through membership change.
`/raft/leaderz` returns 200 only when the current leader has recently observed
quorum and 503 otherwise, which lets an HTTP-aware load balancer prefer a
quorum-fresh leader without making follower proxying part of the hot path.
`/raft/status`, `/raft/progress`, `/raft/learners`, and `/raft/leaderz` expose
`isLeaderReady`, `leaderQuorumAgeMs`, `leaderQuorumTimeoutMs`, `syncLog`,
`syncCommit`, and `unsafeDurability` so operators can distinguish the raw Raft
leader from a leader still fresh enough to accept writes and can spot unsafe
benchmark durability settings in status output.
BrokerRaft's public lock API supports single-key requests plus bounded
multi-key `keys` requests up to 3 distinct keys. The lower replicated cap keeps
per-log-index fencing-token reservations bounded while preserving union-style
overlap semantics. Larger composite requests are rejected before they enter the
Raft batcher or log.

`waitMs` is HTTP long-poll: the broker holds the request open up to that many
milliseconds while waiting for a queued lock to be granted. The default is no
wait; the caller should retry on `acquired:false`. BrokerRaft logs no-wait HTTP
acquires as fail-fast lock attempts, so a contended `waitMs=0` request does not
create a queued waiter or a follow-up cleanup log entry.

HTTP `/v1/lock` and `/v1/unlock` accept an optional `requestId` body field, or
`X-LMX-Request-Id`, `Idempotency-Key`, / `X-Idempotency-Key` headers. BrokerRaft
uses that value as the Raft request UUID, logs HTTP writes with request identity,
and reserves in-flight request IDs before append. A retry with the same request
id and same payload will not append a second command while the first proposal is
pending, and can return the original result after the response is known. Reusing
a request id with a different payload returns `409 Conflict`. The bounded applied
cache is rebuilt from committed log replay and included in Raft snapshots.
Unapplied reservations are not trimmed merely because the completed-response
cache is full; they remain bounded by the leader-local pending queue. Blank or
whitespace-only request ids are rejected before reservation or append. This is
not an unbounded etcd-style client-session ledger.

If `LMX_AUTH_TOKEN` is set, every HTTP call must include either an
`Authorization: Bearer <token>` or `X-LMX-Auth: <token>` header.

## Environment variables

The broker also accepts command-line flags declared in
[`.cli-flags.toml`](.cli-flags.toml). Flags are parsed with
`flags2env` into the same env-var names below, so precedence is:
runtime TOML defaults, then environment variables, then CLI flags. For example,
`dd-rust-network-mutex --tcp-port 7970 --disable-http` behaves like setting
`LMX_TCP_PORT=7970` and `LMX_DISABLE_HTTP=true` for that broker process only.
Run `dd-rust-network-mutex --help` for the generated flag table. Set
`LMX_CLI_FLAGS_CONFIG=/path/to/.cli-flags.toml` if the flag spec is not in the
current directory tree or `/etc/dd-rust-network-mutex/.cli-flags.toml`.

| Variable                | Default          | Notes                                                                                            |
| ----------------------- | ---------------- | ------------------------------------------------------------------------------------------------ |
| `LMX_BIND_HOST`         | `0.0.0.0`        | Bind address for both TCP and HTTP listeners.                                                    |
| `LMX_TCP_PORT`          | `6970`           | TCP port (matches upstream `live-mutex` default).                                                |
| `LMX_HTTP_PORT`         | `6971`           | HTTP port for serverless callers.                                                                |
| `LMX_DISABLE_TCP`       | `false`          | When `true`/`yes`, do not bind TCP.                                                              |
| `LMX_DISABLE_HTTP`      | `false`          | When `true`/`yes`, do not bind HTTP.                                                             |
| `LMX_UDS_PATH`          | unset            | If set, bind a Unix domain socket at that absolute path.                                         |
| `LMX_AUTH_TOKEN`        | unset            | Required handshake token (TCP/UDS) and `Authorization: Bearer …` value (HTTP).                  |
| `LMX_DEFAULT_TTL_MS`    | `4000`           | Default lock TTL in milliseconds.                                                                |
| `LMX_MAX_LOCK_HOLDERS`  | `1`              | Default `max` per key. Per-request `max` overrides.                                              |
| `LMX_MAX_CONCURRENCY_CAP` | `1000`         | Hard ceiling on per-key `max` (semaphore-style locks). Requests above this are silently clamped and counted in `dd_rust_network_mutex_concurrency_cap_clamps_total`. |
| `LMX_MAX_FRAME_BYTES`  | `1048576`        | Hard cap for one TCP/UDS JSONL frame before the broker drops the connection.                              |
| `LMX_FRAME_YIELD_EVERY` | `1024`          | Yield cooperatively after this many inbound TCP/UDS frames while draining a large already-buffered burst. |
| `LMX_MAX_RESPONSE_FRAME_BYTES` | `1048576` | Rust client-side cap for one broker response frame. Falls back to `LMX_MAX_FRAME_BYTES` if unset.         |
| `LMX_RAFT_BIND_ADDR` | `127.0.0.1:7980` | Raft peer RPC listener address; required when BrokerRaft is enabled.                              |
| `LMX_RAFT_ADVERTISE_ADDR` | unset | Address other Raft nodes should dial for this node. Defaults to `LMX_RAFT_BIND_ADDR` when unset.   |
| `LMX_RAFT_DATA_DIR` | `./data/raft/node-1` | Durable Raft log, hard-state, snapshot, learner, and local-voter state directory.                  |
| `LMX_RAFT_DATA_DIR_LOCK` | `true` | Hold `broker-raft.lock` so one live BrokerRaft process owns the durable state directory.           |
| `LMX_RAFT_SNAPSHOT_INTERVAL_MS` | `1800000` | Snapshot cadence for retained-log compaction; must be greater than `0`.                          |
| `LMX_RAFT_SNAPSHOT_MAX_LOG_ENTRIES` | `100000` | Snapshot/compact when retained log entries reach this count. `0` disables the entry-count trigger. |
| `LMX_RAFT_SNAPSHOT_MAX_LOG_BYTES` | `67108864` | Snapshot/compact when retained log bytes reach this size. `0` disables the byte-count trigger.    |
| `LMX_RAFT_SNAPSHOT_MAX_LOG_AGE_MS` | `1800000` | Snapshot/compact committed retained-log prefixes older than this age. `0` disables the age trigger. |
| `LMX_RAFT_TRAILING_LOG_ENTRIES` | `10000` | Retained suffix kept after snapshot compaction so near-lagging followers can use `AppendEntries`.  |
| `LMX_RAFT_MAX_FRAME_BYTES` | `134217728` | Hard cap for one Raft peer RPC JSONL frame before the peer connection is rejected.                         |
| `LMX_RAFT_APPEND_ENTRIES_MAX_ENTRIES` | `256` | Max log entries sent in one Raft `AppendEntries` catch-up batch.                                 |
| `LMX_RAFT_APPEND_ENTRIES_MAX_BYTES` | `1048576` | Approximate serialized entry byte budget for one Raft `AppendEntries` catch-up batch.             |
| `LMX_RAFT_TARGET_QUORUM_EXTRA_FANOUT` | `0` | Extra ready followers to include in foreground target-index replication beyond the minimum quorum need. |
| `LMX_RAFT_INSTALL_SNAPSHOT_CHUNK_BYTES` | `1048576` | Serialized snapshot bytes sent per `InstallSnapshot` RPC chunk.                                  |
| `LMX_RAFT_INSTALL_SNAPSHOT_MAX_STAGED_BYTES` | `134217728` | Max decoded snapshot bytes staged on follower disk for one `InstallSnapshot` transfer.            |
| `LMX_RAFT_INSTALL_SNAPSHOT_MAX_STAGED_TRANSFERS` | `4` | Max concurrent staged `InstallSnapshot` transfers retained on follower disk.                      |
| `LMX_RAFT_INSTALL_SNAPSHOT_STALE_TRANSFER_MS` | `1800000` | Max idle age for an incomplete staged `InstallSnapshot` transfer before follower cleanup.          |
| `LMX_RAFT_CLIENT_BATCH_MAX_ENTRIES` | `32` | Max leader-local client requests appended and committed in one Raft write batch.                  |
| `LMX_RAFT_CLIENT_PIPELINE_MAX_BATCHES` | `4` | Max configured client batches drained into one append/replicate/commit cycle under leader load.  |
| `LMX_RAFT_CLIENT_BATCH_MAX_PENDING` | `8192` | Max leader-local client requests allowed to wait for the Raft write lane before new requests are rejected. |
| `LMX_RAFT_CLIENT_BATCH_MAX_DELAY_MS` | `1` | Coalescing window before a leader-local client request batch is drained.                          |
| `LMX_RAFT_CLIENT_RESPONSE_CACHE_MAX_ENTRIES` | `8192` | Max recent BrokerRaft HTTP request-id responses retained for bounded idempotent retries.          |
| `LMX_RAFT_PROXY_RETRY_BUDGET_MS` | `2000` | Follower retry window for re-discovering and re-forwarding a proxied client request during leader churn. |
| `LMX_RAFT_SYNC_LOG` | `true` | Flush Raft append-log writes to stable storage before acknowledging them. Set `false` only for explicit unsafe throughput/benchmark experiments; crash durability is weakened. |
| `LMX_RAFT_SYNC_COMMIT` | `true` | Flush commit-only hard-state slot writes before applying committed entries. Set `false` only for explicit unsafe throughput/benchmark experiments; crash recovery may replay only through the last synced commit slot. |
| `LMX_RAFT_PEER_TOKEN` | unset | Optional shared secret required on Raft peer RPC frames. Use a Kubernetes Secret or equivalent in multi-node deployments; blank values are treated as unset. |
| `LMX_TTL_SWEEP_INTERVAL_MS` | `10`         | Periodic TTL-eviction sweep cadence (originally `live-mutex#13`). `0` disables auto-eviction.    |
| `LMX_STATUS_PORT`       | unset            | Bind a dedicated read-only HTML status listener on this port (originally `live-mutex#108`). The same page is also served at `/` on `LMX_HTTP_PORT`. |
| `LMX_TCP_NODELAY`       | `true`           | Apply `TCP_NODELAY` on broker-accepted sockets. Experiment from `live-mutex#22`.                 |
| `LMX_TCP_QUICKACK`      | `true`           | Re-apply `TCP_QUICKACK` after every read on Linux. No-op on macOS/BSD. See `live-mutex#22`.      |
| `LMX_TLS_CERT`          | unset            | (`tls` feature) PEM-encoded server certificate path.                                             |
| `LMX_TLS_KEY`           | unset            | (`tls` feature) PEM-encoded server private key path.                                             |
| `LMX_LOG_FORMAT`        | `text`           | Set to `json` for structured logs.                                                               |
| `RUST_LOG`              | `info`           | Standard `tracing` filter (e.g. `lmx=debug,info`).                                               |

## TOML configuration

The binary also reads an optional TOML config file. By default it looks for
`lmx.toml` in the current working directory, then
`/etc/dd-rust-network-mutex/lmx.toml`; set `LMX_CONFIG=/path/to/file.toml` to
choose a specific file. Environment variables still override TOML values.

The checked-in [`lmx.toml`](lmx.toml) mirrors the existing defaults and includes
a disabled `raft` section. That section is intentionally peer-list driven:
a 3-node peer list gives a quorum of 2, and a 5-node peer list gives a quorum
of 3. Quorum is derived as `floor(cluster_size / 2) + 1`; it is not a manual
knob because a too-small quorum would permit split-brain lock grants.

When `[raft].enabled = true`, the binary starts the `BrokerRaft` HTTP backend:
one elected leader orders lock operations, followers proxy HTTP lock/unlock
requests to that leader, and the leader applies a request only after a quorum
has accepted the log entry. The current Raft path is intended for the
single-shot HTTP API; the TCP/UDS persistent-client listeners still use the
regular in-process Broker.

## Socket-tuning experiment (`live-mutex#22`)

Originally requested at [`live-mutex#22`](https://github.com/ORESoftware/live-mutex/issues/22):
two TCP knobs that take the ~40 ms delayed-ACK + Nagle interaction out of the
request/response RPC path:

1. **Client `TCP_NODELAY`** — every client we ship (Rust, TS, Go, Dart,
   Gleam) sets it on connect.
2. **Broker `TCP_NODELAY`** — `LMX_TCP_NODELAY=true` applies it on every
   accepted socket. Counted in
   `dd_rust_network_mutex_tcp_nodelay_applied_total`.
3. **Broker `TCP_QUICKACK`** — Linux-only, one-shot kernel flag. We
   re-apply it inside the read loop after every frame we consume so the
   next inbound segment is ACKed immediately. Counted in
   `dd_rust_network_mutex_tcp_quickack_applied_total`. On macOS/BSD the
   syscall is a documented no-op and the counter stays at 0.

Both knobs default to `true`. To A/B-test, run two brokers on different
ports (one with `LMX_TCP_QUICKACK=true`, one with `false`) and probe each
with `clients/ts/src/latency_probe.ts`, which reports p50/p95/p99/max
latency for a single sequential acquire-release loop. There's a one-shot
runner that does this end-to-end in a Linux container:

```bash
./scripts/quickack-experiment.sh
```

A localhost-loopback run (Linux container, M-class arm64) confirms the
**plumbing** is wired but the **microbenchmark is uninformative**:

```
BROKER A — NODELAY=true,  QUICKACK=true     p50=0.111ms p95=0.180ms p99=0.362ms
BROKER B — NODELAY=false, QUICKACK=false    p50=0.110ms p95=0.138ms p99=0.207ms

dd_rust_network_mutex_tcp_quickack_applied_total{A}=4100
dd_rust_network_mutex_tcp_quickack_applied_total{B}=0
```

This is the expected outcome on loopback: the kernel's delayed-ACK
heuristic is bypassed when both peers live on the same host, so QUICKACK
has nothing to do. The experiment is meaningful only over a real network
path — e.g. between two pods on different EC2 nodes, or between a
client pod and a broker on another node in `dd-next-runtime`. The
counters and env vars are now in place so the same script can be re-run
in-cluster to produce a real comparison; pending an in-cluster run, the
defaults (NODELAY+QUICKACK on) are the safer bet because they hurt
nothing on loopback and only help on real networks.

## Local development

Build and test:

```bash
cargo test --no-default-features         # quick (no rustls compile)
cargo test                               # full test set including TLS feature
cargo build --release
```

Start the broker locally:

```bash
LMX_TCP_PORT=6970 LMX_HTTP_PORT=6971 cargo run --release
```

Talk to it from `curl`:

```bash
curl -s http://127.0.0.1:6971/v1/lock \
  -H 'content-type: application/json' \
  -d '{"key":"orders","ttlMs":5000}' | jq

# … reads {"acquired":true,"keys":["orders"],"lockUuid":"...","fencingTokens":{"orders":1},"queueDepth":0}

curl -s http://127.0.0.1:6971/v1/unlock \
  -H 'content-type: application/json' \
  -d '{"key":"orders","lockUuid":"<lockUuid from above>"}' | jq
```

Or from Rust:

```rust
use std::time::Duration;
use dd_rust_network_mutex::{Client, ClientConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect_tcp("127.0.0.1:6970", ClientConfig::default()).await?;

    let guard = client.acquire("orders", Duration::from_millis(5000)).await?;
    assert!(guard.fencing_token.unwrap() >= 1);
    // … critical section …
    client.release(&guard).await?;

    let composite = client
        .acquire_composite(&["users", "orders"], Duration::from_millis(5000))
        .await?;
    // composite.fencing_tokens => { "orders": N, "users": M }
    client.release(&composite).await?;
    Ok(())
}
```

Reader-writer locks:

```rust
use dd_rust_network_mutex::{ClientConfig, RwClient};

let client = RwClient::connect_tcp("127.0.0.1:6970", ClientConfig::default()).await?;
let read = client.acquire_read("orders").await?;
read.release().await?;
let write = client.acquire_write("orders").await?;
write.release().await?;
```

## Deployment

### As a standalone binary

```bash
cargo install --path .
LMX_TCP_PORT=6970 LMX_HTTP_PORT=6971 dd-rust-network-mutex
```

(The binary name is `dd-rust-network-mutex` for historical reasons —
this is the same code we run internally at ORE Software under that name.)

### As a Docker container

The repo root ships two multi-stage Dockerfiles (build with
`rust:1.90-bookworm`, run on `debian:bookworm-slim`):

- `Dockerfile` builds the regular Broker image with Raft disabled by default.
- `Dockerfile.raft` builds the BrokerRaft image with a Raft-enabled config,
  Raft RPC port `7980`, and writable log storage under
  `/var/lib/dd-rust-network-mutex/raft`.

```bash
# Build (linux/amd64 by default; pass --platform for cross-arch).
docker build -t oresoftware/live-mutex-rs:0.1.127 .
docker build -f Dockerfile.raft -t oresoftware/live-mutex-rs-raft:0.1.127 .

# Run (TCP 6970 + HTTP 6971 — see Environment variables for everything).
docker run --rm -p 6970:6970 -p 6971:6971 \
    oresoftware/live-mutex-rs:0.1.127

# Run BrokerRaft HTTP + Raft RPC. For real clusters, override the node id and
# peer DNS with env/config per pod.
docker run --rm -p 6971:6971 -p 7980:7980 \
    oresoftware/live-mutex-rs-raft:0.1.127
```

The published image at
[`oresoftware/live-mutex-rs`](https://hub.docker.com/r/oresoftware/live-mutex-rs)
is built from `dev` and tagged with the matching `Cargo.toml`
version. To opt out of TLS / OTel for a smaller image, pass
`--build-arg CARGO_BUILD_FLAGS="--no-default-features"` (or
`--no-default-features --features tls` for TLS-only).

If you'd rather build the binary outside Docker, the broker can run from
built-in defaults, `lmx.toml`, or environment variables:
`cargo build --release` plus the config surface above is enough.

### High availability

See [`docs/raft.md`](docs/raft.md) for the state diagram, lock-commit sequence,
failover event trace, and compaction rule.

BrokerRaft uses a peer-list driven quorum: 3 nodes commit with 2 votes, and 5
nodes commit with 3. One leader orders requests, but the leader cannot safely
grant locks alone. Followers proxy HTTP lock/unlock calls to the current
leader, so a load balancer can round-robin across all healthy nodes. The leader
election path uses PreVote so a stale or partitioned node has to prove a quorum
would vote before it increments and persists a new term. Malformed PreVote and
RequestVote frames with term zero or impossible `lastLogIndex` / `lastLogTerm`
summaries are rejected before term/vote mutation.
Startup and reset election deadlines are jittered per node inside the configured
timeout window, and `/metrics` exposes election deadline, PreVote, and candidate
win/loss counters for spotting timer churn or split-vote loops.
The leader also tracks check-quorum heartbeats; if it cannot observe quorum for
an election-timeout window, it steps down so `/raft/leaderz` stops advertising
a partitioned leader. Write admission uses the same quorum-fresh readiness check
and rejects stale leaders before appending new log entries. Leader-local proposal
appends share a small synchronous gate with step-down; demotion publishes
non-leader readiness before taking the gate, so a racing request cannot append a
new local log entry after demotion has already been advertised. The gate also
rechecks check-quorum freshness after waiting, so a queued proposal cannot age
past the quorum-fresh window and then append anyway.
The leader also exposes `GET /raft/membership` and authenticated
`POST /raft/membership` for log-backed membership changes; a change commits a
joint old/new config and
then the final simple config. Use `GET /raft/progress` to inspect lagging
followers, newly added voters, and transient staged learners during membership
changes. Use `POST /raft/learners` to commit non-voting peers into the
replicated staged-learner set and
`DELETE /raft/learners` to remove staged learner IDs before promotion.
`raft.peers` is the active voter set; `raft.node_id` normally appears there,
but a new node can start outside `raft.peers` as a learner/bootstrap node until
it is staged and promoted through consensus. Once a node has ever been promoted
to a voter, BrokerRaft records a local durable marker so a later removal keeps
rejecting AppendEntries and InstallSnapshot after restart.

If a load balancer sits in front, it can round-robin to all nodes and let
followers proxy to the leader, or it can prefer the current leader by health
checking `/raft/leaderz`. Kubernetes ClusterIP does not do leader-aware HTTP
health routing by itself, so the in-cluster Service can stay round-robin while
an HTTP-aware external/internal LB can use `/raft/leaderz` to avoid the proxy
hop. The load balancer's access logs are useful for observability, but the Raft
log must live on the Raft nodes themselves; LB logs are not part of consensus
recovery.

Current BrokerRaft limitations: the cluster-facing API is HTTP-only, public lock
admission supports only bounded single-key and multi-key requests, the leader
commit lane is intentionally serialized, and each committed client-request batch
still performs durable log work before applying. Replication now sends
incremental `AppendEntries` suffixes with
`prevLogIndex` / `prevLogTerm`, `nextIndex`, and `matchIndex` instead of
whole-log follower rewrites. Lagging followers receive bounded `AppendEntries`
batches, and post-commit fan-out immediately coalesces another bounded round
when a recovering follower hits the inline batch cap instead of waiting for the
next heartbeat. Raft peer RPCs reuse open TCP connections instead of
reconnecting for every heartbeat/append or follower-to-leader proxy request.
Leader conflict-hint repair
uses a retained-log term index to move `nextIndex` without rereading the whole
retained log file; `prevLogTerm`, bounded replication batches, and committed
apply ranges also use index lower-bound lookups over the validated retained-log
cache, including exact retained-index lookup for `prevLogTerm`, instead of
reparsing the log file or scanning from the prefix on each hot-path read.
Follower-side request-id fingerprint validation uses retained and snapshotted
idempotency caches, so append-only catch-up does not rescan the retained prefix
before rejecting conflicting retries.
Leader-local appends, candidate self-vote
persistence, leader-side commit finalization, leader step-down persistence, live
`RequestVote` handling, and live follower-side `AppendEntries` receive handling
are offloaded to Tokio's blocking pool so disk durability, commit advancement,
apply, and compaction checks do not occupy core async workers. `InstallSnapshot`
receive handling uses the blocking pool too, keeping large chunk writes,
checksum verification, JSON parsing, snapshot install, and retained-log rewrites
off core peer-RPC workers. Periodic snapshot/compaction maintenance uses the
same blocking boundary. Raft client IDs are namespaced with a stable
node-derived prefix, so replicated `DropClient` cleanup from one leader cannot
collide with client IDs created by another leader. Committed writes trigger an
immediate background `AppendEntries` fan-out so followers learn the updated
`leaderCommit` without waiting for the next heartbeat tick. Bounded catch-up
uses a monotonic leader-progress generation counter to retry immediately after
real `nextIndex`/`matchIndex` movement without cloning the full peer progress map
each round. Pooled Raft peer RPCs validate that response types match the
outstanding request and reset the TCP connection before retrying on mismatch, so
a stale or desynchronized stream is not reused. Target-index, vote, membership,
and proxy RPCs that need a busy pooled peer connection wait only up to the RPC
timeout and expose
`dd_rust_network_mutex_raft_rpc_connection_waits_total`,
`dd_rust_network_mutex_raft_rpc_connection_wait_us_total`, and
`dd_rust_network_mutex_raft_rpc_connection_wait_timeouts_total` so peer-pool
contention is visible in benchmarks. Leader-side proxy responses report the
current term after proxied work finishes, and follower proxy responses that
carry a higher Raft term persist a follower step-down before the proxied
client result is returned, so round-robin LB traffic does not leave the
forwarding node with stale term state. Malformed successful proxy responses from
an otherwise active peer can still advance term, but they do not refresh the
leader hint unless the response correlation id and kind match the original
client request. Followers retry transient proxy failures, stale leader hints,
and leaderless intervals for up to `proxy_retry_budget_ms`, while terminal
request-payload errors still fail
immediately; the loopback cluster suite covers a request landing on a follower
during the leaderless failover window. Follower proxying uses the direct Raft
peer addresses from membership; it does not send proxied requests back through
the load balancer. If a follower lacks a leader hint, it tries active peers
directly until one handles the request as leader or the retry budget expires. A
non-leader proxy response can include a structured leader hint; the forwarding
follower only accepts hints that resolve to configured active peers, and when
both ID and address are supplied both must match the same peer. It then retries
that peer immediately instead of walking unrelated peers first. Each outbound
proxy RPC is capped by the remaining retry budget, so a silent candidate peer
cannot stretch a follower request beyond that configured window. Extremely large
forwarded wait durations saturate instead of wrapping on the Raft wire. After a
successful proxied write in the follower's current term, the forwarding follower
refreshes its volatile leader hint and election deadline, so later round-robin
LB traffic prefers the same direct peer until normal Raft observations replace
that hint.
The current
implementation is still slower
than the regular Broker under concurrent HTTP load, but the leader now drains
up to `client_batch_max_entries * client_pipeline_max_batches` pending requests
into one append/replicate/commit cycle before applying responses in log order.
After waiting for the serialized commit lane, the driver refills a partially
drained pipeline from newly arrived requests before appending, so bursts queued
behind another operation can still share one Raft round. The leader coalescer
wakes early when the pending queue reaches the configured batch size, and the
leader-local pending queue is capped by
`client_batch_max_pending` so a stalled quorum rejects new admissions before
they are appended to the log instead of growing memory without bound. Lagging
peer catch-up retries immediately when a bounded batch or conflict repair moves
`nextIndex`/`matchIndex`, and leader-progress notifications wake other
quorum/catch-up waiters before the heartbeat delay when progress moves in a
separate task. Target-index quorum rounds reuse existing `matchIndex` progress
for already-caught-up peers instead of sending redundant append RPCs before
commit/catch-up can finish; under stable membership they also send the first
target round only to the remaining peers needed for quorum, including
joint-consensus old/new majority needs, rotating the candidate set on retries so
unavailable peers do not monopolize catch-up. If a narrowed attempt makes no
progress while skipped candidates remain, the quorum wait retries the rotated set
immediately before sleeping for the heartbeat delay.
Promoted-voter catch-up resumes from `matchIndex + 1` when the leader has
nonzero progress for that peer, avoiding an unnecessary tail probe after the
final simple membership entry; peers with no known match progress keep the
optimistic tail probe so restart recovery does not resend the whole retained log
up front.
Already-committed waits prefer a real `matchIndex`
quorum and only fall back to a minimal durable committed quorum when volatile
leader progress is not strong enough after restart or leadership churn; cached
progress does not refresh the check-quorum freshness timestamp. Targeted peer
catch-up also rechecks cached `matchIndex` before building an `AppendEntries`
frame, before preparing an `InstallSnapshot` payload, and between snapshot
chunks, so concurrent progress cannot force redundant snapshot serialization or
streaming. `/raft/status`
exposes `currentTerm`, `commitIndex`, `lastApplied`, sync policy fields, and the
leader quorum freshness fields to help spot convergence lag, unsafe durability
settings, or a stale partitioned leader. No-target heartbeat and post-commit fan-out
rounds return after an active quorum instead of waiting for every slow peer task,
skip peers whose pooled RPC connection is already busy with in-flight work, and
post-commit follower wakeups are coalesced through one active fan-out worker
instead of spawning one background replication task per commit. Append-log and
snapshot JSON serialization is
buffered before the same fsync/atomic-rename durability steps, reducing small
write syscall churn without changing durable-before-ack ordering. Raft term/vote
hard state and log entries are persisted; leader-side `commitIndex` advancement
and follower `leaderCommit` advancement are also persisted before applying
committed entries. Exact duplicate hard-state writes are skipped from an
in-memory durable-state cache, while commit-only changes use a checksummed
two-slot sidecar that is pre-sized on first creation and reused while the path
identity remains stable; term/vote changes still go through the same atomic
fsync path before the cache advances. A durable snapshot also
normalizes startup `commitIndex` to at least its `lastIncludedIndex`; startup
first validates the broker-state portion of that recovered snapshot so an
invalid broker snapshot cannot fail reopen after advancing hard state on disk.
Startup now rejects a durable `commitIndex` ahead of the available snapshot/log
boundary instead of silently lowering a committed index after local data loss.
Startup also validates the retained log's staged-learner context from the
recovered snapshot/config membership and retained request-id fingerprints
against the snapshot-cached idempotency entries before committed replay, so old
retained learner entries or duplicate request identities that would conflict
during apply fail reopen instead of poisoning replay.
Local snapshot writes are monotonic: exact duplicate snapshot metadata and
payload are idempotent, while older snapshot indexes or same-index term/payload
changes are rejected before they can replace durable snapshot state. Committed
range reads also verify continuous coverage from either the latest snapshot or
retained log entries, so snapshot-covered suffix catch-up still works while a
retained-log gap cannot silently skip committed entries.
Live `InstallSnapshot` persists hard state, membership, learner, and
response-cache side effects before installing broker state, and broker snapshot
validation now checks TTL deadline records against restored holders before install.
Snapshot-carried idempotency cache entries are also checked against any retained
log suffix before the new snapshot/rewrite is installed, so a valid-looking
snapshot cannot create a later request-id conflict during suffix replay. The
snapshot apply boundary emits structured `lmx::raft` tracing for hard-state,
sidecar, broker-install, and final runtime-index progress. Dynamic membership is
implemented with joint-consensus log entries, and new peer IDs are first
caught up as transient learners before they are promoted to voters. The joint
config entry itself requires the joint old+new quorum, so an old majority alone
cannot apply a membership transition. Joint `quorumSize` reporting uses the
minimum unique ack count that can satisfy both majorities, so progress/status
output does not understate the ack floor during reconfiguration. After
promotion, every newly promoted voter must catch up to the final membership log
index before the membership change returns; operator-staged learner,
transient-learner, and promoted-voter catch-up now run peers concurrently and
retry each peer immediately after bounded-batch progress, or after a cross-task
leader-progress notification, instead of sleeping for the heartbeat interval
between batches. Failed transient membership catch-up
cleanup preserves any operator-staged learner already recorded on disk and
removes only learners added for that failed attempt, and `GET /raft/progress`
exposes staged learner progress.
`GET/POST/DELETE /raft/learners` provides consensus-replicated staged-learner
management for operator add/remove workflows, with a local restart cache and
snapshot coverage. Applying a membership entry that changes an active peer's
address resets that peer's leader progress and drops the old pooled RPC
connection. Re-staging the same learner id with a different address resets that
learner's progress and drops the old pooled RPC connection before catch-up can
succeed. Promotion remains log-backed through the joint-consensus membership
endpoint.
If leader progress for a peer is missing, catch-up starts with a
`lastLogIndex + 1` heartbeat probe, then conflict hints move `nextIndex` down
to the retained snapshot/log floor when the peer is actually behind. When an
election completes, the new leader seeds replication progress from the current
runtime membership and staged learners instead of the stale peer list captured
before vote RPCs. Stale conflict responses cannot rewind a peer's `nextIndex`
below its known `matchIndex + 1`; stale in-memory `nextIndex` values above the
local log tail are clamped to `lastLogIndex + 1` before replication so they do
not force unnecessary snapshot fallback.
Debug-level `lmx::raft` append-progress events
include the sent batch boundary plus conflict-repair/clamp details. Malformed
`AppendEntries` frames with term zero, impossible previous-log summaries,
non-contiguous indexes, or impossible future-term entries are rejected before
leader/term mutation. Raft
`/metrics` also exposes
`dd_rust_network_mutex_raft_append_progress_updates_total`,
`dd_rust_network_mutex_raft_append_conflict_repairs_total`, and
`dd_rust_network_mutex_raft_append_conflict_clamps_total`. Term-bearing conflict
repairs that would move `nextIndex` above the failed probe are rejected, while
no-term compacted-prefix hints may raise a stale-low probe up to the follower's
reported floor without advancing `matchIndex`; hints above the local log tail
are capped and counted in
`dd_rust_network_mutex_raft_append_conflict_high_clamps_total`. Conflict
responses from older in-flight probes are ignored when the peer's `nextIndex`
has already changed and counted in
`dd_rust_network_mutex_raft_append_stale_conflict_responses_total`. Success
responses from older probes that arrive after a newer conflict repair are
counted in `dd_rust_network_mutex_raft_append_stale_success_responses_total`.
These locally stale responses count as contacted diagnostics but do not refresh
leader check-quorum freshness. Conflict hints that prove the follower is below the retained snapshot/log floor are
counted in
`dd_rust_network_mutex_raft_append_conflict_snapshot_fallbacks_total` before the
next repair round uses `InstallSnapshot`. Impossible
`AppendEntries(success=true)` replies that report a `matchIndex` below the
matched previous entry or sent batch are treated as non-progress and increment
`dd_rust_network_mutex_raft_append_invalid_success_responses_total`.
Replies that inflate `matchIndex` beyond the sent batch are capped to the sent
batch boundary, logged, and counted in
`dd_rust_network_mutex_raft_append_capped_success_responses_total`.
Append-path `InstallSnapshot` fallbacks increment
`dd_rust_network_mutex_raft_append_snapshot_fallbacks_total`, split into
`dd_rust_network_mutex_raft_append_snapshot_prev_term_misses_total` for
compacted/missing `prevLogTerm` and
`dd_rust_network_mutex_raft_append_snapshot_suffix_gaps_total` for retained
suffix coverage gaps, and
`dd_rust_network_mutex_raft_append_snapshot_frame_overflows_total` when a
single retained log entry cannot fit in one `AppendEntries` frame; debug-level
`lmx::raft` logs include peer, `nextIndex`, local tail, snapshot boundary, and
target index. If a no-term or unknown-term conflict hint reports a
`conflictIndex` below the retained snapshot/log floor, the leader moves the peer
to the snapshot fallback path instead of retrying an unavailable predecessor and
increments
`dd_rust_network_mutex_raft_append_conflict_snapshot_fallbacks_total`.
Leader-side AppendEntries attempt/outcome counters now cover request volume,
heartbeats, batches, sent entries, log-entry bytes, wire bytes, accepted
successes, conflicts, and RPC errors:
`dd_rust_network_mutex_raft_append_entries_requests_total`,
`dd_rust_network_mutex_raft_append_entries_heartbeats_total`,
`dd_rust_network_mutex_raft_append_entries_batches_total`,
`dd_rust_network_mutex_raft_append_entries_sent_total`,
`dd_rust_network_mutex_raft_append_entries_log_bytes_total`,
`dd_rust_network_mutex_raft_append_entries_wire_bytes_total`,
`dd_rust_network_mutex_raft_append_entries_frame_build_us_total`,
`dd_rust_network_mutex_raft_append_entries_frame_build_blocking_tasks_total`,
`dd_rust_network_mutex_raft_append_entries_request_us_total`,
`dd_rust_network_mutex_raft_append_entries_frame_clamps_total`,
`dd_rust_network_mutex_raft_append_entries_successes_total`,
`dd_rust_network_mutex_raft_append_entries_conflicts_total`, and
`dd_rust_network_mutex_raft_append_entries_rpc_errors_total`. Serialized
AppendEntries batches that would exceed `LMX_RAFT_MAX_FRAME_BYTES` are shortened
by exact frame sizing before the socket write and counted in the frame-clamp
counter. Frame-build microseconds measure time spent sizing/building the
leader-side AppendEntries JSON frame before the peer RPC wait begins; the
selected frame's serialized bytes are reused for the socket write. Large
AppendEntries frame builds are thresholded onto Tokio's blocking pool so
lagging-follower catch-up JSON sizing/serialization does not pin a core async
worker.
Malformed follower-side leader RPCs rejected before leader/term mutation expose
`dd_rust_network_mutex_raft_append_entries_malformed_requests_total`,
`dd_rust_network_mutex_raft_append_entries_context_invalid_staged_learners_total`,
and `dd_rust_network_mutex_raft_install_snapshot_malformed_requests_total`.
Startup retained-log learner and request-identity context scans expose
`dd_rust_network_mutex_raft_open_log_context_validations_total`,
`dd_rust_network_mutex_raft_open_log_context_validation_entries_total`,
`dd_rust_network_mutex_raft_open_log_context_validation_us_total`, and
`dd_rust_network_mutex_raft_open_log_context_validation_errors_total`.
Follower-side AppendEntries payloads whose request-id fingerprints conflict
with retained or snapshotted idempotency context increment
`dd_rust_network_mutex_raft_append_entries_context_invalid_request_identities_total`.
Follower-side sender rejections for unknown, stale, or conflicting leaders are
counted separately in
`dd_rust_network_mutex_raft_follower_append_sender_rejections_total` and
`dd_rust_network_mutex_raft_follower_install_snapshot_sender_rejections_total`.
Generic inbound Raft request/response frames rejected before JSON parsing
because they exceed the frame cap increment
`dd_rust_network_mutex_raft_rpc_inbound_frame_rejections_total`; generic
outbound request/response frames rejected before socket write increment
`dd_rust_network_mutex_raft_rpc_outbound_frame_rejections_total`. Under-cap
request/response frames rejected because they cannot be decoded as the expected
Raft JSON shape increment
`dd_rust_network_mutex_raft_rpc_malformed_frames_total`.
Peer RPC requests rejected because `raft.peer_token` is configured and the
request token is missing or incorrect increment
`dd_rust_network_mutex_raft_rpc_auth_rejections_total`.
Followers also decode incoming `InstallSnapshot` chunks and enforce
`raft.install_snapshot_chunk_bytes` before term/leader mutation; oversized
chunks increment
`dd_rust_network_mutex_raft_install_snapshot_oversized_chunks_total`. A single
staged transfer is also capped by `raft.install_snapshot_max_staged_bytes`, and
limit rejections increment
`dd_rust_network_mutex_raft_install_snapshot_staged_byte_limit_rejections_total`.
Concurrent staged transfers are capped by
`raft.install_snapshot_max_staged_transfers`; rejected transfer starts increment
`dd_rust_network_mutex_raft_install_snapshot_staged_transfer_limit_rejections_total`.
Incomplete staged transfers that make no progress for
`raft.install_snapshot_stale_transfer_ms` are removed before later chunks are
accepted; stale removals increment
`dd_rust_network_mutex_raft_snapshot_transfer_stale_cleanups_total` in addition
to the general cleanup counter.
Leader quorum-wait counters expose the Raft portion of write latency:
`dd_rust_network_mutex_raft_replication_quorum_waits_total`,
`dd_rust_network_mutex_raft_replication_quorum_attempts_total`,
`dd_rust_network_mutex_raft_replication_quorum_progress_retries_total`,
`dd_rust_network_mutex_raft_replication_quorum_progress_wakeups_total`,
`dd_rust_network_mutex_raft_replication_quorum_sleeps_total`,
`dd_rust_network_mutex_raft_replication_quorum_already_committed_short_circuits_total`,
`dd_rust_network_mutex_raft_replication_quorum_successes_total`,
`dd_rust_network_mutex_raft_replication_quorum_timeouts_total`,
`dd_rust_network_mutex_raft_replication_quorum_leadership_losses_total`, and
`dd_rust_network_mutex_raft_replication_quorum_wait_ms_total`. Cached
target-index progress is split out as
`dd_rust_network_mutex_raft_replication_cached_progress_acks_total`,
`dd_rust_network_mutex_raft_replication_cached_quorum_short_circuits_total`, and
`dd_rust_network_mutex_raft_replication_early_quorum_returns_total` so profiling
runs can distinguish zero-RPC commits, already-committed target waits, and
fanout rounds that returned once a quorum had replied. Cached peer `matchIndex`
values above the leader's local log tail are ignored for target-index and commit
quorum accounting, and already-committed fallback quorum shortcuts require the
local log/snapshot to cover the target index. Ordinary target-index foreground
fanout narrowing is counted in
`dd_rust_network_mutex_raft_replication_quorum_limited_fanout_rounds_total` and
`dd_rust_network_mutex_raft_replication_quorum_limited_fanout_skipped_peers_total`.
Membership-change entries use full foreground fanout and wait for busy pooled
peer connections so reconfiguration does not strand old or newly promoted voters
behind quorum-minimum optimization.
Immediate retry rotation across ready skipped foreground candidates is counted in
`dd_rust_network_mutex_raft_replication_quorum_limited_fanout_fast_retries_total`.
Configured foreground spare peers beyond the minimum quorum need are counted in
`dd_rust_network_mutex_raft_replication_quorum_extra_fanout_peers_total`; the
code default is `0`, while the checked-in Raft TOML sets `1` to hedge one extra
ready follower without waiting for it before returning a committed target write.
Busy peers selected by target-quorum fanout but replaced by ready peers in the
same foreground round are counted in
`dd_rust_network_mutex_raft_replication_quorum_busy_peer_replacements_total`.
No-target
heartbeat/post-commit AppendEntries or snapshot
catch-up attempts skipped because the peer already has an in-flight pooled RPC
are counted in
`dd_rust_network_mutex_raft_replication_busy_peer_skips_total`; replication
attempts skipped before building/sending catch-up work because the peer has
already left active membership and staged learners are counted in
`dd_rust_network_mutex_raft_replication_removed_peer_skips_total`. Dynamic
membership and staged-learner catch-up expose
`dd_rust_network_mutex_raft_learner_catchup_attempts_total`,
`dd_rust_network_mutex_raft_learner_catchup_successes_total`,
`dd_rust_network_mutex_raft_learner_catchup_timeouts_total`,
`dd_rust_network_mutex_raft_learner_catchup_leadership_losses_total`,
`dd_rust_network_mutex_raft_learner_catchup_removed_peers_total`,
`dd_rust_network_mutex_raft_learner_catchup_progress_retries_total`,
`dd_rust_network_mutex_raft_learner_catchup_progress_wakeups_total`,
`dd_rust_network_mutex_raft_learner_catchup_sleeps_total`, and
`dd_rust_network_mutex_raft_learner_catchup_wait_ms_total` for learner or
promoted-voter stalls.
Learner bootstrap and removed-local-voter paths expose
`dd_rust_network_mutex_raft_learner_bootstrap_starts_total`,
`dd_rust_network_mutex_raft_local_voter_promotions_total`, and
`dd_rust_network_mutex_raft_local_removed_voter_guard_rejections_total`.
BrokerRaft request-flow metrics help separate broker work from consensus and
load-balancer proxy work during perf runs:
`dd_rust_network_mutex_request_duration_seconds` exposes fixed-route latency
buckets for both regular Broker HTTP routes (`http_acquire`, `http_release`,
etc.), BrokerRaft HTTP routes (`raft_http_acquire`, `raft_http_release`), and
the synchronous persistent socket hot path (`stream_frame`).
`dd_rust_network_mutex_request_payload_bytes` buckets persistent TCP/UDS JSON
frame payload sizes for CPU/profiling correlation without labelling on lock
keys or request ids.
`dd_rust_network_mutex_raft_client_proposals_total`,
`dd_rust_network_mutex_raft_client_proposal_quorum_failures_total`,
`dd_rust_network_mutex_raft_apply_committed_errors_total`,
`dd_rust_network_mutex_raft_leader_commit_advancements_total`,
`dd_rust_network_mutex_raft_leader_commit_advanced_entries_total`,
`dd_rust_network_mutex_raft_post_commit_fanout_rounds_total`,
`dd_rust_network_mutex_raft_post_commit_fanout_errors_total`,
`dd_rust_network_mutex_raft_client_queue_full_total`,
`dd_rust_network_mutex_raft_client_batches_total`,
`dd_rust_network_mutex_raft_client_batch_entries_total`,
`dd_rust_network_mutex_raft_client_batch_queue_wait_us_total`,
`dd_rust_network_mutex_raft_client_batch_pipeline_batches_total`,
`dd_rust_network_mutex_raft_client_batch_refill_rounds_total`,
`dd_rust_network_mutex_raft_client_batch_refilled_entries_total`,
`dd_rust_network_mutex_raft_client_batch_commit_lock_waits_total`,
`dd_rust_network_mutex_raft_client_batch_commit_lock_wait_us_total`,
`dd_rust_network_mutex_raft_serialized_commit_lane_waits_total`,
`dd_rust_network_mutex_raft_serialized_commit_lane_wait_us_total`,
`dd_rust_network_mutex_raft_client_batch_cancelled_requests_total`,
`dd_rust_network_mutex_raft_client_batch_errors_total`,
`dd_rust_network_mutex_raft_client_cache_completed_hits_total`,
`dd_rust_network_mutex_raft_client_cache_pending_hits_total`,
`dd_rust_network_mutex_raft_client_cache_conflicts_total`,
`dd_rust_network_mutex_raft_proxy_requests_forwarded_total`,
`dd_rust_network_mutex_raft_proxy_requests_handled_total`,
`dd_rust_network_mutex_raft_proxy_request_errors_total`,
`dd_rust_network_mutex_raft_apply_committed_entries_total`,
`dd_rust_network_mutex_raft_apply_committed_us_total`,
`dd_rust_network_mutex_raft_client_batch_pending`,
`dd_rust_network_mutex_raft_client_batch_driver_active`, and
`dd_rust_network_mutex_raft_client_response_cache_entries`. That response-cache
gauge is split further into
`dd_rust_network_mutex_raft_client_response_cache_pending_entries`,
`dd_rust_network_mutex_raft_client_response_cache_completed_entries`, and
`dd_rust_network_mutex_raft_client_response_cache_applied_without_response_entries`
so perf runs can distinguish active idempotency reservations from replayable
completed responses and unknown completed outcomes. Queue-full,
cancelled-before-append, batch-failure, and committed-entry apply-failure paths emit warn/error-level
`lmx::raft` logs; proxy forwarding and proxy errors emit debug-level logs with
request id and leader hint fields.
Follower-side `AppendEntries` exposes conflict-response and log-repair counters:
`dd_rust_network_mutex_raft_follower_append_conflicts_total`,
`dd_rust_network_mutex_raft_follower_append_rewrites_total`,
`dd_rust_network_mutex_raft_follower_append_appended_entries_total`,
`dd_rust_network_mutex_raft_follower_append_rewritten_entries_total`, and
`dd_rust_network_mutex_raft_follower_append_truncated_entries_total`. Suffix
repair keeps the retained prefix with lower-bound slicing instead of scanning it
entry by entry, then truncates the log file at the retained-prefix byte offset
and appends the leader suffix instead of rewriting the full retained log.
After the disk write succeeds, the in-memory retained entries, serialized-byte
cache, and term indexes are truncated and extended in place, so conflict repair
does not clone a large retained prefix. If that write path reports an error, the
retained log/index/byte cache is reloaded from disk before the follower returns
the failure. Before either rewriting or filling a missing suffix entry, the
repair path compares against the durable hard-state commit index while holding
the log-state lock, so a concurrent commit advancement cannot let a stale
prepared runtime commit argument rewrite a committed suffix. Append-only and
truncate-and-append log writers reuse the same
serialized JSON buffer to update retained byte-length metadata and write the
disk suffix, then roll the file length back on write/flush/file-sync or
parent-directory-sync failure so partial suffix bytes are not left behind.
Append-only writers reuse an open log file while the path identity stays stable,
and drop that cache before suffix repair, snapshot install, compaction, or path
replacement.
Follower snapshot install
keeps any matching retained suffix with an indexed retained-log slice instead of
clone-filtering the full retained cache. The retained log cache also carries
serialized JSON byte lengths for each entry, so `append_entries_max_bytes`
batch sizing avoids repeated serialization of the same retained entries during
catch-up.
Leader-side `InstallSnapshot` catch-up exposes
`dd_rust_network_mutex_raft_install_snapshot_chunks_total`,
`dd_rust_network_mutex_raft_install_snapshot_bytes_total`,
`dd_rust_network_mutex_raft_install_snapshot_wire_bytes_total`,
`dd_rust_network_mutex_raft_install_snapshot_frame_build_us_total`,
`dd_rust_network_mutex_raft_install_snapshot_frame_build_blocking_tasks_total`,
`dd_rust_network_mutex_raft_install_snapshot_request_us_total`,
`dd_rust_network_mutex_raft_install_snapshot_payload_prepares_total`,
`dd_rust_network_mutex_raft_install_snapshot_payload_cache_hits_total`,
`dd_rust_network_mutex_raft_install_snapshot_payload_prepare_us_total`,
`dd_rust_network_mutex_raft_install_snapshot_payload_prepare_bytes_total`,
`dd_rust_network_mutex_raft_install_snapshot_successes_total`,
`dd_rust_network_mutex_raft_install_snapshot_rejections_total`,
`dd_rust_network_mutex_raft_install_snapshot_rpc_errors_total`, and
`dd_rust_network_mutex_raft_install_snapshot_progress_updates_total`.
Follower-side snapshot payloads whose idempotency cache conflicts with a
retained suffix increment
`dd_rust_network_mutex_raft_install_snapshot_context_invalid_request_identities_total`.
Configured raw snapshot chunks whose serialized Raft RPC frame would exceed
`LMX_RAFT_MAX_FRAME_BYTES` after base64/JSON overhead are clamped by exact frame
sizing and counted in
`dd_rust_network_mutex_raft_install_snapshot_frame_chunk_clamps_total`.
Frame-build microseconds measure time spent sizing/building the leader-side
snapshot chunk frame before the peer RPC wait begins; the selected chunk's
serialized bytes are reused for the socket write. Large snapshot chunk frame
builds are thresholded onto Tokio's blocking pool so base64/JSON work does not
pin a core async worker during lagging-follower catch-up. Success responses
that arrive before the final chunk are ignored for peer progress and counted in
`dd_rust_network_mutex_raft_install_snapshot_premature_success_responses_total`.
Final success responses that underreport the installed snapshot index are also
ignored for progress and counted in
`dd_rust_network_mutex_raft_install_snapshot_invalid_success_responses_total`.
Final success responses that inflate the installed snapshot index beyond the
snapshot just sent are capped, logged, and counted in
`dd_rust_network_mutex_raft_install_snapshot_capped_success_responses_total`.
Follower snapshot staging idempotently accepts duplicate non-final chunks,
including delayed duplicate first chunks, so a lost chunk response does not
force the whole transfer to restart. Duplicate chunks are accepted only when
their bytes match the staged range; divergent duplicate retries are rejected and
the partial transfer is cleared. A new offset-0 chunk that is not a pure
duplicate replaces the older staged file, so a leader can restart
`InstallSnapshot` after a reconnect without requiring an extra failed retry.
`/metrics` also exposes follower-side staged chunks/bytes, duplicate chunks,
duplicate byte mismatches, offset mismatches, local staged-file length
mismatches, staged-transfer cleanups, and pooled Raft RPC response-type
mismatches, including
`dd_rust_network_mutex_raft_install_snapshot_duplicate_chunk_mismatches_total`,
`dd_rust_network_mutex_raft_install_snapshot_staged_file_mismatches_total` and
`dd_rust_network_mutex_raft_rpc_response_mismatches_total`.
Malformed vote/pre-vote rejections increment
`dd_rust_network_mutex_raft_pre_vote_malformed_requests_total` or
`dd_rust_network_mutex_raft_request_vote_malformed_requests_total` and emit
debug-level `lmx::raft` logs with candidate, term, and log-summary fields.
Lower-term PreVote and RequestVote responses are ignored for election quorum and
counted in `dd_rust_network_mutex_raft_pre_vote_stale_term_responses_total` and
`dd_rust_network_mutex_raft_request_vote_stale_term_responses_total`.
Old log entries are compacted only after they are committed, applied, and
covered by a durable snapshot. Snapshots now carry active holders, queued
waiters, fencing counters, and TTL deadlines, so `InstallSnapshot` can catch up
followers that fall behind a compacted prefix using chunked transfer staged on
receiver disk. Snapshot-backed compaction keeps the retained suffix with an
upper-bound slice, and it can reuse an existing snapshot to trim retained log
entries that have become newly eligible before the next fresh snapshot is
needed. It exposes `dd_rust_network_mutex_raft_log_compactions_total`,
`dd_rust_network_mutex_raft_log_compacted_entries_total`,
`dd_rust_network_mutex_raft_log_compaction_threshold_triggers_total`,
`dd_rust_network_mutex_raft_log_compaction_cadence_triggers_total`,
`dd_rust_network_mutex_raft_log_compaction_safety_skips_total`,
`dd_rust_network_mutex_raft_log_write_rollbacks_total`,
`dd_rust_network_mutex_raft_log_write_rollback_errors_total`,
`dd_rust_network_mutex_raft_log_append_file_opens_total`,
`dd_rust_network_mutex_raft_log_append_file_cache_invalidations_total`,
`dd_rust_network_mutex_raft_log_rewrite_temp_cleanups_total`,
`dd_rust_network_mutex_raft_atomic_json_temp_cleanups_total`,
`dd_rust_network_mutex_raft_log_trailing_partial_recoveries_total`,
`dd_rust_network_mutex_raft_log_trailing_partial_recovery_errors_total`,
`dd_rust_network_mutex_raft_log_compacted_bytes_total`,
`dd_rust_network_mutex_raft_log_compaction_us_total`,
`dd_rust_network_mutex_raft_snapshot_transfer_stale_cleanups_total`,
`dd_rust_network_mutex_raft_log_retained_entries`,
`dd_rust_network_mutex_raft_log_bytes`,
`dd_rust_network_mutex_raft_last_log_index`,
`dd_rust_network_mutex_raft_commit_index`,
`dd_rust_network_mutex_raft_last_applied`,
`dd_rust_network_mutex_raft_latest_snapshot_index`,
`dd_rust_network_mutex_raft_latest_snapshot_age_ms`,
`dd_rust_network_mutex_raft_snapshot_maintenance_age_ms`,
`dd_rust_network_mutex_raft_log_compaction_eligible_index`,
`dd_rust_network_mutex_raft_peer_max_lag_entries`,
`dd_rust_network_mutex_raft_leader_quorum_age_ms`, and
`dd_rust_network_mutex_raft_leader_ready`. Log write rollback attempts and
rollback failures also emit warn-level `lmx::raft` events with path, entry
count, target byte length, and sync policy fields. Unbounded full-log read/replace
helpers are test-only; production replication uses bounded suffix or exact range
reads. Successful compaction emits an info-level `lmx::raft` event with
snapshot index, compacted entries/bytes, retained entries/bytes, and elapsed
microseconds. `raft.snapshot_max_log_age_ms` defaults to 30 minutes and triggers
the same snapshot-backed compaction path for old committed/applied retained
prefixes; `0` disables that age trigger. `/metrics` exposes
`dd_rust_network_mutex_raft_log_compaction_age_triggers_total` and
`dd_rust_network_mutex_raft_log_age_compaction_eligible_index` for that path;
the eligibility scan resumes from the last proven prefix while time and the
commit/apply ceiling move forward, avoiding repeated full-prefix scans during
profile scrapes.
Followers verify the snapshot payload SHA-256 checksum before installing
it, and stale or invalid staged snapshot parts are removed when later snapshot
traffic resumes, when offsets mismatch, when validation rejects the staged
payload, or when a node reopens its Raft data directory; removed staged snapshot
files sync the parent directory and discarded transfers increment
`dd_rust_network_mutex_raft_snapshot_transfer_cleanups_total`; stale transfer
cleanup also increments
`dd_rust_network_mutex_raft_snapshot_transfer_stale_cleanups_total`. Tracked
stale transfers are checked on each later chunk, but orphaned part-file
directory scans are paced to avoid rescanning the Raft data directory for every
chunk in a large snapshot stream; when a
valid higher-term leader clears staged snapshot files from superseded leaders,
it also increments
`dd_rust_network_mutex_raft_snapshot_transfer_superseded_leader_cleanups_total`.
Restored waiters
keep their original client ids for later replicated cleanup, but old HTTP
response channels are not resurrected after restart or snapshot install. Lock
UUID and fencing-token replay are deterministic across nodes because committed
client-request log entries carry grant metadata.
BrokerRaft should be treated as experimental, not as an etcd/ZooKeeper-grade
consensus service. Before making an etcd/ZooKeeper-grade claim, it still needs
sustained external fault-injection and linearizability evidence, live/staging
rolling-upgrade and version-skew coverage, and real deployment soak across
storage, network partitions, pod restarts, and membership churn. The local
regression gate is:

```bash
scripts/raft-hardening-gate.sh
```

The gate also scans `src/broker_raft.rs` for direct full-log read/rewrite
primitive calls and only allows them in startup, recovery, compaction, snapshot
install, and test helper paths. A new exception should be documented in
`docs/raft.md`; replication and proposal hot paths should keep using retained
caches, bounded `AppendEntries`, append-only writes, truncate+append repair, or
`InstallSnapshot`.
It also promotes maintenance and compaction regressions into the normal gate:
stale snapshot-transfer cleanup cadence, snapshot-maintenance blocking/gate
behavior, threshold/overdue compaction, and trim-failure retry observability.
The quick gate also covers the current BrokerRaft unit-test inventory through
broad prefix slices plus an explicit singleton tail, and self-audits those
filters against `cargo test --lib -- --list` so new BrokerRaft unit tests cannot
silently miss quick mode. Full mode still adds the complete no-default-features
library suite.
It also runs all-targets Clippy by default so test helpers, examples, and live
smoke harnesses are linted along with the library. Set `CLIPPY_SCOPE=lib` for a
shorter library-only diagnostic run.

To collect repeatable local or staging soak evidence with per-iteration logs,
a per-run TSV summary, an aggregate summary index, a run manifest, and captured
git/env context:

```bash
SOAK_ITERATIONS=10 LMX_RAFT_GATE_MODE=quick RUN_BENCH=false scripts/raft-soak.sh
```

For staging soak against a live BrokerRaft Kubernetes Service, include the
ignored live smoke tests explicitly. The failover variant deletes the observed
leader pod while recording a bounded no-wait history for linearizability
checking, so keep it opt-in. The soak/gate path requires stable per-node
metrics endpoints by default so it can prove every Raft pod avoided full-log
hot-path fallback:

```bash
LMX_LIVE_RAFT_HTTP=live-mutex-rs-raft.default.svc.cluster.local:6971 \
LMX_LIVE_RAFT_METRICS_ENDPOINTS=live-mutex-rs-raft-0.live-mutex-rs-raft.default.svc.cluster.local:6971,live-mutex-rs-raft-1.live-mutex-rs-raft.default.svc.cluster.local:6971,live-mutex-rs-raft-2.live-mutex-rs-raft.default.svc.cluster.local:6971 \
RUN_K8S_RAFT_LIVE=true \
RUN_K8S_RAFT_LIVE_REQUIRE_METRICS=true \
LMX_LIVE_RAFT_KUBECTL_FAILOVER=true \
SOAK_ITERATIONS=10 \
LMX_RAFT_GATE_MODE=quick \
RUN_BENCH=false \
scripts/raft-soak.sh
```

For mixed-binary rolling-upgrade evidence, provide an older BrokerRaft binary
or a git ref to build one, then opt into the ignored version-skew harness:

```bash
LMX_RAFT_OLD_REF=HEAD~1 scripts/raft-version-skew.sh
```

That local `HEAD~1` to current harness passed on 2026-06-08 against old ref
`44eff3c`; it is useful loopback compatibility evidence, not a substitute for a
live/staging rolling upgrade.

That gate includes focused Raft regressions, multi-key/fencing wire fuzz checks,
`raft_cluster`, and local Broker/BrokerRaft benchmark metric guards.

## HTML status page (`live-mutex#108`)

Originally requested at [`live-mutex#108`](https://github.com/ORESoftware/live-mutex/issues/108)
("a simple html via tcp or uds etc with status page"). The broker serves one at:

- `GET /` and `GET /status` on the main HTTP listener (`LMX_HTTP_PORT`).
- A dedicated read-only listener on `LMX_STATUS_PORT` if set. The
  dedicated port serves only `/`, `/status`, `/healthz`, `/readyz`, and
  `/metrics` — no `/v1/*` API surface — which is the deployment posture
  this repo prefers (public gateway routes the auth-gated API; operators
  reach the status page over VPN/bastion).

The page is server-rendered HTML with no JS or external assets:
`<meta http-equiv="refresh" content="5">` keeps it fresh in a browser
tab, `prefers-color-scheme` picks light/dark automatically, and the raw
Prometheus exposition is embedded as a `<pre>` block so the same URL is
useful for both humans and `curl | rg`. The HTML is rendered from a
single `Broker::metrics()` snapshot plus `Broker::top_keys(10)` — one
mutex acquire per request.

**XSS posture.** Lock keys flow through `html_escape` before being
rendered, with an explicit unit test
(`html_escapes_keys_to_prevent_xss_via_lock_key`) asserting that a
malicious key like `<script>x="y"</script>` cannot bypass the escape.

What the page shows:

- Connected clients, tracked keys, active holders, queued waiters.
- Pending TTL deadlines and the cumulative TTL evictions counter
  (the `live-mutex#13` series — high values are an operator signal
  that callers are dying with held locks).
- Top 10 keys by contention with per-key fencing-counter values.
- Listener configuration (TCP / UDS / HTTP / status, auth, TLS,
  socket-tuning knobs, default TTL, sweep cadence, max holders).
- The `/metrics` exposition embedded inline.

## Multi-key (composite) locking

A `lock` request can carry **either** a single `key` **or** a `keys` array
(1..=5). Any request with `keys` is a *composite* lock: the broker either
acquires every requested key atomically or none of them. The wire response
arrives as `compositeLock` (not `lock`) and includes a per-key `fencingTokens`
map so callers can fence each protected resource independently.
BrokerRaft also supports public `keys` requests, but with a lower replicated
admission cap of 3 distinct keys. That cap keeps deterministic per-log-index
fencing-token reservation bounded while preserving the same union-overlap
semantics as the regular Broker.

### Why a multi-key lock instead of N single-key acquires

The textbook way to "lock A and B" is to acquire them in some agreed
global order. That gets you correctness, but you still own the
bookkeeping:

- You have to remember the canonical order and apply it everywhere a
  caller wants both keys, forever.
- A timeout or panic between acquiring `A` and acquiring `B` leaves `A`
  held and orphaned until its TTL fires.
- A second caller that wants only `B` can grab it between your two
  acquires — you never observe a quiet moment when both are held by no
  one else.
- Releasing requires two calls; if the second `release` fails or the
  process dies between them, you leak one key until the sweeper
  catches up.

`acquire_composite` collapses all of that into one broker round-trip:
the broker checks every key, grants only when *all* of them are free,
and emits a single `lockUuid` that releases the whole set with one
call. There is no in-between state visible to anyone — including the
caller's own retries.

Concretely, callers use it for things like: transferring an item
between two queues, rotating a two-key credential without ever exposing
a window where neither key is held, or running a migration that has to
hold the source and destination shard locks simultaneously.

### Guarantees

The two properties below hold *by construction*, not by polling or
retries:

1. **Atomicity.** While a composite holder owns `lockUuid X` covering
   keys `[A, B]`, no other caller will ever be granted `A` alone, `B`
   alone, or any composite that overlaps `{A, B}`. Equivalently: no
   subset of your keys is ever observable to another holder.

   This is preserved through every failure mode the broker handles:
   contention with single-key acquires on overlapping keys, contention
   with another composite, partial-grant races (the broker rolls back
   any keys it tentatively claimed if a later key in the set is held),
   the centralised TTL sweeper expiring one of your keys, and the
   owning client's TCP socket closing — `drop_client` releases every
   member of the set together.

2. **Deadlock freedom via global lexicographic ordering.** The broker
   sorts every `keys` array into ascending Unicode-code-point order
   before queueing. Two callers issuing `acquire_composite(["A","B"])`
   and `acquire_composite(["B","A"])` therefore wait on the same key's
   notify queue and one wins outright; neither can hold one half while
   waiting on the other. Worked example:

   ```text
   t=0  caller-1 sends keys=[A, B]   → broker sorts → wait on A.notify
   t=0  caller-2 sends keys=[B, A]   → broker sorts → wait on A.notify
   t=1  A is free → caller-1 wins A, then atomically claims B
   t=1  caller-2 stays in A.notify   (A is now held by caller-1)
   t=N  caller-1 releases lockUuid   → A and B both free
   t=N  caller-2 wakes, claims A and B atomically
   ```

   Without the broker-side sort, caller-1 could claim `A`, caller-2
   could claim `B`, and both would block forever waiting for the
   other's key — a classic two-resource deadlock.

### Rust API

```rust
use std::time::Duration;
use dd_rust_network_mutex::{Client, ClientConfig};

let client = Client::connect_tcp("broker:6970", ClientConfig::default()).await?;

let composite = client
    .acquire_composite(&["users", "orders"], Duration::from_millis(5_000))
    .await?;

assert_eq!(composite.keys.len(), 2);
// composite.fencing_tokens => HashMap<String, u64>
//   { "orders": 7, "users": 3 } (alphabetical; tokens are minted broker-side)

// … critical section that touches both resources …

// Single release call drops every member of the set atomically. The
// broker rejects any release that doesn't match the original lockUuid,
// so one caller can't accidentally release another caller's composite.
client.release(&composite).await?;
```

### HTTP

```bash
curl -s http://127.0.0.1:6971/v1/lock \
  -H 'content-type: application/json' \
  -d '{"keys":["users","orders"],"ttlMs":5000}' | jq
# => { "acquired": true, "keys":["orders","users"],
#      "lockUuid":"…", "fencingTokens": {"orders": 7, "users": 3},
#      "queueDepth": 0 }

curl -s http://127.0.0.1:6971/v1/unlock \
  -H 'content-type: application/json' \
  -d '{"keys":["users","orders"],"lockUuid":"<uuid>"}' | jq
```

### Direct TCP wire format

Any client that speaks the newline-delimited JSON protocol can drive a
composite lock without going through the Rust client:

```text
client → broker
{ "type": "lock",
  "uuid": "<correlation-id>",
  "keys": ["users", "orders"],
  "ttl": 5000 }

broker → client (when every key is free)
{ "type": "compositeLock",
  "uuid": "<correlation-id>",
  "acquired": true,
  "keys": ["orders", "users"],
  "lockUuid": "...",
  "fencingTokens": { "orders": 7, "users": 3 } }

client → broker
{ "type": "unlock",
  "uuid": "<correlation-id-2>",
  "keys": ["users", "orders"],
  "lockUuid": "..." }
```

Under contention the broker first responds with `acquired: false` plus
the current `queueDepth`, and emits a second `compositeLock` frame
with `acquired: true` once every key is held.

### Constraints

- `keys` is bounded to **1..=5** by the broker. Larger sets are rejected
  with a 400 (HTTP) or a `compositeLock { acquired: false, error: "..." }`
  frame (TCP) before any state mutation.
- BrokerRaft accepts composite requests up to **3 distinct keys**. Duplicate
  entries in the `keys` array are sorted/deduped by the broker, so the cap is
  based on the distinct member set rather than the raw array length. Larger
  replicated composites are rejected before batching or Raft log append.
- Empty `keys` arrays and requests that set both `key` and `keys` are
  rejected the same way — there is no "fall back to single-key on empty
  composite" sentinel.
- `max` is **single-key only**. Composite locks always behave like
  `max=1` per member — combining semaphore-style concurrency with
  composite is a deadlock-prone surface area; see the dedicated
  discussion in [Composite locks and `max`](#composite-locks-and-max).
- Composite locks are exclusive across *all* lock kinds on a member
  key: while a composite is held on `A`, no exclusive `acquire("A")`,
  semaphore-slot acquire, RW reader, or RW writer can grant. The
  inverse also holds.
- A composite acquirer that disconnects while holding some keys has
  every member released by the broker's `drop_client` path — the
  partial-grant tracker guarantees no leaked sub-keys.

## Semaphore-style locks (concurrency &gt; 1)

Every `lock` request can carry an optional `max` field that sets the
per-key concurrency level. `max=1` (the default) is classic mutex
semantics; `max=N` admits up to `N` simultaneous holders for the key,
turning it into a counting semaphore — exactly the behavior upstream
`live-mutex` exposes.

Each holder still gets:

- A unique `lock_uuid` (so the broker can reject another holder's
  release attempt).
- A unique, strictly-increasing fencing token from the same per-key
  monotonic counter — so a downstream resource can disambiguate slot N
  from slot M without clients coordinating.

```rust
use dd_rust_network_mutex::{Client, ClientConfig};
use std::time::Duration;

let client = Client::connect_tcp(("broker", 6970), ClientConfig::default()).await?;

// Up to 5 concurrent workers for "render-pipeline":
let guard = client
    .acquire_with_max("render-pipeline", 5, Duration::from_millis(30_000))
    .await?;
// guard.fencing_token is unique across the up-to-5 holders.
client.release(&guard).await?;
```

### Cap and clamping

The broker enforces a configurable hard ceiling
(`LMX_MAX_CONCURRENCY_CAP`, default `1000` — see
[`DEFAULT_MAX_CONCURRENCY_CAP`](src/protocol.rs)). A `lock` request with
`max` above the cap is **silently clamped** to the ceiling and counted
in `dd_rust_network_mutex_concurrency_cap_clamps_total`. The HTTP
status page surfaces both numbers as a "Cap clamps (total)" card and a
"Concurrency cap (ceiling)" listener row, so an operator can spot the
mismatch without reading logs.

We chose silent-clamp over hard-reject because the lock still works
correctly under the ceiling — rejecting it would push the failure to
the caller's catch path with no operational benefit. Operators who
*want* a hard reject can alert on the clamp counter.

### Composite locks and `max`

`max` is **single-key only**. Composite (multi-key) locks always use
`max=1` per member key — combining semaphore and composite is a
deadlock-prone surface area (you'd need to lock K slots across N keys
in some agreed order and the right answer is workload-specific).
Composite callers that need parallel-with-overlap should split the work
into independent single-key semaphore acquires instead.

### Behavior of mismatched `max` values across callers

A `lock` request that **omits** `max` preserves the existing per-key
cap rather than resetting it to `1`. This means once a caller opts the
key into semaphore semantics with `max=N`, follow-up single-caller
acquires don't accidentally revert the key to mutex behavior.
Conversely, a caller that explicitly sends `max=M` (with `M >= 1`)
sets the cap to `M` (clamped to the ceiling) — useful for dynamically
scaling concurrency up. Scaling *down* keeps existing holders in their
slots but admits no new ones until the count drops below the new cap,
which is the safest of the three plausible behaviors here.

### `max = 0` is a request error, not a sentinel

Earlier revisions silently treated `max: 0` the same as `max: null`.
That was a foot-gun: a misconfigured caller passing `0` would be
told their lock was acquired with whatever cap the key already had,
masking the bug in their code. The current broker rejects `max: 0`
eagerly with a clear `error` field on the response, **before** any
per-key state is mutated:

- The Rust client returns `ClientError::Invalid` from
  `acquire_with_max(_, 0, _)` without sending a request at all.
- A raw TCP/UDS request with `"max": 0` comes back as
  `Response::Lock { acquired: false, error: Some("`max` must be >= 1; …") }`
  (or `Response::CompositeLock` with the same shape on the composite
  path). No holder is created, no waiter queued, no `LockState`
  allocated for the key.
- `POST /v1/lock {"key": "...", "max": 0}` returns HTTP **400** with
  `{"acquired": false, "error": "..."}`.
- An end-to-end check (`tests::raw_tcp_max_zero_rejected_with_error_and_no_side_effect`)
  asserts `holders`, `waiters`, and `keys` all stay at `0` in
  `/metrics` after the rejection.

If you genuinely want "use whatever the per-key cap currently is",
**omit** the `max` field — that's the documented sentinel.

## TTL eviction (`live-mutex#13`)

Originally requested at [`live-mutex#13`](https://github.com/ORESoftware/live-mutex/issues/13),
which flagged the cost of doing a per-request timer ("instead create a
setTimeout, every 10 ms or so"). We implement it that way:

- Every successful exclusive grant — single-key or composite — registers a
  `(deadline, lock_uuid, keys, client)` row in a single broker-wide
  `BTreeMap<(Instant, u64), DeadlineEntry>` (`schedule_deadline`).
- One periodic task (`Broker::spawn_ttl_sweeper`, started by `server::run`)
  ticks every `LMX_TTL_SWEEP_INTERVAL_MS` (default `10ms`). On each tick it
  pops `range(..=now)` from the BTreeMap in one pass — `O(log n + k)` for
  the `k` expired entries — force-releases each holder, and tries to grant
  the next pending request on every freed key.
- Releases (`handle_unlock`) and disconnects (`drop_client`) deliberately
  do **not** remove the matching deadline entry. The sweep does a
  cheap "is this lock_uuid still actually held?" check and skips stale
  rows, keeping the unlock fast path off the BTreeMap entirely.
- Tests can drive eviction synchronously via `Broker::tick_ttl(now: Instant)`,
  no real wall time required.

### Observability

Two new Prometheus series surface the sweeper:

- `dd_rust_network_mutex_pending_deadlines` (gauge) — rows in the BTreeMap.
- `dd_rust_network_mutex_ttl_evictions_total` (counter) — cumulative
  evictions. Going up means at least one client died/wedged with a held
  lock and the sweeper had to clean up; an alert on this counter is a
  great early-warning for misbehaving callers.

To disable auto-eviction (e.g. for tests that want to call `tick_ttl`
themselves with a synthetic `Instant`) set `LMX_TTL_SWEEP_INTERVAL_MS=0`.

## Known limitations

- **Single broker replica.** See above.
- **Drop semantics.** `LockGuard` does not auto-release on drop because Rust's
  `Drop` cannot run async code reliably. Callers must invoke
  `Client::release` (or `RwReadGuard::release` / `RwWriteGuard::release`)
  explicitly. A future scoped-guard helper that owns a tokio task can layer
  on top. **However**, dropping the underlying `Client` *does* close the
  socket cleanly: the spawned reader task is aborted on `Drop`, both
  halves of the `tokio::io::split` are released, and the broker observes
  EOF and runs `drop_client` — so any locks the dying client still held
  are released and the next waiter is granted (verified end-to-end by
  `tests::dropped_client_releases_held_locks_for_other_waiters`).
- **HTTP holds locks indefinitely until `/v1/unlock`** *unless* the
  caller passes `ttlMs`. When `ttlMs` is set, an HTTP acquirer that never
  calls `/v1/unlock` is cleaned up by the periodic sweeper described
  above.

## Relationship to upstream `live-mutex`

The wire protocol is **not** byte-compatible with the Node.js
[`live-mutex`](https://github.com/ORESoftware/live-mutex) broker. The two
projects diverged on field naming (e.g. `lockUuid` here vs. `_uuid` upstream),
on response framing (this broker emits a `compositeLock` response type for
multi-key acquires), and on additions (fencing tokens, semaphore-style `max`,
HTML status page). Cross-broker comparison code lives in
[`clients/ts/src/compare.ts`](clients/ts/src/compare.ts) and uses adapters,
not a single shared client.

If you are looking for the **Node.js** original, use
[`oresoftware/live-mutex`](https://github.com/ORESoftware/live-mutex). If you
want the same shape of broker in Rust with the additions listed above, use
this crate.

## Observability — `routineId` and OpenTelemetry

Every top-level function/method in this crate starts with a single line:

```rust
fn handle_request(...) {
    crate::routine_id!("ddl-routine-XYZ123abc");
    // ...
}
```

This expands to a `const ROUTINE_ID: &str = "ddl-routine-XYZ123abc";` plus a
brief `tracing::info_span!(…)` that emits an `info!("enter")` log line tagged
with both `routine_id` (the static nanoid) and `code.function` (the
`module_path!`). The IDs are **statically embedded literals** — never
generated at runtime — so the same string appears in source, in stdout
logs, and as a span attribute in OTel telemetry. To find the source of any
log line:

```bash
rg ddl-routine-XYZ123abc
```

…will land you on the exact function. No fuzzy log-text matching.

### Wiring OTel

`init_tracing()` (re-exported from the crate root) checks the standard OTel
env vars at startup:

| Env var                         | Effect                                                                                                |
| ------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `OTEL_EXPORTER_OTLP_ENDPOINT`   | When set, install a `tracing-opentelemetry` layer that exports spans + events to that OTLP/gRPC URL.  |
| `OTEL_SERVICE_NAME`             | Service name attribute (default `dd-rust-network-mutex`).                                             |
| `OTEL_RESOURCE_ATTRIBUTES`      | Honored by `opentelemetry_sdk` for arbitrary `k=v` resource attributes.                               |
| `LMX_LOG_FORMAT`                | `text` (default) or `json` for the stdout layer. Independent of OTel.                                 |
| `RUST_LOG`                      | Standard `tracing` filter (e.g. `lmx=debug,info`).                                                    |

If `OTEL_EXPORTER_OTLP_ENDPOINT` is unset, the binary stays a single-process
broker that writes structured logs to stdout — no extra dependencies wake up.

For a standard Kubernetes/Grafana stack:

- scrape `GET /metrics` with Prometheus and build Grafana dashboards from the
  `dd_rust_network_mutex_*` series.
- set `LMX_LOG_FORMAT=json` and ship stdout/stderr to Loki with your normal
  log collector.
- set `OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317` when you want
  spans/events exported through an OpenTelemetry Collector.

You can also disable the OTel exporter at compile time with
`--no-default-features --features tls` (drops the `opentelemetry*` crates).
This produces a smaller binary suitable for environments that can't afford
the gRPC/protobuf footprint.

## Contributing

Pull requests welcome. Please:

1. Run `cargo test` (full suite, including TLS) before opening a PR.
2. Keep `PROTOCOL.md` and the cross-runtime clients (`clients/{ts,go,dart,gleam}/`)
   in sync if you touch the wire format.
3. Add a regression test in `tests/integration.rs` for any behavior change.
4. New top-level fns/methods should start with a `crate::routine_id!(...)`
   call. Generate a fresh nanoid (e.g.
   `python3 -c "import secrets; print('ddl-routine-' + secrets.token_urlsafe(15)[:18])"`)
   and use it as a literal — do not generate it at runtime.

## License

[MIT](LICENSE) © ORE Software. Same license as upstream
[`live-mutex`](https://github.com/ORESoftware/live-mutex).
