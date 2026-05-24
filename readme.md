# live-mutex-rs

[![CI: cargo test](https://img.shields.io/badge/test-cargo%20test-blue?style=flat-square)](#local-development)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat-square)](LICENSE)

A Rust port of the Node.js [`live-mutex`](https://github.com/ORESoftware/live-mutex)
networked-mutex library. A single-process broker that synchronizes access to a
per-key lock state, plus a Rust client library that talks to it over TCP, Unix
domain sockets, or HTTP (for serverless / Lambda callers).

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

## Wire protocol (TCP / UDS)

Each frame is one JSON object terminated by `\n`. Every request carries a
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

`waitMs` is HTTP long-poll: the broker holds the request open up to that many
milliseconds while waiting for a queued lock to be granted. The default is no
wait; the caller should retry on `acquired:false`.

If `LMX_AUTH_TOKEN` is set, every HTTP call must include either an
`Authorization: Bearer <token>` or `X-LMX-Auth: <token>` header.

## Environment variables

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
| `LMX_TTL_SWEEP_INTERVAL_MS` | `10`         | Periodic TTL-eviction sweep cadence (originally `live-mutex#13`). `0` disables auto-eviction.    |
| `LMX_STATUS_PORT`       | unset            | Bind a dedicated read-only HTML status listener on this port (originally `live-mutex#108`). The same page is also served at `/` on `LMX_HTTP_PORT`. |
| `LMX_TCP_NODELAY`       | `true`           | Apply `TCP_NODELAY` on broker-accepted sockets. Experiment from `live-mutex#22`.                 |
| `LMX_TCP_QUICKACK`      | `true`           | Re-apply `TCP_QUICKACK` after every read on Linux. No-op on macOS/BSD. See `live-mutex#22`.      |
| `LMX_TLS_CERT`          | unset            | (`tls` feature) PEM-encoded server certificate path.                                             |
| `LMX_TLS_KEY`           | unset            | (`tls` feature) PEM-encoded server private key path.                                             |
| `LMX_LOG_FORMAT`        | `text`           | Set to `json` for structured logs.                                                               |
| `RUST_LOG`              | `info`           | Standard `tracing` filter (e.g. `lmx=debug,info`).                                               |

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

The repo root ships a multi-stage `Dockerfile` (build with
`rust:1.90-bookworm`, run on `debian:bookworm-slim`) that produces a
small, non-root image with TLS and OTel features enabled by default:

```bash
# Build (linux/amd64 by default; pass --platform for cross-arch).
docker build -t oresoftware/live-mutex-rs:0.1.123 .

# Run (TCP 6970 + HTTP 6971 — see Environment variables for everything).
docker run --rm -p 6970:6970 -p 6971:6971 \
    oresoftware/live-mutex-rs:0.1.123
```

The published image at
[`oresoftware/live-mutex-rs`](https://hub.docker.com/r/oresoftware/live-mutex-rs)
is built from `dev` and tagged with the matching `Cargo.toml`
version. To opt out of TLS / OTel for a smaller image, pass
`--build-arg CARGO_BUILD_FLAGS="--no-default-features"` (or
`--no-default-features --features tls` for TLS-only).

If you'd rather build the binary outside Docker, the broker reads
everything it needs from environment variables and has no config file:
`cargo build --release` plus the env vars in the table above is enough.

### High availability

The reference deployment is **single-replica** because all lock state lives in
process memory. A future HA mode would either:

1. Run an active-passive pair with a single-leader gate (e.g. a Postgres
   advisory lock), so only the leader serves clients and the passive replica
   picks up if the leader dies, **or**
2. Replicate the operations log to a quorum (Raft) so any replica can serve
   reads while writes go through the leader.

Both are straightforward extensions of the broker `state` machine but neither
is implemented in this initial version.

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

Two correctness properties hold by construction:

1. **Atomicity.** A composite acquirer always either holds *all* of its
   keys or *none* — even under concurrent contention, sweeper TTL
   evictions, partial-grant races, or owning-client disconnects. The
   broker tracks partial grants and rolls them back if any later key in
   the set turns out to be already held.
2. **Deadlock freedom via lexicographic ordering.** Two callers issuing
   `acquire_composite(["A","B"])` and `acquire_composite(["B","A"])`
   could deadlock under naive grant order. The broker normalises the
   request to lexicographic order before queueing, so both callers wait
   on the same key's notify queue and one always wins outright.

Composite locks are a primary feature of this broker, used in production
to guard cross-shard operations that touch more than one logical
resource (e.g. transferring an item between two queues, or rotating a
two-key credential without exposing a window where neither key is held).

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
//   { "orders": 7, "users": 3 } (order is alphabetical, mint is broker-side)

// … critical section that touches both resources …

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

### Constraints and interaction with semaphores

- `keys` is bounded to **1..=5** by the broker. Larger sets are rejected
  with a 400 (HTTP) or `error: "..."` (TCP) before any state mutation.
- `max` is **single-key only**. Composite locks always use `max=1` per
  member key — combining semaphore and composite is a deadlock-prone
  surface area (see the dedicated discussion in
  [Composite locks and `max`](#composite-locks-and-max) below).
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
