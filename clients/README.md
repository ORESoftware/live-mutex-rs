# `rust-network-mutex-rs` clients

Eight language clients that all speak the same JSON wire protocol (see
`../PROTOCOL.md`) to the Rust broker. Each client mirrors the Rust
`Request` / `Response` enum in its native idiom — **no magic strings**,
unlike the upstream Node `live-mutex` library which uses `if (data.type
=== '…')` chains in `broker.js`.

| Runtime    | Path                | Discriminator construct                      | Smoke command                                                |
|------------|---------------------|----------------------------------------------|--------------------------------------------------------------|
| Rust       | `../src/client.rs`  | serde tagged enum (`#[serde(tag="type")]`)   | `cargo test --no-default-features` (in deployment root)      |
| TypeScript | `ts/`               | discriminated union (`type Request = … \| …`) | `pnpm --dir clients/ts smoke`                                |
| Go         | `go/`               | typed `RequestType` const block + switch     | `go run ./clients/go/cmd/smoke`                              |
| Dart       | `dart/`             | `sealed class Request` + pattern matching   | `dart run clients/dart/bin/smoke.dart`                       |
| Gleam      | `gleam/`            | `pub type Request { … }` (true ADT)          | `LIVE_MUTEX_SMOKE=1 gleam test` (in `clients/gleam/`)        |
| Python     | `python/`           | `enum.Enum` `RequestType` + typed builders   | `python3 clients/python/smoke.py`                            |
| C++        | `cpp/`              | `enum class RequestType` + `switch`          | `make -C clients/cpp run`                                    |
| Java       | `java/`             | `enum RequestType` + `switch` (records)      | `clients/java/build.sh && java -cp clients/java/out com.oresoftware.networkmutex.Smoke` |

The TypeScript client also ships a head-to-head benchmark harness
(`ts/src/compare.ts`) that runs the same workload against
`oresoftware/live-mutex` (the upstream Node broker) and our Rust broker
in the same Node process and prints a side-by-side throughput / latency
report. Sample local result on M-class darwin (`WORKERS=8 KEYS=4
DURATION_MS=2000`):

```
ours      total= 102775  throughput=   51388 ops/s  avg=   0.16ms  max=   1.35ms  errors=0
theirs    total=  76411  throughput=   38206 ops/s  avg=   0.21ms  max=   3.69ms  errors=0
[compare] ratio (ours / theirs) = 1.35x
```

## Wait / No-Wait Acquire

All exclusive-lock clients now expose both blocking acquire and fail-fast
try-lock behavior for single-key and composite locks. Blocking acquire sends
`wait:true` and keeps the request registered through the queued
`acquired:false` notice until the final `acquired:true` grant. Try-lock helpers
send `wait:false`, read exactly one response, and return no handle on
contention, so failed attempts cannot leave a deferred waiter behind.

## Run every smoke test

```bash
# 1. start the broker (in another terminal)
cargo run --release --no-default-features --bin dd-rust-network-mutex

# 2. run every client smoke
./scripts/run-all-client-smokes.sh
```

The script auto-falls back to running the Dart smoke inside `dart:stable`
in Docker if no local Dart SDK is installed.

## Why an enum (instead of magic strings)?

Adding a new request variant on the broker side (in `src/protocol.rs`) is
a *compile error* in every client until the client adds the matching
constructor:

- Rust → enum exhaustiveness is enforced by `match`.
- TypeScript → `assertNever(value: never): never` makes the `switch`
  exhaustiveness-checked by `tsc`.
- Go → `switch` over `RequestType` plus `staticcheck`'s `exhaustive`
  linting rule.
- Dart → `sealed class` + Dart 3 pattern matching is exhaustive by
  construction.
- Gleam → custom types are real ADTs; non-exhaustive `case` is a compile
  error.

The upstream `live-mutex` broker switches on bare strings, so a typo
silently routes to "no handler". This is the structural fix.
