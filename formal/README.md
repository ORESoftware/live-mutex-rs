# Formal verification contract

This directory defines the safety properties that `live-mutex-rs` must preserve across the broker, wire protocol, and every generated/hand-written client.

The executable model checker lives in `tests/formal_fencing_model.rs`. It performs bounded exhaustive state-space exploration for one canonical resource key and two independent clients. The implementation/integration tests then connect those abstract obligations to the real broker.

## Safety invariants

For a canonical resource key:

1. **Exclusive ownership** — at most one exclusive owner is current at a time.
2. **Monotonic authority** — every successor grant receives a fencing token strictly greater than every prior grant for that key.
3. **No rollback on release** — releasing ownership never decreases or reuses the authority high-watermark.
4. **Stale-writer rejection** — a downstream effect carrying a token below the downstream high-watermark is rejected.
5. **Exact replay only** — an effect carrying the current token is a successful no-op only when its operation identity and payload identity are exactly the same as the accepted effect.
6. **Token-reuse rejection** — the current token cannot authorize different work after it has already been accepted for another operation/payload.
7. **Per-key scope** — a token is authority only for the resource key for which it was granted. Multi-key grants therefore preserve one token per key.
8. **Lossless client propagation** — every supported client must expose the broker-provided fencing token on its grant/handle surface instead of deriving, truncating, rounding, or silently dropping it.

The model intentionally permits a released/superseded client to attempt a later write. That is the zombie-writer case fencing exists to make safe.

## What is proven

`tests/formal_fencing_model.rs` explores every reachable state in a finite model with two clients, two distinct work identities, and four successive grants. State de-duplication makes the exploration exhaustive within those bounds rather than random/property-sampling.

`tests/formal_client_fencing_surface.rs` is an implementation-conformance gate over every client directory. It requires a source-level fencing-token surface in each client, so adding a new runtime without carrying the authority token fails the normal Rust test suite.

The existing real-broker fencing integration test is a refinement witness: it obtains actual sequential broker grants and proves the newer grant fences a delayed older writer at the external-effect boundary.

## Scope

This is bounded model checking, not an unbounded theorem proof. Increasing the grant bound should not change the invariants because the token state is monotone; the small bound is chosen to make exhaustive exploration fast enough for ordinary CI. Any change to grant ordering, token serialization, replay rules, multi-key behavior, or client grant types must update the model and conformance tests in the same PR.
