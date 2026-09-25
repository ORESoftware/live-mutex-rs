# Formal lock/refinement procedure

This directory contains a finite exhaustive abstraction of the live-mutex exclusive-lock contract. It is intentionally independent of the Rust implementation so a defect in production code is not copied into the model.

The model checks single-holder exclusion, FIFO waiter progression, request-id idempotence, monotonically increasing fencing tokens, renewal without token replacement, fail-closed stale release, and lease-expiry progress.

`formal/fm.toml` names the production refinement boundary. Changes to any listed broker, protocol, Rust client, or cross-runtime client path must keep the bounded model green and keep that runtime's native tests green. This is a bounded proof of the abstraction, not a proof of the full networked implementation; Raft membership, packet loss, persistence, scheduler fairness, and unbounded state still require their own tests/models.

Run locally:

```bash
node formal/model.mjs
printf '%s\n' '{"actions":[{"kind":"acquire","client":"a","requestId":"a-1"},{"kind":"acquire","client":"b","requestId":"b-1"},{"kind":"release","client":"a","fence":1}]}' | node formal/model.mjs --json-stdin
```
