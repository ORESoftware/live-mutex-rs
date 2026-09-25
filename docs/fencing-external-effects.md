# Fencing tokens and external effects

Every successful live-mutex-rs grant carries a fencing token. The token must follow the protected mutation whenever work leaves the broker's authority boundary.

A lock/lease can expire or be superseded while an old holder is paused. The broker can prevent that holder from reacquiring authority, but it cannot revoke an HTTP request or database write that the holder performs after waking up.

```text
A gets fence 41
A pauses
B becomes the current owner with fence 42
B commits external state using 42
A resumes and attempts a delayed write using 41
```

The external system must reject `41`.

## Fence-aware datastore contract

Keep a durable high-watermark for each canonical resource key and make the fence check atomic with the mutation:

```text
incoming token > current token       -> advance + mutate
incoming token < current token       -> stale; reject
same token + same operation/payload  -> replay; successful no-op
same token + different work          -> token_reuse; reject
```

For PostgreSQL, put the watermark row and business-row mutation in the same transaction. For Redis/Valkey, use one script/function or another atomic compare-and-set mechanism. Do not check the token in one system and perform the protected write in a second system.

A downstream watermark is intentionally long-lived. Deleting and recreating the business row must not reset the fence for the same logical resource identity while delayed work could still arrive.

## HTTP APIs without numeric fencing

Many external APIs do not accept a fencing token. Use a durable idempotency key / operation id when the provider supports one. Generate it before issuing the effect, persist it with the work intent/outbox, and reuse exactly the same id when retrying an uncertain response.

A timeout does not prove the remote side failed. Creating a new idempotency key after a timeout can duplicate the effect.

If an API supports conditional versions in addition to idempotency, use both mechanisms.

## Standalone and Raft modes

The broker emits monotonic tokens for ownership changes and Raft snapshots carry fencing state with replicated lock state. Regardless of broker deployment mode, callers must still enforce the token at every independently authoritative datastore.

An in-memory or wall-clock-derived counter is not a substitute for the downstream watermark. If a restarted authority ever presents an older/equal token, the datastore must fail closed rather than accept a stale write. This protects safety even when availability requires the authority to advance beyond a previously accepted watermark.

## Multi-key grants

Composite acquisitions return one fencing token per resource key. Pass the token belonging to the exact resource being mutated. Do not choose the largest token from the set and reuse it for unrelated keys; fencing watermarks are scoped to the protected resource identity.

## Executable contract

`tests/fencing_external_effects.rs` obtains real broker grants for two sequential owners, advances a simulated external datastore with the newer grant, and verifies that the delayed old owner is rejected. It also distinguishes exact idempotent replay from unsafe equal-token reuse for different work.
