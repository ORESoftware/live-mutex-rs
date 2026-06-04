# BrokerRaft Architecture

BrokerRaft is the high-availability HTTP backend for `live-mutex-rs`.
It is a separate deployment from the regular single-node Broker: the
regular Broker keeps the TCP + HTTP API on one pod, while BrokerRaft runs
three or five HTTP-only pods with a Raft RPC peer service.

The leader orders lock operations. A quorum commits each operation before
the in-process broker state is changed. Followers can receive HTTP lock
requests from a round-robin load balancer and proxy them to the current
leader.

## State Diagram

```mermaid
stateDiagram-v2
  [*] --> Follower: start or restart

  Follower --> Candidate: election timeout
  Candidate --> Leader: receives quorum votes
  Candidate --> Follower: sees higher term or valid leader
  Leader --> Follower: sees higher term

  Follower --> Follower: AppendEntries heartbeat\nreset election deadline
  Leader --> Leader: heartbeat peers\nreplicate committed log

  state Leader {
    [*] --> AppendLocal
    AppendLocal --> Replicate: append client command to local log
    Replicate --> Commit: quorum acknowledges index
    Commit --> Apply: persist commitIndex
    Apply --> Compact: apply to Broker state
    Compact --> [*]: snapshot/compact only when safe
  }
```

## Lock Commit Sequence

```mermaid
sequenceDiagram
  autonumber
  participant Client
  participant LB as "Load balancer / Service"
  participant F as "Follower"
  participant L as "Leader"
  participant P as "Peer quorum"
  participant B as "Local Broker state"
  participant Disk as "Raft log + hard state"

  Client->>LB: POST /v1/lock
  LB->>F: round-robin request
  F->>L: ProxyRequest(request, wait_ms)
  L->>Disk: append ClientRequest
  L->>P: AppendEntries(entries, leader_commit)
  P-->>L: success from quorum
  L->>Disk: persist commitIndex
  L->>B: apply committed request
  B-->>L: lock response
  L-->>F: ProxyResponse(response)
  F-->>LB: HTTP 200
  LB-->>Client: acquired + lockUuid
```

If the load balancer can prefer the leader, it should use
`GET /raft/leaderz` as the leader-only health check. That removes the proxy
hop shown above. Correctness does not depend on leader-aware routing, because
followers proxy writes and the leader still requires quorum before applying.

## Failover Event Trace

```mermaid
flowchart TD
  A["Leader is serving writes"] --> B["Leader pod/process fails"]
  B --> C["Followers stop receiving heartbeats"]
  C --> D["Election timeout expires with jitter"]
  D --> E["Candidate increments term and requests votes"]
  E --> F{"Quorum votes?"}
  F -- "no" --> D
  F -- "yes" --> G["Candidate becomes leader"]
  G --> H["New leader accepts proxied /v1/lock requests"]
  H --> I["AppendEntries reaches quorum"]
  I --> J["Commit index is persisted"]
  J --> K["Operation applies to local Broker state"]
  K --> L["Old leader replacement rejoins as follower"]
```

## Log Compaction Rule

BrokerRaft does not delete old committed log entries just because they are
older than a wall-clock threshold. Instead it writes a durable snapshot and
compacts entries only when all of these are true:

- the entries are committed,
- the entries are applied,
- the snapshot covers the compacted index,
- the local Broker state is idle.

That conservative rule saves disk during idle periods while preserving replay
safety for live locks and waiters.
