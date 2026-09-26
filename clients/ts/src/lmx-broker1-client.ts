// Multiplexed NDJSON client for oresoftware/live-mutex Broker1 used by
// cross-implementation conformance. Successful grants are rejected unless they
// carry complete, exact fencing authority.

import { createConnection, type Socket } from "node:net";
import { randomUUID } from "node:crypto";

const MAX_FENCING_TOKEN = Number.MAX_SAFE_INTEGER;

function fence(value: unknown, context: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 1 || value > MAX_FENCING_TOKEN) {
    throw new Error(`${context}: invalid or missing fencing token`);
  }
  return value;
}

function fenceMap(value: unknown, keys: string[], context: string): Record<string, number> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${context}: missing fencing token map`);
  }
  const raw = value as Record<string, unknown>;
  if (Object.keys(raw).length !== keys.length || new Set(keys).size !== keys.length) {
    throw new Error(`${context}: fencing token/key cardinality mismatch`);
  }
  const out: Record<string, number> = {};
  for (const key of keys) {
    if (!(key in raw)) {
      throw new Error(`${context}: missing fencing token for ${key}`);
    }
    out[key] = fence(raw[key], `${context}[${key}]`);
  }
  return out;
}

export interface Broker1SingleHandle {
  key: string;
  lockUuid: string;
  fencingToken: number;
}

export interface Broker1CompositeHandle {
  keys: string[];
  lockUuid: string;
  fencingTokens: Record<string, number>;
}

interface Waiter {
  resolve(msg: Record<string, unknown>): void;
  reject(err: Error): void;
  untilGrant: boolean;
}

export class Broker1Client {
  private socket: Socket | null = null;
  private buffer = "";
  private inflight = new Map<string, Waiter>();
  private pid = process.pid;

  constructor(private readonly host: string, private readonly port: number) {}

  async connect(): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      const sock = createConnection({ host: this.host, port: this.port }, () => {
        sock.setNoDelay(true);
        this.socket = sock;
        resolve();
      });
      sock.once("error", reject);
      sock.on("data", (chunk) => this.onData(chunk));
      sock.on("close", () => this.failAll(new Error("connection closed")));
    });
    this.write({ type: "version", value: "0.2.25" });
  }

  private onData(chunk: Buffer): void {
    this.buffer += chunk.toString("utf8");
    let nl: number;
    while ((nl = this.buffer.indexOf("\n")) >= 0) {
      const line = this.buffer.slice(0, nl);
      this.buffer = this.buffer.slice(nl + 1);
      if (!line.trim()) {
        continue;
      }
      let msg: Record<string, unknown>;
      try {
        msg = JSON.parse(line) as Record<string, unknown>;
      } catch {
        continue;
      }
      const uuid = msg["uuid"] as string | undefined;
      if (!uuid) {
        continue;
      }
      const w = this.inflight.get(uuid);
      if (!w) {
        continue;
      }
      if (w.untilGrant) {
        const acquired = msg["acquired"] === true;
        const hasError = typeof msg["error"] === "string";
        if (!acquired && !hasError) {
          continue;
        }
      }
      this.inflight.delete(uuid);
      w.resolve(msg);
    }
  }

  private failAll(err: Error): void {
    for (const [, w] of this.inflight) {
      w.reject(err);
    }
    this.inflight.clear();
  }

  private write(obj: Record<string, unknown>): void {
    if (!this.socket) {
      throw new Error("not connected");
    }
    this.socket.write(JSON.stringify(obj) + "\n");
  }

  private roundtrip(obj: Record<string, unknown>, uuid: string, untilGrant: boolean): Promise<Record<string, unknown>> {
    return new Promise((resolve, reject) => {
      this.inflight.set(uuid, { resolve, reject, untilGrant });
      this.write(obj);
    });
  }

  async acquire(key: string, ttlMs = 30_000): Promise<Broker1SingleHandle> {
    const uuid = randomUUID();
    const reply = await this.roundtrip(
      { type: "lock", uuid, key, pid: this.pid, keepLocksAfterDeath: false, ttl: ttlMs > 0 ? ttlMs : null, wait: true },
      uuid, true,
    );
    if (reply["acquired"] !== true) {
      throw new Error(`lock(${key}) not acquired: ${reply["error"] ?? JSON.stringify(reply)}`);
    }
    return { key, lockUuid: uuid, fencingToken: fence(reply["fencingToken"], `lock(${key})`) };
  }

  async tryAcquire(key: string, ttlMs = 30_000): Promise<Broker1SingleHandle | null> {
    const uuid = randomUUID();
    const reply = await this.roundtrip(
      { type: "lock", uuid, key, pid: this.pid, keepLocksAfterDeath: false, ttl: ttlMs > 0 ? ttlMs : null, wait: false },
      uuid, false,
    );
    if (typeof reply["error"] === "string") {
      throw new Error(`tryAcquire(${key}) error: ${reply["error"]}`);
    }
    if (reply["acquired"] !== true) {
      return null;
    }
    return { key, lockUuid: uuid, fencingToken: fence(reply["fencingToken"], `tryAcquire(${key})`) };
  }

  async release(h: Broker1SingleHandle): Promise<void> {
    const uuid = randomUUID();
    const reply = await this.roundtrip(
      { type: "unlock", uuid, _uuid: h.lockUuid, key: h.key, force: false }, uuid, false,
    );
    if (reply["unlocked"] !== true) {
      throw new Error(`unlock(${h.key}) rejected: ${reply["error"] ?? JSON.stringify(reply)}`);
    }
  }

  async acquireMany(keys: string[], ttlMs = 30_000): Promise<Broker1CompositeHandle> {
    const uuid = randomUUID();
    const reply = await this.roundtrip(
      { type: "acquire-many", uuid, keys, ttl: ttlMs > 0 ? ttlMs : null, wait: true }, uuid, true,
    );
    if (reply["acquired"] !== true) {
      throw new Error(`acquire-many rejected: ${reply["error"] ?? reply["contendedKey"] ?? JSON.stringify(reply)}`);
    }
    const grantedKeys = Array.isArray(reply["keys"]) ? reply["keys"] as string[] : keys;
    const lockUuid = reply["lockUuid"];
    if (typeof lockUuid !== "string" || lockUuid.length === 0) {
      throw new Error("acquire-many omitted lockUuid");
    }
    return { keys: grantedKeys, lockUuid, fencingTokens: fenceMap(reply["fencingTokens"], grantedKeys, "acquire-many") };
  }

  async tryAcquireMany(keys: string[], ttlMs = 30_000): Promise<Broker1CompositeHandle | null> {
    const uuid = randomUUID();
    const reply = await this.roundtrip(
      { type: "acquire-many", uuid, keys, ttl: ttlMs > 0 ? ttlMs : null, wait: false }, uuid, false,
    );
    if (typeof reply["error"] === "string") {
      throw new Error(`tryAcquireMany error: ${reply["error"]}`);
    }
    if (reply["acquired"] !== true) {
      return null;
    }
    const grantedKeys = Array.isArray(reply["keys"]) ? reply["keys"] as string[] : keys;
    const lockUuid = reply["lockUuid"];
    if (typeof lockUuid !== "string" || lockUuid.length === 0) {
      throw new Error("tryAcquireMany omitted lockUuid");
    }
    return { keys: grantedKeys, lockUuid, fencingTokens: fenceMap(reply["fencingTokens"], grantedKeys, "tryAcquireMany") };
  }

  async releaseMany(h: Broker1CompositeHandle): Promise<void> {
    const uuid = randomUUID();
    const reply = await this.roundtrip({ type: "release-many", uuid, lockUuid: h.lockUuid }, uuid, false);
    if (reply["released"] !== true) {
      throw new Error(`release-many rejected: ${reply["error"] ?? JSON.stringify(reply)}`);
    }
  }

  async close(): Promise<void> {
    this.socket?.destroy();
    this.socket = null;
  }
}
