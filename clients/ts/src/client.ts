// TCP client for the live-mutex-rs broker. Authority-bearing successful
// grants fail closed unless every required fencing token is an exact positive
// integer in JavaScript's lossless JSON domain.

import { createConnection, type Socket } from "node:net";
import { randomUUID } from "node:crypto";
import {
  assertNever,
  type LockRequest,
  type Request,
  type Response,
  type UnlockRequest,
} from "./protocol.ts";

export const MAX_FENCING_TOKEN = Number.MAX_SAFE_INTEGER;

function fence(value: unknown, context: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 1 || value > MAX_FENCING_TOKEN) {
    throw new Error(`${context}: missing or invalid fencing token`);
  }
  return value;
}

function fenceMap(value: unknown, keys: string[], context: string): Record<string, number> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${context}: missing fencing token map`);
  }
  const raw = value as Record<string, unknown>;
  const result: Record<string, number> = {};
  for (const key of keys) {
    if (!(key in raw)) throw new Error(`${context}: missing fencing token for ${key}`);
    result[key] = fence(raw[key], `${context}[${key}]`);
  }
  if (Object.keys(raw).length !== keys.length) {
    throw new Error(`${context}: fencing token/key cardinality mismatch`);
  }
  return result;
}

export interface ClientOptions {
  host?: string;
  port?: number;
  token?: string;
  connectTimeoutMs?: number;
}

export interface AcquireOptions { ttlMs?: number; waitMs?: number; }
export interface TryAcquireOptions { ttlMs?: number; replyTimeoutMs?: number; }

export interface SingleLockHandle {
  kind: "single";
  key: string;
  lockUuid: string;
  fencingToken: number;
}

export interface CompositeLockHandle {
  kind: "composite";
  keys: string[];
  lockUuid: string;
  fencingTokens: Record<string, number>;
}

export type LockHandle = SingleLockHandle | CompositeLockHandle;

interface Inflight {
  resolve(resp: Response): void;
  reject(err: Error): void;
  multi: boolean;
}

export class NetworkMutexClient {
  private socket: Socket | null = null;
  private buffer = "";
  private inflight = new Map<string, Inflight>();
  private connected = false;

  constructor(private readonly opts: ClientOptions = {}) {}

  async connect(): Promise<void> {
    if (this.connected) return;
    const host = this.opts.host ?? "127.0.0.1";
    const port = this.opts.port ?? 6970;
    const timeoutMs = this.opts.connectTimeoutMs ?? 5_000;

    await new Promise<void>((resolve, reject) => {
      const sock = createConnection({ host, port }, () => {
        sock.setNoDelay(true);
        this.socket = sock;
        this.connected = true;
        resolve();
      });
      const t = setTimeout(() => sock.destroy(new Error(`connect timeout after ${timeoutMs}ms`)), timeoutMs);
      sock.once("error", (err) => { clearTimeout(t); reject(err); });
      sock.on("data", (chunk) => this.onData(chunk));
      sock.on("close", () => {
        this.connected = false;
        const err = new Error("connection closed");
        for (const inf of this.inflight.values()) inf.reject(err);
        this.inflight.clear();
      });
    });

    if (this.opts.token) {
      const resp = await this.send({ type: "auth", uuid: randomUUID(), token: this.opts.token }, { multi: false });
      if (resp.type !== "auth" || !resp.ok) throw new Error(`auth failed: ${JSON.stringify(resp)}`);
    }
  }

  async close(): Promise<void> {
    this.connected = false;
    this.socket?.end();
    this.socket = null;
  }

  send(req: Request, { multi = false }: { multi?: boolean } = {}): Promise<Response> {
    if (!this.socket || !this.connected) return Promise.reject(new Error("not connected"));
    const uuid = req.uuid;
    return new Promise<Response>((resolve, reject) => {
      this.inflight.set(uuid, { resolve, reject, multi });
      this.socket!.write(JSON.stringify(req) + "\n", (err) => {
        if (err) { this.inflight.delete(uuid); reject(err); }
      });
    });
  }

  async acquire(key: string, opts: AcquireOptions = {}): Promise<SingleLockHandle> {
    const req: LockRequest = {
      type: "lock", uuid: randomUUID(), key, ttl: opts.ttlMs ?? 30_000,
      keepLocksAfterDeath: false, wait: true,
    };
    const grant = await this.awaitGrant(req, opts.waitMs ?? 30_000);
    if (grant.type !== "lock" || !grant.acquired || !grant.lockUuid) {
      throw new Error(`acquire(${key}) failed: ${JSON.stringify(grant)}`);
    }
    return { kind: "single", key, lockUuid: grant.lockUuid, fencingToken: fence(grant.fencingToken, `acquire(${key})`) };
  }

  async acquireMany(keys: string[], opts: AcquireOptions = {}): Promise<CompositeLockHandle> {
    if (keys.length === 0 || keys.length > 5) throw new Error(`composite key count must be 1..=5, got ${keys.length}`);
    const req: LockRequest = {
      type: "lock", uuid: randomUUID(), keys, ttl: opts.ttlMs ?? 30_000,
      keepLocksAfterDeath: false, wait: true,
    };
    const grant = await this.awaitGrant(req, opts.waitMs ?? 30_000);
    if (grant.type !== "compositeLock" || !grant.acquired || !grant.lockUuid) {
      throw new Error(`acquireMany([${keys.join(",")}]) failed: ${JSON.stringify(grant)}`);
    }
    const grantedKeys = grant.keys ?? keys;
    return {
      kind: "composite",
      keys: grantedKeys,
      lockUuid: grant.lockUuid,
      fencingTokens: fenceMap(grant.fencingTokens, grantedKeys, "acquireMany"),
    };
  }

  async tryAcquire(key: string, opts: TryAcquireOptions = {}): Promise<SingleLockHandle | null> {
    const req: LockRequest = {
      type: "lock", uuid: randomUUID(), key, ttl: opts.ttlMs ?? 30_000,
      keepLocksAfterDeath: false, wait: false,
    };
    const resp = await this.send(req, { multi: false });
    if (resp.type === "error") throw new Error(`tryAcquire(${key}) error: ${resp.error}`);
    if (resp.type !== "lock") throw new Error(`tryAcquire(${key}) unexpected: ${resp.type}`);
    if (!resp.acquired || !resp.lockUuid) return null;
    return { kind: "single", key, lockUuid: resp.lockUuid, fencingToken: fence(resp.fencingToken, `tryAcquire(${key})`) };
  }

  async tryAcquireMany(keys: string[], opts: TryAcquireOptions = {}): Promise<CompositeLockHandle | null> {
    if (keys.length === 0 || keys.length > 5) throw new Error(`composite key count must be 1..=5, got ${keys.length}`);
    const req: LockRequest = {
      type: "lock", uuid: randomUUID(), keys, ttl: opts.ttlMs ?? 30_000,
      keepLocksAfterDeath: false, wait: false,
    };
    const resp = await this.send(req, { multi: false });
    if (resp.type === "error") throw new Error(`tryAcquireMany error: ${resp.error}`);
    if (resp.type !== "compositeLock") throw new Error(`tryAcquireMany unexpected: ${resp.type}`);
    if (!resp.acquired || !resp.lockUuid) return null;
    const grantedKeys = resp.keys ?? keys;
    return {
      kind: "composite", keys: grantedKeys, lockUuid: resp.lockUuid,
      fencingTokens: fenceMap(resp.fencingTokens, grantedKeys, "tryAcquireMany"),
    };
  }

  async release(handle: LockHandle): Promise<void> {
    const req: UnlockRequest = handle.kind === "single"
      ? { type: "unlock", uuid: randomUUID(), key: handle.key, lockUuid: handle.lockUuid }
      : { type: "unlock", uuid: randomUUID(), keys: handle.keys, lockUuid: handle.lockUuid };
    const resp = await this.send(req, { multi: false });
    if (resp.type !== "unlock" || !resp.unlocked) throw new Error(`release failed: ${JSON.stringify(resp)}`);
  }

  async acquireRead(key: string): Promise<{ lockUuid: string; fencingToken: number }> {
    const req: Request = { type: "registerRead", uuid: randomUUID(), key };
    const resp = await this.awaitRwGrant(req, "registerReadResult");
    if (!resp.lockUuid) throw new Error(`acquireRead(${key}): missing lockUuid`);
    return { lockUuid: resp.lockUuid, fencingToken: fence(resp.fencingToken, `acquireRead(${key})`) };
  }

  async releaseRead(key: string): Promise<void> {
    await this.send({ type: "endRead", uuid: randomUUID(), key }, { multi: false });
  }

  async acquireWrite(key: string): Promise<{ lockUuid: string; fencingToken: number }> {
    const req: Request = { type: "registerWrite", uuid: randomUUID(), key };
    const resp = await this.awaitRwGrant(req, "registerWriteResult");
    if (!resp.lockUuid) throw new Error(`acquireWrite(${key}): missing lockUuid`);
    return { lockUuid: resp.lockUuid, fencingToken: fence(resp.fencingToken, `acquireWrite(${key})`) };
  }

  async releaseWrite(key: string): Promise<void> {
    await this.send({ type: "endWrite", uuid: randomUUID(), key }, { multi: false });
  }

  private onData(chunk: Buffer): void {
    this.buffer += chunk.toString("utf8");
    let nl: number;
    while ((nl = this.buffer.indexOf("\n")) >= 0) {
      const line = this.buffer.slice(0, nl).trim();
      this.buffer = this.buffer.slice(nl + 1);
      if (!line) continue;
      let resp: Response;
      try { resp = JSON.parse(line) as Response; }
      catch (err) {
        const next = this.inflight.values().next().value;
        if (next) next.reject(new Error(`bad frame: ${(err as Error).message}`));
        continue;
      }
      this.dispatch(resp);
    }
  }

  private dispatch(resp: Response): void {
    const uuid = resp.uuid;
    const inf = this.inflight.get(uuid);
    if (!inf) return;
    switch (resp.type) {
      case "version":
      case "auth":
      case "unlock":
      case "endReadResult":
      case "endWriteResult":
      case "lockInfo":
      case "lsResult":
      case "ok":
      case "error":
        this.inflight.delete(uuid); inf.resolve(resp); return;
      case "lock":
      case "compositeLock":
        if (resp.acquired || resp.error) { this.inflight.delete(uuid); inf.resolve(resp); }
        else if (!inf.multi) { this.inflight.delete(uuid); inf.resolve(resp); }
        return;
      case "registerReadResult":
      case "registerWriteResult":
        if (resp.granted) { this.inflight.delete(uuid); inf.resolve(resp); }
        return;
      case "reelection": return;
      default: return assertNever(resp);
    }
  }

  private async awaitGrant(req: Request, waitMs: number): Promise<Response> {
    const timeoutHandle = (() => {
      let id: NodeJS.Timeout | undefined;
      const promise = new Promise<Response>((_, reject) => {
        id = setTimeout(() => {
          this.inflight.delete(req.uuid);
          reject(new Error(`grant timeout after ${waitMs}ms`));
        }, waitMs);
      });
      return { promise, cancel: () => id !== undefined && clearTimeout(id) };
    })();
    try {
      const sendPromise = this.send(req, { multi: true });
      return await Promise.race([sendPromise, timeoutHandle.promise]);
    } finally { timeoutHandle.cancel(); }
  }

  private async awaitRwGrant(
    req: Request,
    expected: "registerReadResult" | "registerWriteResult",
  ): Promise<RegisterReadOrWriteResponse> {
    const resp = await this.send(req, { multi: true });
    if (resp.type !== expected) throw new Error(`expected ${expected}, got ${resp.type}`);
    return resp as RegisterReadOrWriteResponse;
  }
}

type RegisterReadOrWriteResponse = Extract<
  Response,
  { type: "registerReadResult" } | { type: "registerWriteResult" }
>;
