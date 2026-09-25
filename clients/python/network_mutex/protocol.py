"""Wire protocol for ``live-mutex-rs`` with fail-closed fencing admission.

Successful lock/RW grants are authority-bearing frames.  The decoder therefore
rejects any such frame that omits a fencing token, supplies a boolean/fraction,
uses zero, exceeds JavaScript's exact JSON integer ceiling, or returns an
incomplete per-key token map for a composite grant.
"""

from __future__ import annotations

import enum
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

MAX_COMPOSITE_KEYS = 5
MAX_FENCING_TOKEN = 9_007_199_254_740_991
PROTOCOL_VERSION = "0.1.0"


class RequestType(str, enum.Enum):
    VERSION = "version"
    AUTH = "auth"
    LOCK = "lock"
    UNLOCK = "unlock"
    REGISTER_READ = "registerRead"
    REGISTER_WRITE = "registerWrite"
    END_READ = "endRead"
    END_WRITE = "endWrite"
    LOCK_INFO = "lockInfo"
    LS = "ls"
    HEARTBEAT = "heartbeat"


class ResponseType(str, enum.Enum):
    VERSION = "version"
    AUTH = "auth"
    LOCK = "lock"
    COMPOSITE_LOCK = "compositeLock"
    UNLOCK = "unlock"
    REGISTER_READ_RESULT = "registerReadResult"
    REGISTER_WRITE_RESULT = "registerWriteResult"
    END_READ_RESULT = "endReadResult"
    END_WRITE_RESULT = "endWriteResult"
    LOCK_INFO = "lockInfo"
    LS_RESULT = "lsResult"
    REELECTION = "reelection"
    ERROR = "error"
    OK = "ok"

    @classmethod
    def parse(cls, raw: str) -> "ResponseType":
        try:
            return cls(raw)
        except ValueError as exc:
            raise ValueError(f"unknown response type from broker: {raw!r}") from exc


def _frame(payload: Dict[str, Any]) -> bytes:
    compact = {k: v for k, v in payload.items() if v is not None}
    return (json.dumps(compact, separators=(",", ":")) + "\n").encode("utf-8")


def version_request(uuid: str, value: str = PROTOCOL_VERSION) -> bytes:
    return _frame({"type": RequestType.VERSION.value, "uuid": uuid, "value": value})


def auth_request(uuid: str, token: str) -> bytes:
    return _frame({"type": RequestType.AUTH.value, "uuid": uuid, "token": token})


def lock_request(
    uuid: str,
    *,
    key: Optional[str] = None,
    keys: Optional[List[str]] = None,
    pid: Optional[int] = None,
    ttl_ms: Optional[int] = None,
    max_holders: Optional[int] = None,
    force: bool = False,
    keep_locks_after_death: bool = False,
    wait: Optional[bool] = None,
) -> bytes:
    if (key is None) == (keys is None):
        raise ValueError("lock_request: pass exactly one of key= or keys=")
    if keys is not None and not (1 <= len(keys) <= MAX_COMPOSITE_KEYS):
        raise ValueError(
            f"composite key count must be 1..={MAX_COMPOSITE_KEYS}, got {len(keys)}"
        )
    return _frame({
        "type": RequestType.LOCK.value,
        "uuid": uuid,
        "key": key,
        "keys": keys,
        "pid": pid,
        "ttl": ttl_ms,
        "max": max_holders,
        "force": force or None,
        "keepLocksAfterDeath": keep_locks_after_death or None,
        "wait": wait,
    })


def unlock_request(
    uuid: str,
    *,
    key: Optional[str] = None,
    keys: Optional[List[str]] = None,
    lock_uuid: Optional[str] = None,
    force: bool = False,
) -> bytes:
    return _frame({
        "type": RequestType.UNLOCK.value,
        "uuid": uuid,
        "key": key,
        "keys": keys,
        "lockUuid": lock_uuid,
        "force": force or None,
    })


def register_read_request(uuid: str, key: str) -> bytes:
    return _frame({"type": RequestType.REGISTER_READ.value, "uuid": uuid, "key": key})


def register_write_request(uuid: str, key: str) -> bytes:
    return _frame({"type": RequestType.REGISTER_WRITE.value, "uuid": uuid, "key": key})


def end_read_request(uuid: str, key: str) -> bytes:
    return _frame({"type": RequestType.END_READ.value, "uuid": uuid, "key": key})


def end_write_request(uuid: str, key: str) -> bytes:
    return _frame({"type": RequestType.END_WRITE.value, "uuid": uuid, "key": key})


def lock_info_request(uuid: str, key: str) -> bytes:
    return _frame({"type": RequestType.LOCK_INFO.value, "uuid": uuid, "key": key})


def ls_request(uuid: str) -> bytes:
    return _frame({"type": RequestType.LS.value, "uuid": uuid})


def heartbeat_request(uuid: str) -> bytes:
    return _frame({"type": RequestType.HEARTBEAT.value, "uuid": uuid})


def _fence(value: Any, field_name: str) -> int:
    # bool is a subclass of int in Python and must never become authority.
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name}: fencing token must be an exact integer")
    if value < 1 or value > MAX_FENCING_TOKEN:
        raise ValueError(f"{field_name}: fencing token outside 1..{MAX_FENCING_TOKEN}")
    return value


def _validate_authority(data: Dict[str, Any], response_type: ResponseType) -> None:
    if response_type is ResponseType.LOCK and data.get("acquired") is True:
        if not isinstance(data.get("lockUuid"), str) or not data["lockUuid"]:
            raise ValueError("acquired lock omitted lockUuid")
        _fence(data.get("fencingToken"), "fencingToken")
        return

    if response_type is ResponseType.COMPOSITE_LOCK and data.get("acquired") is True:
        keys = data.get("keys")
        tokens = data.get("fencingTokens")
        if not isinstance(data.get("lockUuid"), str) or not data["lockUuid"]:
            raise ValueError("acquired composite lock omitted lockUuid")
        if not isinstance(keys, list) or not keys or any(not isinstance(k, str) or not k for k in keys):
            raise ValueError("acquired composite lock omitted valid keys")
        if len(set(keys)) != len(keys):
            raise ValueError("acquired composite lock contains duplicate keys")
        if not isinstance(tokens, dict) or set(tokens) != set(keys):
            raise ValueError("acquired composite lock has incomplete fencing token map")
        for key in keys:
            _fence(tokens[key], f"fencingTokens[{key!r}]")
        return

    if response_type in (
        ResponseType.REGISTER_READ_RESULT,
        ResponseType.REGISTER_WRITE_RESULT,
    ) and data.get("granted") is True:
        if not isinstance(data.get("lockUuid"), str) or not data["lockUuid"]:
            raise ValueError("granted rw lock omitted lockUuid")
        _fence(data.get("fencingToken"), "fencingToken")


@dataclass
class Response:
    type: ResponseType
    uuid: str
    raw: Dict[str, Any] = field(repr=False, default_factory=dict)
    broker_version: Optional[str] = None
    ok: Optional[bool] = None
    error: Optional[str] = None
    key: Optional[str] = None
    keys: Optional[List[str]] = None
    acquired: Optional[bool] = None
    unlocked: Optional[bool] = None
    lock_request_count: Optional[int] = None
    lock_uuid: Optional[str] = None
    fencing_token: Optional[int] = None
    fencing_tokens: Optional[Dict[str, int]] = None
    readers_count: Optional[int] = None
    writer_flag: Optional[bool] = None
    granted: Optional[bool] = None
    is_locked: Optional[bool] = None
    lockholder_uuids: Optional[List[str]] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Response":
        if not isinstance(data, dict) or not isinstance(data.get("type"), str):
            raise ValueError("broker response must be an object with string type")
        response_type = ResponseType.parse(data["type"])
        _validate_authority(data, response_type)
        return cls(
            type=response_type,
            uuid=data.get("uuid", ""),
            raw=data,
            broker_version=data.get("brokerVersion"),
            ok=data.get("ok"),
            error=data.get("error"),
            key=data.get("key"),
            keys=data.get("keys"),
            acquired=data.get("acquired"),
            unlocked=data.get("unlocked"),
            lock_request_count=data.get("lockRequestCount"),
            lock_uuid=data.get("lockUuid"),
            fencing_token=data.get("fencingToken"),
            fencing_tokens=data.get("fencingTokens"),
            readers_count=data.get("readersCount"),
            writer_flag=data.get("writerFlag"),
            granted=data.get("granted"),
            is_locked=data.get("isLocked"),
            lockholder_uuids=data.get("lockholderUuids"),
        )

    @classmethod
    def decode(cls, line: bytes) -> "Response":
        return cls.from_dict(json.loads(line))
