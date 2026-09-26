// Dart mirror of `src/protocol.rs` with fail-closed fencing admission.

import 'dart:convert';

const int maxFencingToken = 9007199254740991;

int _fence(dynamic value, String field) {
  if (value is! int || value < 1 || value > maxFencingToken) {
    throw FormatException('$field must be an exact fencing token in 1..$maxFencingToken');
  }
  return value;
}

Map<String, int> _fenceMap(dynamic value, List<String> keys) {
  if (value is! Map) {
    throw const FormatException('acquired composite lock omitted fencingTokens');
  }
  if (keys.isEmpty || keys.toSet().length != keys.length || value.length != keys.length) {
    throw const FormatException('acquired composite lock has invalid key/token cardinality');
  }
  final out = <String, int>{};
  for (final key in keys) {
    if (!value.containsKey(key)) {
      throw FormatException('missing fencing token for $key');
    }
    out[key] = _fence(value[key], 'fencingTokens[$key]');
  }
  return out;
}

// ---------- Request ------------------------------------------------------

sealed class Request {
  const Request({required this.uuid});
  final String uuid;
  String get type;
  Map<String, dynamic> toJson();
  String encode() => '${jsonEncode(toJson())}\n';
}

class VersionRequest extends Request {
  const VersionRequest({required super.uuid, required this.value});
  final String value;
  @override String get type => 'version';
  @override Map<String, dynamic> toJson() => {'type': type, 'uuid': uuid, 'value': value};
}

class AuthRequest extends Request {
  const AuthRequest({required super.uuid, required this.token});
  final String token;
  @override String get type => 'auth';
  @override Map<String, dynamic> toJson() => {'type': type, 'uuid': uuid, 'token': token};
}

class LockRequest extends Request {
  const LockRequest({
    required super.uuid,
    this.key,
    this.keys,
    this.pid,
    this.ttl = 30000,
    this.max,
    this.force = false,
    this.retryCount = 0,
    this.keepLocksAfterDeath = false,
    this.wait,
  });
  final String? key;
  final List<String>? keys;
  final int? pid;
  final int ttl;
  final int? max;
  final bool force;
  final int retryCount;
  final bool keepLocksAfterDeath;
  final bool? wait;
  @override String get type => 'lock';
  @override
  Map<String, dynamic> toJson() {
    final m = <String, dynamic>{
      'type': type,
      'uuid': uuid,
      'force': force,
      'retryCount': retryCount,
      'keepLocksAfterDeath': keepLocksAfterDeath,
      'ttl': ttl,
    };
    if (key != null) {
      m['key'] = key;
    }
    if (keys != null) {
      m['keys'] = keys;
    }
    if (pid != null) {
      m['pid'] = pid;
    }
    if (max != null) {
      m['max'] = max;
    }
    if (wait != null) {
      m['wait'] = wait;
    }
    return m;
  }
}

class UnlockRequest extends Request {
  const UnlockRequest({required super.uuid, this.key, this.keys, this.lockUuid, this.force = false});
  final String? key;
  final List<String>? keys;
  final String? lockUuid;
  final bool force;
  @override String get type => 'unlock';
  @override
  Map<String, dynamic> toJson() {
    final m = <String, dynamic>{'type': type, 'uuid': uuid, 'force': force};
    if (key != null) {
      m['key'] = key;
    }
    if (keys != null) {
      m['keys'] = keys;
    }
    if (lockUuid != null) {
      m['lockUuid'] = lockUuid;
    }
    return m;
  }
}

class RegisterReadRequest extends Request {
  const RegisterReadRequest({required super.uuid, required this.key});
  final String key;
  @override String get type => 'registerRead';
  @override Map<String, dynamic> toJson() => {'type': type, 'uuid': uuid, 'key': key};
}

class RegisterWriteRequest extends Request {
  const RegisterWriteRequest({required super.uuid, required this.key});
  final String key;
  @override String get type => 'registerWrite';
  @override Map<String, dynamic> toJson() => {'type': type, 'uuid': uuid, 'key': key};
}

class EndReadRequest extends Request {
  const EndReadRequest({required super.uuid, required this.key});
  final String key;
  @override String get type => 'endRead';
  @override Map<String, dynamic> toJson() => {'type': type, 'uuid': uuid, 'key': key};
}

class EndWriteRequest extends Request {
  const EndWriteRequest({required super.uuid, required this.key});
  final String key;
  @override String get type => 'endWrite';
  @override Map<String, dynamic> toJson() => {'type': type, 'uuid': uuid, 'key': key};
}

class LockInfoRequest extends Request {
  const LockInfoRequest({required super.uuid, required this.key});
  final String key;
  @override String get type => 'lockInfo';
  @override Map<String, dynamic> toJson() => {'type': type, 'uuid': uuid, 'key': key};
}

class LsRequest extends Request {
  const LsRequest({required super.uuid});
  @override String get type => 'ls';
  @override Map<String, dynamic> toJson() => {'type': type, 'uuid': uuid};
}

class HeartbeatRequest extends Request {
  const HeartbeatRequest({required super.uuid});
  @override String get type => 'heartbeat';
  @override Map<String, dynamic> toJson() => {'type': type, 'uuid': uuid};
}

// ---------- Response -----------------------------------------------------

sealed class Response {
  const Response({required this.uuid});
  final String uuid;
  String get type;

  static Response decode(String line) {
    final decoded = jsonDecode(line);
    if (decoded is! Map<String, dynamic>) {
      throw const FormatException('broker response must be an object');
    }
    final j = decoded;
    final t = j['type'];
    if (t is! String) {
      throw const FormatException('broker response type must be a string');
    }
    final u = j['uuid'] as String? ?? '';
    switch (t) {
      case 'version':
        return VersionResponse(uuid: u, brokerVersion: j['brokerVersion'] as String? ?? '', ok: j['ok'] as bool? ?? false, error: j['error'] as String?);
      case 'auth':
        return AuthResponse(uuid: u, ok: j['ok'] as bool? ?? false, error: j['error'] as String?);
      case 'lock':
        final acquired = j['acquired'] as bool? ?? false;
        final lockUuid = j['lockUuid'] as String?;
        final fencingToken = acquired ? _fence(j['fencingToken'], 'fencingToken') : null;
        if (acquired && (lockUuid == null || lockUuid.isEmpty)) {
          throw const FormatException('acquired lock omitted lockUuid');
        }
        return LockResponse(
          uuid: u,
          key: j['key'] as String? ?? '',
          acquired: acquired,
          lockRequestCount: j['lockRequestCount'] is int ? j['lockRequestCount'] as int : 0,
          lockUuid: lockUuid,
          fencingToken: fencingToken,
          readersCount: j['readersCount'] is int ? j['readersCount'] as int : null,
          error: j['error'] as String?,
        );
      case 'compositeLock':
        final keys = ((j['keys'] as List?) ?? const []).map((e) => e as String).toList();
        final acquired = j['acquired'] as bool? ?? false;
        final lockUuid = j['lockUuid'] as String?;
        if (acquired && (lockUuid == null || lockUuid.isEmpty)) {
          throw const FormatException('acquired composite lock omitted lockUuid');
        }
        final tokens = acquired ? _fenceMap(j['fencingTokens'], keys) : null;
        return CompositeLockResponse(uuid: u, keys: keys, acquired: acquired, lockUuid: lockUuid, fencingTokens: tokens, error: j['error'] as String?);
      case 'unlock':
        return UnlockResponse(
          uuid: u,
          keys: ((j['keys'] as List?) ?? const []).map((e) => e as String).toList(),
          unlocked: j['unlocked'] as bool? ?? false,
          lockRequestCount: j['lockRequestCount'] is int ? j['lockRequestCount'] as int : 0,
          error: j['error'] as String?,
        );
      case 'registerReadResult':
        final granted = j['granted'] as bool? ?? false;
        final lockUuid = j['lockUuid'] as String?;
        if (granted && (lockUuid == null || lockUuid.isEmpty)) {
          throw const FormatException('granted read lock omitted lockUuid');
        }
        return RegisterReadResultResponse(
          uuid: u,
          key: j['key'] as String? ?? '',
          readersCount: j['readersCount'] is int ? j['readersCount'] as int : 0,
          writerFlag: j['writerFlag'] as bool? ?? false,
          granted: granted,
          lockUuid: lockUuid,
          fencingToken: granted ? _fence(j['fencingToken'], 'fencingToken') : null,
        );
      case 'registerWriteResult':
        final granted = j['granted'] as bool? ?? false;
        final lockUuid = j['lockUuid'] as String?;
        if (granted && (lockUuid == null || lockUuid.isEmpty)) {
          throw const FormatException('granted write lock omitted lockUuid');
        }
        return RegisterWriteResultResponse(
          uuid: u,
          key: j['key'] as String? ?? '',
          readersCount: j['readersCount'] is int ? j['readersCount'] as int : 0,
          writerFlag: j['writerFlag'] as bool? ?? false,
          granted: granted,
          lockUuid: lockUuid,
          fencingToken: granted ? _fence(j['fencingToken'], 'fencingToken') : null,
        );
      case 'endReadResult':
        return EndReadResultResponse(uuid: u, key: j['key'] as String? ?? '', readersCount: j['readersCount'] is int ? j['readersCount'] as int : 0);
      case 'endWriteResult':
        return EndWriteResultResponse(uuid: u, key: j['key'] as String? ?? '', readersCount: j['readersCount'] is int ? j['readersCount'] as int : 0, writerFlag: j['writerFlag'] as bool? ?? false);
      case 'lockInfo':
        return LockInfoResponse(
          uuid: u,
          key: j['key'] as String? ?? '',
          isLocked: j['isLocked'] as bool? ?? false,
          lockholderUuids: ((j['lockholderUuids'] as List?) ?? const []).map((e) => e as String).toList(),
          lockRequestCount: j['lockRequestCount'] is int ? j['lockRequestCount'] as int : 0,
          readersCount: j['readersCount'] is int ? j['readersCount'] as int : 0,
          writerFlag: j['writerFlag'] as bool? ?? false,
        );
      case 'lsResult':
        return LsResultResponse(uuid: u, keys: ((j['keys'] as List?) ?? const []).map((e) => e as String).toList());
      case 'reelection':
        return ReelectionResponse(uuid: u, key: j['key'] as String? ?? '');
      case 'error':
        return ErrorResponse(uuid: u, error: j['error'] as String? ?? 'unknown');
      case 'ok':
        return OkResponse(uuid: u);
      default:
        throw FormatException('unknown response type: $t');
    }
  }
}

class VersionResponse extends Response {
  const VersionResponse({required super.uuid, required this.brokerVersion, required this.ok, this.error});
  final String brokerVersion;
  final bool ok;
  final String? error;
  @override String get type => 'version';
}

class AuthResponse extends Response {
  const AuthResponse({required super.uuid, required this.ok, this.error});
  final bool ok;
  final String? error;
  @override String get type => 'auth';
}

class LockResponse extends Response {
  const LockResponse({required super.uuid, required this.key, required this.acquired, required this.lockRequestCount, this.lockUuid, this.fencingToken, this.readersCount, this.error});
  final String key;
  final bool acquired;
  final int lockRequestCount;
  final String? lockUuid;
  final int? fencingToken;
  final int? readersCount;
  final String? error;
  @override String get type => 'lock';
}

class CompositeLockResponse extends Response {
  const CompositeLockResponse({required super.uuid, required this.keys, required this.acquired, this.lockUuid, this.fencingTokens, this.error});
  final List<String> keys;
  final bool acquired;
  final String? lockUuid;
  final Map<String, int>? fencingTokens;
  final String? error;
  @override String get type => 'compositeLock';
}

class UnlockResponse extends Response {
  const UnlockResponse({required super.uuid, required this.keys, required this.unlocked, required this.lockRequestCount, this.error});
  final List<String> keys;
  final bool unlocked;
  final int lockRequestCount;
  final String? error;
  @override String get type => 'unlock';
}

class RegisterReadResultResponse extends Response {
  const RegisterReadResultResponse({required super.uuid, required this.key, required this.readersCount, required this.writerFlag, required this.granted, this.lockUuid, this.fencingToken});
  final String key;
  final int readersCount;
  final bool writerFlag;
  final bool granted;
  final String? lockUuid;
  final int? fencingToken;
  @override String get type => 'registerReadResult';
}

class RegisterWriteResultResponse extends Response {
  const RegisterWriteResultResponse({required super.uuid, required this.key, required this.readersCount, required this.writerFlag, required this.granted, this.lockUuid, this.fencingToken});
  final String key;
  final int readersCount;
  final bool writerFlag;
  final bool granted;
  final String? lockUuid;
  final int? fencingToken;
  @override String get type => 'registerWriteResult';
}

class EndReadResultResponse extends Response {
  const EndReadResultResponse({required super.uuid, required this.key, required this.readersCount});
  final String key;
  final int readersCount;
  @override String get type => 'endReadResult';
}

class EndWriteResultResponse extends Response {
  const EndWriteResultResponse({required super.uuid, required this.key, required this.readersCount, required this.writerFlag});
  final String key;
  final int readersCount;
  final bool writerFlag;
  @override String get type => 'endWriteResult';
}

class LockInfoResponse extends Response {
  const LockInfoResponse({required super.uuid, required this.key, required this.isLocked, required this.lockholderUuids, required this.lockRequestCount, required this.readersCount, required this.writerFlag});
  final String key;
  final bool isLocked;
  final List<String> lockholderUuids;
  final int lockRequestCount;
  final int readersCount;
  final bool writerFlag;
  @override String get type => 'lockInfo';
}

class LsResultResponse extends Response {
  const LsResultResponse({required super.uuid, required this.keys});
  final List<String> keys;
  @override String get type => 'lsResult';
}

class ReelectionResponse extends Response {
  const ReelectionResponse({required super.uuid, required this.key});
  final String key;
  @override String get type => 'reelection';
}

class ErrorResponse extends Response {
  const ErrorResponse({required super.uuid, required this.error});
  final String error;
  @override String get type => 'error';
}

class OkResponse extends Response {
  const OkResponse({required super.uuid});
  @override String get type => 'ok';
}
