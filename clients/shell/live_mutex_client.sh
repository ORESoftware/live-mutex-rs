#!/usr/bin/env bash
# live_mutex_client.sh — Bash client for the dd-rust-network-mutex broker.
#
# Speaks the newline-delimited JSON wire protocol that is the single source of
# truth in ../../PROTOCOL.md (generated from ../../src/protocol.rs). Like the
# other language clients (Python/Go/Rust/…), every wire `type` value lives in a
# named constant below instead of being sprinkled around as a magic string, so
# adding a broker variant means adding a constant here too.
#
# Transport is Bash's built-in /dev/tcp, so the only dependency is bash itself
# (3.2+, compiled with net redirections — the default on macOS and Linux).
# zsh/sh users run the scripts directly: the shebang selects bash. Source this
# file and call the lmx_* functions; see smoke.sh for an end-to-end example.

# shellcheck disable=SC2034

readonly LMX_REQ_VERSION="version"
readonly LMX_REQ_AUTH="auth"
readonly LMX_REQ_LOCK="lock"
readonly LMX_REQ_UNLOCK="unlock"
readonly LMX_REQ_REGISTER_READ="registerRead"
readonly LMX_REQ_REGISTER_WRITE="registerWrite"
readonly LMX_REQ_END_READ="endRead"
readonly LMX_REQ_END_WRITE="endWrite"
readonly LMX_REQ_LOCK_INFO="lockInfo"
readonly LMX_REQ_LS="ls"
readonly LMX_REQ_HEARTBEAT="heartbeat"

readonly LMX_RES_VERSION="version"
readonly LMX_RES_AUTH="auth"
readonly LMX_RES_LOCK="lock"
readonly LMX_RES_COMPOSITE_LOCK="compositeLock"
readonly LMX_RES_UNLOCK="unlock"
readonly LMX_RES_REGISTER_READ_RESULT="registerReadResult"
readonly LMX_RES_REGISTER_WRITE_RESULT="registerWriteResult"
readonly LMX_RES_END_READ_RESULT="endReadResult"
readonly LMX_RES_END_WRITE_RESULT="endWriteResult"
readonly LMX_RES_LOCK_INFO="lockInfo"
readonly LMX_RES_LS_RESULT="lsResult"
readonly LMX_RES_REELECTION="reelection"
readonly LMX_RES_ERROR="error"
readonly LMX_RES_OK="ok"
readonly LMX_MAX_FENCING_TOKEN="9007199254740991"

: "${LMX_TIMEOUT:=30}"

LMX_REPLY=""
LMX_ERROR=""
LMX_LOCK_UUID=""
LMX_FENCE=""
LMX_FENCES=""
LMX_KEYS=""

lmx_uuid() {
  if command -v uuidgen >/dev/null 2>&1; then
    uuidgen | tr '[:upper:]' '[:lower:]'
  elif [ -r /proc/sys/kernel/random/uuid ]; then
    cat /proc/sys/kernel/random/uuid
  else
    printf '%s-%s-%s-%s' "$RANDOM$RANDOM" "$RANDOM" "$$" "$(date +%s)"
  fi
}

lmx_json_escape() {
  local s=$1
  s=${s//\\/\\\\}
  s=${s//\"/\\\"}
  s=${s//$'\n'/\\n}
  s=${s//$'\r'/\\r}
  s=${s//$'\t'/\\t}
  printf '%s' "$s"
}

lmx_json_str() {
  sed -n "s/.*\"$1\":\"\([^\"]*\)\".*/\1/p"
}

lmx_json_num() {
  sed -n "s/.*\"$1\":\([0-9][0-9]*\).*/\1/p"
}

lmx_json_array() {
  local out="" k
  for k in "$@"; do
    out="$out,\"$(lmx_json_escape "$k")\""
  done
  printf '[%s]' "${out:1}"
}

_lmx_valid_fencing_token() {
  local token="$1"
  if [[ ! "$token" =~ ^[0-9]+$ ]]; then
    return 1
  fi
  if (( token < 1 || token > LMX_MAX_FENCING_TOKEN )); then
    return 1
  fi
  return 0
}

_lmx_validate_single_authority() {
  local expected_key="$1" context="$2" returned_key token lock_uuid
  returned_key="$(lmx_json_str key <<<"$LMX_REPLY")"
  lock_uuid="$(lmx_json_str lockUuid <<<"$LMX_REPLY")"
  token="$(lmx_json_num fencingToken <<<"$LMX_REPLY")"

  if [ -z "$returned_key" ] || [ "$returned_key" != "$expected_key" ]; then
    LMX_ERROR="$context: successful grant returned unexpected/missing key"
    return 1
  fi
  if [ -z "$lock_uuid" ]; then
    LMX_ERROR="$context: successful grant omitted lockUuid"
    return 1
  fi
  if ! _lmx_valid_fencing_token "$token"; then
    LMX_ERROR="$context: successful grant omitted valid fencingToken"
    return 1
  fi

  LMX_LOCK_UUID="$lock_uuid"
  LMX_FENCE="$token"
  return 0
}

_lmx_validate_composite_authority() {
  local context="$1"
  shift
  local expected_keys=("$@") lock_uuid raw_fences token count key escaped

  if [ "${#expected_keys[@]}" -lt 1 ] || [ "${#expected_keys[@]}" -gt 5 ]; then
    LMX_ERROR="$context: composite grant key count outside 1..=5"
    return 1
  fi

  lock_uuid="$(lmx_json_str lockUuid <<<"$LMX_REPLY")"
  raw_fences="$(sed -n 's/.*\("fencingTokens":{[^}]*}\).*/\1/p' <<<"$LMX_REPLY")"
  if [ -z "$lock_uuid" ] || [ -z "$raw_fences" ]; then
    LMX_ERROR="$context: successful composite grant omitted authority"
    return 1
  fi

  for key in "${expected_keys[@]}"; do
    escaped="$(lmx_json_escape "$key")"
    if ! grep -Fq "\"$escaped\":" <<<"$raw_fences"; then
      LMX_ERROR="$context: fencing map omitted key $key"
      return 1
    fi
  done

  count=0
  while IFS= read -r token; do
    if [ -z "$token" ]; then
      continue
    fi
    if ! _lmx_valid_fencing_token "$token"; then
      LMX_ERROR="$context: composite grant contained invalid fencing token"
      return 1
    fi
    count=$((count + 1))
  done < <(grep -o ':[0-9][0-9]*' <<<"$raw_fences" | cut -c2-)

  if [ "$count" -ne "${#expected_keys[@]}" ]; then
    LMX_ERROR="$context: fencing-token map cardinality mismatch"
    return 1
  fi

  LMX_LOCK_UUID="$lock_uuid"
  LMX_FENCES="$raw_fences"
  return 0
}

lmx_connect() {
  local host="${1:-127.0.0.1}" port="${2:-6970}" token="${3:-}"
  exec 3<>"/dev/tcp/${host}/${port}" || {
    LMX_ERROR="connect ${host}:${port} failed"
    return 1
  }
  _lmx_after_connect "$token"
}

lmx_connect_uds() {
  local path="$1" token="${2:-}"
  exec 3<>"$path" || {
    LMX_ERROR="connect ${path} failed"
    return 1
  }
  _lmx_after_connect "$token"
}

_lmx_after_connect() {
  local token="$1"
  if [ -z "$token" ]; then
    return 0
  fi
  local uuid
  uuid="$(lmx_uuid)"
  _lmx_send "$(printf '{"type":"%s","uuid":"%s","token":"%s"}' \
    "$LMX_REQ_AUTH" "$uuid" "$(lmx_json_escape "$token")")"
  if ! _lmx_read_reply "$uuid"; then
    LMX_ERROR="auth: no reply"
    return 1
  fi
  case "$LMX_REPLY" in
    *'"ok":true'*) return 0 ;;
    *) LMX_ERROR="auth rejected: $LMX_REPLY"; return 1 ;;
  esac
}

lmx_disconnect() {
  exec 3>&- 2>/dev/null
  exec 3<&- 2>/dev/null
  return 0
}

_lmx_send() {
  printf '%s\n' "$1" >&3
}

_lmx_read_reply() {
  local want="$1" line
  while IFS= read -r -t "$LMX_TIMEOUT" line <&3; do
    if [ -z "$line" ]; then
      continue
    fi
    case "$line" in
      *"\"uuid\":\"$want\""*) LMX_REPLY="$line"; return 0 ;;
    esac
  done
  return 1
}

_lmx_read_grant() {
  local want="$1" line
  while IFS= read -r -t "$LMX_TIMEOUT" line <&3; do
    if [ -z "$line" ]; then
      continue
    fi
    case "$line" in
      *"\"uuid\":\"$want\""*) ;;
      *) continue ;;
    esac
    case "$line" in
      *'"acquired":true'*) LMX_REPLY="$line"; return 0 ;;
      *'"error":'*) LMX_REPLY="$line"; return 0 ;;
      *'"acquired":false'*) continue ;;
      *) LMX_REPLY="$line"; return 0 ;;
    esac
  done
  return 1
}

_lmx_read_until_granted() {
  local want="$1" line
  while IFS= read -r -t "$LMX_TIMEOUT" line <&3; do
    if [ -z "$line" ]; then
      continue
    fi
    case "$line" in
      *"\"uuid\":\"$want\""*) ;;
      *) continue ;;
    esac
    case "$line" in
      *'"granted":true'*) LMX_REPLY="$line"; return 0 ;;
      *'"error":'*) LMX_REPLY="$line"; return 1 ;;
    esac
  done
  return 1
}

lmx_acquire() {
  local key="$1" ttl="${2:-0}" max="${3:-}" uuid
  uuid="$(lmx_uuid)"
  local maxf=""
  if [ -n "$max" ]; then
    maxf=",\"max\":$max"
  fi
  _lmx_send "$(printf '{"type":"%s","uuid":"%s","key":"%s","ttl":%s,"wait":true%s}' \
    "$LMX_REQ_LOCK" "$uuid" "$(lmx_json_escape "$key")" "$ttl" "$maxf")"
  if ! _lmx_read_grant "$uuid"; then
    LMX_ERROR="acquire($key): timeout"
    return 1
  fi
  case "$LMX_REPLY" in
    *'"acquired":true'*) ;;
    *) LMX_ERROR="acquire($key): $LMX_REPLY"; return 1 ;;
  esac
  _lmx_validate_single_authority "$key" "acquire($key)"
}

lmx_try_acquire() {
  local key="$1" ttl="${2:-0}" max="${3:-}" uuid
  uuid="$(lmx_uuid)"
  local maxf=""
  if [ -n "$max" ]; then
    maxf=",\"max\":$max"
  fi
  _lmx_send "$(printf '{"type":"%s","uuid":"%s","key":"%s","ttl":%s,"wait":false%s}' \
    "$LMX_REQ_LOCK" "$uuid" "$(lmx_json_escape "$key")" "$ttl" "$maxf")"
  if ! _lmx_read_reply "$uuid"; then
    LMX_ERROR="try_acquire($key): timeout"
    return 1
  fi
  case "$LMX_REPLY" in
    *'"error":'*) LMX_ERROR="try_acquire($key): $LMX_REPLY"; return 1 ;;
    *'"acquired":true'*) _lmx_validate_single_authority "$key" "try_acquire($key)"; return $? ;;
    *) return 2 ;;
  esac
}

lmx_release() {
  local key="$1" lock="$2" uuid
  uuid="$(lmx_uuid)"
  _lmx_send "$(printf '{"type":"%s","uuid":"%s","key":"%s","lockUuid":"%s"}' \
    "$LMX_REQ_UNLOCK" "$uuid" "$(lmx_json_escape "$key")" "$(lmx_json_escape "$lock")")"
  if ! _lmx_read_reply "$uuid"; then
    LMX_ERROR="release($key): timeout"
    return 1
  fi
  case "$LMX_REPLY" in
    *'"unlocked":true'*) return 0 ;;
    *) LMX_ERROR="release($key): $LMX_REPLY"; return 1 ;;
  esac
}

lmx_force_unlock() {
  local key="$1" uuid
  uuid="$(lmx_uuid)"
  _lmx_send "$(printf '{"type":"%s","uuid":"%s","key":"%s","force":true}' \
    "$LMX_REQ_UNLOCK" "$uuid" "$(lmx_json_escape "$key")")"
  if ! _lmx_read_reply "$uuid"; then
    LMX_ERROR="force_unlock($key): timeout"
    return 1
  fi
  case "$LMX_REPLY" in
    *'"error":'*) LMX_ERROR="force_unlock($key): $LMX_REPLY"; return 1 ;;
    *) return 0 ;;
  esac
}

lmx_acquire_many() {
  local ttl="$1"
  shift
  if [ "${1:-}" = "--" ]; then
    shift
  fi
  local keys=("$@") uuid
  if [ "${#keys[@]}" -lt 1 ] || [ "${#keys[@]}" -gt 5 ]; then
    LMX_ERROR="acquire_many requires 1..=5 keys"
    return 1
  fi
  uuid="$(lmx_uuid)"
  _lmx_send "$(printf '{"type":"%s","uuid":"%s","keys":%s,"ttl":%s,"wait":true}' \
    "$LMX_REQ_LOCK" "$uuid" "$(lmx_json_array "${keys[@]}")" "$ttl")"
  if ! _lmx_read_grant "$uuid"; then
    LMX_ERROR="acquire_many: timeout"
    return 1
  fi
  case "$LMX_REPLY" in
    *'"acquired":true'*) ;;
    *) LMX_ERROR="acquire_many: $LMX_REPLY"; return 1 ;;
  esac
  _lmx_validate_composite_authority "acquire_many" "${keys[@]}"
}

lmx_release_many() {
  local lock="$1"
  shift
  if [ "${1:-}" = "--" ]; then
    shift
  fi
  local uuid
  uuid="$(lmx_uuid)"
  _lmx_send "$(printf '{"type":"%s","uuid":"%s","keys":%s,"lockUuid":"%s"}' \
    "$LMX_REQ_UNLOCK" "$uuid" "$(lmx_json_array "$@")" "$(lmx_json_escape "$lock")")"
  if ! _lmx_read_reply "$uuid"; then
    LMX_ERROR="release_many: timeout"
    return 1
  fi
  case "$LMX_REPLY" in
    *'"unlocked":true'*) return 0 ;;
    *) LMX_ERROR="release_many: $LMX_REPLY"; return 1 ;;
  esac
}

lmx_acquire_read() {
  local key="$1" uuid
  uuid="$(lmx_uuid)"
  _lmx_send "$(printf '{"type":"%s","uuid":"%s","key":"%s"}' \
    "$LMX_REQ_REGISTER_READ" "$uuid" "$(lmx_json_escape "$key")")"
  if ! _lmx_read_until_granted "$uuid"; then
    LMX_ERROR="acquire_read($key): not granted"
    return 1
  fi
  _lmx_validate_single_authority "$key" "acquire_read($key)"
}

lmx_acquire_write() {
  local key="$1" uuid
  uuid="$(lmx_uuid)"
  _lmx_send "$(printf '{"type":"%s","uuid":"%s","key":"%s"}' \
    "$LMX_REQ_REGISTER_WRITE" "$uuid" "$(lmx_json_escape "$key")")"
  if ! _lmx_read_until_granted "$uuid"; then
    LMX_ERROR="acquire_write($key): not granted"
    return 1
  fi
  _lmx_validate_single_authority "$key" "acquire_write($key)"
}

lmx_release_read() {
  local key="$1" uuid
  uuid="$(lmx_uuid)"
  _lmx_send "$(printf '{"type":"%s","uuid":"%s","key":"%s"}' \
    "$LMX_REQ_END_READ" "$uuid" "$(lmx_json_escape "$key")")"
  _lmx_read_reply "$uuid"
}

lmx_release_write() {
  local key="$1" uuid
  uuid="$(lmx_uuid)"
  _lmx_send "$(printf '{"type":"%s","uuid":"%s","key":"%s"}' \
    "$LMX_REQ_END_WRITE" "$uuid" "$(lmx_json_escape "$key")")"
  _lmx_read_reply "$uuid"
}

lmx_ls() {
  local uuid
  uuid="$(lmx_uuid)"
  _lmx_send "$(printf '{"type":"%s","uuid":"%s"}' "$LMX_REQ_LS" "$uuid")"
  if ! _lmx_read_reply "$uuid"; then
    LMX_ERROR="ls: timeout"
    return 1
  fi
  LMX_KEYS="$(sed -n 's/.*\("keys":\[[^]]*\]\).*/\1/p' <<<"$LMX_REPLY")"
}

lmx_lock_info() {
  local key="$1" uuid
  uuid="$(lmx_uuid)"
  _lmx_send "$(printf '{"type":"%s","uuid":"%s","key":"%s"}' \
    "$LMX_REQ_LOCK_INFO" "$uuid" "$(lmx_json_escape "$key")")"
  if ! _lmx_read_reply "$uuid"; then
    LMX_ERROR="lock_info($key): timeout"
    return 1
  fi
}

lmx_heartbeat() {
  local uuid
  uuid="$(lmx_uuid)"
  _lmx_send "$(printf '{"type":"%s","uuid":"%s"}' "$LMX_REQ_HEARTBEAT" "$uuid")"
}
