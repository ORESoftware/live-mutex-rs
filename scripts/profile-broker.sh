#!/usr/bin/env bash
set -euo pipefail

mode="${1:-sample}"
profile="${PROFILE:-profiling}"
out_dir="${OUT_DIR:-target/profiles}"
profile_target="${PROFILE_TARGET:-broker}"
broker_host="${BROKER_HOST:-127.0.0.1}"
broker_http_port="${BROKER_HTTP_PORT:-6971}"
raft_http_base_port="${RAFT_HTTP_BASE_PORT:-6972}"
raft_rpc_base_port="${RAFT_RPC_BASE_PORT:-7980}"
raft_data_root="${RAFT_DATA_ROOT:-$out_dir/raft-profile-cluster}"
raft_sync_log="${RAFT_SYNC_LOG:-true}"
bench_workers="${BENCH_WORKERS:-8}"
bench_keys="${BENCH_KEYS:-256}"
bench_duration_ms="${BENCH_DURATION_MS:-10000}"
bench_ttl_ms="${BENCH_TTL_MS:-5000}"
bench_io_timeout_ms="${BENCH_IO_TIMEOUT_MS:-5000}"
sample_seconds="${SAMPLE_SECONDS:-8}"
sample_interval_ms="${SAMPLE_INTERVAL_MS:-1}"
perf_freq="${PERF_FREQ:-997}"

mkdir -p "$out_dir"

export RUSTFLAGS="${RUSTFLAGS:-} -C force-frame-pointers=yes"

server_pids=()
profile_pid=""
bench_pid=""

cleanup() {
  if [ -n "$bench_pid" ]; then
    kill "$bench_pid" >/dev/null 2>&1 || true
    wait "$bench_pid" >/dev/null 2>&1 || true
  fi
  for pid in "${server_pids[@]}"; do
    kill "$pid" >/dev/null 2>&1 || true
  done
  for pid in "${server_pids[@]}"; do
    wait "$pid" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

usage() {
  echo "usage: $0 [sample|perf|flamegraph]" >&2
  echo "env: PROFILE_TARGET=$profile_target PROFILE=$profile OUT_DIR=$out_dir BROKER_HOST=$broker_host BROKER_HTTP_PORT=$broker_http_port" >&2
  echo "env: RAFT_HTTP_BASE_PORT=$raft_http_base_port RAFT_RPC_BASE_PORT=$raft_rpc_base_port RAFT_SYNC_LOG=$raft_sync_log" >&2
  echo "env: BENCH_WORKERS=$bench_workers BENCH_KEYS=$bench_keys BENCH_DURATION_MS=$bench_duration_ms" >&2
}

case "$profile_target" in
  broker | raft) ;;
  *)
    echo "PROFILE_TARGET must be broker or raft; got $profile_target" >&2
    usage
    exit 2
    ;;
esac

case "$raft_sync_log" in
  true | false) ;;
  1) raft_sync_log=true ;;
  0) raft_sync_log=false ;;
  *)
    echo "RAFT_SYNC_LOG must be true or false; got $raft_sync_log" >&2
    exit 2
    ;;
esac

case "$mode" in
  sample)
    if ! command -v sample >/dev/null 2>&1; then
      echo "Apple sample(1) is not available on this host" >&2
      exit 1
    fi
    ;;
  perf)
    if ! command -v perf >/dev/null 2>&1; then
      echo "Linux perf is not available on this host" >&2
      exit 1
    fi
    ;;
  flamegraph)
    if ! command -v flamegraph >/dev/null 2>&1; then
      echo "flamegraph is not installed. Install with: cargo install flamegraph" >&2
      exit 1
    fi
    if ! command -v timeout >/dev/null 2>&1; then
      echo "flamegraph PID profiling needs timeout(1) to stop capture after SAMPLE_SECONDS" >&2
      exit 1
    fi
    ;;
  *)
    usage
    exit 2
    ;;
esac

port_open() {
  local host="${1:-$broker_host}"
  local port="${2:-$broker_http_port}"
  (exec 3<>"/dev/tcp/$host/$port") >/dev/null 2>&1
}

wait_for_port() {
  local host="$1"
  local port="$2"
  local pid="$3"
  for _ in $(seq 1 100); do
    if port_open "$host" "$port"; then
      return 0
    fi
    if [ -n "$pid" ] && ! kill -0 "$pid" >/dev/null 2>&1; then
      echo "server exited before opening $host:$port" >&2
      return 1
    fi
    sleep 0.05
  done
  echo "timed out waiting for server on $host:$port" >&2
  return 1
}

raft_leader_ready() {
  local port="$1"
  local status=""
  exec 3<>"/dev/tcp/$broker_host/$port" || return 1
  printf 'GET /raft/leaderz HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n' "$broker_host" >&3
  IFS= read -r status <&3 || true
  exec 3<&-
  exec 3>&-
  case "$status" in
    *" 200 "*) return 0 ;;
    *) return 1 ;;
  esac
}

wait_for_raft_leader() {
  for _ in $(seq 1 200); do
    local idx=0
    for port in "${raft_http_ports[@]}"; do
      if raft_leader_ready "$port"; then
        profile_pid="${server_pids[$idx]}"
        raft_leader_http_port="$port"
        return 0
      fi
      idx=$((idx + 1))
    done
    for pid in "${server_pids[@]}"; do
      if ! kill -0 "$pid" >/dev/null 2>&1; then
        echo "BrokerRaft node pid $pid exited before a ready leader was elected" >&2
        return 1
      fi
    done
    sleep 0.05
  done
  echo "timed out waiting for a ready BrokerRaft leader" >&2
  return 1
}

build_binaries() {
  cargo build --no-default-features --profile "$profile" \
    --bin dd-rust-network-mutex \
    --example redis_vs_raft_bench >/dev/null
}

start_broker() {
  if port_open "$broker_host" "$broker_http_port"; then
    echo "$broker_host:$broker_http_port is already in use" >&2
    exit 1
  fi
  server_log="$out_dir/broker-server.log"
  rm -f "$server_log"
  LMX_DISABLE_TCP="${LMX_DISABLE_TCP:-true}" \
    LMX_DISABLE_HTTP="${LMX_DISABLE_HTTP:-false}" \
    LMX_BIND_HOST="$broker_host" \
    LMX_HTTP_PORT="$broker_http_port" \
    LMX_TTL_SWEEP_INTERVAL_MS="${LMX_TTL_SWEEP_INTERVAL_MS:-0}" \
    "target/$profile/dd-rust-network-mutex" >"$server_log" 2>&1 &
  profile_pid=$!
  server_pids+=("$profile_pid")
  wait_for_port "$broker_host" "$broker_http_port" "$profile_pid"
}

write_raft_config() {
  local config_path="$1"
  local node_id="$2"
  local http_port="$3"
  local rpc_port="$4"
  local data_dir="$5"

  cat >"$config_path" <<EOF
[server]
bind_host = "$broker_host"
tcp_port = 0
http_port = $http_port
disable_tcp = true
disable_http = false
tcp_nodelay = true
tcp_quickack = true

[broker]
default_ttl_ms = 4000
max_lock_holders = 1
max_concurrency_cap = 1000
ttl_sweep_interval_ms = 0
idle_key_grace_ms = 60000

[raft]
enabled = true
node_id = "$node_id"
bind_addr = "$broker_host:$rpc_port"
advertise_addr = "$broker_host:$rpc_port"
data_dir = "$data_dir"
heartbeat_interval_ms = 50
election_timeout_min_ms = 150
election_timeout_max_ms = 300
snapshot_interval_ms = 1800000
snapshot_max_log_entries = 100000
snapshot_max_log_bytes = 67108864
trailing_log_entries = 10000
append_entries_max_entries = 256
append_entries_max_bytes = 1048576
install_snapshot_chunk_bytes = 1048576
install_snapshot_max_staged_bytes = 134217728
install_snapshot_max_staged_transfers = 4
install_snapshot_stale_transfer_ms = 1800000
client_batch_max_entries = 32
client_pipeline_max_batches = 4
client_batch_max_pending = 8192
client_batch_max_delay_ms = 1
client_response_cache_max_entries = 8192
sync_log = $raft_sync_log

[[raft.peers]]
id = "node-1"
addr = "$broker_host:$((raft_rpc_base_port + 0))"

[[raft.peers]]
id = "node-2"
addr = "$broker_host:$((raft_rpc_base_port + 1))"

[[raft.peers]]
id = "node-3"
addr = "$broker_host:$((raft_rpc_base_port + 2))"
EOF
}

start_raft_cluster() {
  rm -rf "$raft_data_root"
  mkdir -p "$raft_data_root"
  raft_http_ports=()
  raft_leader_http_port=""

  for offset in 0 1 2; do
    local http_port=$((raft_http_base_port + offset))
    local rpc_port=$((raft_rpc_base_port + offset))
    if port_open "$broker_host" "$http_port"; then
      echo "$broker_host:$http_port is already in use" >&2
      exit 1
    fi
    if port_open "$broker_host" "$rpc_port"; then
      echo "$broker_host:$rpc_port is already in use" >&2
      exit 1
    fi
  done

  for offset in 0 1 2; do
    local node_num=$((offset + 1))
    local node_id="node-$node_num"
    local http_port=$((raft_http_base_port + offset))
    local rpc_port=$((raft_rpc_base_port + offset))
    local node_dir="$raft_data_root/$node_id"
    local config_path="$raft_data_root/$node_id.toml"
    local server_log="$out_dir/raft-$node_id-server.log"
    mkdir -p "$node_dir"
    write_raft_config "$config_path" "$node_id" "$http_port" "$rpc_port" "$node_dir"
    rm -f "$server_log"
    LMX_CONFIG="$config_path" \
      "target/$profile/dd-rust-network-mutex" >"$server_log" 2>&1 &
    local pid=$!
    server_pids+=("$pid")
    raft_http_ports+=("$http_port")
  done

  local idx=0
  for port in "${raft_http_ports[@]}"; do
    wait_for_port "$broker_host" "$port" "${server_pids[$idx]}"
    idx=$((idx + 1))
  done
  wait_for_raft_leader
}

start_benchmark() {
  bench_log="$out_dir/$profile_target-bench.out"
  rm -f "$bench_log"
  if [ "$profile_target" = "raft" ]; then
    BENCH_TARGET=raft \
      BENCH_RAFT="$broker_host:$raft_leader_http_port" \
      BENCH_WORKERS="$bench_workers" \
      BENCH_KEYS="$bench_keys" \
      BENCH_DURATION_MS="$bench_duration_ms" \
      BENCH_TTL_MS="$bench_ttl_ms" \
      BENCH_IO_TIMEOUT_MS="$bench_io_timeout_ms" \
      "target/$profile/examples/redis_vs_raft_bench" >"$bench_log" 2>&1 &
  else
    BENCH_TARGET=broker \
      BENCH_BROKER="$broker_host:$broker_http_port" \
      BENCH_WORKERS="$bench_workers" \
      BENCH_KEYS="$bench_keys" \
      BENCH_DURATION_MS="$bench_duration_ms" \
      BENCH_TTL_MS="$bench_ttl_ms" \
      BENCH_IO_TIMEOUT_MS="$bench_io_timeout_ms" \
      "target/$profile/examples/redis_vs_raft_bench" >"$bench_log" 2>&1 &
  fi
  bench_pid=$!
}

build_binaries
case "$profile_target" in
  broker)
    start_broker
    ;;
  raft)
    start_raft_cluster
    ;;
esac

case "$mode" in
  sample)
    out="$out_dir/$profile_target-server.sample.txt"
    rm -f "$out"
    start_benchmark
    sample_status=0
    sample "$profile_pid" "$sample_seconds" "$sample_interval_ms" -mayDie -file "$out" || sample_status=$?
    wait "$bench_pid" || true
    bench_pid=""
    if [ "$sample_status" -ne 0 ]; then
      echo "sample failed with status $sample_status; macOS may require sudo or full Xcode/Instruments permissions" >&2
      exit "$sample_status"
    fi
    echo "$out"
    ;;
  perf)
    out="$out_dir/$profile_target-server.perf.data"
    start_benchmark
    perf record -F "$perf_freq" -g -p "$profile_pid" -o "$out" -- sleep "$sample_seconds"
    wait "$bench_pid" || true
    bench_pid=""
    perf report -i "$out" --stdio | tee "$out.report.txt"
    echo "$out"
    ;;
  flamegraph)
    out="$out_dir/$profile_target-server.svg"
    start_benchmark
    timeout "$sample_seconds" flamegraph --pid "$profile_pid" --output "$out" || status=$?
    wait "$bench_pid" || true
    bench_pid=""
    if [ "${status:-0}" -ne 0 ] && [ "${status:-0}" -ne 124 ]; then
      exit "$status"
    fi
    echo "$out"
    ;;
esac
