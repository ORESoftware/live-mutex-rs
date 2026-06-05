#!/usr/bin/env bash
set -euo pipefail

mode="${1:-sample}"
profile="${PROFILE:-profiling}"
out_dir="${OUT_DIR:-target/profiles}"
broker_host="${BROKER_HOST:-127.0.0.1}"
broker_http_port="${BROKER_HTTP_PORT:-6971}"
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

server_pid=""
bench_pid=""

cleanup() {
  if [ -n "$bench_pid" ]; then
    kill "$bench_pid" >/dev/null 2>&1 || true
    wait "$bench_pid" >/dev/null 2>&1 || true
  fi
  if [ -n "$server_pid" ]; then
    kill "$server_pid" >/dev/null 2>&1 || true
    wait "$server_pid" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

usage() {
  echo "usage: $0 [sample|perf|flamegraph]" >&2
  echo "env: PROFILE=$profile OUT_DIR=$out_dir BROKER_HOST=$broker_host BROKER_HTTP_PORT=$broker_http_port" >&2
  echo "env: BENCH_WORKERS=$bench_workers BENCH_KEYS=$bench_keys BENCH_DURATION_MS=$bench_duration_ms" >&2
}

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
  (exec 3<>"/dev/tcp/$broker_host/$broker_http_port") >/dev/null 2>&1
}

wait_for_broker() {
  for _ in $(seq 1 100); do
    if port_open; then
      return 0
    fi
    if [ -n "$server_pid" ] && ! kill -0 "$server_pid" >/dev/null 2>&1; then
      echo "broker server exited before opening $broker_host:$broker_http_port" >&2
      return 1
    fi
    sleep 0.05
  done
  echo "timed out waiting for broker on $broker_host:$broker_http_port" >&2
  return 1
}

build_binaries() {
  cargo build --no-default-features --profile "$profile" \
    --bin dd-rust-network-mutex \
    --example redis_vs_raft_bench >/dev/null
}

start_broker() {
  if port_open; then
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
  server_pid=$!
  wait_for_broker
}

start_benchmark() {
  bench_log="$out_dir/broker-bench.out"
  rm -f "$bench_log"
  BENCH_TARGET=broker \
    BENCH_BROKER="$broker_host:$broker_http_port" \
    BENCH_WORKERS="$bench_workers" \
    BENCH_KEYS="$bench_keys" \
    BENCH_DURATION_MS="$bench_duration_ms" \
    BENCH_TTL_MS="$bench_ttl_ms" \
    BENCH_IO_TIMEOUT_MS="$bench_io_timeout_ms" \
    "target/$profile/examples/redis_vs_raft_bench" >"$bench_log" 2>&1 &
  bench_pid=$!
}

build_binaries
start_broker

case "$mode" in
  sample)
    out="$out_dir/broker-server.sample.txt"
    rm -f "$out"
    start_benchmark
    sample_status=0
    sample "$server_pid" "$sample_seconds" "$sample_interval_ms" -mayDie -file "$out" || sample_status=$?
    wait "$bench_pid" || true
    bench_pid=""
    if [ "$sample_status" -ne 0 ]; then
      echo "sample failed with status $sample_status; macOS may require sudo or full Xcode/Instruments permissions" >&2
      exit "$sample_status"
    fi
    echo "$out"
    ;;
  perf)
    out="$out_dir/broker-server.perf.data"
    start_benchmark
    perf record -F "$perf_freq" -g -p "$server_pid" -o "$out" -- sleep "$sample_seconds"
    wait "$bench_pid" || true
    bench_pid=""
    perf report -i "$out" --stdio | tee "$out.report.txt"
    echo "$out"
    ;;
  flamegraph)
    out="$out_dir/broker-server.svg"
    start_benchmark
    timeout "$sample_seconds" flamegraph --pid "$server_pid" --output "$out" || status=$?
    wait "$bench_pid" || true
    bench_pid=""
    if [ "${status:-0}" -ne 0 ] && [ "${status:-0}" -ne 124 ]; then
      exit "$status"
    fi
    echo "$out"
    ;;
esac
