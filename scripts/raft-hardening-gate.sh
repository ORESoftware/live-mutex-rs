#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"

mode="${LMX_RAFT_GATE_MODE:-full}"
profile="${PROFILE:-profiling}"
bench_workers="${BENCH_WORKERS:-4}"
bench_keys="${BENCH_KEYS:-128}"
bench_duration_ms="${BENCH_DURATION_MS:-3000}"
run_bench="${RUN_BENCH:-true}"
run_clippy="${RUN_CLIPPY:-true}"
run_secret_scan="${RUN_SECRET_SCAN:-true}"
run_redis_bench="${RUN_REDIS_BENCH:-false}"
run_k8s_raft_live="${RUN_K8S_RAFT_LIVE:-false}"
run_k8s_raft_live_require_metrics="${RUN_K8S_RAFT_LIVE_REQUIRE_METRICS:-true}"
run_raft_version_skew="${RUN_RAFT_VERSION_SKEW:-false}"
test_threads="${TEST_THREADS:-1}"
redis_host="${REDIS_HOST:-127.0.0.1}"
redis_port="${REDIS_PORT:-17379}"

usage() {
  cat >&2 <<EOF
usage: LMX_RAFT_GATE_MODE=quick|full RUN_BENCH=true|false $0

quick: formatting, benchmark-example tests, focused BrokerRaft hardening tests, raft_cluster
full:  quick checks plus full no-default-features lib tests and local Broker/BrokerRaft bench evidence

env:
  PROFILE=$profile
  BENCH_WORKERS=$bench_workers
  BENCH_KEYS=$bench_keys
  BENCH_DURATION_MS=$bench_duration_ms
  RUN_BENCH=$run_bench
  RUN_CLIPPY=$run_clippy
  RUN_SECRET_SCAN=$run_secret_scan
  RUN_REDIS_BENCH=$run_redis_bench
  RUN_K8S_RAFT_LIVE=$run_k8s_raft_live
  RUN_K8S_RAFT_LIVE_REQUIRE_METRICS=$run_k8s_raft_live_require_metrics
  RUN_RAFT_VERSION_SKEW=$run_raft_version_skew
  TEST_THREADS=$test_threads
  REDIS_HOST=$redis_host
  REDIS_PORT=$redis_port
  BENCH_FAIL_ON_ZERO_SUCCESS=true|false
  BENCH_MIN_REDIS_OPS_PER_SEC=<optional>
  BENCH_MIN_BROKER_OPS_PER_SEC=<optional>
  BENCH_MIN_RAFT_OPS_PER_SEC=<optional>
  BENCH_MAX_REDIS_P99_MS=<optional>
  BENCH_MAX_BROKER_P99_MS=<optional>
  BENCH_MAX_RAFT_P99_MS=<optional>
EOF
}

case "$mode" in
  quick | full) ;;
  *)
    usage
    exit 2
    ;;
esac

case "$run_bench" in
  true | false) ;;
  1) run_bench=true ;;
  0) run_bench=false ;;
  *)
    echo "RUN_BENCH must be true or false; got $run_bench" >&2
    exit 2
    ;;
esac

case "$run_clippy" in
  true | false) ;;
  1) run_clippy=true ;;
  0) run_clippy=false ;;
  *)
    echo "RUN_CLIPPY must be true or false; got $run_clippy" >&2
    exit 2
    ;;
esac

case "$run_secret_scan" in
  true | false) ;;
  1) run_secret_scan=true ;;
  0) run_secret_scan=false ;;
  *)
    echo "RUN_SECRET_SCAN must be true or false; got $run_secret_scan" >&2
    exit 2
    ;;
esac

case "$run_redis_bench" in
  true | false) ;;
  1) run_redis_bench=true ;;
  0) run_redis_bench=false ;;
  *)
    echo "RUN_REDIS_BENCH must be true or false; got $run_redis_bench" >&2
    exit 2
    ;;
esac

if ! [[ "$redis_port" =~ ^[1-9][0-9]*$ ]]; then
  echo "REDIS_PORT must be a positive integer; got $redis_port" >&2
  exit 2
fi

case "$run_k8s_raft_live" in
  true | false) ;;
  1) run_k8s_raft_live=true ;;
  0) run_k8s_raft_live=false ;;
  *)
    echo "RUN_K8S_RAFT_LIVE must be true or false; got $run_k8s_raft_live" >&2
    exit 2
    ;;
esac

case "$run_k8s_raft_live_require_metrics" in
  true | false) ;;
  1) run_k8s_raft_live_require_metrics=true ;;
  0) run_k8s_raft_live_require_metrics=false ;;
  *)
    echo "RUN_K8S_RAFT_LIVE_REQUIRE_METRICS must be true or false; got $run_k8s_raft_live_require_metrics" >&2
    exit 2
    ;;
esac

case "$run_raft_version_skew" in
  true | false) ;;
  1) run_raft_version_skew=true ;;
  0) run_raft_version_skew=false ;;
  *)
    echo "RUN_RAFT_VERSION_SKEW must be true or false; got $run_raft_version_skew" >&2
    exit 2
    ;;
esac

run() {
  printf '\n==> %s\n' "$*" >&2
  "$@"
}

require_live_metrics_endpoints_for_k8s_gate() {
  if [ "$run_k8s_raft_live" != true ] || [ "$run_k8s_raft_live_require_metrics" != true ]; then
    return
  fi
  if ! printf '%s' "${LMX_LIVE_RAFT_METRICS_ENDPOINTS:-}" | rg -q '[^[:space:],]'; then
    cat >&2 <<EOF
RUN_K8S_RAFT_LIVE=true requires LMX_LIVE_RAFT_METRICS_ENDPOINTS by default so the live gate guards every Raft pod against full-log hot-path fallback.
Set LMX_LIVE_RAFT_METRICS_ENDPOINTS to one stable HTTP endpoint per expected Raft node, or set RUN_K8S_RAFT_LIVE_REQUIRE_METRICS=false for a behavior-only live smoke.
EOF
    exit 2
  fi
}

check_secret_patterns() {
  printf '\n==> repository secret pattern scan\n' >&2
  local sts_token_prefix="IQoJb3Jp"
  local sts_token_suffix="Z2luX2Vj"
  local aws_access_key_id_name="ACCESS_KEY_ID"
  local aws_secret_access_key_name="SECRET_ACCESS_KEY"
  local aws_session_token_name="SESSION_TOKEN"
  local aws_credential_expiration_name="CREDENTIAL_EXPIRATION"
  local pattern="ASIA[0-9A-Z]{16}|${sts_token_prefix}${sts_token_suffix}|AWS_(${aws_access_key_id_name}|${aws_secret_access_key_name}|${aws_session_token_name}|${aws_credential_expiration_name})[[:space:]]*="
  local output=""
  local status=0
  set +e
  output="$(
    rg -n --hidden \
      --glob '!target/**' \
      --glob '!.git/**' \
      --glob '!vendor/**' \
      "$pattern" \
      .
  )"
  status=$?
  set -e
  case "$status" in
    0)
      printf '%s\n' "$output" >&2
      echo "credential-like AWS patterns found in repository files" >&2
      exit 1
      ;;
    1)
      ;;
    *)
      printf '%s\n' "$output" >&2
      echo "secret pattern scan failed with status $status" >&2
      exit "$status"
      ;;
  esac
}

check_old_protocol_fixture_contract() {
  printf '\n==> old-protocol raft RPC fixture contract\n' >&2
  if grep -n "minProtocolVersion" tests/fixtures/raft/old_protocol_*.json; then
    echo "old-protocol raft RPC fixtures must not contain minProtocolVersion" >&2
    exit 1
  fi
}

run cargo fmt --check
check_old_protocol_fixture_contract
require_live_metrics_endpoints_for_k8s_gate
if [ "$run_secret_scan" = true ]; then
  check_secret_patterns
fi

if [ "$run_clippy" = true ]; then
  run cargo clippy --no-default-features --lib -- \
    -A warnings \
    -D clippy::too_many_arguments \
    -D clippy::result_large_err
fi

run cargo test --no-default-features --example redis_vs_raft_bench

run cargo test --no-default-features \
  broker_raft::tests::fuzz_append_entries_conflict_repair_converges_in_bounded_batches \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::change_membership_catches_up_new_peers_before_promotion \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::maintenance_sweeps_stale_snapshot_transfers_without_new_chunks \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::install_snapshot_stream_stops_and_counts_when_prepared_snapshot_goes_stale \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::client_request_queue_limit_rejects_before_append \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::active_quorum_admission_failure_rejects_client_write_before_append \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::concurrent_client_requests_share_one_replicated_batch \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::client_pipeline_drains_multiple_configured_batches_in_one_quorum_round \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::client_batch_refills_pipeline_after_commit_lock_wait \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::client_batch_prunes_requests_cancelled_while_waiting_for_commit_lock \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::client_batch_short_commit_index_result_releases_unapplied_identity \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::follower_proxy_saturates_oversized_wait_duration_in_rpc \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::target_replication_peer_selection_replaces_busy_joint_overlap_peer \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::target_replication_limits_simple_fanout_to_quorum_need \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::target_replication_extra_fanout_contacts_spare_but_returns_on_quorum \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::target_replication_limits_joint_fanout_to_both_quorum_needs \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::membership_replication_limits_joint_fanout_before_joint_is_active \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::target_replication_limited_fanout_rotates_after_failed_peer \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::target_replication_rotates_around_busy_selected_peer \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::no_target_fanout_catches_up_over_bounded_append_batches \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::no_target_fanout_yields_after_inline_append_batch_cap \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::post_commit_fanout_refills_after_inline_append_batch_yield \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::raft_rpc_requests_tolerate_unknown_future_fields_and_defaulted_old_fields \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::raft_rpc_responses_tolerate_unknown_future_fields_and_defaulted_old_fields \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::literal_old_protocol_raft_rpc_request_wire_fixtures_decode \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::literal_old_protocol_raft_rpc_response_wire_fixtures_decode \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::raft_rpc_serializers_advertise_current_protocol_minimum \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::raft_rpc_protocol_minimum_rejections_are_counted_and_exported \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::raft_rpc_protocol_minimum_scan_only_uses_top_level_field \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::future_protocol_pre_vote_response_does_not_start_election \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::future_protocol_request_vote_response_does_not_elect_candidate \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::future_protocol_proxy_response_does_not_return_client_success \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::future_protocol_append_response_does_not_count_as_quorum_contact \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::future_protocol_install_snapshot_response_does_not_update_progress \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::missing_peer_rpc_token_warning_only_targets_network_clusters \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::raft_rpc_stream_returns_error_for_unsupported_protocol_minimum \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::old_protocol_pre_vote_stream_accepts_missing_protocol_minimum \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::old_protocol_rpc_stream_accepts_missing_protocol_minimum \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::old_protocol_append_stream_replicates_without_protocol_minimum \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::old_protocol_install_snapshot_stream_installs_without_protocol_minimum \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::old_protocol_proxy_request_stream_reaches_leader_without_protocol_minimum \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  broker_raft::tests::future_protocol_inbound_requests_do_not_mutate_raft_state \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  sim::tests::in_memory_concurrent_partition_heal_preserves_single_key_linearizability \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  sim::tests::in_memory_partition_heal_no_wait_history_is_linearizable \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  sim::tests::in_memory_stale_leader_restart_no_wait_history_is_linearizable \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  sim::tests::in_memory_lagging_follower_catches_up_over_bounded_append_entries_without_snapshot \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  sim::tests::in_memory_lagging_follower_recovers_via_install_snapshot_after_compaction \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  sim::tests::in_memory_rolling_restart_no_wait_history_is_linearizable \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features \
  sim::tests::in_memory_shrunk_membership_failover_restarts_and_history_stay_incremental \
  --lib -- --test-threads="$test_threads"

run cargo test --no-default-features --test fuzz_fencing_multikey -- --test-threads="$test_threads"

run cargo test --no-default-features --test stress_concurrency -- --test-threads="$test_threads"

run cargo test --no-default-features --test raft_version_skew --no-run

run cargo test --no-default-features --test k8s_raft_live_smoke --no-run

if [ "$mode" = full ]; then
  run cargo test --no-default-features --lib -- --test-threads="$test_threads"
fi

run cargo test --no-default-features --test raft_cluster \
  raft_five_voter_cluster_commits_with_three_node_quorum \
  -- --test-threads="$test_threads"

run cargo test --no-default-features --test raft_cluster \
  raft_five_voter_minority_rejects_public_write_without_phantom_lock \
  -- --test-threads="$test_threads"

run cargo test --no-default-features --test raft_cluster -- --test-threads="$test_threads"

if [ "$run_raft_version_skew" = true ]; then
  run cargo test --no-default-features --test raft_version_skew -- --ignored --nocapture --test-threads="$test_threads"
fi

if [ "$run_k8s_raft_live" = true ]; then
  run cargo test --no-default-features --test k8s_raft_live_smoke -- --ignored --nocapture --test-threads="$test_threads"
fi

if [ "$run_bench" = true ]; then
  if [ "$run_redis_bench" = true ]; then
    run env PROFILE="$profile" \
      PROFILE_TARGET=redis \
      REDIS_HOST="$redis_host" \
      REDIS_PORT="$redis_port" \
      BENCH_WORKERS="$bench_workers" \
      BENCH_KEYS="$bench_keys" \
      BENCH_DURATION_MS="$bench_duration_ms" \
      CAPTURE_METRICS=true \
      "$script_dir/profile-broker.sh" bench
  fi

  run env PROFILE="$profile" \
    PROFILE_TARGET=broker \
    BENCH_WORKERS="$bench_workers" \
    BENCH_KEYS="$bench_keys" \
    BENCH_DURATION_MS="$bench_duration_ms" \
    CAPTURE_METRICS=true \
    "$script_dir/profile-broker.sh" bench

  run env PROFILE="$profile" \
    PROFILE_TARGET=raft \
    RAFT_BENCH_ROUTE=leader \
    BENCH_WORKERS="$bench_workers" \
    BENCH_KEYS="$bench_keys" \
    BENCH_DURATION_MS="$bench_duration_ms" \
    CAPTURE_METRICS=true \
    BENCH_RAFT_METRICS=true \
    "$script_dir/profile-broker.sh" bench

  run env PROFILE="$profile" \
    PROFILE_TARGET=raft \
    RAFT_BENCH_ROUTE=round-robin \
    BENCH_WORKERS="$bench_workers" \
    BENCH_KEYS="$bench_keys" \
    BENCH_DURATION_MS="$bench_duration_ms" \
    CAPTURE_METRICS=true \
    BENCH_RAFT_METRICS=true \
    "$script_dir/profile-broker.sh" bench
fi

run git diff --check
