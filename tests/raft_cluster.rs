//! End-to-end smoke tests for the HTTP BrokerRaft backend.
//!
//! These run three loopback Raft nodes, wait for election, then exercise the
//! load-balancer shape: HTTP requests can land on followers and get proxied to
//! the elected leader, while commits still require a quorum.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dd_rust_network_mutex::{server, BrokerConfig, BrokerRaftConfig, RaftPeerConfig, ServerConfig};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

static RAFT_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct RaftCluster {
    http_ports: Vec<u16>,
    peers: Vec<RaftPeerConfig>,
    data_dir: PathBuf,
    handles: Vec<Option<JoinHandle<()>>>,
}

impl RaftCluster {
    async fn abort_node(&mut self, index: usize) {
        if let Some(handle) = self.handles[index].take() {
            handle.abort();
            let _ = handle.await;
        }
        wait_for_http_unavailable(self.http_ports[index], "/raft/status").await;
    }
}

impl Drop for RaftCluster {
    fn drop(&mut self) {
        for handle in &self.handles {
            if let Some(handle) = handle {
                handle.abort();
            }
        }
        let _ = fs::remove_dir_all(&self.data_dir);
    }
}

#[derive(Debug, Clone)]
struct RaftClusterTuning {
    heartbeat_interval: Duration,
    election_timeout_min: Duration,
    election_timeout_max: Duration,
    snapshot_interval: Duration,
    snapshot_max_log_entries: u64,
    snapshot_max_log_bytes: u64,
    trailing_log_entries: u64,
    append_entries_max_entries: usize,
    append_entries_max_bytes: usize,
    install_snapshot_chunk_bytes: usize,
    proxy_retry_budget: Duration,
    peer_token: Option<String>,
}

impl Default for RaftClusterTuning {
    fn default() -> Self {
        let defaults = BrokerRaftConfig::default();
        Self {
            heartbeat_interval: Duration::from_millis(50),
            election_timeout_min: Duration::from_millis(600),
            election_timeout_max: Duration::from_millis(1_200),
            snapshot_interval: defaults.snapshot_interval,
            snapshot_max_log_entries: defaults.snapshot_max_log_entries,
            snapshot_max_log_bytes: defaults.snapshot_max_log_bytes,
            trailing_log_entries: defaults.trailing_log_entries,
            append_entries_max_entries: defaults.append_entries_max_entries,
            append_entries_max_bytes: defaults.append_entries_max_bytes,
            install_snapshot_chunk_bytes: defaults.install_snapshot_chunk_bytes,
            proxy_retry_budget: defaults.proxy_retry_budget,
            peer_token: None,
        }
    }
}

struct TestLb {
    port: u16,
    handle: JoinHandle<()>,
}

impl Drop for TestLb {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

struct RegularHttpServer {
    port: u16,
    handle: JoinHandle<()>,
}

impl Drop for RegularHttpServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn pick_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

async fn start_regular_http_server() -> RegularHttpServer {
    let port = pick_port().await;
    let config = ServerConfig {
        tcp_bind: None,
        uds_path: None,
        http_bind: Some(format!("127.0.0.1:{port}").parse().unwrap()),
        auth_token: None,
        broker: BrokerConfig::default(),
        tcp_nodelay: true,
        tcp_quickack: true,
        status_bind: None,
        #[cfg(feature = "tls")]
        tls: None,
    };
    let handle = tokio::spawn(async move {
        let _ = server::run(config).await;
    });
    wait_for_http_port(port, "/healthz").await;
    RegularHttpServer { port, handle }
}

async fn start_cluster() -> RaftCluster {
    start_cluster_with_nodes(3, 3).await
}

async fn start_cluster_with_nodes(total_nodes: usize, initial_voters: usize) -> RaftCluster {
    start_cluster_with_tuning(total_nodes, initial_voters, RaftClusterTuning::default()).await
}

async fn start_cluster_with_tuning(
    total_nodes: usize,
    initial_voters: usize,
    tuning: RaftClusterTuning,
) -> RaftCluster {
    assert!(initial_voters <= total_nodes);
    let mut raft_ports = Vec::new();
    let mut http_ports = Vec::new();
    for _ in 0..total_nodes {
        raft_ports.push(pick_port().await);
        http_ports.push(pick_port().await);
    }

    let peers: Vec<RaftPeerConfig> = raft_ports
        .iter()
        .enumerate()
        .map(|(idx, port)| RaftPeerConfig {
            id: format!("node-{}", idx + 1),
            addr: format!("127.0.0.1:{port}"),
        })
        .collect();
    let initial_peers = peers
        .iter()
        .take(initial_voters)
        .cloned()
        .collect::<Vec<_>>();

    let data_dir = std::env::temp_dir().join(format!("lmx-raft-cluster-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&data_dir).unwrap();

    let mut handles = Vec::new();
    for idx in 0..total_nodes {
        handles.push(Some(spawn_raft_node(
            idx,
            &raft_ports,
            &http_ports,
            &data_dir,
            &initial_peers,
            &tuning,
        )));
    }

    let cluster = RaftCluster {
        http_ports,
        peers,
        data_dir,
        handles,
    };
    wait_for_http(&cluster).await;
    cluster
}

fn spawn_raft_node(
    index: usize,
    raft_ports: &[u16],
    http_ports: &[u16],
    data_dir: &std::path::Path,
    initial_peers: &[RaftPeerConfig],
    tuning: &RaftClusterTuning,
) -> JoinHandle<()> {
    let node_id = format!("node-{}", index + 1);
    let mut raft = BrokerRaftConfig::default();
    raft.enabled = true;
    raft.node_id = node_id.clone();
    raft.bind_addr = Some(format!("127.0.0.1:{}", raft_ports[index]).parse().unwrap());
    raft.advertise_addr = Some(format!("127.0.0.1:{}", raft_ports[index]));
    raft.data_dir = data_dir.join(&node_id);
    raft.heartbeat_interval = tuning.heartbeat_interval;
    raft.election_timeout_min = tuning.election_timeout_min;
    raft.election_timeout_max = tuning.election_timeout_max;
    raft.snapshot_interval = tuning.snapshot_interval;
    raft.snapshot_max_log_entries = tuning.snapshot_max_log_entries;
    raft.snapshot_max_log_bytes = tuning.snapshot_max_log_bytes;
    raft.trailing_log_entries = tuning.trailing_log_entries;
    raft.append_entries_max_entries = tuning.append_entries_max_entries;
    raft.append_entries_max_bytes = tuning.append_entries_max_bytes;
    raft.install_snapshot_chunk_bytes = tuning.install_snapshot_chunk_bytes;
    raft.proxy_retry_budget = tuning.proxy_retry_budget;
    raft.peer_token = tuning.peer_token.clone();
    raft.peers = initial_peers.to_vec();
    raft.broker = BrokerConfig::default();

    let config = ServerConfig {
        tcp_bind: None,
        uds_path: None,
        http_bind: Some(format!("127.0.0.1:{}", http_ports[index]).parse().unwrap()),
        auth_token: None,
        broker: BrokerConfig::default(),
        tcp_nodelay: true,
        tcp_quickack: true,
        status_bind: None,
        #[cfg(feature = "tls")]
        tls: None,
    };

    tokio::spawn(async move {
        let _ = server::run_raft(config, raft).await;
    })
}

async fn wait_for_http(cluster: &RaftCluster) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let mut up = 0usize;
        for port in &cluster.http_ports {
            if http_get_json(*port, "/raft/status").await.is_some() {
                up += 1;
            }
        }
        if up == cluster.http_ports.len() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for raft HTTP listeners"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_http_port(port: u16, path: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if http_get_json(port, path).await.is_some() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for HTTP listener on port {port}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_http_unavailable(port: u16, path: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if http_get_json(port, path).await.is_none() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for HTTP listener on port {port} to stop serving {path}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_leader(cluster: &RaftCluster) -> usize {
    let nodes: Vec<usize> = (0..cluster.http_ports.len()).collect();
    wait_for_leader_among(cluster, &nodes).await
}

async fn wait_for_leader_among(cluster: &RaftCluster, nodes: &[usize]) -> usize {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        let mut statuses = Vec::new();
        for index in nodes {
            let port = cluster.http_ports[*index];
            if let Some(status) = http_get_json(port, "/raft/status").await {
                statuses.push((*index, status));
            }
        }
        if statuses.len() == nodes.len() {
            let leaders: Vec<usize> = statuses
                .iter()
                .filter_map(|(idx, status)| {
                    status["isLeader"].as_bool().filter(|v| *v).map(|_| *idx)
                })
                .collect();
            if leaders.len() == 1 {
                let leader_id = statuses
                    .iter()
                    .find(|(idx, _)| *idx == leaders[0])
                    .and_then(|(_, status)| status["nodeId"].as_str())
                    .unwrap();
                let all_know_leader = statuses
                    .iter()
                    .all(|(_, status)| status["leaderId"].as_str() == Some(leader_id));
                if all_know_leader {
                    return leaders[0];
                }
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for single raft leader; latest statuses={statuses:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_simple_membership(
    cluster: &RaftCluster,
    nodes: &[usize],
    expected_ids: &BTreeSet<String>,
    expected_cluster_size: u64,
    expected_quorum_size: u64,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut latest = Vec::new();
    loop {
        latest.clear();
        let mut converged = true;
        for index in nodes {
            let port = cluster.http_ports[*index];
            let Some(status) = http_get_json(port, "/raft/status").await else {
                converged = false;
                latest.push(format!("{port}: status unavailable"));
                continue;
            };
            let peer_ids = simple_membership_peer_ids(&status["membership"]);
            let ok = status["membershipJoint"] == false
                && status["clusterSize"].as_u64() == Some(expected_cluster_size)
                && status["quorumSize"].as_u64() == Some(expected_quorum_size)
                && peer_ids.as_ref() == Some(expected_ids);
            latest.push(format!("{port}: status={status:?} peers={peer_ids:?}"));
            converged &= ok;
        }
        if converged {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for raft membership convergence; latest={latest:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn simple_membership_peer_ids(membership: &Value) -> Option<BTreeSet<String>> {
    if membership["state"] != "simple" {
        return None;
    }
    Some(
        membership
            .get("peers")?
            .as_array()?
            .iter()
            .filter_map(|peer| peer["id"].as_str().map(str::to_string))
            .collect(),
    )
}

fn membership_target_peer_ids(membership: &Value) -> Option<BTreeSet<String>> {
    match membership["state"].as_str()? {
        "simple" => Some(
            membership
                .get("peers")?
                .as_array()?
                .iter()
                .filter_map(|peer| peer["id"].as_str().map(str::to_string))
                .collect(),
        ),
        "joint" => Some(
            membership
                .get("newPeers")?
                .as_array()?
                .iter()
                .filter_map(|peer| peer["id"].as_str().map(str::to_string))
                .collect(),
        ),
        _ => None,
    }
}

async fn wait_for_zero_holders_and_waiters(cluster: &RaftCluster) {
    let nodes: Vec<usize> = (0..cluster.http_ports.len()).collect();
    wait_for_zero_holders_and_waiters_among(cluster, &nodes).await;
}

async fn wait_for_zero_holders_and_waiters_among(cluster: &RaftCluster, nodes: &[usize]) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut last = Vec::new();
    loop {
        let mut all_zero = true;
        last.clear();
        for index in nodes {
            let port = cluster.http_ports[*index];
            let Some(metrics) = http_get_text(port, "/metrics").await else {
                all_zero = false;
                last.push(format!("{port}: metrics unavailable"));
                continue;
            };
            let holders = metric_value(&metrics, "dd_rust_network_mutex_holders");
            let waiters = metric_value(&metrics, "dd_rust_network_mutex_waiters");
            let status = http_get_json(port, "/raft/status").await;
            last.push(format!(
                "{port}: holders={holders:?} waiters={waiters:?} status={status:?}"
            ));
            all_zero &= holders == Some(0) && waiters == Some(0);
        }
        if all_zero {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for raft nodes to clear holders/waiters; latest={last:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_zero_holders_and_waiters_on_port(port: u16) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut latest = String::new();
    loop {
        if let Some(metrics) = http_get_text(port, "/metrics").await {
            let holders = metric_value(&metrics, "dd_rust_network_mutex_holders");
            let waiters = metric_value(&metrics, "dd_rust_network_mutex_waiters");
            latest = format!("holders={holders:?} waiters={waiters:?}");
            if holders == Some(0) && waiters == Some(0) {
                return;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for backend on port {port} to clear holders/waiters; latest={latest}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_status_index_at_least(port: u16, field: &str, min: u64) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut latest = None;
    loop {
        if let Some(status) = http_get_json(port, "/raft/status").await {
            let value = status[field].as_u64();
            latest = Some(status.clone());
            if value.is_some_and(|value| value >= min) {
                return status;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for /raft/status field {field} >= {min} on port {port}; latest={latest:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_status_indexes_at_least_among(
    cluster: &RaftCluster,
    nodes: &[usize],
    min_commit_index: u64,
    min_last_applied: u64,
) -> Vec<Value> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut latest = Vec::new();
    loop {
        latest.clear();
        let mut converged = true;
        let mut statuses = Vec::new();
        for index in nodes {
            let port = cluster.http_ports[*index];
            let Some(status) = http_get_json(port, "/raft/status").await else {
                converged = false;
                latest.push(format!("{port}: status unavailable"));
                continue;
            };
            let commit_index = status["commitIndex"].as_u64();
            let last_applied = status["lastApplied"].as_u64();
            latest.push(format!(
                "{port}: commitIndex={commit_index:?} lastApplied={last_applied:?} status={status:?}"
            ));
            converged &= commit_index.is_some_and(|value| value >= min_commit_index)
                && last_applied.is_some_and(|value| value >= min_last_applied);
            statuses.push(status);
        }
        if converged && statuses.len() == nodes.len() {
            return statuses;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for raft status indexes commitIndex >= {min_commit_index} and lastApplied >= {min_last_applied}; latest={latest:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_metric_at_least(port: u16, name: &str, min: u64) -> u64 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut latest = None;
    loop {
        if let Some(metrics) = http_get_text(port, "/metrics").await {
            let value = metric_value(&metrics, name);
            latest = value;
            if value.is_some_and(|value| value >= min) {
                return value.unwrap();
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for metric {name} >= {min} on port {port}; latest={latest:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn current_metric(port: u16, name: &str) -> u64 {
    let metrics = http_get_text(port, "/metrics")
        .await
        .unwrap_or_else(|| panic!("metrics unavailable on port {port}"));
    metric_value(&metrics, name).unwrap_or_else(|| panic!("metric {name} missing on port {port}"))
}

async fn assert_no_raft_auth_rejections(cluster: &RaftCluster, nodes: &[usize], label: &str) {
    for index in nodes {
        let port = cluster.http_ports[*index];
        assert_eq!(
            current_metric(port, "dd_rust_network_mutex_raft_rpc_auth_rejections_total").await,
            0,
            "{label}: node {index} rejected at least one authenticated Raft peer RPC"
        );
    }
}

async fn start_round_robin_lb(backends: Vec<u16>) -> TestLb {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let next = Arc::new(AtomicUsize::new(0));
    let handle = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let backends = backends.clone();
            let next = next.clone();
            tokio::spawn(async move {
                let _ = proxy_http_once(stream, backends, next).await;
            });
        }
    });
    TestLb { port, handle }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_http_followers_proxy_acquire_release_after_quorum_commit() {
    let _guard = RAFT_TEST_LOCK.lock().await;
    let cluster = start_cluster().await;
    let leader = wait_for_leader(&cluster).await;
    for (idx, port) in cluster.http_ports.iter().enumerate() {
        let (status, body) = http_request("GET", *port, "/raft/leaderz", None)
            .await
            .expect("leaderz request");
        let parsed: Value = serde_json::from_str(&body).expect("leaderz JSON");
        assert_eq!(parsed["syncLog"], true, "leaderz body: {parsed:?}");
        assert_eq!(parsed["syncCommit"], true, "leaderz body: {parsed:?}");
        assert_eq!(
            parsed["unsafeDurability"], false,
            "leaderz body: {parsed:?}"
        );
        if idx == leader {
            assert_eq!(status, 200, "leader leaderz body: {parsed:?}");
            assert_eq!(parsed["isLeader"], true);
            assert_eq!(parsed["isLeaderReady"], true);
            assert!(
                parsed["leaderQuorumAgeMs"].as_u64().is_some(),
                "leader should expose quorum freshness age: {parsed:?}"
            );
            assert!(
                parsed["leaderQuorumTimeoutMs"]
                    .as_u64()
                    .is_some_and(|timeout| timeout > 0),
                "leader should expose quorum freshness timeout: {parsed:?}"
            );
        } else {
            assert_eq!(status, 503, "follower leaderz body: {parsed:?}");
            assert_eq!(parsed["isLeader"], false);
            assert_eq!(parsed["isLeaderReady"], false);
            assert_eq!(parsed["leaderQuorumAgeMs"], Value::Null);
            assert!(parsed["leaderId"].as_str().is_some());
            assert!(
                parsed["leaderQuorumTimeoutMs"]
                    .as_u64()
                    .is_some_and(|timeout| timeout > 0),
                "follower should expose quorum freshness timeout: {parsed:?}"
            );
        }
    }
    let progress = http_get_json(cluster.http_ports[leader], "/raft/progress")
        .await
        .expect("leader progress");
    assert_eq!(progress["isLeader"], true, "progress body: {progress:?}");
    assert_eq!(progress["syncLog"], true, "progress body: {progress:?}");
    assert_eq!(progress["syncCommit"], true, "progress body: {progress:?}");
    assert_eq!(
        progress["unsafeDurability"], false,
        "progress body: {progress:?}"
    );
    let status = http_get_json(cluster.http_ports[leader], "/raft/status")
        .await
        .expect("leader status");
    assert_eq!(status["syncLog"], true, "status body: {status:?}");
    assert_eq!(status["syncCommit"], true, "status body: {status:?}");
    assert_eq!(status["unsafeDurability"], false, "status body: {status:?}");
    assert_eq!(
        progress["peers"].as_array().map(Vec::len),
        Some(3),
        "progress should list all active peers: {progress:?}"
    );
    assert!(
        progress["peers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|peer| peer["isSelf"] == true
                && peer["caughtUp"] == true
                && peer["membershipRole"] == "voter"),
        "progress should include caught-up self voter: {progress:?}"
    );
    let learners = http_get_json(cluster.http_ports[leader], "/raft/learners")
        .await
        .expect("leader learners");
    assert_eq!(learners["isLeader"], true, "learners body: {learners:?}");
    assert_eq!(
        learners["learners"].as_array().map(Vec::len),
        Some(0),
        "fresh cluster should not have staged learners: {learners:?}"
    );
    let follower_a = (leader + 1) % 3;
    let follower_b = (leader + 2) % 3;
    let key = format!("raft-key-{}", uuid::Uuid::new_v4());

    let (status, acquire) = http_post_json(
        cluster.http_ports[follower_a],
        "/v1/lock",
        json!({"key": key, "ttlMs": 5000}),
    )
    .await;
    assert_eq!(status, 200, "acquire response: {acquire:?}");
    assert_eq!(acquire["acquired"], true, "acquire response: {acquire:?}");
    let lock_uuid = acquire["lockUuid"].as_str().unwrap().to_string();

    let (status, queued) = http_post_json(
        cluster.http_ports[follower_b],
        "/v1/lock",
        json!({"key": key, "ttlMs": 5000, "waitMs": 50}),
    )
    .await;
    assert_eq!(status, 200, "queued response: {queued:?}");
    assert_eq!(
        queued["acquired"], false,
        "contended short-poll acquire should time out: {queued:?}"
    );

    let (status, release) = http_post_json(
        cluster.http_ports[follower_b],
        "/v1/unlock",
        json!({"key": key, "lockUuid": lock_uuid}),
    )
    .await;
    assert_eq!(status, 200, "release response: {release:?}");
    assert_eq!(release["unlocked"], true, "release response: {release:?}");
    wait_for_zero_holders_and_waiters(&cluster).await;

    let (status, reacquire) = http_post_json(
        cluster.http_ports[follower_a],
        "/v1/lock",
        json!({"key": key, "ttlMs": 5000}),
    )
    .await;
    assert_eq!(status, 200, "reacquire response: {reacquire:?}");
    assert_eq!(
        reacquire["acquired"], true,
        "lock should be acquirable after quorum release: {reacquire:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_http_follower_proxy_uses_authenticated_direct_peer_rpc() {
    let _guard = RAFT_TEST_LOCK.lock().await;
    let tuning = RaftClusterTuning {
        peer_token: Some(format!("raft-peer-token-{}", uuid::Uuid::new_v4())),
        ..RaftClusterTuning::default()
    };
    let cluster = start_cluster_with_tuning(3, 3, tuning).await;
    let leader = wait_for_leader(&cluster).await;
    let follower = (leader + 1) % 3;
    let follower_status = http_get_json(cluster.http_ports[follower], "/raft/status")
        .await
        .expect("follower status");
    assert_eq!(
        follower_status["isLeader"], false,
        "test target should be a follower before public HTTP write: {follower_status:?}"
    );

    let key = format!("raft-auth-proxy-key-{}", uuid::Uuid::new_v4());
    let (status, acquire) = http_post_json(
        cluster.http_ports[follower],
        "/v1/lock",
        json!({"key": key, "ttlMs": 5000}),
    )
    .await;
    assert_eq!(status, 200, "follower acquire response: {acquire:?}");
    assert_eq!(
        acquire["acquired"], true,
        "follower acquire response: {acquire:?}"
    );
    let lock_uuid = acquire["lockUuid"].as_str().unwrap().to_string();

    let (status, release) = http_post_json(
        cluster.http_ports[follower],
        "/v1/unlock",
        json!({"key": key, "lockUuid": lock_uuid}),
    )
    .await;
    assert_eq!(status, 200, "follower release response: {release:?}");
    assert_eq!(
        release["unlocked"], true,
        "follower release response: {release:?}"
    );
    wait_for_zero_holders_and_waiters(&cluster).await;

    wait_for_metric_at_least(
        cluster.http_ports[follower],
        "dd_rust_network_mutex_raft_proxy_requests_forwarded_total",
        2,
    )
    .await;
    wait_for_metric_at_least(
        cluster.http_ports[leader],
        "dd_rust_network_mutex_raft_proxy_requests_handled_total",
        2,
    )
    .await;
    assert_no_raft_auth_rejections(
        &cluster,
        &[leader],
        "direct peer proxy should carry raft.peer_token",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_membership_change_uses_authenticated_peer_rpc_for_learner_catchup() {
    let _guard = RAFT_TEST_LOCK.lock().await;
    let tuning = RaftClusterTuning {
        append_entries_max_entries: 2,
        append_entries_max_bytes: usize::MAX,
        peer_token: Some(format!("raft-peer-token-{}", uuid::Uuid::new_v4())),
        ..RaftClusterTuning::default()
    };
    let cluster = start_cluster_with_tuning(4, 3, tuning).await;
    let initial_nodes = vec![0, 1, 2];
    let leader = wait_for_leader_among(&cluster, &initial_nodes).await;

    for step in 0..4 {
        let key = format!(
            "raft-auth-membership-catchup-{step}-{}",
            uuid::Uuid::new_v4()
        );
        let (status, acquire) = http_post_json(
            cluster.http_ports[leader],
            "/v1/lock",
            json!({"key": key, "ttlMs": 5000}),
        )
        .await;
        assert_eq!(status, 200, "pre-membership acquire response: {acquire:?}");
        assert_eq!(
            acquire["acquired"], true,
            "pre-membership acquire response: {acquire:?}"
        );
        let lock_uuid = acquire["lockUuid"].as_str().unwrap().to_string();
        let (status, release) = http_post_json(
            cluster.http_ports[leader],
            "/v1/unlock",
            json!({"key": key, "lockUuid": lock_uuid}),
        )
        .await;
        assert_eq!(status, 200, "pre-membership release response: {release:?}");
        assert_eq!(
            release["unlocked"], true,
            "pre-membership release response: {release:?}"
        );
    }
    let before_new_node_appended = current_metric(
        cluster.http_ports[3],
        "dd_rust_network_mutex_raft_follower_append_appended_entries_total",
    )
    .await;

    let (status, membership) = http_post_json(
        cluster.http_ports[leader],
        "/raft/membership",
        json!({"peers": cluster.peers.clone()}),
    )
    .await;
    assert_eq!(
        status, 200,
        "authenticated membership change response: {membership:?}"
    );
    assert_eq!(membership["clusterSize"].as_u64(), Some(4));
    assert_eq!(membership["quorumSize"].as_u64(), Some(3));

    let all_nodes = vec![0, 1, 2, 3];
    let expected_ids = cluster
        .peers
        .iter()
        .map(|peer| peer.id.clone())
        .collect::<BTreeSet<_>>();
    wait_for_simple_membership(&cluster, &all_nodes, &expected_ids, 4, 3).await;
    wait_for_zero_holders_and_waiters_among(&cluster, &all_nodes).await;
    assert!(
        current_metric(
            cluster.http_ports[3],
            "dd_rust_network_mutex_raft_follower_append_appended_entries_total"
        )
        .await
            > before_new_node_appended,
        "new voter should catch up through authenticated AppendEntries RPCs"
    );
    assert_no_raft_auth_rejections(
        &cluster,
        &all_nodes,
        "membership change and learner catch-up should use raft.peer_token",
    )
    .await;

    let active_leader = wait_for_leader_among(&cluster, &all_nodes).await;
    let key = format!("raft-auth-membership-post-{}", uuid::Uuid::new_v4());
    let (status, acquire) = http_post_json(
        cluster.http_ports[active_leader],
        "/v1/lock",
        json!({"key": key, "ttlMs": 5000}),
    )
    .await;
    assert_eq!(status, 200, "post-membership acquire response: {acquire:?}");
    assert_eq!(
        acquire["acquired"], true,
        "post-membership acquire response: {acquire:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_lb_round_robin_survives_leader_failover() {
    let _guard = RAFT_TEST_LOCK.lock().await;
    let mut cluster = start_cluster().await;
    let old_leader = wait_for_leader(&cluster).await;
    let lb = start_round_robin_lb(cluster.http_ports.clone()).await;
    let key = format!("raft-failover-key-{}", uuid::Uuid::new_v4());

    let (status, acquire) =
        http_post_json(lb.port, "/v1/lock", json!({"key": key, "ttlMs": 5000})).await;
    assert_eq!(status, 200, "acquire response: {acquire:?}");
    assert_eq!(acquire["acquired"], true, "acquire response: {acquire:?}");
    let lock_uuid = acquire["lockUuid"].as_str().unwrap().to_string();
    let acquired_status =
        wait_for_status_index_at_least(cluster.http_ports[old_leader], "lastApplied", 1).await;
    let acquired_index = acquired_status["lastApplied"]
        .as_u64()
        .expect("old leader lastApplied after acquire");

    cluster.abort_node(old_leader).await;
    let survivors: Vec<usize> = (0..cluster.http_ports.len())
        .filter(|idx| *idx != old_leader)
        .collect();
    let new_leader = wait_for_leader_among(&cluster, &survivors).await;
    assert_ne!(new_leader, old_leader);
    wait_for_status_indexes_at_least_among(&cluster, &survivors, acquired_index, acquired_index)
        .await;

    let (status, release) = http_post_json(
        lb.port,
        "/v1/unlock",
        json!({"key": key, "lockUuid": lock_uuid}),
    )
    .await;
    assert_eq!(status, 200, "release response: {release:?}");
    assert_eq!(release["unlocked"], true, "release response: {release:?}");
    let release_status = wait_for_status_index_at_least(
        cluster.http_ports[new_leader],
        "lastApplied",
        acquired_index.saturating_add(1),
    )
    .await;
    let release_index = release_status["lastApplied"]
        .as_u64()
        .expect("new leader lastApplied after release");
    wait_for_status_indexes_at_least_among(&cluster, &survivors, release_index, release_index)
        .await;
    wait_for_zero_holders_and_waiters_among(&cluster, &survivors).await;

    let (status, reacquire) =
        http_post_json(lb.port, "/v1/lock", json!({"key": key, "ttlMs": 5000})).await;
    assert_eq!(status, 200, "reacquire response: {reacquire:?}");
    assert_eq!(
        reacquire["acquired"], true,
        "LB should route to the surviving Raft quorum after leader failover: {reacquire:?}"
    );
    let reacquire_status = wait_for_status_index_at_least(
        cluster.http_ports[new_leader],
        "lastApplied",
        release_index.saturating_add(1),
    )
    .await;
    let reacquire_index = reacquire_status["lastApplied"]
        .as_u64()
        .expect("new leader lastApplied after reacquire");
    wait_for_status_indexes_at_least_among(&cluster, &survivors, reacquire_index, reacquire_index)
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_follower_proxy_survives_leaderless_failover_window() {
    let _guard = RAFT_TEST_LOCK.lock().await;
    let tuning = RaftClusterTuning {
        election_timeout_min: Duration::from_millis(1_200),
        election_timeout_max: Duration::from_millis(1_600),
        proxy_retry_budget: Duration::from_secs(5),
        ..RaftClusterTuning::default()
    };
    let mut cluster = start_cluster_with_tuning(3, 3, tuning).await;
    let old_leader = wait_for_leader(&cluster).await;
    let target_follower = (old_leader + 1) % 3;
    let key = format!("raft-proxy-failover-key-{}", uuid::Uuid::new_v4());

    let (status, acquire) = http_post_json(
        cluster.http_ports[old_leader],
        "/v1/lock",
        json!({"key": key, "ttlMs": 5000}),
    )
    .await;
    assert_eq!(status, 200, "acquire response: {acquire:?}");
    assert_eq!(acquire["acquired"], true, "acquire response: {acquire:?}");
    let lock_uuid = acquire["lockUuid"].as_str().unwrap().to_string();
    let acquired_status =
        wait_for_status_index_at_least(cluster.http_ports[old_leader], "lastApplied", 1).await;
    let acquired_index = acquired_status["lastApplied"]
        .as_u64()
        .expect("old leader lastApplied after acquire");
    let all_nodes = vec![0, 1, 2];
    wait_for_status_indexes_at_least_among(&cluster, &all_nodes, acquired_index, acquired_index)
        .await;

    cluster.abort_node(old_leader).await;
    let target_port = cluster.http_ports[target_follower];
    let (status, release) = http_post_json(
        target_port,
        "/v1/unlock",
        json!({"key": key, "lockUuid": lock_uuid}),
    )
    .await;
    assert_eq!(
        status, 200,
        "follower proxy should retry through leader churn and release: {release:?}"
    );
    assert_eq!(
        release["unlocked"], true,
        "follower proxy should release after failover: {release:?}"
    );
    let forwarded = current_metric(
        target_port,
        "dd_rust_network_mutex_raft_proxy_requests_forwarded_total",
    )
    .await;
    let retries = current_metric(
        target_port,
        "dd_rust_network_mutex_raft_proxy_request_retries_total",
    )
    .await;
    let target_status = http_get_json(target_port, "/raft/status")
        .await
        .expect("target status after failover release");
    assert!(
        forwarded >= 1 || target_status["isLeader"] == true,
        "failover release should either be proxied by the target follower or handled after it became leader; forwarded={forwarded}, retries={retries}, status={target_status:?}, release={release:?}"
    );

    let survivors: Vec<usize> = (0..cluster.http_ports.len())
        .filter(|idx| *idx != old_leader)
        .collect();
    let new_leader = wait_for_leader_among(&cluster, &survivors).await;
    assert_ne!(new_leader, old_leader);
    let release_status = wait_for_status_index_at_least(
        cluster.http_ports[new_leader],
        "lastApplied",
        acquired_index.saturating_add(1),
    )
    .await;
    let release_index = release_status["lastApplied"]
        .as_u64()
        .expect("new leader lastApplied after release");
    wait_for_status_indexes_at_least_among(&cluster, &survivors, release_index, release_index)
        .await;
    wait_for_zero_holders_and_waiters_among(&cluster, &survivors).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_membership_promotes_new_voters_and_survives_old_majority_loss() {
    let _guard = RAFT_TEST_LOCK.lock().await;
    let mut cluster = start_cluster_with_nodes(5, 3).await;
    let initial_nodes = vec![0, 1, 2];
    let old_leader = wait_for_leader_among(&cluster, &initial_nodes).await;
    let key = format!("raft-membership-key-{}", uuid::Uuid::new_v4());

    let (status, acquire) = http_post_json(
        cluster.http_ports[old_leader],
        "/v1/lock",
        json!({"key": key, "ttlMs": 5000}),
    )
    .await;
    assert_eq!(status, 200, "pre-membership acquire response: {acquire:?}");
    assert_eq!(
        acquire["acquired"], true,
        "pre-membership acquire response: {acquire:?}"
    );
    let lock_uuid = acquire["lockUuid"].as_str().unwrap().to_string();

    let (status, membership) = http_post_json(
        cluster.http_ports[old_leader],
        "/raft/membership",
        json!({"peers": cluster.peers.clone()}),
    )
    .await;
    assert_eq!(status, 200, "membership response: {membership:?}");
    assert_eq!(membership["clusterSize"].as_u64(), Some(5));
    assert_eq!(membership["quorumSize"].as_u64(), Some(3));
    let expected_ids = cluster
        .peers
        .iter()
        .map(|peer| peer.id.clone())
        .collect::<BTreeSet<_>>();
    let all_nodes = vec![0, 1, 2, 3, 4];
    wait_for_simple_membership(&cluster, &all_nodes, &expected_ids, 5, 3).await;

    let second_old_voter = initial_nodes
        .iter()
        .copied()
        .find(|idx| *idx != old_leader)
        .unwrap();
    let survivors = (0..cluster.http_ports.len())
        .filter(|idx| *idx != old_leader && *idx != second_old_voter)
        .collect::<Vec<_>>();
    assert_eq!(
        survivors.len(),
        3,
        "test should leave exactly a new 5-node quorum online"
    );
    let shrink_target = survivors
        .iter()
        .map(|idx| cluster.peers[*idx].clone())
        .collect::<Vec<_>>();
    let (status, removal) = http_post_json(
        cluster.http_ports[old_leader],
        "/raft/membership",
        json!({"peers": shrink_target}),
    )
    .await;
    assert_eq!(
        status, 200,
        "membership removal response should commit even when it removes the current leader: {removal:?}"
    );
    assert_eq!(removal["clusterSize"].as_u64(), Some(3));
    assert_eq!(removal["quorumSize"].as_u64(), Some(2));
    let expected_survivor_ids = survivors
        .iter()
        .map(|idx| cluster.peers[*idx].id.clone())
        .collect::<BTreeSet<_>>();
    wait_for_simple_membership(&cluster, &survivors, &expected_survivor_ids, 3, 2).await;
    for removed in [old_leader, second_old_voter] {
        let status = http_get_json(cluster.http_ports[removed], "/raft/status")
            .await
            .unwrap_or_else(|| panic!("removed node {removed} should still serve status"));
        assert_eq!(
            status["isLeader"], false,
            "removed node must step down: {status:?}"
        );
        assert_eq!(
            membership_target_peer_ids(&status["membership"]),
            Some(expected_survivor_ids.clone()),
            "removed node should observe a survivor-only target membership even if it is still in the old side of joint consensus: {status:?}"
        );
        let removed_key = format!(
            "raft-removed-node-write-{}-{}",
            removed,
            uuid::Uuid::new_v4()
        );
        let (status, rejected) = http_post_json(
            cluster.http_ports[removed],
            "/v1/lock",
            json!({"key": removed_key, "ttlMs": 5000, "waitMs": 0}),
        )
        .await;
        assert_eq!(
            status, 503,
            "removed node must not serve direct Raft writes after applying final membership: {rejected:?}"
        );
        assert!(
            rejected["error"].as_str().is_some(),
            "removed node rejection should include an error body: {rejected:?}"
        );
    }

    cluster.abort_node(old_leader).await;
    cluster.abort_node(second_old_voter).await;
    let _new_leader = wait_for_leader_among(&cluster, &survivors).await;
    let survivor_ports = survivors
        .iter()
        .map(|idx| cluster.http_ports[*idx])
        .collect::<Vec<_>>();
    let lb = start_round_robin_lb(survivor_ports).await;

    let (status, release) = http_post_json(
        lb.port,
        "/v1/unlock",
        json!({"key": key, "lockUuid": lock_uuid}),
    )
    .await;
    assert_eq!(
        status, 200,
        "release after membership failover response: {release:?}"
    );
    assert_eq!(
        release["unlocked"], true,
        "release after membership failover response: {release:?}"
    );
    wait_for_zero_holders_and_waiters_among(&cluster, &survivors).await;

    let (status, reacquire) =
        http_post_json(lb.port, "/v1/lock", json!({"key": key, "ttlMs": 5000})).await;
    assert_eq!(
        status, 200,
        "reacquire after membership failover response: {reacquire:?}"
    );
    assert_eq!(
        reacquire["acquired"], true,
        "reacquire after membership failover response: {reacquire:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_membership_grows_and_shrinks_through_even_size() {
    // Even-sized clusters are valid (quorum is always a strict majority), so a
    // cluster can pass through a 4-node intermediate in both directions:
    //   grow:   3 -> 4 -> 5   (promote staged learners one at a time)
    //   shrink: 5 -> 4 -> 3   (remove non-leaders one at a time)
    // A write must commit at every stable config, and clusterSize/quorumSize
    // must reflect the strict-majority rule (4 -> quorum 3).
    let _guard = RAFT_TEST_LOCK.lock().await;
    // 3 initial voters {0,1,2}; nodes 3 and 4 start as staged learners.
    let cluster = start_cluster_with_nodes(5, 3).await;

    let peers_for = |idxs: &[usize]| {
        idxs.iter()
            .map(|i| cluster.peers[*i].clone())
            .collect::<Vec<_>>()
    };
    let ids_for = |idxs: &[usize]| {
        idxs.iter()
            .map(|i| cluster.peers[*i].id.clone())
            .collect::<BTreeSet<_>>()
    };

    // Post a membership change to whichever node currently leads the given set,
    // and assert the committed cluster/quorum sizes. A membership change right
    // after another can transiently race leader readiness (503 NotLeader), so
    // re-discover the leader and retry for a bounded window, as operator tooling
    // would.
    async fn change_to(
        cluster: &RaftCluster,
        current_nodes: &[usize],
        target: &[RaftPeerConfig],
        expected_size: u64,
        expected_quorum: u64,
        label: &str,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let leader = wait_for_leader_among(cluster, current_nodes).await;
            let (status, membership) = http_post_json(
                cluster.http_ports[leader],
                "/raft/membership",
                json!({ "peers": target }),
            )
            .await;
            if status == 200 {
                assert_eq!(
                    membership["clusterSize"].as_u64(),
                    Some(expected_size),
                    "{label} clusterSize: {membership:?}"
                );
                assert_eq!(
                    membership["quorumSize"].as_u64(),
                    Some(expected_quorum),
                    "{label} quorumSize: {membership:?}"
                );
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{label} membership change never committed; last response {status}: {membership:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn write_succeeds(cluster: &RaftCluster, nodes: &[usize], key: &str) {
        let leader = wait_for_leader_among(cluster, nodes).await;
        let (status, acquire) = http_post_json(
            cluster.http_ports[leader],
            "/v1/lock",
            json!({ "key": key, "ttlMs": 5000 }),
        )
        .await;
        assert_eq!(status, 200, "write at {} nodes: {acquire:?}", nodes.len());
        assert_eq!(
            acquire["acquired"],
            true,
            "write at {} nodes must commit: {acquire:?}",
            nodes.len()
        );
        let lock_uuid = acquire["lockUuid"].as_str().unwrap().to_string();
        let leader = wait_for_leader_among(cluster, nodes).await;
        let (status, release) = http_post_json(
            cluster.http_ports[leader],
            "/v1/unlock",
            json!({ "key": key, "lockUuid": lock_uuid }),
        )
        .await;
        assert_eq!(status, 200, "release at {} nodes: {release:?}", nodes.len());
        assert_eq!(release["unlocked"], true, "release: {release:?}");
    }

    let v3: Vec<usize> = vec![0, 1, 2];
    let v4: Vec<usize> = vec![0, 1, 2, 3];
    let v5: Vec<usize> = vec![0, 1, 2, 3, 4];

    // Baseline: the 3-voter cluster elects a leader and commits a write.
    wait_for_leader_among(&cluster, &v3).await;
    write_succeeds(&cluster, &v3, "even-grow-at3").await;

    // GROW 3 -> 4 -> 5, promoting one staged learner per step.
    change_to(&cluster, &v3, &peers_for(&v4), 4, 3, "3->4").await;
    wait_for_simple_membership(&cluster, &v4, &ids_for(&v4), 4, 3).await;
    write_succeeds(&cluster, &v4, "even-grow-at4").await;

    change_to(&cluster, &v4, &peers_for(&v5), 5, 3, "4->5").await;
    wait_for_simple_membership(&cluster, &v5, &ids_for(&v5), 5, 3).await;
    write_succeeds(&cluster, &v5, "even-grow-at5").await;

    // SHRINK 5 -> 4 -> 3, removing non-leaders so leadership stays put.
    let leader = wait_for_leader_among(&cluster, &v5).await;
    let non_leaders: Vec<usize> = v5.iter().copied().filter(|i| *i != leader).collect();
    let victim_a = non_leaders[0];
    let victim_b = non_leaders[1];
    let s4: Vec<usize> = v5.iter().copied().filter(|i| *i != victim_a).collect();
    let s3: Vec<usize> = s4.iter().copied().filter(|i| *i != victim_b).collect();

    change_to(&cluster, &v5, &peers_for(&s4), 4, 3, "5->4").await;
    wait_for_simple_membership(&cluster, &s4, &ids_for(&s4), 4, 3).await;
    write_succeeds(&cluster, &s4, "even-shrink-at4").await;

    change_to(&cluster, &s4, &peers_for(&s3), 3, 2, "4->3").await;
    wait_for_simple_membership(&cluster, &s3, &ids_for(&s3), 3, 2).await;
    write_succeeds(&cluster, &s3, "even-shrink-at3").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_staged_learner_catches_up_with_install_snapshot_after_compaction() {
    let _guard = RAFT_TEST_LOCK.lock().await;
    let tuning = RaftClusterTuning {
        snapshot_max_log_entries: 3,
        snapshot_max_log_bytes: u64::MAX,
        trailing_log_entries: 0,
        append_entries_max_entries: 2,
        append_entries_max_bytes: usize::MAX,
        install_snapshot_chunk_bytes: 128,
        ..RaftClusterTuning::default()
    };
    let cluster = start_cluster_with_tuning(4, 3, tuning).await;
    let leader = wait_for_leader_among(&cluster, &[0, 1, 2]).await;
    let learner = cluster.peers[3].clone();

    for step in 0..8 {
        let key = format!("raft-snapshot-learner-{step}-{}", uuid::Uuid::new_v4());
        let (status, acquire) = http_post_json(
            cluster.http_ports[leader],
            "/v1/lock",
            json!({"key": key, "ttlMs": 5000}),
        )
        .await;
        assert_eq!(
            status, 200,
            "snapshot learner acquire response: {acquire:?}"
        );
        assert_eq!(
            acquire["acquired"], true,
            "snapshot learner acquire response: {acquire:?}"
        );
        let lock_uuid = acquire["lockUuid"].as_str().unwrap().to_string();
        let (status, release) = http_post_json(
            cluster.http_ports[leader],
            "/v1/unlock",
            json!({"key": key, "lockUuid": lock_uuid}),
        )
        .await;
        assert_eq!(
            status, 200,
            "snapshot learner release response: {release:?}"
        );
        assert_eq!(
            release["unlocked"], true,
            "snapshot learner release response: {release:?}"
        );
    }

    let leader_status = http_get_json(cluster.http_ports[leader], "/raft/status")
        .await
        .expect("leader status after compaction writes");
    let pre_stage_commit = leader_status["commitIndex"]
        .as_u64()
        .expect("leader commit index");
    let leader_snapshot_index = wait_for_metric_at_least(
        cluster.http_ports[leader],
        "dd_rust_network_mutex_raft_latest_snapshot_index",
        1,
    )
    .await;
    wait_for_metric_at_least(
        cluster.http_ports[leader],
        "dd_rust_network_mutex_raft_log_compactions_total",
        1,
    )
    .await;
    assert!(
        leader_snapshot_index < pre_stage_commit,
        "test should leave a retained suffix after the compacted prefix; snapshot={leader_snapshot_index} commit={pre_stage_commit}"
    );
    let learner_status = http_get_json(cluster.http_ports[3], "/raft/status")
        .await
        .expect("bootstrap learner status before staging");
    assert_eq!(
        learner_status["lastLogIndex"].as_u64(),
        Some(0),
        "bootstrap learner should start without the leader's compacted log: {learner_status:?}"
    );
    let before_leader_batches = current_metric(
        cluster.http_ports[leader],
        "dd_rust_network_mutex_raft_append_entries_batches_total",
    )
    .await;
    let before_leader_sent = current_metric(
        cluster.http_ports[leader],
        "dd_rust_network_mutex_raft_append_entries_sent_total",
    )
    .await;
    let before_learner_appended = current_metric(
        cluster.http_ports[3],
        "dd_rust_network_mutex_raft_follower_append_appended_entries_total",
    )
    .await;

    let (status, staged) = http_post_json(
        cluster.http_ports[leader],
        "/raft/learners",
        json!({"peers": [learner]}),
    )
    .await;
    assert_eq!(status, 200, "stage learner response: {staged:?}");
    assert_eq!(
        staged["learners"].as_array().map(Vec::len),
        Some(1),
        "stage learner response should list the caught-up learner: {staged:?}"
    );
    let stage_target = staged["progress"]["lastLogIndex"]
        .as_u64()
        .expect("leader last log index after learner staging");

    wait_for_metric_at_least(
        cluster.http_ports[3],
        "dd_rust_network_mutex_raft_install_snapshot_staged_chunks_total",
        1,
    )
    .await;
    wait_for_metric_at_least(
        cluster.http_ports[3],
        "dd_rust_network_mutex_raft_latest_snapshot_index",
        leader_snapshot_index,
    )
    .await;
    wait_for_status_index_at_least(cluster.http_ports[3], "lastApplied", stage_target).await;
    let after_leader_batches = current_metric(
        cluster.http_ports[leader],
        "dd_rust_network_mutex_raft_append_entries_batches_total",
    )
    .await;
    let after_leader_sent = current_metric(
        cluster.http_ports[leader],
        "dd_rust_network_mutex_raft_append_entries_sent_total",
    )
    .await;
    let after_learner_appended = current_metric(
        cluster.http_ports[3],
        "dd_rust_network_mutex_raft_follower_append_appended_entries_total",
    )
    .await;
    let learner_snapshot_index = current_metric(
        cluster.http_ports[3],
        "dd_rust_network_mutex_raft_latest_snapshot_index",
    )
    .await;
    let appended_delta = after_learner_appended.saturating_sub(before_learner_appended);
    let sent_delta = after_leader_sent.saturating_sub(before_leader_sent);
    if learner_snapshot_index < stage_target {
        let retained_suffix_entries = stage_target.saturating_sub(learner_snapshot_index);
        let expected_suffix_batches = retained_suffix_entries.saturating_add(1) / 2;
        assert!(
            appended_delta >= retained_suffix_entries,
            "learner should append retained suffix entries after installing snapshot; before={before_learner_appended} after={after_learner_appended} suffix={retained_suffix_entries} snapshot={learner_snapshot_index} target={stage_target}"
        );
        assert!(
            after_leader_batches.saturating_sub(before_leader_batches) >= expected_suffix_batches,
            "snapshot catch-up should continue with bounded AppendEntries batches for the retained suffix; before={before_leader_batches} after={after_leader_batches} expected_at_least={expected_suffix_batches}"
        );
        assert!(
            sent_delta >= retained_suffix_entries,
            "leader should send retained suffix entries after InstallSnapshot; before={before_leader_sent} after={after_leader_sent} suffix={retained_suffix_entries}"
        );
    } else {
        assert_eq!(
            appended_delta, 0,
            "when the installed snapshot already covers the staging target, learner should not need retained suffix appends"
        );
    }

    let progress = http_get_json(cluster.http_ports[leader], "/raft/progress")
        .await
        .expect("leader progress after learner catch-up");
    assert!(
        progress["peers"].as_array().unwrap().iter().any(|peer| {
            peer["id"].as_str() == Some("node-4")
                && peer["stagedLearner"] == true
                && peer["caughtUp"] == true
        }),
        "leader progress should show node-4 as a caught-up staged learner: {progress:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_staged_learner_catches_up_over_bounded_append_entries_without_snapshot() {
    let _guard = RAFT_TEST_LOCK.lock().await;
    let tuning = RaftClusterTuning {
        snapshot_max_log_entries: 100_000,
        snapshot_max_log_bytes: u64::MAX,
        append_entries_max_entries: 2,
        append_entries_max_bytes: usize::MAX,
        ..RaftClusterTuning::default()
    };
    let cluster = start_cluster_with_tuning(4, 3, tuning).await;
    let leader = wait_for_leader_among(&cluster, &[0, 1, 2]).await;
    let learner = cluster.peers[3].clone();

    for step in 0..5 {
        let key = format!("raft-append-learner-{step}-{}", uuid::Uuid::new_v4());
        let (status, acquire) = http_post_json(
            cluster.http_ports[leader],
            "/v1/lock",
            json!({"key": key, "ttlMs": 5000}),
        )
        .await;
        assert_eq!(status, 200, "append learner acquire response: {acquire:?}");
        assert_eq!(
            acquire["acquired"], true,
            "append learner acquire response: {acquire:?}"
        );
        let lock_uuid = acquire["lockUuid"].as_str().unwrap().to_string();
        let (status, release) = http_post_json(
            cluster.http_ports[leader],
            "/v1/unlock",
            json!({"key": key, "lockUuid": lock_uuid}),
        )
        .await;
        assert_eq!(status, 200, "append learner release response: {release:?}");
        assert_eq!(
            release["unlocked"], true,
            "append learner release response: {release:?}"
        );
    }

    let leader_status = http_get_json(cluster.http_ports[leader], "/raft/status")
        .await
        .expect("leader status before append-only learner staging");
    let pre_stage_commit = leader_status["commitIndex"]
        .as_u64()
        .expect("leader commit index");
    assert!(
        pre_stage_commit >= 10,
        "test should build a multi-batch retained suffix before staging learner: {leader_status:?}"
    );
    assert_eq!(
        current_metric(
            cluster.http_ports[leader],
            "dd_rust_network_mutex_raft_latest_snapshot_index"
        )
        .await,
        0,
        "append-only catch-up test should not compact before staging"
    );
    let learner_status = http_get_json(cluster.http_ports[3], "/raft/status")
        .await
        .expect("bootstrap learner status before append-only staging");
    assert_eq!(
        learner_status["lastLogIndex"].as_u64(),
        Some(0),
        "bootstrap learner should start empty before append-only catch-up: {learner_status:?}"
    );

    let before_batches = current_metric(
        cluster.http_ports[leader],
        "dd_rust_network_mutex_raft_append_entries_batches_total",
    )
    .await;
    let before_sent = current_metric(
        cluster.http_ports[leader],
        "dd_rust_network_mutex_raft_append_entries_sent_total",
    )
    .await;
    let before_fallbacks = current_metric(
        cluster.http_ports[leader],
        "dd_rust_network_mutex_raft_append_snapshot_fallbacks_total",
    )
    .await;

    let (status, staged) = http_post_json(
        cluster.http_ports[leader],
        "/raft/learners",
        json!({"peers": [learner]}),
    )
    .await;
    assert_eq!(
        status, 200,
        "append-only stage learner response: {staged:?}"
    );
    let stage_target = staged["progress"]["lastLogIndex"]
        .as_u64()
        .expect("leader last log index after append-only learner staging");
    wait_for_status_index_at_least(cluster.http_ports[3], "lastApplied", stage_target).await;

    let after_batches = current_metric(
        cluster.http_ports[leader],
        "dd_rust_network_mutex_raft_append_entries_batches_total",
    )
    .await;
    let after_sent = current_metric(
        cluster.http_ports[leader],
        "dd_rust_network_mutex_raft_append_entries_sent_total",
    )
    .await;
    let after_fallbacks = current_metric(
        cluster.http_ports[leader],
        "dd_rust_network_mutex_raft_append_snapshot_fallbacks_total",
    )
    .await;
    assert_eq!(
        after_fallbacks, before_fallbacks,
        "append-only learner catch-up should not fall back to InstallSnapshot"
    );
    assert_eq!(
        current_metric(
            cluster.http_ports[3],
            "dd_rust_network_mutex_raft_install_snapshot_staged_chunks_total"
        )
        .await,
        0,
        "append-only learner should not stage snapshot chunks"
    );
    assert_eq!(
        current_metric(
            cluster.http_ports[3],
            "dd_rust_network_mutex_raft_latest_snapshot_index"
        )
        .await,
        0,
        "append-only learner should not install a snapshot"
    );
    let minimum_catchup_batches = (stage_target + 1) / 2;
    assert!(
        after_batches.saturating_sub(before_batches) >= minimum_catchup_batches,
        "bounded catch-up should require multiple AppendEntries batches; before={before_batches} after={after_batches} target={stage_target}"
    );
    assert!(
        after_sent.saturating_sub(before_sent) >= stage_target,
        "learner catch-up should send the retained log entries rather than one full-log rewrite; before={before_sent} after={after_sent} target={stage_target}"
    );

    let progress = http_get_json(cluster.http_ports[leader], "/raft/progress")
        .await
        .expect("leader progress after append-only learner catch-up");
    assert!(
        progress["peers"].as_array().unwrap().iter().any(|peer| {
            peer["id"].as_str() == Some("node-4")
                && peer["stagedLearner"] == true
                && peer["caughtUp"] == true
                && peer["matchIndex"]
                    .as_u64()
                    .is_some_and(|idx| idx >= stage_target)
        }),
        "leader progress should show node-4 caught up through bounded AppendEntries: {progress:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broker_and_raft_http_contract_match() {
    let _guard = RAFT_TEST_LOCK.lock().await;
    let broker = start_regular_http_server().await;
    assert_http_lock_contract(broker.port, "broker").await;

    let cluster = start_cluster().await;
    let _leader = wait_for_leader(&cluster).await;
    let lb = start_round_robin_lb(cluster.http_ports.clone()).await;
    assert_http_lock_contract(lb.port, "raft").await;
    wait_for_zero_holders_and_waiters(&cluster).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raft_lb_seeded_lock_model_fuzz() {
    let _guard = RAFT_TEST_LOCK.lock().await;
    let cluster = start_cluster().await;
    let _leader = wait_for_leader(&cluster).await;
    let lb = start_round_robin_lb(cluster.http_ports.clone()).await;
    run_seeded_http_lock_model(lb.port, 0xB10C_AADE_5EED, 180).await;
    wait_for_zero_holders_and_waiters(&cluster).await;
}

async fn assert_http_lock_contract(port: u16, label: &str) {
    let key = format!("{label}-contract-{}", uuid::Uuid::new_v4());
    let (status, invalid) = http_post_json(
        port,
        "/v1/lock",
        json!({"key": key, "keys": ["other"], "ttlMs": 5000}),
    )
    .await;
    assert_eq!(
        status, 400,
        "{label} invalid key+keys response: {invalid:?}"
    );
    assert!(
        invalid["error"].as_str().is_some(),
        "{label} invalid response should include error: {invalid:?}"
    );

    let (status, acquire) =
        http_post_json(port, "/v1/lock", json!({"key": key, "ttlMs": 5000})).await;
    assert_eq!(status, 200, "{label} acquire response: {acquire:?}");
    assert_eq!(
        acquire["acquired"], true,
        "{label} acquire response: {acquire:?}"
    );
    assert!(
        acquire["fencingTokens"][&key].as_u64().is_some(),
        "{label} acquire should include a fencing token: {acquire:?}"
    );
    let lock_uuid = acquire["lockUuid"].as_str().unwrap().to_string();

    let (status, contended) = http_post_json(
        port,
        "/v1/lock",
        json!({"key": key, "ttlMs": 5000, "waitMs": 25}),
    )
    .await;
    assert_eq!(status, 200, "{label} contended response: {contended:?}");
    assert_eq!(
        contended["acquired"], false,
        "{label} contended short-poll acquire should time out: {contended:?}"
    );

    let (status, wrong_release) = http_post_json(
        port,
        "/v1/unlock",
        json!({"key": key, "lockUuid": "not-the-lock"}),
    )
    .await;
    assert_eq!(
        status, 200,
        "{label} wrong release response: {wrong_release:?}"
    );
    assert_eq!(
        wrong_release["unlocked"], false,
        "{label} wrong UUID must not unlock: {wrong_release:?}"
    );

    let (status, release) = http_post_json(
        port,
        "/v1/unlock",
        json!({"key": key, "lockUuid": lock_uuid}),
    )
    .await;
    assert_eq!(status, 200, "{label} release response: {release:?}");
    assert_eq!(
        release["unlocked"], true,
        "{label} release response: {release:?}"
    );

    let keys = vec![
        format!("{label}-composite-a-{}", uuid::Uuid::new_v4()),
        format!("{label}-composite-b-{}", uuid::Uuid::new_v4()),
    ];
    // Bounded composite (<= RAFT_MAX_COMPOSITE_KEYS distinct keys) is supported
    // identically by the single-node broker and the replicated BrokerRaft front
    // door, so both labels exercise the same union-overlap semantics here.
    {
        let (status, composite) =
            http_post_json(port, "/v1/lock", json!({"keys": keys, "ttlMs": 5000})).await;
        assert_eq!(status, 200, "{label} composite response: {composite:?}");
        assert_eq!(
            composite["acquired"], true,
            "{label} composite response: {composite:?}"
        );
        let composite_uuid = composite["lockUuid"].as_str().unwrap().to_string();
        let composite_keys = composite["keys"]
            .as_array()
            .unwrap()
            .iter()
            .map(|key| key.as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            composite_keys.len(),
            2,
            "{label} composite should hold both requested keys: {composite:?}"
        );
        for key in &composite_keys {
            assert!(
                composite["fencingTokens"][key].as_u64().is_some(),
                "{label} composite should include fencing token for {key}: {composite:?}"
            );
        }

        let (status, overlapping_single) = http_post_json(
            port,
            "/v1/lock",
            json!({"key": composite_keys[0].clone(), "ttlMs": 5000, "waitMs": 50}),
        )
        .await;
        assert_eq!(
            status, 200,
            "{label} overlapping single response: {overlapping_single:?}"
        );
        assert_eq!(
            overlapping_single["acquired"], false,
            "{label} single-key acquire must not grant while composite holds that key: {overlapping_single:?}"
        );

        let overlap_extra_key = format!("{label}-composite-overlap-{}", uuid::Uuid::new_v4());
        let (status, overlapping_composite) = http_post_json(
            port,
            "/v1/lock",
            json!({"keys": [composite_keys[1].clone(), overlap_extra_key], "ttlMs": 5000, "waitMs": 50}),
        )
        .await;
        assert_eq!(
            status, 200,
            "{label} overlapping composite response: {overlapping_composite:?}"
        );
        assert_eq!(
            overlapping_composite["acquired"], false,
            "{label} composite acquire must use union overlap semantics, not intersection-only semantics: {overlapping_composite:?}"
        );

        let disjoint_key = format!("{label}-composite-disjoint-{}", uuid::Uuid::new_v4());
        let (status, disjoint) = http_post_json(
            port,
            "/v1/lock",
            json!({"key": disjoint_key, "ttlMs": 5000}),
        )
        .await;
        assert_eq!(status, 200, "{label} disjoint response: {disjoint:?}");
        assert_eq!(
            disjoint["acquired"], true,
            "{label} disjoint key should still grant while composite is held: {disjoint:?}"
        );
        let disjoint_uuid = disjoint["lockUuid"].as_str().unwrap().to_string();
        let (status, disjoint_release) = http_post_json(
            port,
            "/v1/unlock",
            json!({"key": disjoint_key, "lockUuid": disjoint_uuid}),
        )
        .await;
        assert_eq!(
            status, 200,
            "{label} disjoint release response: {disjoint_release:?}"
        );
        assert_eq!(
            disjoint_release["unlocked"], true,
            "{label} disjoint release response: {disjoint_release:?}"
        );

        let (status, composite_release) = http_post_json(
            port,
            "/v1/unlock",
            json!({"keys": composite_keys, "lockUuid": composite_uuid}),
        )
        .await;
        assert_eq!(
            status, 200,
            "{label} composite release response: {composite_release:?}"
        );
        assert_eq!(
            composite_release["unlocked"], true,
            "{label} composite release response: {composite_release:?}"
        );
    }

    // The replicated front door caps composite admission at 3 distinct keys; a
    // 4-key acquire is rejected before append. (The single-node broker accepts
    // up to MAX_COMPOSITE_KEYS, so this bound is BrokerRaft-specific.)
    if label == "raft" {
        let oversized_keys = (0..4)
            .map(|i| format!("{label}-oversized-{i}-{}", uuid::Uuid::new_v4()))
            .collect::<Vec<_>>();
        let (status, oversized) = http_post_json(
            port,
            "/v1/lock",
            json!({"keys": oversized_keys, "ttlMs": 5000}),
        )
        .await;
        assert_eq!(status, 400, "{label} oversized composite: {oversized:?}");
        assert_eq!(oversized["acquired"], false);
        assert!(
            oversized["error"]
                .as_str()
                .is_some_and(|error| error.contains("at most 3 distinct keys")),
            "{label} BrokerRaft should reject composite above the key cap: {oversized:?}"
        );
    }

    let force_key = format!("{label}-force-{}", uuid::Uuid::new_v4());
    let (status, force_acquire) =
        http_post_json(port, "/v1/lock", json!({"key": force_key, "ttlMs": 5000})).await;
    assert_eq!(
        status, 200,
        "{label} force acquire response: {force_acquire:?}"
    );
    assert_eq!(force_acquire["acquired"], true);
    let (status, forced) =
        http_post_json(port, "/v1/unlock", json!({"key": force_key, "force": true})).await;
    assert_eq!(status, 200, "{label} force release response: {forced:?}");
    assert_eq!(
        forced["unlocked"], true,
        "{label} force release response: {forced:?}"
    );

    wait_for_zero_holders_and_waiters_on_port(port).await;
}

async fn run_seeded_http_lock_model(port: u16, seed: u64, steps: usize) {
    let mut rng = seed;
    let mut held: BTreeMap<String, String> = BTreeMap::new();
    let mut fencing_watermark: BTreeMap<String, u64> = BTreeMap::new();
    for step in 0..steps {
        let key = format!("raft-fuzz-key-{}", next_fuzz(&mut rng) % 7);
        if let Some(lock_uuid) = held.get(&key).cloned() {
            match next_fuzz(&mut rng) % 4 {
                0 => {
                    let (status, denied) = http_post_json(
                        port,
                        "/v1/lock",
                        json!({"key": key, "ttlMs": 5000, "waitMs": 10}),
                    )
                    .await;
                    assert_eq!(status, 200, "step={step} denied response: {denied:?}");
                    assert_eq!(
                        denied["acquired"], false,
                        "step={step} held key should not double-grant: {denied:?}"
                    );
                }
                1 => {
                    let (status, wrong_release) = http_post_json(
                        port,
                        "/v1/unlock",
                        json!({"key": key, "lockUuid": "wrong-lock-uuid"}),
                    )
                    .await;
                    assert_eq!(
                        status, 200,
                        "step={step} wrong release response: {wrong_release:?}"
                    );
                    assert_eq!(
                        wrong_release["unlocked"], false,
                        "step={step} wrong UUID must not unlock: {wrong_release:?}"
                    );
                }
                2 => {
                    let (status, release) = http_post_json(
                        port,
                        "/v1/unlock",
                        json!({"key": key, "lockUuid": lock_uuid}),
                    )
                    .await;
                    assert_eq!(status, 200, "step={step} release response: {release:?}");
                    assert_eq!(
                        release["unlocked"], true,
                        "step={step} release: {release:?}"
                    );
                    held.remove(&key);
                }
                _ => {
                    let (status, forced) =
                        http_post_json(port, "/v1/unlock", json!({"key": key, "force": true}))
                            .await;
                    assert_eq!(status, 200, "step={step} force response: {forced:?}");
                    assert_eq!(forced["unlocked"], true, "step={step} force: {forced:?}");
                    held.remove(&key);
                }
            }
        } else {
            let (status, acquire) = http_post_json(
                port,
                "/v1/lock",
                json!({"key": key, "ttlMs": 5000, "waitMs": 25}),
            )
            .await;
            assert_eq!(status, 200, "step={step} acquire response: {acquire:?}");
            assert_eq!(
                acquire["acquired"], true,
                "step={step} unheld key should acquire: {acquire:?}"
            );
            let token = acquire["fencingTokens"][&key]
                .as_u64()
                .unwrap_or_else(|| panic!("step={step} missing fencing token: {acquire:?}"));
            if let Some(previous) = fencing_watermark.insert(key.clone(), token) {
                assert!(
                    token > previous,
                    "step={step} fencing token did not increase for {key}: previous={previous} token={token}"
                );
            }
            held.insert(
                key,
                acquire["lockUuid"].as_str().expect("lockUuid").to_string(),
            );
        }
    }

    for (key, lock_uuid) in held {
        let (status, release) = http_post_json(
            port,
            "/v1/unlock",
            json!({"key": key, "lockUuid": lock_uuid}),
        )
        .await;
        assert_eq!(status, 200, "cleanup release response: {release:?}");
        assert_eq!(
            release["unlocked"], true,
            "cleanup release response: {release:?}"
        );
    }
}

fn next_fuzz(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *state
}

async fn http_get_json(port: u16, path: &str) -> Option<Value> {
    let (status, body) = http_request("GET", port, path, None).await.ok()?;
    if status != 200 {
        return None;
    }
    serde_json::from_str(&body).ok()
}

async fn http_get_text(port: u16, path: &str) -> Option<String> {
    let (status, body) = http_request("GET", port, path, None).await.ok()?;
    (status == 200).then_some(body)
}

async fn http_post_json(port: u16, path: &str, body: Value) -> (u16, Value) {
    let (status, body) = http_request("POST", port, path, Some(body))
        .await
        .expect("HTTP request failed");
    let parsed = serde_json::from_str(&body)
        .unwrap_or_else(|err| panic!("failed to parse JSON body: {err}; body={body:?}"));
    (status, parsed)
}

async fn http_request(
    method: &str,
    port: u16,
    path: &str,
    body: Option<Value>,
) -> std::io::Result<(u16, String)> {
    let body = body
        .map(|value| serde_json::to_vec(&value).unwrap())
        .unwrap_or_default();
    let request = format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: 127.0.0.1\r\n\
         Connection: close\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         \r\n",
        body.len()
    );
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await?;
    stream.write_all(request.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;

    let mut raw = String::new();
    stream.read_to_string(&mut raw).await?;
    let status = raw
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or(0);
    let body = raw
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or_default();
    Ok((status, body))
}

async fn proxy_http_once(
    mut inbound: TcpStream,
    backends: Vec<u16>,
    next: Arc<AtomicUsize>,
) -> std::io::Result<()> {
    let request = read_http_message(&mut inbound).await?;
    for _ in 0..backends.len() {
        let idx = next.fetch_add(1, Ordering::Relaxed) % backends.len();
        let Ok(mut upstream) = TcpStream::connect(("127.0.0.1", backends[idx])).await else {
            continue;
        };
        upstream.write_all(&request).await?;
        upstream.flush().await?;

        let mut response = Vec::new();
        upstream.read_to_end(&mut response).await?;
        inbound.write_all(&response).await?;
        inbound.flush().await?;
        return Ok(());
    }

    inbound
        .write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n")
        .await?;
    inbound.flush().await
}

async fn read_http_message(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(buf);
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(total) = http_message_len(&buf) {
            if buf.len() >= total {
                return Ok(buf);
            }
        }
    }
}

fn http_message_len(buf: &[u8]) -> Option<usize> {
    let header_end = buf.windows(4).position(|w| w == b"\r\n\r\n")?;
    let headers = std::str::from_utf8(&buf[..header_end]).ok()?;
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    Some(header_end + 4 + content_length)
}

fn metric_value(metrics: &str, name: &str) -> Option<u64> {
    metrics.lines().find_map(|line| {
        let mut parts = line.split_whitespace();
        (parts.next()? == name)
            .then(|| parts.next()?.parse::<u64>().ok())
            .flatten()
    })
}
