//! End-to-end smoke tests for the HTTP BrokerRaft backend.
//!
//! These run three loopback Raft nodes, wait for election, then exercise the
//! load-balancer shape: HTTP requests can land on followers and get proxied to
//! the elected leader, while commits still require a quorum.

use std::collections::BTreeMap;
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
    _data_dir: PathBuf,
    handles: Vec<JoinHandle<()>>,
}

impl RaftCluster {
    async fn abort_node(&mut self, index: usize) {
        self.handles[index].abort();
        let _ = (&mut self.handles[index]).await;
    }
}

impl Drop for RaftCluster {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
        let _ = fs::remove_dir_all(&self._data_dir);
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
    let mut raft_ports = Vec::new();
    let mut http_ports = Vec::new();
    for _ in 0..3 {
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

    let data_dir = std::env::temp_dir().join(format!("lmx-raft-cluster-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&data_dir).unwrap();

    let mut handles = Vec::new();
    for idx in 0..3 {
        let node_id = format!("node-{}", idx + 1);
        let mut raft = BrokerRaftConfig::default();
        raft.enabled = true;
        raft.node_id = node_id.clone();
        raft.bind_addr = Some(format!("127.0.0.1:{}", raft_ports[idx]).parse().unwrap());
        raft.advertise_addr = Some(format!("127.0.0.1:{}", raft_ports[idx]));
        raft.data_dir = data_dir.join(&node_id);
        raft.heartbeat_interval = Duration::from_millis(50);
        raft.election_timeout_min = Duration::from_millis(600);
        raft.election_timeout_max = Duration::from_millis(1_200);
        raft.peers = peers.clone();
        raft.broker = BrokerConfig::default();

        let config = ServerConfig {
            tcp_bind: None,
            uds_path: None,
            http_bind: Some(format!("127.0.0.1:{}", http_ports[idx]).parse().unwrap()),
            auth_token: None,
            broker: BrokerConfig::default(),
            tcp_nodelay: true,
            tcp_quickack: true,
            status_bind: None,
            #[cfg(feature = "tls")]
            tls: None,
        };

        handles.push(tokio::spawn(async move {
            let _ = server::run_raft(config, raft).await;
        }));
    }

    let cluster = RaftCluster {
        http_ports,
        _data_dir: data_dir,
        handles,
    };
    wait_for_http(&cluster).await;
    cluster
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
        if idx == leader {
            assert_eq!(status, 200, "leader leaderz body: {parsed:?}");
            assert_eq!(parsed["isLeader"], true);
        } else {
            assert_eq!(status, 503, "follower leaderz body: {parsed:?}");
            assert_eq!(parsed["isLeader"], false);
            assert!(parsed["leaderId"].as_str().is_some());
        }
    }
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

    cluster.abort_node(old_leader).await;
    let survivors: Vec<usize> = (0..cluster.http_ports.len())
        .filter(|idx| *idx != old_leader)
        .collect();
    let new_leader = wait_for_leader_among(&cluster, &survivors).await;
    assert_ne!(new_leader, old_leader);

    let (status, release) = http_post_json(
        lb.port,
        "/v1/unlock",
        json!({"key": key, "lockUuid": lock_uuid}),
    )
    .await;
    assert_eq!(status, 200, "release response: {release:?}");
    assert_eq!(release["unlocked"], true, "release response: {release:?}");
    wait_for_zero_holders_and_waiters_among(&cluster, &survivors).await;

    let (status, reacquire) =
        http_post_json(lb.port, "/v1/lock", json!({"key": key, "ttlMs": 5000})).await;
    assert_eq!(status, 200, "reacquire response: {reacquire:?}");
    assert_eq!(
        reacquire["acquired"], true,
        "LB should route to the surviving Raft quorum after leader failover: {reacquire:?}"
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
    let (status, composite) =
        http_post_json(port, "/v1/lock", json!({"keys": keys, "ttlMs": 5000})).await;
    assert_eq!(status, 200, "{label} composite response: {composite:?}");
    assert_eq!(
        composite["acquired"], true,
        "{label} composite response: {composite:?}"
    );
    let composite_uuid = composite["lockUuid"].as_str().unwrap().to_string();
    for key in composite["keys"].as_array().unwrap() {
        assert!(
            composite["fencingTokens"][key.as_str().unwrap()]
                .as_u64()
                .is_some(),
            "{label} composite should include fencing token for {key:?}: {composite:?}"
        );
    }

    let (status, composite_release) = http_post_json(
        port,
        "/v1/unlock",
        json!({"keys": composite["keys"], "lockUuid": composite_uuid}),
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
