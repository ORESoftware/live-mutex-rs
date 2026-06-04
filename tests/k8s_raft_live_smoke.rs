//! End-to-end smoke test against a deployed BrokerRaft HTTP service.
//!
//! Skipped by default. Run from a network location that can reach the
//! Kubernetes Service / load balancer with:
//!
//!   LMX_LIVE_RAFT_HTTP=dd-rust-network-mutex-raft.default.svc.cluster.local:6971 \
//!   cargo test --test k8s_raft_live_smoke -- --ignored --nocapture

use std::env;

use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn require_http() -> String {
    env::var("LMX_LIVE_RAFT_HTTP").expect(
        "LMX_LIVE_RAFT_HTTP must be set (e.g. dd-rust-network-mutex-raft.default.svc.cluster.local:6971)",
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn live_raft_http_acquire_release_via_lb_service() {
    let endpoint = require_http();
    let key = format!("lmx-live-raft-{}", uuid_short());

    let (status, status_body) = http_json(&endpoint, "GET", "/raft/status", None).await;
    assert_eq!(status, 200, "raft status response: {status_body:?}");
    assert_eq!(status_body["clusterSize"].as_u64(), Some(3));
    assert_eq!(status_body["quorumSize"].as_u64(), Some(2));

    let (status, acquired) = http_json(
        &endpoint,
        "POST",
        "/v1/lock",
        Some(json!({"key": key, "ttlMs": 5000})),
    )
    .await;
    assert_eq!(status, 200, "acquire response: {acquired:?}");
    assert_eq!(acquired["acquired"], true, "acquire response: {acquired:?}");
    let lock_uuid = acquired["lockUuid"].as_str().expect("lockUuid").to_string();

    let (status, released) = http_json(
        &endpoint,
        "POST",
        "/v1/unlock",
        Some(json!({"key": key, "lockUuid": lock_uuid})),
    )
    .await;
    assert_eq!(status, 200, "release response: {released:?}");
    assert_eq!(released["unlocked"], true, "release response: {released:?}");
}

async fn http_json(endpoint: &str, method: &str, path: &str, body: Option<Value>) -> (u16, Value) {
    let (status, body) = http_request(endpoint, method, path, body)
        .await
        .expect("HTTP request failed");
    let parsed = serde_json::from_str(&body)
        .unwrap_or_else(|err| panic!("failed to parse JSON body: {err}; body={body:?}"));
    (status, parsed)
}

async fn http_request(
    endpoint: &str,
    method: &str,
    path: &str,
    body: Option<Value>,
) -> std::io::Result<(u16, String)> {
    let (host, port) = parse_host_port(endpoint);
    let body = body
        .map(|value| serde_json::to_vec(&value).unwrap())
        .unwrap_or_default();
    let auth = env::var("LMX_LIVE_RAFT_AUTH_TOKEN")
        .ok()
        .map(|token| format!("Authorization: Bearer {token}\r\n"))
        .unwrap_or_default();
    let request = format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: {host}\r\n\
         Connection: close\r\n\
         Content-Type: application/json\r\n\
         {auth}\
         Content-Length: {}\r\n\
         \r\n",
        body.len()
    );

    let mut stream = TcpStream::connect((host.as_str(), port)).await?;
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

fn parse_host_port(endpoint: &str) -> (String, u16) {
    let endpoint = endpoint
        .strip_prefix("http://")
        .unwrap_or(endpoint)
        .trim_end_matches('/');
    let (host, port) = endpoint
        .rsplit_once(':')
        .unwrap_or_else(|| panic!("LMX_LIVE_RAFT_HTTP must be host:port, got {endpoint:?}"));
    let port = port
        .parse::<u16>()
        .unwrap_or_else(|_| panic!("invalid port in LMX_LIVE_RAFT_HTTP: {endpoint:?}"));
    (host.to_string(), port)
}

fn uuid_short() -> String {
    let s = uuid::Uuid::new_v4().to_string();
    s.split('-').next().unwrap().to_string()
}
