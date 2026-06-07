//! Rough Redis / Broker / BrokerRaft lock benchmark.
//!
//! This intentionally ignores fencing tokens. It measures successful
//! acquire+release cycles for:
//! - Redis: SET key token NX PX ttl, then EVAL compare-and-del.
//! - Broker: POST /v1/lock, then POST /v1/unlock against one regular broker.
//! - BrokerRaft: POST /v1/lock, then POST /v1/unlock.
//!
//! The HTTP paths default to one short-lived connection per request, matching
//! the simple LB-facing API. Set BENCH_HTTP_KEEPALIVE=true to reuse one HTTP
//! socket per worker per endpoint during server-side CPU profiling.

use std::collections::BTreeMap;
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::Barrier;
use tokio::time::timeout;

const REDIS_UNLOCK_LUA: &str =
    "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
const DEFAULT_IO_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_REDIS_ADDR: &str = "127.0.0.1:6379";
const DEFAULT_BROKER_ADDR: &str = "127.0.0.1:6971";
const DEFAULT_RAFT_ADDR: &str = "127.0.0.1:6972";
const DEFAULT_TARGET: &str = "redis-raft";
const MAX_HTTP_RESPONSE_HEADER_BYTES: usize = 64 * 1024;
const MAX_HTTP_RESPONSE_BODY_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    Redis,
    Broker,
    Raft,
    BrokerRaft,
    RedisRaft,
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RaftRoute {
    RoundRobin,
    Leader,
}

#[derive(Debug, Clone)]
struct Config {
    redis_addr: String,
    broker_addr: String,
    raft_addrs: Vec<String>,
    raft_route: RaftRoute,
    workers: usize,
    keys: usize,
    duration: Duration,
    ttl_ms: u64,
    io_timeout: Duration,
    target: Target,
    auth_token: Option<String>,
    http_keep_alive: bool,
}

#[derive(Debug, Default)]
struct WorkerStats {
    ok: u64,
    not_acquired: u64,
    errors: u64,
    latencies_us: Vec<u64>,
}

#[derive(Debug, Default)]
struct Summary {
    ok: u64,
    not_acquired: u64,
    errors: u64,
    latencies_us: Vec<u64>,
}

impl Summary {
    fn add(&mut self, worker: WorkerStats) {
        self.ok += worker.ok;
        self.not_acquired += worker.not_acquired;
        self.errors += worker.errors;
        self.latencies_us.extend(worker.latencies_us);
    }
}

#[derive(Debug)]
enum RedisValue {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array,
}

#[tokio::main]
async fn main() {
    if env::args()
        .skip(1)
        .any(|arg| arg == "--help" || arg == "-h")
    {
        print_usage();
        return;
    }

    let config = Config::from_env();
    println!(
        "workers={} keys={} duration_ms={} ttl_ms={} io_timeout_ms={} redis={} broker={} raft={} raft_route={}",
        config.workers,
        config.keys,
        config.duration.as_millis(),
        config.ttl_ms,
        config.io_timeout.as_millis(),
        config.redis_addr,
        config.broker_addr,
        config.raft_addrs.join(","),
        config.raft_route.label()
    );
    if config.http_keep_alive {
        println!("http_keep_alive=true");
    }

    let mut redis_summary = None;
    let mut broker_summary = None;
    let mut raft_summary = None;

    if matches!(config.target, Target::Redis | Target::RedisRaft) {
        let summary = run_redis(config.clone()).await;
        print_summary("redis", &summary, config.duration);
        redis_summary = Some(summary);
    }
    if matches!(
        config.target,
        Target::Broker | Target::BrokerRaft | Target::All
    ) {
        let summary = run_broker(config.clone()).await;
        print_summary("broker", &summary, config.duration);
        broker_summary = Some(summary);
    }
    if matches!(
        config.target,
        Target::Raft | Target::RedisRaft | Target::BrokerRaft | Target::All
    ) {
        let summary = run_raft(config.clone()).await;
        print_summary("raft", &summary, config.duration);
        raft_summary = Some(summary);
    }
    if matches!(config.target, Target::All) {
        let summary = run_redis(config.clone()).await;
        print_summary("redis", &summary, config.duration);
        redis_summary = Some(summary);
    }

    if let (Some(broker), Some(raft)) = (&broker_summary, &raft_summary) {
        print_ratio("broker", broker, "raft", raft);
    }
    if let (Some(redis), Some(raft)) = (&redis_summary, &raft_summary) {
        print_ratio("redis", redis, "raft", raft);
    }
}

impl Config {
    fn from_env() -> Self {
        let target_value = env_string("BENCH_TARGET").unwrap_or_else(|| DEFAULT_TARGET.into());
        let target = match parse_target(&target_value) {
            Ok(target) => target,
            Err(other) => panic!(
                "BENCH_TARGET must be redis, broker, raft, broker-raft, redis-raft, or all; got {other:?}"
            ),
        };
        let raft_route_value =
            env_string("BENCH_RAFT_ROUTE").unwrap_or_else(|| "round-robin".into());
        let raft_route = match parse_raft_route(&raft_route_value) {
            Ok(route) => route,
            Err(other) => panic!("BENCH_RAFT_ROUTE must be round-robin or leader; got {other:?}"),
        };
        let workers = env_parse("BENCH_WORKERS", 8).max(1);
        Self {
            redis_addr: env_string("BENCH_REDIS").unwrap_or_else(|| DEFAULT_REDIS_ADDR.into()),
            broker_addr: env_string("BENCH_BROKER").unwrap_or_else(|| DEFAULT_BROKER_ADDR.into()),
            raft_addrs: parse_endpoint_list(
                &env_string("BENCH_RAFT").unwrap_or_else(|| DEFAULT_RAFT_ADDR.into()),
            ),
            raft_route,
            workers,
            keys: env_parse("BENCH_KEYS", workers * 16).max(1),
            duration: Duration::from_millis(env_parse("BENCH_DURATION_MS", 10_000)),
            ttl_ms: env_parse("BENCH_TTL_MS", 5_000),
            io_timeout: Duration::from_millis(env_parse(
                "BENCH_IO_TIMEOUT_MS",
                DEFAULT_IO_TIMEOUT_MS,
            )),
            target,
            auth_token: env_string("BENCH_HTTP_AUTH_TOKEN")
                .or_else(|| env_string("BENCH_RAFT_AUTH_TOKEN"))
                .or_else(|| env_string("LMX_LIVE_RAFT_AUTH_TOKEN")),
            http_keep_alive: env_bool("BENCH_HTTP_KEEPALIVE", false),
        }
    }
}

fn parse_target(value: &str) -> Result<Target, String> {
    match value.trim() {
        "redis" => Ok(Target::Redis),
        "broker" => Ok(Target::Broker),
        "raft" => Ok(Target::Raft),
        "broker-raft" | "brokervsraft" | "broker_vs_raft" => Ok(Target::BrokerRaft),
        "redis-raft" | "redis_vs_raft" | "both" => Ok(Target::RedisRaft),
        "all" => Ok(Target::All),
        other => Err(other.to_string()),
    }
}

fn parse_raft_route(value: &str) -> Result<RaftRoute, String> {
    match value.trim() {
        "round-robin" | "round_robin" | "rr" | "lb" => Ok(RaftRoute::RoundRobin),
        "leader" | "leader-preferred" | "leader_preferred" => Ok(RaftRoute::Leader),
        other => Err(other.to_string()),
    }
}

impl RaftRoute {
    fn label(self) -> &'static str {
        match self {
            Self::RoundRobin => "round-robin",
            Self::Leader => "leader",
        }
    }
}

async fn run_redis(config: Config) -> Summary {
    let barrier = Arc::new(Barrier::new(config.workers));
    let deadline = Instant::now() + config.duration;
    let mut handles = Vec::new();
    for worker_id in 0..config.workers {
        let cfg = config.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            redis_worker(cfg, worker_id, deadline).await
        }));
    }
    collect(handles).await
}

async fn redis_worker(config: Config, worker_id: usize, deadline: Instant) -> WorkerStats {
    let mut stats = WorkerStats::default();
    let mut conn = match RedisConn::connect(&config.redis_addr, config.io_timeout).await {
        Ok(conn) => conn,
        Err(err) => {
            eprintln!("redis worker {worker_id} connect error: {err}");
            stats.errors += 1;
            return stats;
        }
    };
    let mut seq = 0u64;
    let mut rng = worker_id as u64 + 1;
    while Instant::now() < deadline {
        seq += 1;
        let key = bench_key("redis", next_key(&mut rng, config.keys));
        let token = format!("{worker_id}-{seq}-{rng}");
        let start = Instant::now();
        match redis_lock_cycle(&mut conn, &key, &token, config.ttl_ms, config.io_timeout).await {
            Ok(true) => {
                stats.ok += 1;
                stats.latencies_us.push(start.elapsed().as_micros() as u64);
            }
            Ok(false) => stats.not_acquired += 1,
            Err(err) => {
                stats.errors += 1;
                eprintln!("redis worker {worker_id} error: {err}");
                match RedisConn::connect(&config.redis_addr, config.io_timeout).await {
                    Ok(new_conn) => conn = new_conn,
                    Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
                }
            }
        }
    }
    stats
}

async fn redis_lock_cycle(
    conn: &mut RedisConn,
    key: &str,
    token: &str,
    ttl_ms: u64,
    io_timeout: Duration,
) -> Result<bool, String> {
    let set = conn
        .command(
            &["SET", key, token, "NX", "PX", &ttl_ms.to_string()],
            io_timeout,
        )
        .await?;
    match set {
        RedisValue::Simple(s) if s == "OK" => {}
        RedisValue::Bulk(None) => return Ok(false),
        RedisValue::Error(err) => return Err(err),
        other => return Err(format!("unexpected SET response: {other:?}")),
    }
    let unlock = conn
        .command(&["EVAL", REDIS_UNLOCK_LUA, "1", key, token], io_timeout)
        .await?;
    match unlock {
        RedisValue::Integer(1) => Ok(true),
        RedisValue::Integer(0) => Ok(false),
        RedisValue::Error(err) => Err(err),
        other => Err(format!("unexpected unlock response: {other:?}")),
    }
}

async fn run_broker(config: Config) -> Summary {
    let endpoint = config.broker_addr.clone();
    run_http_target(config, "broker", vec![endpoint]).await
}

async fn run_raft(config: Config) -> Summary {
    let endpoints = raft_benchmark_endpoints(&config).await;
    run_http_target(config, "raft", endpoints).await
}

async fn raft_benchmark_endpoints(config: &Config) -> Vec<String> {
    match config.raft_route {
        RaftRoute::RoundRobin => config.raft_addrs.clone(),
        RaftRoute::Leader => match find_ready_raft_leader(config).await {
            Some(endpoint) => {
                println!("raft leader route selected {endpoint}");
                vec![endpoint]
            }
            None => {
                eprintln!(
                    "BENCH_RAFT_ROUTE=leader could not find a /raft/leaderz endpoint; falling back to round-robin configured endpoints"
                );
                config.raft_addrs.clone()
            }
        },
    }
}

async fn find_ready_raft_leader(config: &Config) -> Option<String> {
    for endpoint in &config.raft_addrs {
        match http_status(
            endpoint,
            "GET",
            "/raft/leaderz",
            config.auth_token.as_deref(),
            config.io_timeout,
        )
        .await
        {
            Ok(200) => return Some(endpoint.clone()),
            Ok(_) => continue,
            Err(err) => {
                eprintln!("leader probe {endpoint}/raft/leaderz failed: {err}");
            }
        }
    }
    None
}

async fn run_http_target(config: Config, name: &'static str, endpoints: Vec<String>) -> Summary {
    let barrier = Arc::new(Barrier::new(config.workers));
    let deadline = Instant::now() + config.duration;
    let endpoints = Arc::new(endpoints);
    let mut handles = Vec::new();
    for worker_id in 0..config.workers {
        let cfg = config.clone();
        let barrier = barrier.clone();
        let endpoints = endpoints.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            http_worker(cfg, name, endpoints, worker_id, deadline).await
        }));
    }
    collect(handles).await
}

async fn http_worker(
    config: Config,
    name: &str,
    endpoints: Arc<Vec<String>>,
    worker_id: usize,
    deadline: Instant,
) -> WorkerStats {
    let mut stats = WorkerStats::default();
    let mut seq = 0u64;
    let mut rng = worker_id as u64 + 17;
    let mut client = HttpWorkerClient::new(
        config.http_keep_alive,
        config.auth_token.clone(),
        config.io_timeout,
    );
    while Instant::now() < deadline {
        seq += 1;
        let key = bench_key(name, next_key(&mut rng, config.keys));
        let (acquire_endpoint, release_endpoint) = endpoints_for_cycle(&endpoints, worker_id, seq);
        let start = Instant::now();
        match http_lock_cycle(
            &mut client,
            &config,
            acquire_endpoint,
            release_endpoint,
            &key,
        )
        .await
        {
            Ok(true) => {
                stats.ok += 1;
                stats.latencies_us.push(start.elapsed().as_micros() as u64);
            }
            Ok(false) => stats.not_acquired += 1,
            Err(err) => {
                stats.errors += 1;
                eprintln!("{name} worker {worker_id} op {seq} error: {err}");
            }
        }
    }
    stats
}

struct HttpWorkerClient {
    keep_alive: bool,
    auth_token: Option<String>,
    io_timeout: Duration,
    connections: BTreeMap<String, HttpConn>,
}

impl HttpWorkerClient {
    fn new(keep_alive: bool, auth_token: Option<String>, io_timeout: Duration) -> Self {
        Self {
            keep_alive,
            auth_token,
            io_timeout,
            connections: BTreeMap::new(),
        }
    }

    async fn json(
        &mut self,
        endpoint: &str,
        method: &str,
        path: &str,
        body: Option<Value>,
    ) -> Result<(u16, Value), String> {
        let (status, text) = if self.keep_alive {
            self.request_keep_alive(endpoint, method, path, body)
                .await?
        } else {
            timeout(
                self.io_timeout,
                http_request(endpoint, method, path, body, self.auth_token.as_deref()),
            )
            .await
            .map_err(|_| {
                format!(
                    "HTTP {method} {path} to {endpoint} timed out after {:?}",
                    self.io_timeout
                )
            })??
        };
        let parsed = serde_json::from_str(&text).map_err(|err| {
            format!("failed to parse HTTP JSON status={status}: {err}; body={text:?}")
        })?;
        if status / 100 == 2 {
            Ok((status, parsed))
        } else {
            Err(format!("HTTP {status}: {parsed:?}"))
        }
    }

    async fn request_keep_alive(
        &mut self,
        endpoint: &str,
        method: &str,
        path: &str,
        body: Option<Value>,
    ) -> Result<(u16, String), String> {
        let connection = match self.connections.get_mut(endpoint) {
            Some(connection) => connection,
            None => {
                let connection = HttpConn::connect(endpoint, self.io_timeout).await?;
                self.connections.insert(endpoint.to_string(), connection);
                self.connections
                    .get_mut(endpoint)
                    .expect("inserted HTTP connection")
            }
        };
        match connection
            .request(
                method,
                path,
                body,
                self.auth_token.as_deref(),
                self.io_timeout,
            )
            .await
        {
            Ok(response) => Ok(response),
            Err(err) => {
                self.connections.remove(endpoint);
                Err(err)
            }
        }
    }
}

fn parse_endpoint_list(value: &str) -> Vec<String> {
    let endpoints = value
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if endpoints.is_empty() {
        vec![DEFAULT_RAFT_ADDR.into()]
    } else {
        endpoints
    }
}

fn endpoint_for(endpoints: &[String], worker_id: usize, seq: u64) -> &str {
    let index = worker_id.wrapping_add(seq as usize).wrapping_sub(1) % endpoints.len().max(1);
    endpoints
        .get(index)
        .map(String::as_str)
        .unwrap_or(DEFAULT_RAFT_ADDR)
}

fn endpoints_for_cycle(endpoints: &[String], worker_id: usize, cycle_seq: u64) -> (&str, &str) {
    (
        endpoint_for(
            endpoints,
            worker_id,
            cycle_seq.saturating_mul(2).saturating_sub(1),
        ),
        endpoint_for(endpoints, worker_id, cycle_seq.saturating_mul(2)),
    )
}

async fn http_lock_cycle(
    client: &mut HttpWorkerClient,
    config: &Config,
    acquire_endpoint: &str,
    release_endpoint: &str,
    key: &str,
) -> Result<bool, String> {
    let (_, acquire) = client
        .json(
            acquire_endpoint,
            "POST",
            "/v1/lock",
            Some(json!({"key": key, "ttlMs": config.ttl_ms})),
        )
        .await?;
    if acquire["acquired"] != true {
        return Ok(false);
    }
    let lock_uuid = acquire["lockUuid"]
        .as_str()
        .ok_or_else(|| format!("missing lockUuid in acquire response: {acquire:?}"))?;
    let (_, release) = client
        .json(
            release_endpoint,
            "POST",
            "/v1/unlock",
            Some(json!({"key": key, "lockUuid": lock_uuid})),
        )
        .await?;
    if release["unlocked"] == true {
        Ok(true)
    } else {
        Err(format!("unlock failed: {release:?}"))
    }
}

async fn collect(handles: Vec<tokio::task::JoinHandle<WorkerStats>>) -> Summary {
    let mut summary = Summary::default();
    for handle in handles {
        match handle.await {
            Ok(stats) => summary.add(stats),
            Err(err) => {
                eprintln!("worker join error: {err}");
                summary.errors += 1;
            }
        }
    }
    summary
}

fn print_summary(name: &str, summary: &Summary, duration: Duration) {
    let mut latencies = summary.latencies_us.clone();
    latencies.sort_unstable();
    let throughput = summary.ok as f64 / duration.as_secs_f64();
    let avg = if latencies.is_empty() {
        0.0
    } else {
        latencies.iter().sum::<u64>() as f64 / latencies.len() as f64 / 1000.0
    };
    println!(
        "{name:5} total={:>8} ops/s={:>10.0} avg_ms={:>8.3} p50_ms={:>8.3} p95_ms={:>8.3} p99_ms={:>8.3} max_ms={:>8.3} not_acquired={} errors={}",
        summary.ok,
        throughput,
        avg,
        percentile_ms(&latencies, 0.50),
        percentile_ms(&latencies, 0.95),
        percentile_ms(&latencies, 0.99),
        latencies.last().copied().unwrap_or(0) as f64 / 1000.0,
        summary.not_acquired,
        summary.errors,
    );
}

fn print_ratio(a_name: &str, a: &Summary, b_name: &str, b: &Summary) {
    if b.ok == 0 {
        println!("[ratio] {a_name}/{b_name}: unavailable because {b_name} completed 0 ops");
        return;
    }
    println!(
        "[ratio] {a_name}/{b_name} throughput = {:.2}x ({} / {} successful cycles)",
        a.ok as f64 / b.ok as f64,
        a.ok,
        b.ok
    );
}

fn percentile_ms(values: &[u64], p: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let idx = ((values.len() - 1) as f64 * p).round() as usize;
    values[idx] as f64 / 1000.0
}

fn bench_key(prefix: &str, idx: usize) -> String {
    format!("lmx-bench-{prefix}-{idx}")
}

fn next_key(state: &mut u64, keys: usize) -> usize {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1);
    ((*state >> 32) as usize) % keys
}

fn env_string(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_parse<T>(key: &str, default: T) -> T
where
    T: std::str::FromStr,
{
    env_string(key)
        .and_then(|value| value.parse::<T>().ok())
        .unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    env_string(key)
        .and_then(|value| parse_bool(&value))
        .unwrap_or(default)
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Some(true),
        "0" | "false" | "no" | "n" | "off" => Some(false),
        _ => None,
    }
}

fn print_usage() {
    println!(
        "Rough Redis / Broker / BrokerRaft acquire+release benchmark.\n\
\n\
Configuration is environment driven:\n\
  BENCH_TARGET=redis|broker|raft|broker-raft|redis-raft|all\n\
  BENCH_REDIS=127.0.0.1:6379\n\
  BENCH_BROKER=127.0.0.1:6971\n\
  BENCH_RAFT=127.0.0.1:6972[,127.0.0.1:6973,...]\n\
  BENCH_RAFT_ROUTE=round-robin|leader\n\
  BENCH_WORKERS=8\n\
  BENCH_KEYS=128\n\
  BENCH_DURATION_MS=10000\n\
  BENCH_TTL_MS=5000\n\
  BENCH_IO_TIMEOUT_MS=5000\n\
  BENCH_HTTP_AUTH_TOKEN=<token>\n\
  BENCH_HTTP_KEEPALIVE=false\n\
\n\
Example:\n\
  BENCH_TARGET=broker-raft BENCH_BROKER=127.0.0.1:6971 BENCH_RAFT=127.0.0.1:6972 \\\n\
    cargo run --release --no-default-features --example redis_vs_raft_bench"
    );
}

async fn http_status(
    endpoint: &str,
    method: &str,
    path: &str,
    auth_token: Option<&str>,
    io_timeout: Duration,
) -> Result<u16, String> {
    let (status, _) = timeout(
        io_timeout,
        http_request(endpoint, method, path, None, auth_token),
    )
    .await
    .map_err(|_| format!("HTTP {method} {path} to {endpoint} timed out after {io_timeout:?}"))??;
    Ok(status)
}

async fn http_request(
    endpoint: &str,
    method: &str,
    path: &str,
    body: Option<Value>,
    auth_token: Option<&str>,
) -> Result<(u16, String), String> {
    let (host, port) = parse_host_port(endpoint)?;
    let body = body
        .map(|value| serde_json::to_vec(&value).expect("JSON encode"))
        .unwrap_or_default();
    let auth = auth_token
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

    let mut stream = TcpStream::connect((host.as_str(), port))
        .await
        .map_err(|err| format!("connect {endpoint}: {err}"))?;
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|err| err.to_string())?;
    stream
        .write_all(&body)
        .await
        .map_err(|err| err.to_string())?;
    stream.flush().await.map_err(|err| err.to_string())?;

    let mut raw = String::new();
    stream
        .read_to_string(&mut raw)
        .await
        .map_err(|err| err.to_string())?;
    let status = raw
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| format!("missing HTTP status line: {raw:?}"))?;
    let body = raw
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or_default();
    Ok((status, body))
}

struct HttpConn {
    host: String,
    reader: BufReader<TcpStream>,
}

impl HttpConn {
    async fn connect(endpoint: &str, io_timeout: Duration) -> Result<Self, String> {
        let (host, port) = parse_host_port(endpoint)?;
        let stream = timeout(io_timeout, TcpStream::connect((host.as_str(), port)))
            .await
            .map_err(|_| format!("connect {endpoint} timed out after {io_timeout:?}"))?
            .map_err(|err| format!("connect {endpoint}: {err}"))?;
        Ok(Self {
            host,
            reader: BufReader::new(stream),
        })
    }

    async fn request(
        &mut self,
        method: &str,
        path: &str,
        body: Option<Value>,
        auth_token: Option<&str>,
        io_timeout: Duration,
    ) -> Result<(u16, String), String> {
        let body = body
            .map(|value| serde_json::to_vec(&value).expect("JSON encode"))
            .unwrap_or_default();
        let auth = auth_token
            .map(|token| format!("Authorization: Bearer {token}\r\n"))
            .unwrap_or_default();
        let request = format!(
            "{method} {path} HTTP/1.1\r\n\
             Host: {}\r\n\
             Connection: keep-alive\r\n\
             Content-Type: application/json\r\n\
             {auth}\
             Content-Length: {}\r\n\
             \r\n",
            self.host,
            body.len()
        );

        timeout(io_timeout, async {
            self.reader
                .get_mut()
                .write_all(request.as_bytes())
                .await
                .map_err(|err| err.to_string())?;
            self.reader
                .get_mut()
                .write_all(&body)
                .await
                .map_err(|err| err.to_string())?;
            self.reader
                .get_mut()
                .flush()
                .await
                .map_err(|err| err.to_string())?;
            read_http_response(&mut self.reader).await
        })
        .await
        .map_err(|_| format!("HTTP {method} {path} timed out after {io_timeout:?}"))?
    }
}

async fn read_http_response(reader: &mut BufReader<TcpStream>) -> Result<(u16, String), String> {
    let mut status_line = String::new();
    let mut header_bytes = reader
        .read_line(&mut status_line)
        .await
        .map_err(|err| err.to_string())?;
    if header_bytes == 0 {
        return Err("connection closed before HTTP status line".into());
    }
    if header_bytes > MAX_HTTP_RESPONSE_HEADER_BYTES {
        return Err(format!(
            "HTTP response headers exceeded {MAX_HTTP_RESPONSE_HEADER_BYTES} bytes"
        ));
    }
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| format!("missing HTTP status line: {status_line:?}"))?;

    let mut content_length = None;
    let mut chunked = false;
    loop {
        let mut line = String::new();
        let read = reader
            .read_line(&mut line)
            .await
            .map_err(|err| err.to_string())?;
        if read == 0 {
            return Err("connection closed before HTTP response headers ended".into());
        }
        header_bytes = header_bytes
            .checked_add(read)
            .ok_or_else(|| "HTTP response header byte count overflowed".to_string())?;
        if header_bytes > MAX_HTTP_RESPONSE_HEADER_BYTES {
            return Err(format!(
                "HTTP response headers exceeded {MAX_HTTP_RESPONSE_HEADER_BYTES} bytes"
            ));
        }
        let header = line.trim_end_matches(['\r', '\n']);
        if header.is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                let len = value
                    .trim()
                    .parse::<usize>()
                    .map_err(|err| format!("invalid Content-Length {value:?}: {err}"))?;
                if len > MAX_HTTP_RESPONSE_BODY_BYTES {
                    return Err(format!(
                        "HTTP response body length {len} exceeds {MAX_HTTP_RESPONSE_BODY_BYTES} bytes"
                    ));
                }
                content_length = Some(len);
            } else if name.eq_ignore_ascii_case("transfer-encoding")
                && value
                    .split(',')
                    .any(|part| part.trim().eq_ignore_ascii_case("chunked"))
            {
                chunked = true;
            }
        }
    }

    let body = if chunked {
        read_chunked_body(reader).await?
    } else {
        let len = content_length
            .ok_or_else(|| "keep-alive HTTP response is missing Content-Length".to_string())?;
        let mut bytes = vec![0u8; len];
        reader
            .read_exact(&mut bytes)
            .await
            .map_err(|err| err.to_string())?;
        bytes
    };
    String::from_utf8(body)
        .map(|body| (status, body))
        .map_err(|err| err.to_string())
}

async fn read_chunked_body(reader: &mut BufReader<TcpStream>) -> Result<Vec<u8>, String> {
    let mut body = Vec::new();
    loop {
        let mut size_line = String::new();
        let read = reader
            .read_line(&mut size_line)
            .await
            .map_err(|err| err.to_string())?;
        if read == 0 {
            return Err("connection closed before chunk size".into());
        }
        let size_hex = size_line
            .trim_end_matches(['\r', '\n'])
            .split_once(';')
            .map(|(size, _)| size)
            .unwrap_or_else(|| size_line.trim_end_matches(['\r', '\n']));
        let size = usize::from_str_radix(size_hex, 16)
            .map_err(|err| format!("invalid chunk size {size_hex:?}: {err}"))?;
        if size == 0 {
            let mut trailer_bytes = 0usize;
            loop {
                let mut trailer = String::new();
                let read = reader
                    .read_line(&mut trailer)
                    .await
                    .map_err(|err| err.to_string())?;
                if read == 0 {
                    return Err("connection closed before chunk trailers ended".into());
                }
                trailer_bytes = trailer_bytes
                    .checked_add(read)
                    .ok_or_else(|| "HTTP chunk trailer byte count overflowed".to_string())?;
                if trailer_bytes > MAX_HTTP_RESPONSE_HEADER_BYTES {
                    return Err(format!(
                        "HTTP chunk trailers exceeded {MAX_HTTP_RESPONSE_HEADER_BYTES} bytes"
                    ));
                }
                if trailer.trim_end_matches(['\r', '\n']).is_empty() {
                    return Ok(body);
                }
            }
        }
        if body.len().saturating_add(size) > MAX_HTTP_RESPONSE_BODY_BYTES {
            return Err(format!(
                "chunked HTTP response body exceeds {MAX_HTTP_RESPONSE_BODY_BYTES} bytes"
            ));
        }
        let read_len = size
            .checked_add(2)
            .ok_or_else(|| "chunk size overflowed".to_string())?;
        let mut chunk = vec![0u8; read_len];
        reader
            .read_exact(&mut chunk)
            .await
            .map_err(|err| err.to_string())?;
        if !chunk.ends_with(b"\r\n") {
            return Err("chunk body missing CRLF terminator".into());
        }
        body.extend_from_slice(&chunk[..size]);
    }
}

fn parse_host_port(endpoint: &str) -> Result<(String, u16), String> {
    let endpoint = endpoint
        .strip_prefix("http://")
        .unwrap_or(endpoint)
        .trim_end_matches('/');
    let (host, port) = endpoint
        .rsplit_once(':')
        .ok_or_else(|| format!("endpoint must be host:port, got {endpoint:?}"))?;
    let port = port
        .parse::<u16>()
        .map_err(|_| format!("invalid port in endpoint {endpoint:?}"))?;
    Ok((host.to_string(), port))
}

struct RedisConn {
    reader: BufReader<TcpStream>,
}

impl RedisConn {
    async fn connect(addr: &str, io_timeout: Duration) -> Result<Self, String> {
        let stream = timeout(io_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| format!("connect {addr} timed out after {io_timeout:?}"))?
            .map_err(|err| format!("connect {addr}: {err}"))?;
        Ok(Self {
            reader: BufReader::new(stream),
        })
    }

    async fn command(&mut self, args: &[&str], io_timeout: Duration) -> Result<RedisValue, String> {
        let encoded = encode_resp(args);
        timeout(io_timeout, async {
            self.reader
                .get_mut()
                .write_all(&encoded)
                .await
                .map_err(|err| err.to_string())?;
            self.reader
                .get_mut()
                .flush()
                .await
                .map_err(|err| err.to_string())?;
            read_redis_value(&mut self.reader).await
        })
        .await
        .map_err(|_| format!("Redis command timed out after {io_timeout:?}"))?
    }
}

fn encode_resp(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        out.extend_from_slice(arg.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out
}

async fn read_redis_value(reader: &mut BufReader<TcpStream>) -> Result<RedisValue, String> {
    let mut prefix = [0u8; 1];
    reader
        .read_exact(&mut prefix)
        .await
        .map_err(|err| err.to_string())?;
    match prefix[0] {
        b'+' => Ok(RedisValue::Simple(read_line(reader).await?)),
        b'-' => Ok(RedisValue::Error(read_line(reader).await?)),
        b':' => read_line(reader)
            .await?
            .parse::<i64>()
            .map(RedisValue::Integer)
            .map_err(|err| err.to_string()),
        b'$' => {
            let len = read_line(reader)
                .await?
                .parse::<isize>()
                .map_err(|err| err.to_string())?;
            if len < 0 {
                return Ok(RedisValue::Bulk(None));
            }
            let mut data = vec![0u8; len as usize + 2];
            reader
                .read_exact(&mut data)
                .await
                .map_err(|err| err.to_string())?;
            data.truncate(len as usize);
            Ok(RedisValue::Bulk(Some(data)))
        }
        b'*' => {
            let len = read_line(reader)
                .await?
                .parse::<usize>()
                .map_err(|err| err.to_string())?;
            for _ in 0..len {
                let _ = Box::pin(read_redis_value(reader)).await?;
            }
            Ok(RedisValue::Array)
        }
        other => Err(format!("unknown Redis response prefix byte {other}")),
    }
}

async fn read_line(reader: &mut BufReader<TcpStream>) -> Result<String, String> {
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .await
        .map_err(|err| err.to_string())?;
    Ok(line.trim_end_matches(['\r', '\n']).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_broker_and_raft_endpoints_are_distinct() {
        assert_eq!(DEFAULT_BROKER_ADDR, "127.0.0.1:6971");
        assert_eq!(DEFAULT_RAFT_ADDR, "127.0.0.1:6972");
        assert_ne!(
            DEFAULT_BROKER_ADDR, DEFAULT_RAFT_ADDR,
            "default broker-vs-raft runs must not point both targets at the same HTTP service"
        );
    }

    #[test]
    fn parse_target_accepts_documented_aliases() {
        assert_eq!(parse_target("redis").unwrap(), Target::Redis);
        assert_eq!(parse_target("broker").unwrap(), Target::Broker);
        assert_eq!(parse_target("raft").unwrap(), Target::Raft);
        assert_eq!(parse_target("broker-raft").unwrap(), Target::BrokerRaft);
        assert_eq!(parse_target("brokervsraft").unwrap(), Target::BrokerRaft);
        assert_eq!(parse_target("broker_vs_raft").unwrap(), Target::BrokerRaft);
        assert_eq!(parse_target("redis-raft").unwrap(), Target::RedisRaft);
        assert_eq!(parse_target("redis_vs_raft").unwrap(), Target::RedisRaft);
        assert_eq!(parse_target("both").unwrap(), Target::RedisRaft);
        assert_eq!(parse_target(DEFAULT_TARGET).unwrap(), Target::RedisRaft);
        assert_eq!(parse_target("all").unwrap(), Target::All);
        assert!(parse_target("brokerraft").is_err());
    }

    #[test]
    fn parse_raft_route_accepts_lb_and_leader_modes() {
        assert_eq!(
            parse_raft_route("round-robin").unwrap(),
            RaftRoute::RoundRobin
        );
        assert_eq!(
            parse_raft_route("round_robin").unwrap(),
            RaftRoute::RoundRobin
        );
        assert_eq!(parse_raft_route("rr").unwrap(), RaftRoute::RoundRobin);
        assert_eq!(parse_raft_route("lb").unwrap(), RaftRoute::RoundRobin);
        assert_eq!(parse_raft_route("leader").unwrap(), RaftRoute::Leader);
        assert_eq!(
            parse_raft_route("leader-preferred").unwrap(),
            RaftRoute::Leader
        );
        assert_eq!(
            parse_raft_route("leader_preferred").unwrap(),
            RaftRoute::Leader
        );
        assert!(parse_raft_route("primary").is_err());
    }

    #[test]
    fn parse_host_port_accepts_plain_or_http_endpoint() {
        assert_eq!(
            parse_host_port("127.0.0.1:6972").unwrap(),
            ("127.0.0.1".to_string(), 6972)
        );
        assert_eq!(
            parse_host_port("http://localhost:6972/").unwrap(),
            ("localhost".to_string(), 6972)
        );
    }

    #[test]
    fn parse_endpoint_list_trims_commas_and_defaults_when_empty() {
        assert_eq!(
            parse_endpoint_list("127.0.0.1:6972, 127.0.0.1:6973,,127.0.0.1:6974"),
            vec![
                "127.0.0.1:6972".to_string(),
                "127.0.0.1:6973".to_string(),
                "127.0.0.1:6974".to_string(),
            ]
        );
        assert_eq!(
            parse_endpoint_list(" , "),
            vec![DEFAULT_RAFT_ADDR.to_string()]
        );
    }

    #[test]
    fn endpoint_for_round_robins_by_worker_and_http_request() {
        let endpoints = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        assert_eq!(endpoint_for(&endpoints, 0, 1), "a");
        assert_eq!(endpoint_for(&endpoints, 0, 2), "b");
        assert_eq!(endpoint_for(&endpoints, 0, 3), "c");
        assert_eq!(endpoint_for(&endpoints, 1, 1), "b");
        assert_eq!(endpoint_for(&endpoints, 2, 1), "c");
    }

    #[test]
    fn endpoints_for_cycle_can_split_acquire_and_release() {
        let endpoints = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        assert_eq!(endpoints_for_cycle(&endpoints, 0, 1), ("a", "b"));
        assert_eq!(endpoints_for_cycle(&endpoints, 0, 2), ("c", "a"));
        assert_eq!(endpoints_for_cycle(&endpoints, 1, 1), ("b", "c"));

        let single = vec!["leader".to_string()];
        assert_eq!(
            endpoints_for_cycle(&single, 4, 99),
            ("leader", "leader"),
            "single endpoint mode remains leader-preferred"
        );
    }

    #[test]
    fn parse_bool_accepts_common_env_values() {
        assert_eq!(parse_bool("true"), Some(true));
        assert_eq!(parse_bool("1"), Some(true));
        assert_eq!(parse_bool("on"), Some(true));
        assert_eq!(parse_bool("false"), Some(false));
        assert_eq!(parse_bool("0"), Some(false));
        assert_eq!(parse_bool("off"), Some(false));
        assert_eq!(parse_bool("maybe"), None);
    }

    #[tokio::test]
    async fn keep_alive_http_client_reuses_one_socket_for_multiple_json_requests() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test HTTP server");
        let endpoint = listener.local_addr().expect("server addr").to_string();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept HTTP client");
            let mut reader = BufReader::new(stream);
            let mut paths = Vec::new();
            for idx in 0..2 {
                let (path, body) = read_test_http_request(&mut reader).await;
                paths.push((path, body));
                let response = json!({"ok": true, "idx": idx});
                let body = serde_json::to_vec(&response).expect("response JSON");
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
                    body.len()
                );
                reader
                    .get_mut()
                    .write_all(header.as_bytes())
                    .await
                    .expect("write response header");
                reader
                    .get_mut()
                    .write_all(&body)
                    .await
                    .expect("write response body");
                reader.get_mut().flush().await.expect("flush response");
            }
            paths
        });

        let mut client = HttpWorkerClient::new(true, None, Duration::from_secs(1));
        let (_, first) = client
            .json(&endpoint, "POST", "/first", Some(json!({"n": 1})))
            .await
            .expect("first keep-alive request");
        let (_, second) = client
            .json(&endpoint, "POST", "/second", Some(json!({"n": 2})))
            .await
            .expect("second keep-alive request");

        assert_eq!(first["idx"], 0);
        assert_eq!(second["idx"], 1);
        assert_eq!(client.connections.len(), 1);
        let paths = server.await.expect("server task");
        assert_eq!(paths[0], ("/first".to_string(), r#"{"n":1}"#.to_string()));
        assert_eq!(paths[1], ("/second".to_string(), r#"{"n":2}"#.to_string()));
    }

    #[tokio::test]
    async fn keep_alive_http_client_reads_chunked_json_response() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test HTTP server");
        let endpoint = listener.local_addr().expect("server addr").to_string();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept HTTP client");
            let mut reader = BufReader::new(stream);
            let _ = read_test_http_request(&mut reader).await;
            let response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n7\r\n{\"ok\":t\r\n4\r\nrue}\r\n0\r\n\r\n";
            reader
                .get_mut()
                .write_all(response)
                .await
                .expect("write chunked response");
            reader.get_mut().flush().await.expect("flush response");
            stream = reader.into_inner();
            stream.shutdown().await.expect("shutdown server stream");
        });

        let mut client = HttpWorkerClient::new(true, None, Duration::from_secs(1));
        let (_, response) = client
            .json(&endpoint, "POST", "/chunked", Some(json!({"n": 1})))
            .await
            .expect("chunked keep-alive request");

        assert_eq!(response["ok"], true);
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn keep_alive_http_client_rejects_oversized_content_length() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test HTTP server");
        let endpoint = listener.local_addr().expect("server addr").to_string();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept HTTP client");
            let mut reader = BufReader::new(stream);
            let _ = read_test_http_request(&mut reader).await;
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
                MAX_HTTP_RESPONSE_BODY_BYTES + 1
            );
            reader
                .get_mut()
                .write_all(header.as_bytes())
                .await
                .expect("write oversized response header");
            reader.get_mut().flush().await.expect("flush response");
            stream = reader.into_inner();
            stream.shutdown().await.expect("shutdown server stream");
        });

        let mut client = HttpWorkerClient::new(true, None, Duration::from_secs(1));
        let err = client
            .json(&endpoint, "POST", "/oversized", Some(json!({"n": 1})))
            .await
            .expect_err("oversized HTTP body must be rejected before allocation");

        assert!(err.contains("exceeds"), "unexpected error: {err}");
        assert!(
            client.connections.is_empty(),
            "failed keep-alive request should drop the suspect connection"
        );
        server.await.expect("server task");
    }

    async fn read_test_http_request(reader: &mut BufReader<TcpStream>) -> (String, String) {
        let mut request_line = String::new();
        reader
            .read_line(&mut request_line)
            .await
            .expect("read request line");
        let path = request_line
            .split_whitespace()
            .nth(1)
            .expect("request path")
            .to_string();
        let mut content_length = 0usize;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).await.expect("read header");
            let header = line.trim_end_matches(['\r', '\n']);
            if header.is_empty() {
                break;
            }
            if let Some((name, value)) = header.split_once(':') {
                if name.eq_ignore_ascii_case("content-length") {
                    content_length = value.trim().parse().expect("content length");
                }
            }
        }
        let mut body = vec![0u8; content_length];
        reader.read_exact(&mut body).await.expect("read body");
        (path, String::from_utf8(body).expect("request body utf8"))
    }
}
