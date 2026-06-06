//! Rough Redis / Broker / BrokerRaft lock benchmark.
//!
//! This intentionally ignores fencing tokens. It measures successful
//! acquire+release cycles for:
//! - Redis: SET key token NX PX ttl, then EVAL compare-and-del.
//! - Broker: POST /v1/lock, then POST /v1/unlock against one regular broker.
//! - BrokerRaft: POST /v1/lock, then POST /v1/unlock.
//!
//! The HTTP paths use one short-lived connection per request, matching the
//! simple LB-facing API and avoiding extra client dependencies.

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
    while Instant::now() < deadline {
        seq += 1;
        let key = bench_key(name, next_key(&mut rng, config.keys));
        let (acquire_endpoint, release_endpoint) = endpoints_for_cycle(&endpoints, worker_id, seq);
        let start = Instant::now();
        match http_lock_cycle(&config, acquire_endpoint, release_endpoint, &key).await {
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
    config: &Config,
    acquire_endpoint: &str,
    release_endpoint: &str,
    key: &str,
) -> Result<bool, String> {
    let (_, acquire) = http_json(
        acquire_endpoint,
        "POST",
        "/v1/lock",
        Some(json!({"key": key, "ttlMs": config.ttl_ms})),
        config.auth_token.as_deref(),
        config.io_timeout,
    )
    .await?;
    if acquire["acquired"] != true {
        return Ok(false);
    }
    let lock_uuid = acquire["lockUuid"]
        .as_str()
        .ok_or_else(|| format!("missing lockUuid in acquire response: {acquire:?}"))?;
    let (_, release) = http_json(
        release_endpoint,
        "POST",
        "/v1/unlock",
        Some(json!({"key": key, "lockUuid": lock_uuid})),
        config.auth_token.as_deref(),
        config.io_timeout,
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

async fn http_json(
    endpoint: &str,
    method: &str,
    path: &str,
    body: Option<Value>,
    auth_token: Option<&str>,
    io_timeout: Duration,
) -> Result<(u16, Value), String> {
    let (status, text) = timeout(
        io_timeout,
        http_request(endpoint, method, path, body, auth_token),
    )
    .await
    .map_err(|_| format!("HTTP {method} {path} to {endpoint} timed out after {io_timeout:?}"))??;
    let parsed = serde_json::from_str(&text).map_err(|err| {
        format!("failed to parse HTTP JSON status={status}: {err}; body={text:?}")
    })?;
    if status / 100 == 2 {
        Ok((status, parsed))
    } else {
        Err(format!("HTTP {status}: {parsed:?}"))
    }
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
}
