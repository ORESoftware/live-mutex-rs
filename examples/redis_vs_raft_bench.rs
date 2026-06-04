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

const REDIS_UNLOCK_LUA: &str =
    "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    Redis,
    Broker,
    Raft,
    BrokerRaft,
    Both,
    All,
}

#[derive(Debug, Clone)]
struct Config {
    redis_addr: String,
    broker_addr: String,
    raft_addr: String,
    workers: usize,
    keys: usize,
    duration: Duration,
    ttl_ms: u64,
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
    let config = Config::from_env();
    println!(
        "workers={} keys={} duration_ms={} ttl_ms={} redis={} broker={} raft={}",
        config.workers,
        config.keys,
        config.duration.as_millis(),
        config.ttl_ms,
        config.redis_addr,
        config.broker_addr,
        config.raft_addr
    );

    let mut redis_summary = None;
    let mut broker_summary = None;
    let mut raft_summary = None;

    if matches!(config.target, Target::Redis | Target::Both) {
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
        Target::Raft | Target::Both | Target::BrokerRaft | Target::All
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
        let target = match env_string("BENCH_TARGET")
            .unwrap_or_else(|| "both".into())
            .as_str()
        {
            "redis" => Target::Redis,
            "broker" => Target::Broker,
            "raft" => Target::Raft,
            "broker-raft" | "brokervsraft" | "broker_vs_raft" => Target::BrokerRaft,
            "both" => Target::Both,
            "all" => Target::All,
            other => panic!(
                "BENCH_TARGET must be redis, broker, raft, broker-raft, both, or all; got {other:?}"
            ),
        };
        let workers = env_parse("BENCH_WORKERS", 8).max(1);
        Self {
            redis_addr: env_string("BENCH_REDIS").unwrap_or_else(|| "127.0.0.1:6379".into()),
            broker_addr: env_string("BENCH_BROKER").unwrap_or_else(|| "127.0.0.1:6971".into()),
            raft_addr: env_string("BENCH_RAFT").unwrap_or_else(|| "127.0.0.1:6971".into()),
            workers,
            keys: env_parse("BENCH_KEYS", workers * 16).max(1),
            duration: Duration::from_millis(env_parse("BENCH_DURATION_MS", 10_000)),
            ttl_ms: env_parse("BENCH_TTL_MS", 5_000),
            target,
            auth_token: env_string("BENCH_HTTP_AUTH_TOKEN")
                .or_else(|| env_string("BENCH_RAFT_AUTH_TOKEN"))
                .or_else(|| env_string("LMX_LIVE_RAFT_AUTH_TOKEN")),
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
    let mut conn = match RedisConn::connect(&config.redis_addr).await {
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
        match redis_lock_cycle(&mut conn, &key, &token, config.ttl_ms).await {
            Ok(true) => {
                stats.ok += 1;
                stats.latencies_us.push(start.elapsed().as_micros() as u64);
            }
            Ok(false) => stats.not_acquired += 1,
            Err(err) => {
                stats.errors += 1;
                eprintln!("redis worker {worker_id} error: {err}");
                match RedisConn::connect(&config.redis_addr).await {
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
) -> Result<bool, String> {
    let set = conn
        .command(&["SET", key, token, "NX", "PX", &ttl_ms.to_string()])
        .await?;
    match set {
        RedisValue::Simple(s) if s == "OK" => {}
        RedisValue::Bulk(None) => return Ok(false),
        RedisValue::Error(err) => return Err(err),
        other => return Err(format!("unexpected SET response: {other:?}")),
    }
    let unlock = conn
        .command(&["EVAL", REDIS_UNLOCK_LUA, "1", key, token])
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
    run_http_target(config, "broker", endpoint).await
}

async fn run_raft(config: Config) -> Summary {
    let endpoint = config.raft_addr.clone();
    run_http_target(config, "raft", endpoint).await
}

async fn run_http_target(config: Config, name: &'static str, endpoint: String) -> Summary {
    let barrier = Arc::new(Barrier::new(config.workers));
    let deadline = Instant::now() + config.duration;
    let mut handles = Vec::new();
    for worker_id in 0..config.workers {
        let cfg = config.clone();
        let barrier = barrier.clone();
        let endpoint = endpoint.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            http_worker(cfg, name, endpoint, worker_id, deadline).await
        }));
    }
    collect(handles).await
}

async fn http_worker(
    config: Config,
    name: &str,
    endpoint: String,
    worker_id: usize,
    deadline: Instant,
) -> WorkerStats {
    let mut stats = WorkerStats::default();
    let mut seq = 0u64;
    let mut rng = worker_id as u64 + 17;
    while Instant::now() < deadline {
        seq += 1;
        let key = bench_key(name, next_key(&mut rng, config.keys));
        let start = Instant::now();
        match http_lock_cycle(&config, &endpoint, &key).await {
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

async fn http_lock_cycle(config: &Config, endpoint: &str, key: &str) -> Result<bool, String> {
    let (_, acquire) = http_json(
        endpoint,
        "POST",
        "/v1/lock",
        Some(json!({"key": key, "ttlMs": config.ttl_ms})),
        config.auth_token.as_deref(),
    )
    .await?;
    if acquire["acquired"] != true {
        return Ok(false);
    }
    let lock_uuid = acquire["lockUuid"]
        .as_str()
        .ok_or_else(|| format!("missing lockUuid in acquire response: {acquire:?}"))?;
    let (_, release) = http_json(
        endpoint,
        "POST",
        "/v1/unlock",
        Some(json!({"key": key, "lockUuid": lock_uuid})),
        config.auth_token.as_deref(),
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

async fn http_json(
    endpoint: &str,
    method: &str,
    path: &str,
    body: Option<Value>,
    auth_token: Option<&str>,
) -> Result<(u16, Value), String> {
    let (status, text) = http_request(endpoint, method, path, body, auth_token).await?;
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
    async fn connect(addr: &str) -> Result<Self, String> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|err| format!("connect {addr}: {err}"))?;
        Ok(Self {
            reader: BufReader::new(stream),
        })
    }

    async fn command(&mut self, args: &[&str]) -> Result<RedisValue, String> {
        let encoded = encode_resp(args);
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
