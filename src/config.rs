//! Runtime configuration loading.
//!
//! The binary now supports an optional TOML file while preserving the existing
//! `LMX_*` environment-variable contract. File values provide defaults; env
//! vars win so Kubernetes and container deployments can keep their current
//! override style.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
#[cfg(feature = "tls")]
use tracing::warn;

use crate::broker::BrokerConfig;
use crate::broker_raft::{BrokerRaftConfig, BrokerRaftError, RaftPeerConfig};
use crate::server::ServerConfig;

pub const CONFIG_PATH_ENV: &str = "LMX_CONFIG";
pub const DEFAULT_CONFIG_PATHS: &[&str] = &["lmx.toml", "/etc/dd-rust-network-mutex/lmx.toml"];

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub server: ServerConfig,
    pub raft: BrokerRaftConfig,
    pub source_path: Option<PathBuf>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file `{path}`: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse config file `{path}`: {source}")]
    Parse {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("invalid socket address `{value}` from {from}")]
    InvalidSocketAddr { value: String, from: String },
    #[error(transparent)]
    Raft(#[from] BrokerRaftError),
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct ConfigFile {
    server: ServerFileConfig,
    broker: BrokerFileConfig,
    raft: RaftFileConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct ServerFileConfig {
    bind_host: Option<String>,
    tcp_port: Option<u16>,
    http_port: Option<u16>,
    disable_tcp: Option<bool>,
    disable_http: Option<bool>,
    uds_path: Option<PathBuf>,
    auth_token: Option<String>,
    status_port: Option<u16>,
    tcp_nodelay: Option<bool>,
    tcp_quickack: Option<bool>,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct BrokerFileConfig {
    default_ttl_ms: Option<u64>,
    max_lock_holders: Option<u32>,
    max_concurrency_cap: Option<u32>,
    ttl_sweep_interval_ms: Option<u64>,
    idle_key_grace_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
struct RaftFileConfig {
    enabled: Option<bool>,
    node_id: Option<String>,
    bind_addr: Option<String>,
    advertise_addr: Option<String>,
    data_dir: Option<PathBuf>,
    data_dir_lock: Option<bool>,
    heartbeat_interval_ms: Option<u64>,
    election_timeout_min_ms: Option<u64>,
    election_timeout_max_ms: Option<u64>,
    snapshot_interval_ms: Option<u64>,
    snapshot_max_log_entries: Option<u64>,
    snapshot_max_log_bytes: Option<u64>,
    trailing_log_entries: Option<u64>,
    append_entries_max_entries: Option<usize>,
    append_entries_max_bytes: Option<usize>,
    append_entries_max_inline_batches: Option<usize>,
    install_snapshot_chunk_bytes: Option<usize>,
    install_snapshot_max_staged_bytes: Option<u64>,
    install_snapshot_max_staged_transfers: Option<usize>,
    install_snapshot_stale_transfer_ms: Option<u64>,
    client_batch_max_entries: Option<usize>,
    client_pipeline_max_batches: Option<usize>,
    client_batch_max_pending: Option<usize>,
    client_batch_max_delay_ms: Option<u64>,
    client_response_cache_max_entries: Option<usize>,
    proxy_retry_budget_ms: Option<u64>,
    sync_log: Option<bool>,
    peer_token: Option<String>,
    peers: Vec<RaftPeerConfig>,
}

pub fn load_runtime_config() -> Result<RuntimeConfig, ConfigError> {
    crate::routine_id!("ddl-routine-config-load-runtime-1");
    let explicit = env_string(CONFIG_PATH_ENV).map(PathBuf::from);
    let (file, source_path) = load_config_file(explicit.as_deref())?;
    build_runtime_config(file, source_path)
}

fn load_config_file(
    explicit_path: Option<&Path>,
) -> Result<(ConfigFile, Option<PathBuf>), ConfigError> {
    crate::routine_id!("ddl-routine-config-load-file-1");
    if let Some(path) = explicit_path {
        return read_config_file(path).map(|file| (file, Some(path.to_path_buf())));
    }

    for candidate in DEFAULT_CONFIG_PATHS {
        let path = Path::new(candidate);
        if path.exists() {
            return read_config_file(path).map(|file| (file, Some(path.to_path_buf())));
        }
    }

    Ok((ConfigFile::default(), None))
}

fn read_config_file(path: &Path) -> Result<ConfigFile, ConfigError> {
    crate::routine_id!("ddl-routine-config-read-file-1");
    let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    toml::from_str(&text).map_err(|source| ConfigError::Parse {
        path: path.to_path_buf(),
        source,
    })
}

fn build_runtime_config(
    file: ConfigFile,
    source_path: Option<PathBuf>,
) -> Result<RuntimeConfig, ConfigError> {
    crate::routine_id!("ddl-routine-config-build-runtime-1");
    let broker = build_broker_config(&file.broker);
    let server = build_server_config(&file.server, broker.clone())?;
    let mut raft = build_raft_config(&file.raft)?;
    raft.broker = broker;
    raft.validate()?;
    Ok(RuntimeConfig {
        server,
        raft,
        source_path,
    })
}

fn build_server_config(
    file: &ServerFileConfig,
    broker: BrokerConfig,
) -> Result<ServerConfig, ConfigError> {
    crate::routine_id!("ddl-routine-config-build-server-1");
    let bind_host = env_string("LMX_BIND_HOST")
        .or_else(|| non_empty(file.bind_host.clone()))
        .unwrap_or_else(|| "0.0.0.0".into());
    let tcp_port = env_parse("LMX_TCP_PORT").or(file.tcp_port).unwrap_or(6970);
    let http_port = env_parse("LMX_HTTP_PORT")
        .or(file.http_port)
        .unwrap_or(6971);

    let disable_tcp = env_bool("LMX_DISABLE_TCP")
        .or(file.disable_tcp)
        .unwrap_or(false);
    let disable_http = env_bool("LMX_DISABLE_HTTP")
        .or(file.disable_http)
        .unwrap_or(false);

    let tcp_bind = if disable_tcp {
        None
    } else {
        Some(parse_addr(
            &format!("{bind_host}:{tcp_port}"),
            "server.bind_host/server.tcp_port",
        )?)
    };
    let http_bind = if disable_http {
        None
    } else {
        Some(parse_addr(
            &format!("{bind_host}:{http_port}"),
            "server.bind_host/server.http_port",
        )?)
    };

    let status_bind = env_parse("LMX_STATUS_PORT")
        .or(file.status_port)
        .map(|port| {
            parse_addr(
                &format!("{bind_host}:{port}"),
                "server.bind_host/server.status_port",
            )
        })
        .transpose()?;

    let uds_path = env_path("LMX_UDS_PATH").or_else(|| file.uds_path.clone());
    let auth_token = env_string("LMX_AUTH_TOKEN").or_else(|| non_empty(file.auth_token.clone()));

    Ok(ServerConfig {
        tcp_bind,
        uds_path,
        http_bind,
        auth_token,
        broker,
        tcp_nodelay: env_bool("LMX_TCP_NODELAY")
            .or(file.tcp_nodelay)
            .unwrap_or(true),
        tcp_quickack: env_bool("LMX_TCP_QUICKACK")
            .or(file.tcp_quickack)
            .unwrap_or(true),
        status_bind,
        #[cfg(feature = "tls")]
        tls: build_tls_config(file),
    })
}

fn build_broker_config(file: &BrokerFileConfig) -> BrokerConfig {
    crate::routine_id!("ddl-routine-config-build-broker-1");
    BrokerConfig {
        default_ttl: Duration::from_millis(
            env_parse("LMX_DEFAULT_TTL_MS")
                .or(file.default_ttl_ms)
                .unwrap_or(4000),
        ),
        max_lock_holders: env_parse("LMX_MAX_LOCK_HOLDERS")
            .or(file.max_lock_holders)
            .unwrap_or(1)
            .max(1),
        ttl_sweep_interval: Duration::from_millis(
            env_parse("LMX_TTL_SWEEP_INTERVAL_MS")
                .or(file.ttl_sweep_interval_ms)
                .unwrap_or(10),
        ),
        max_concurrency_cap: env_parse("LMX_MAX_CONCURRENCY_CAP")
            .or(file.max_concurrency_cap)
            .unwrap_or(crate::protocol::DEFAULT_MAX_CONCURRENCY_CAP)
            .max(1),
        idle_key_grace: Duration::from_millis(
            env_parse("LMX_IDLE_KEY_GRACE_MS")
                .or(file.idle_key_grace_ms)
                .unwrap_or(60_000),
        ),
    }
}

fn build_raft_config(file: &RaftFileConfig) -> Result<BrokerRaftConfig, ConfigError> {
    crate::routine_id!("ddl-routine-config-build-raft-1");
    let mut cfg = BrokerRaftConfig::default();
    cfg.enabled = env_bool("LMX_RAFT_ENABLED")
        .or(file.enabled)
        .unwrap_or(cfg.enabled);
    cfg.node_id = env_string("LMX_RAFT_NODE_ID")
        .or_else(|| non_empty(file.node_id.clone()))
        .unwrap_or(cfg.node_id);
    cfg.bind_addr = env_string("LMX_RAFT_BIND_ADDR")
        .or_else(|| non_empty(file.bind_addr.clone()))
        .map(|addr| parse_addr(&addr, "raft.bind_addr"))
        .transpose()?
        .or(cfg.bind_addr);
    cfg.advertise_addr = env_string("LMX_RAFT_ADVERTISE_ADDR")
        .or_else(|| non_empty(file.advertise_addr.clone()))
        .or(cfg.advertise_addr);
    cfg.data_dir = env_path("LMX_RAFT_DATA_DIR")
        .or_else(|| file.data_dir.clone())
        .unwrap_or(cfg.data_dir);
    cfg.data_dir_lock = env_bool("LMX_RAFT_DATA_DIR_LOCK")
        .or(file.data_dir_lock)
        .unwrap_or(cfg.data_dir_lock);
    cfg.heartbeat_interval = Duration::from_millis(
        env_parse("LMX_RAFT_HEARTBEAT_INTERVAL_MS")
            .or(file.heartbeat_interval_ms)
            .unwrap_or(cfg.heartbeat_interval.as_millis() as u64),
    );
    cfg.election_timeout_min = Duration::from_millis(
        env_parse("LMX_RAFT_ELECTION_TIMEOUT_MIN_MS")
            .or(file.election_timeout_min_ms)
            .unwrap_or(cfg.election_timeout_min.as_millis() as u64),
    );
    cfg.election_timeout_max = Duration::from_millis(
        env_parse("LMX_RAFT_ELECTION_TIMEOUT_MAX_MS")
            .or(file.election_timeout_max_ms)
            .unwrap_or(cfg.election_timeout_max.as_millis() as u64),
    );
    cfg.snapshot_interval = Duration::from_millis(
        env_parse("LMX_RAFT_SNAPSHOT_INTERVAL_MS")
            .or(file.snapshot_interval_ms)
            .unwrap_or(cfg.snapshot_interval.as_millis() as u64),
    );
    cfg.snapshot_max_log_entries = env_parse("LMX_RAFT_SNAPSHOT_MAX_LOG_ENTRIES")
        .or(file.snapshot_max_log_entries)
        .unwrap_or(cfg.snapshot_max_log_entries);
    cfg.snapshot_max_log_bytes = env_parse("LMX_RAFT_SNAPSHOT_MAX_LOG_BYTES")
        .or(file.snapshot_max_log_bytes)
        .unwrap_or(cfg.snapshot_max_log_bytes);
    cfg.trailing_log_entries = env_parse("LMX_RAFT_TRAILING_LOG_ENTRIES")
        .or(file.trailing_log_entries)
        .unwrap_or(cfg.trailing_log_entries);
    cfg.append_entries_max_entries = env_parse("LMX_RAFT_APPEND_ENTRIES_MAX_ENTRIES")
        .or(file.append_entries_max_entries)
        .unwrap_or(cfg.append_entries_max_entries);
    cfg.append_entries_max_bytes = env_parse("LMX_RAFT_APPEND_ENTRIES_MAX_BYTES")
        .or(file.append_entries_max_bytes)
        .unwrap_or(cfg.append_entries_max_bytes);
    cfg.append_entries_max_inline_batches = env_parse("LMX_RAFT_APPEND_ENTRIES_MAX_INLINE_BATCHES")
        .or(file.append_entries_max_inline_batches)
        .unwrap_or(cfg.append_entries_max_inline_batches);
    cfg.install_snapshot_chunk_bytes = env_parse("LMX_RAFT_INSTALL_SNAPSHOT_CHUNK_BYTES")
        .or(file.install_snapshot_chunk_bytes)
        .unwrap_or(cfg.install_snapshot_chunk_bytes);
    cfg.install_snapshot_max_staged_bytes = env_parse("LMX_RAFT_INSTALL_SNAPSHOT_MAX_STAGED_BYTES")
        .or(file.install_snapshot_max_staged_bytes)
        .unwrap_or(cfg.install_snapshot_max_staged_bytes);
    cfg.install_snapshot_max_staged_transfers =
        env_parse("LMX_RAFT_INSTALL_SNAPSHOT_MAX_STAGED_TRANSFERS")
            .or(file.install_snapshot_max_staged_transfers)
            .unwrap_or(cfg.install_snapshot_max_staged_transfers);
    cfg.install_snapshot_stale_transfer_after = Duration::from_millis(
        env_parse("LMX_RAFT_INSTALL_SNAPSHOT_STALE_TRANSFER_MS")
            .or(file.install_snapshot_stale_transfer_ms)
            .unwrap_or(cfg.install_snapshot_stale_transfer_after.as_millis() as u64),
    );
    cfg.client_batch_max_entries = env_parse("LMX_RAFT_CLIENT_BATCH_MAX_ENTRIES")
        .or(file.client_batch_max_entries)
        .unwrap_or(cfg.client_batch_max_entries);
    cfg.client_pipeline_max_batches = env_parse("LMX_RAFT_CLIENT_PIPELINE_MAX_BATCHES")
        .or(file.client_pipeline_max_batches)
        .unwrap_or(cfg.client_pipeline_max_batches);
    cfg.client_batch_max_pending = env_parse("LMX_RAFT_CLIENT_BATCH_MAX_PENDING")
        .or(file.client_batch_max_pending)
        .unwrap_or(cfg.client_batch_max_pending);
    cfg.client_batch_max_delay = Duration::from_millis(
        env_parse("LMX_RAFT_CLIENT_BATCH_MAX_DELAY_MS")
            .or(file.client_batch_max_delay_ms)
            .unwrap_or(cfg.client_batch_max_delay.as_millis() as u64),
    );
    cfg.client_response_cache_max_entries = env_parse("LMX_RAFT_CLIENT_RESPONSE_CACHE_MAX_ENTRIES")
        .or(file.client_response_cache_max_entries)
        .unwrap_or(cfg.client_response_cache_max_entries);
    cfg.proxy_retry_budget = Duration::from_millis(
        env_parse("LMX_RAFT_PROXY_RETRY_BUDGET_MS")
            .or(file.proxy_retry_budget_ms)
            .unwrap_or(cfg.proxy_retry_budget.as_millis() as u64),
    );
    cfg.sync_log = env_bool("LMX_RAFT_SYNC_LOG")
        .or(file.sync_log)
        .unwrap_or(cfg.sync_log);
    cfg.peer_token =
        env_string("LMX_RAFT_PEER_TOKEN").or_else(|| non_empty(file.peer_token.clone()));
    cfg.peers = file.peers.clone();
    Ok(cfg)
}

#[cfg(feature = "tls")]
fn build_tls_config(file: &ServerFileConfig) -> Option<crate::server::TlsConfig> {
    crate::routine_id!("ddl-routine-config-build-tls-1");
    let cert_path = env_path("LMX_TLS_CERT").or_else(|| file.tls_cert.clone());
    let key_path = env_path("LMX_TLS_KEY").or_else(|| file.tls_key.clone());
    match (cert_path, key_path) {
        (Some(cert_path), Some(key_path)) => Some(crate::server::TlsConfig {
            cert_path,
            key_path,
        }),
        (Some(_), None) | (None, Some(_)) => {
            warn!("LMX_TLS_CERT and LMX_TLS_KEY must both be set; TLS disabled");
            None
        }
        (None, None) => None,
    }
}

fn parse_addr(value: &str, source: &str) -> Result<SocketAddr, ConfigError> {
    crate::routine_id!("ddl-routine-config-parse-addr-1");
    value.parse().map_err(|_| ConfigError::InvalidSocketAddr {
        value: value.to_string(),
        from: source.to_string(),
    })
}

fn env_string(key: &str) -> Option<String> {
    crate::routine_id!("ddl-routine-config-env-string-1");
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn env_path(key: &str) -> Option<PathBuf> {
    crate::routine_id!("ddl-routine-config-env-path-1");
    env_string(key).map(PathBuf::from)
}

fn env_parse<T>(key: &str) -> Option<T>
where
    T: std::str::FromStr,
{
    crate::routine_id!("ddl-routine-config-env-parse-1");
    env_string(key).and_then(|v| v.parse::<T>().ok())
}

fn env_bool(key: &str) -> Option<bool> {
    crate::routine_id!("ddl-routine-config-env-bool-1");
    env_string(key).map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
}

fn non_empty(value: Option<String>) -> Option<String> {
    crate::routine_id!("ddl-routine-config-non-empty-1");
    value
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_raft_peer_quorum_from_toml() {
        let cfg: ConfigFile = toml::from_str(
            r#"
            [raft]
            enabled = false
            node_id = "node-1"
            data_dir_lock = false
            append_entries_max_entries = 17
            append_entries_max_bytes = 12345
            append_entries_max_inline_batches = 9
            install_snapshot_chunk_bytes = 54321
            install_snapshot_max_staged_bytes = 654321
            install_snapshot_max_staged_transfers = 5
            install_snapshot_stale_transfer_ms = 1234
            client_batch_max_entries = 19
            client_pipeline_max_batches = 3
            client_batch_max_pending = 77
            client_batch_max_delay_ms = 7
            client_response_cache_max_entries = 55
            proxy_retry_budget_ms = 456
            sync_log = false
            peer_token = "cluster-secret"

            [[raft.peers]]
            id = "node-1"
            addr = "127.0.0.1:7980"

            [[raft.peers]]
            id = "node-2"
            addr = "127.0.0.1:7981"

            [[raft.peers]]
            id = "node-3"
            addr = "127.0.0.1:7982"
            "#,
        )
        .expect("valid toml");

        let raft = build_raft_config(&cfg.raft).expect("valid raft config");
        assert_eq!(raft.cluster_size(), 3);
        assert_eq!(raft.quorum_size(), 2);
        assert_eq!(raft.append_entries_max_entries, 17);
        assert!(!raft.data_dir_lock);
        assert_eq!(raft.append_entries_max_bytes, 12345);
        assert_eq!(raft.append_entries_max_inline_batches, 9);
        assert_eq!(raft.install_snapshot_chunk_bytes, 54321);
        assert_eq!(raft.install_snapshot_max_staged_bytes, 654321);
        assert_eq!(raft.install_snapshot_max_staged_transfers, 5);
        assert_eq!(
            raft.install_snapshot_stale_transfer_after,
            Duration::from_millis(1234)
        );
        assert_eq!(raft.client_batch_max_entries, 19);
        assert_eq!(raft.client_pipeline_max_batches, 3);
        assert_eq!(raft.client_batch_max_pending, 77);
        assert_eq!(raft.client_batch_max_delay, Duration::from_millis(7));
        assert_eq!(raft.client_response_cache_max_entries, 55);
        assert_eq!(raft.proxy_retry_budget, Duration::from_millis(456));
        assert!(!raft.sync_log);
        assert_eq!(raft.peer_token.as_deref(), Some("cluster-secret"));
    }

    #[test]
    fn shipped_regular_config_keeps_raft_disabled_with_three_node_plan() {
        let cfg: ConfigFile =
            toml::from_str(include_str!("../lmx.toml")).expect("shipped lmx.toml parses");

        assert_eq!(cfg.raft.enabled, Some(false));
        assert_eq!(cfg.raft.node_id.as_deref(), Some("node-1"));
        assert_eq!(cfg.raft.bind_addr.as_deref(), Some("127.0.0.1:7980"));
        assert_eq!(
            cfg.raft.data_dir.as_deref(),
            Some(Path::new("./data/raft/node-1"))
        );
        assert_eq!(cfg.raft.data_dir_lock, Some(true));
        assert_eq!(cfg.raft.snapshot_interval_ms, Some(1_800_000));
        assert_eq!(cfg.raft.snapshot_max_log_entries, Some(100_000));
        assert_eq!(cfg.raft.snapshot_max_log_bytes, Some(67_108_864));
        assert_eq!(cfg.raft.trailing_log_entries, Some(10_000));

        let peer_ids = cfg
            .raft
            .peers
            .iter()
            .map(|peer| peer.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(peer_ids, vec!["node-1", "node-2", "node-3"]);

        let raft = build_raft_config(&cfg.raft).expect("regular raft config builds");
        assert!(!raft.enabled);
        assert_eq!(raft.cluster_size(), 3);
        assert_eq!(raft.quorum_size(), 2);
    }

    #[test]
    fn shipped_raft_config_enables_bounded_two_of_three_cluster() {
        let cfg: ConfigFile =
            toml::from_str(include_str!("../lmx-raft.toml")).expect("shipped lmx-raft.toml parses");

        assert_eq!(cfg.server.disable_tcp, Some(true));
        assert_eq!(cfg.server.disable_http, Some(false));
        assert_eq!(cfg.raft.enabled, Some(true));
        assert_eq!(cfg.raft.node_id.as_deref(), Some("node-1"));
        assert_eq!(cfg.raft.bind_addr.as_deref(), Some("0.0.0.0:7980"));
        assert_eq!(cfg.raft.advertise_addr.as_deref(), Some("node-1:7980"));
        assert_eq!(
            cfg.raft.data_dir.as_deref(),
            Some(Path::new("/var/lib/dd-rust-network-mutex/raft"))
        );
        assert_eq!(cfg.raft.data_dir_lock, Some(true));
        assert_eq!(cfg.raft.heartbeat_interval_ms, Some(50));
        assert_eq!(cfg.raft.election_timeout_min_ms, Some(150));
        assert_eq!(cfg.raft.election_timeout_max_ms, Some(300));
        assert_eq!(cfg.raft.snapshot_interval_ms, Some(1_800_000));
        assert_eq!(cfg.raft.snapshot_max_log_entries, Some(100_000));
        assert_eq!(cfg.raft.snapshot_max_log_bytes, Some(67_108_864));
        assert_eq!(cfg.raft.trailing_log_entries, Some(10_000));
        assert_eq!(cfg.raft.append_entries_max_entries, Some(256));
        assert_eq!(cfg.raft.append_entries_max_bytes, Some(1_048_576));
        assert_eq!(cfg.raft.append_entries_max_inline_batches, Some(64));
        assert_eq!(cfg.raft.install_snapshot_chunk_bytes, Some(1_048_576));
        assert_eq!(
            cfg.raft.install_snapshot_max_staged_bytes,
            Some(134_217_728)
        );
        assert_eq!(cfg.raft.install_snapshot_max_staged_transfers, Some(4));
        assert_eq!(cfg.raft.install_snapshot_stale_transfer_ms, Some(1_800_000));
        assert_eq!(cfg.raft.client_batch_max_entries, Some(32));
        assert_eq!(cfg.raft.client_pipeline_max_batches, Some(4));
        assert_eq!(cfg.raft.client_batch_max_pending, Some(8192));
        assert_eq!(cfg.raft.client_batch_max_delay_ms, Some(1));
        assert_eq!(cfg.raft.client_response_cache_max_entries, Some(8192));
        assert_eq!(cfg.raft.proxy_retry_budget_ms, Some(2000));
        assert_eq!(cfg.raft.sync_log, Some(true));

        let peer_ids = cfg
            .raft
            .peers
            .iter()
            .map(|peer| peer.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(peer_ids, vec!["node-1", "node-2", "node-3"]);

        let raft = build_raft_config(&cfg.raft).expect("BrokerRaft config builds");
        assert!(raft.enabled);
        assert_eq!(raft.cluster_size(), 3);
        assert_eq!(raft.quorum_size(), 2);
        assert_eq!(raft.append_entries_max_entries, 256);
        assert!(raft.data_dir_lock);
        assert_eq!(raft.append_entries_max_bytes, 1_048_576);
        assert_eq!(raft.append_entries_max_inline_batches, 64);
        assert_eq!(raft.install_snapshot_chunk_bytes, 1_048_576);
        assert_eq!(raft.client_batch_max_entries, 32);
        assert_eq!(raft.client_pipeline_max_batches, 4);
        assert_eq!(raft.client_batch_max_pending, 8192);
        assert_eq!(raft.client_batch_max_delay, Duration::from_millis(1));
        assert_eq!(raft.proxy_retry_budget, Duration::from_millis(2000));
        assert!(raft.sync_log);
    }
}
