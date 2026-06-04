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
    heartbeat_interval_ms: Option<u64>,
    election_timeout_min_ms: Option<u64>,
    election_timeout_max_ms: Option<u64>,
    snapshot_interval_ms: Option<u64>,
    snapshot_max_log_entries: Option<u64>,
    snapshot_max_log_bytes: Option<u64>,
    trailing_log_entries: Option<u64>,
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
    }
}
