//! Entrypoint for the `dd-rust-network-mutex` broker.
//!
//! All configuration comes from environment variables so the binary plays
//! nicely with `envFrom: secretRef` and ConfigMap-style Kubernetes wiring.
//!
//! Required: at least one of `LMX_TCP_PORT`, `LMX_UDS_PATH`, or
//! `LMX_HTTP_PORT` must produce a listener. Defaults bind TCP on 6970 and HTTP
//! on 6971; UDS is off unless `LMX_UDS_PATH` is set.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use dd_rust_network_mutex::{
    routine_id,
    server::{self, ServerConfig},
    BrokerConfig,
};
use tracing::info;
#[cfg(feature = "tls")]
use tracing::warn;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Initialise tracing/OTel BEFORE we touch the macro, so the very first
    // `info!("enter")` line lands on a real subscriber. The init helper is
    // idempotent.
    dd_rust_network_mutex::init_tracing();
    routine_id!("ddl-routine-jmlgJQLHYkR5XOM7Ck");

    let config = config_from_env();
    log_startup(&config);
    let result = server::run(config).await;
    dd_rust_network_mutex::shutdown_tracing();
    result
}

fn first_env(keys: &[&str]) -> Option<String> {
    routine_id!("ddl-routine-ObElLFTW0TL2fkl83k");
    keys.iter().find_map(|k| {
        std::env::var(k)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    })
}

fn env_u16(key: &str, fallback: u16) -> u16 {
    routine_id!("ddl-routine-DfmvzJsJuxMBgbDtas");
    first_env(&[key])
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(fallback)
}

fn env_u64(key: &str, fallback: u64) -> u64 {
    routine_id!("ddl-routine-ifRK_T_kP8pmKnSSB2");
    first_env(&[key])
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(fallback)
}

fn env_u32(key: &str, fallback: u32) -> u32 {
    routine_id!("ddl-routine-choQZ1kLzCYF-iYA__");
    first_env(&[key])
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(fallback)
}

fn env_bool(key: &str, fallback: bool) -> bool {
    routine_id!("ddl-routine-HsjfOa-Tg6-hh7tLgI");
    first_env(&[key])
        .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(fallback)
}

fn config_from_env() -> ServerConfig {
    routine_id!("ddl-routine-mGtrA-R_d5EDpUGhov");

    let tcp_port = env_u16("LMX_TCP_PORT", 6970);
    let http_port = env_u16("LMX_HTTP_PORT", 6971);
    let bind_host = first_env(&["LMX_BIND_HOST"]).unwrap_or_else(|| "0.0.0.0".into());

    let tcp_bind = if env_bool("LMX_DISABLE_TCP", false) {
        None
    } else {
        Some(format!("{bind_host}:{tcp_port}").parse::<SocketAddr>().expect("invalid LMX_BIND_HOST/LMX_TCP_PORT"))
    };
    let http_bind = if env_bool("LMX_DISABLE_HTTP", false) {
        None
    } else {
        Some(format!("{bind_host}:{http_port}").parse::<SocketAddr>().expect("invalid LMX_BIND_HOST/LMX_HTTP_PORT"))
    };

    let status_bind = first_env(&["LMX_STATUS_PORT"])
        .and_then(|v| v.parse::<u16>().ok())
        .map(|p| {
            format!("{bind_host}:{p}")
                .parse::<SocketAddr>()
                .expect("invalid LMX_BIND_HOST/LMX_STATUS_PORT")
        });

    let uds_path = first_env(&["LMX_UDS_PATH"]).map(PathBuf::from);
    let auth_token = first_env(&["LMX_AUTH_TOKEN"]);

    let broker = BrokerConfig {
        default_ttl: Duration::from_millis(env_u64("LMX_DEFAULT_TTL_MS", 4000)),
        max_lock_holders: env_u32("LMX_MAX_LOCK_HOLDERS", 1).max(1),
        ttl_sweep_interval: Duration::from_millis(env_u64("LMX_TTL_SWEEP_INTERVAL_MS", 10)),
        max_concurrency_cap: env_u32(
            "LMX_MAX_CONCURRENCY_CAP",
            dd_rust_network_mutex::protocol::DEFAULT_MAX_CONCURRENCY_CAP,
        )
        .max(1),
        // 60s grace by default. Set `LMX_IDLE_KEY_GRACE_MS=0` to
        // disable empty-key pruning entirely (`state.locks` will
        // grow monotonically with the set of distinct keys ever
        // observed — the historical behaviour).
        idle_key_grace: Duration::from_millis(env_u64("LMX_IDLE_KEY_GRACE_MS", 60_000)),
    };

    #[cfg(feature = "tls")]
    let tls = build_tls_config();

    ServerConfig {
        tcp_bind,
        uds_path,
        http_bind,
        auth_token,
        broker,
        tcp_nodelay: env_bool("LMX_TCP_NODELAY", true),
        tcp_quickack: env_bool("LMX_TCP_QUICKACK", true),
        status_bind,
        #[cfg(feature = "tls")]
        tls,
    }
}

#[cfg(feature = "tls")]
fn build_tls_config() -> Option<server::TlsConfig> {
    routine_id!("ddl-routine-Q3i3_rKP4NzTLTeM5d");

    match (first_env(&["LMX_TLS_CERT"]), first_env(&["LMX_TLS_KEY"])) {
        (Some(cert), Some(key)) => Some(server::TlsConfig {
            cert_path: PathBuf::from(cert),
            key_path: PathBuf::from(key),
        }),
        (Some(_), None) | (None, Some(_)) => {
            warn!(
                routine_id = ROUTINE_ID,
                "LMX_TLS_CERT and LMX_TLS_KEY must both be set; TLS disabled"
            );
            None
        }
        _ => None,
    }
}

fn log_startup(config: &ServerConfig) {
    routine_id!("ddl-routine-glxWxKt782M6i2YFXl");

    info!(
        target: "lmx",
        routine_id = ROUTINE_ID,
        tcp = ?config.tcp_bind,
        uds = ?config.uds_path,
        http = ?config.http_bind,
        status = ?config.status_bind,
        auth = config.auth_token.is_some(),
        max_lock_holders = config.broker.max_lock_holders,
        max_concurrency_cap = config.broker.max_concurrency_cap,
        default_ttl_ms = config.broker.default_ttl.as_millis() as u64,
        ttl_sweep_interval_ms = config.broker.ttl_sweep_interval.as_millis() as u64,
        tcp_nodelay = config.tcp_nodelay,
        tcp_quickack_effective =
            config.tcp_quickack && dd_rust_network_mutex::sockopt::quickack_supported(),
        protocol = dd_rust_network_mutex::PROTOCOL_VERSION,
        "starting dd-rust-network-mutex"
    );
}
