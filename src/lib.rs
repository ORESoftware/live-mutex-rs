//! `dd-rust-network-mutex` — networked mutex broker and clients.
//!
//! The public [`Client`] / [`RwClient`] exports fail closed on successful
//! responses that do not contain complete fencing authority. Raw historical
//! clients remain available as [`RawClient`] / [`RawRwClient`] for migration
//! and protocol-level testing.

pub mod broker;
pub mod broker_raft;
pub mod cli_flags;
pub mod client;
pub mod config;
pub mod fenced_client;
pub mod metrics;
pub mod protocol;
pub mod queue;
pub mod routine;
pub mod server;
pub mod sim;
pub mod sockopt;
pub mod status;

pub use broker::{Broker, BrokerConfig, BrokerMetrics};
pub use broker_raft::{
    BrokerRaft, BrokerRaftConfig, BrokerRaftError, RaftCommand, RaftCompactionReport, RaftLogEntry,
    RaftLogStore, RaftMembership, RaftPeerConfig, RaftSnapshotMetadata,
};
pub use cli_flags::{load_broker_cli_config, BrokerCliConfig, BrokerCliEnv, CliFlagError};
pub use client::{
    Client as RawClient, ClientConfig, ClientError, LockGuard, LockInfo,
    RwClient as RawRwClient, RwReadGuard, RwWriteGuard,
};
pub use config::{load_runtime_config, ConfigError, RuntimeConfig};
pub use fenced_client::{Client, RwClient, MAX_FENCING_TOKEN};
pub use protocol::{Request, Response, MAX_COMPOSITE_KEYS, PROTOCOL_VERSION};
pub use routine::{
    current_log_level, init_tracing, is_otel_enabled, set_log_level, set_otel_enabled,
    shutdown_tracing,
};
pub use server::{run as run_server, ServerConfig};
pub use sim::{RaftSim, RaftSimConfig, RaftSimError, RaftSimLock};

#[cfg(feature = "tls")]
pub use server::TlsConfig;
