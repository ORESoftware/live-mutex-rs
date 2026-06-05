//! Raft-facing broker wrapper and durable local log plumbing.
//!
//! This module provides the `BrokerRaft` server backend: peer-list config,
//! leader election, quorum replication, durable append-only logs, snapshot
//! metadata, and compaction-by-snapshot-index.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Mutex as AsyncMutex, Notify};
use tokio::task::{JoinHandle, JoinSet};
use tracing::{debug, error, info, warn};

use crate::broker::{Broker, BrokerConfig, ClientId, GrantOverrides};
use crate::protocol::{Request, Response, MAX_COMPOSITE_KEYS};

const LOG_FILE: &str = "raft-log.ndjson";
const SNAPSHOT_FILE: &str = "raft-snapshot.json";
const HARD_STATE_FILE: &str = "raft-hard-state.json";
const LEARNERS_FILE: &str = "raft-learners.json";
const SNAPSHOT_PART_FILE_PREFIX: &str = "raft-install-snapshot-";
const SNAPSHOT_PART_FILE_SUFFIX: &str = ".json.part";
const DEFAULT_RAFT_RPC_MAX_FRAME_BYTES: usize = 128 * 1024 * 1024;
const DEFAULT_APPEND_ENTRIES_MAX_ENTRIES: usize = 256;
const DEFAULT_APPEND_ENTRIES_MAX_BYTES: usize = 1024 * 1024;
const DEFAULT_INSTALL_SNAPSHOT_CHUNK_BYTES: usize = 1024 * 1024;
const DEFAULT_CLIENT_BATCH_MAX_ENTRIES: usize = 32;
const DEFAULT_CLIENT_PIPELINE_MAX_BATCHES: usize = 4;
const DEFAULT_CLIENT_BATCH_MAX_PENDING: usize = 8192;
const DEFAULT_CLIENT_BATCH_MAX_DELAY: Duration = Duration::from_millis(1);
const DEFAULT_CLIENT_RESPONSE_CACHE_MAX_ENTRIES: usize = 8192;
const SNAPSHOT_TRANSFER_STALE_MS: u64 = 30 * 60 * 1000;
const RAFT_FENCING_TOKEN_BASE: u64 = 4_000_000_000_000_000;
const RAFT_CLIENT_ID_PREFIX_SHIFT: u64 = 48;
const RAFT_CLIENT_ID_SEQUENCE_MASK: u64 = (1_u64 << RAFT_CLIENT_ID_PREFIX_SHIFT) - 1;
const RAFT_ROLE_CACHE_FOLLOWER: u8 = 0;
const RAFT_ROLE_CACHE_CANDIDATE: u8 = 1;
const RAFT_ROLE_CACHE_LEADER: u8 = 2;

#[derive(Debug, Clone)]
pub struct BrokerRaftConfig {
    pub broker: BrokerConfig,
    pub enabled: bool,
    pub node_id: String,
    pub bind_addr: Option<SocketAddr>,
    pub advertise_addr: Option<String>,
    pub data_dir: PathBuf,
    pub heartbeat_interval: Duration,
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    /// Target cadence for writing snapshots. Operators can set this to
    /// `30min` to get the "compact about every 30 minutes" behavior, while
    /// compaction itself remains index/snapshot based instead of wall-clock
    /// deletion.
    pub snapshot_interval: Duration,
    pub snapshot_max_log_entries: u64,
    pub snapshot_max_log_bytes: u64,
    /// Keep a small suffix after snapshot compaction for debugging and for
    /// followers that only lag slightly. Entries at or before the installed
    /// snapshot index are still safe to delete.
    pub trailing_log_entries: u64,
    /// Maximum number of log entries sent to one peer in a single
    /// `AppendEntries` RPC. Lagging followers catch up over multiple bounded
    /// batches instead of receiving the entire suffix in one frame.
    pub append_entries_max_entries: usize,
    /// Approximate serialized JSON byte budget for one `AppendEntries` entry
    /// batch. The first entry is still sent even if it exceeds this budget so
    /// progress cannot stall on one large command.
    pub append_entries_max_bytes: usize,
    /// Serialized snapshot payload bytes per `InstallSnapshot` chunk. Base64
    /// encoding makes the wire frame larger than this raw byte budget.
    pub install_snapshot_chunk_bytes: usize,
    /// Max leader-local client requests to append and replicate in one commit
    /// batch. Membership changes and client-drop cleanup stay serialized.
    pub client_batch_max_entries: usize,
    /// Max configured client batches drained into one append/replicate/commit
    /// cycle when the leader write lane is already under load.
    pub client_pipeline_max_batches: usize,
    /// Max leader-local client requests allowed to wait for the Raft write
    /// lane. This bounds memory during stalled quorum or overloaded leader
    /// periods and rejects before appending to the replicated log.
    pub client_batch_max_pending: usize,
    /// Small coalescing window for leader-local client request batches.
    pub client_batch_max_delay: Duration,
    /// Max recent HTTP/request-id responses retained on a leader for
    /// idempotent retries. This is bounded memory only; the Raft log remains
    /// the source of truth.
    pub client_response_cache_max_entries: usize,
    /// When true, every Raft append-log write is flushed to stable storage
    /// before the node acknowledges it. Disabling this is an explicit
    /// performance/benchmark tradeoff and weakens crash durability.
    pub sync_log: bool,
    /// Optional shared secret required on Raft peer RPC frames. When unset,
    /// the peer listener preserves the existing trusted-network behavior.
    pub peer_token: Option<String>,
    pub peers: Vec<RaftPeerConfig>,
}

impl Default for BrokerRaftConfig {
    fn default() -> Self {
        crate::routine_id!("ddl-routine-broker-raft-config-default-1");
        Self {
            broker: BrokerConfig::default(),
            enabled: false,
            node_id: "node-1".into(),
            bind_addr: Some("127.0.0.1:7980".parse().expect("valid default addr")),
            advertise_addr: None,
            data_dir: PathBuf::from("./data/raft/node-1"),
            heartbeat_interval: Duration::from_millis(100),
            election_timeout_min: Duration::from_millis(800),
            election_timeout_max: Duration::from_millis(1_600),
            snapshot_interval: Duration::from_secs(30 * 60),
            snapshot_max_log_entries: 100_000,
            snapshot_max_log_bytes: 64 * 1024 * 1024,
            trailing_log_entries: 10_000,
            append_entries_max_entries: DEFAULT_APPEND_ENTRIES_MAX_ENTRIES,
            append_entries_max_bytes: DEFAULT_APPEND_ENTRIES_MAX_BYTES,
            install_snapshot_chunk_bytes: DEFAULT_INSTALL_SNAPSHOT_CHUNK_BYTES,
            client_batch_max_entries: DEFAULT_CLIENT_BATCH_MAX_ENTRIES,
            client_pipeline_max_batches: DEFAULT_CLIENT_PIPELINE_MAX_BATCHES,
            client_batch_max_pending: DEFAULT_CLIENT_BATCH_MAX_PENDING,
            client_batch_max_delay: DEFAULT_CLIENT_BATCH_MAX_DELAY,
            client_response_cache_max_entries: DEFAULT_CLIENT_RESPONSE_CACHE_MAX_ENTRIES,
            sync_log: true,
            peer_token: None,
            peers: Vec::new(),
        }
    }
}

impl BrokerRaftConfig {
    pub fn cluster_size(&self) -> usize {
        crate::routine_id!("ddl-routine-broker-raft-cluster-size-1");
        self.peers.len()
    }

    pub fn quorum_size(&self) -> usize {
        crate::routine_id!("ddl-routine-broker-raft-quorum-size-1");
        quorum_for(self.cluster_size())
    }

    pub fn validate(&self) -> Result<(), BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-validate-1");
        if !self.enabled {
            return Ok(());
        }
        if self.node_id.trim().is_empty() {
            return Err(BrokerRaftError::InvalidConfig(
                "raft.node_id cannot be empty when Raft is enabled".into(),
            ));
        }
        let peers = validate_membership_peers(self.peers.clone())?;
        if !peers.iter().any(|p| p.id == self.node_id) {
            return Err(BrokerRaftError::InvalidConfig(format!(
                "raft.node_id `{}` must appear in raft.peers",
                self.node_id
            )));
        }
        if peers.len() != self.peers.len() {
            return Err(BrokerRaftError::InvalidConfig(
                "raft.peers must not contain duplicate peer IDs".into(),
            ));
        }
        if self.election_timeout_min >= self.election_timeout_max {
            return Err(BrokerRaftError::InvalidConfig(
                "raft.election_timeout_min_ms must be less than raft.election_timeout_max_ms"
                    .into(),
            ));
        }
        if self.heartbeat_interval >= self.election_timeout_min {
            return Err(BrokerRaftError::InvalidConfig(
                "raft.heartbeat_interval_ms must be less than raft.election_timeout_min_ms".into(),
            ));
        }
        if self.append_entries_max_entries == 0 {
            return Err(BrokerRaftError::InvalidConfig(
                "raft.append_entries_max_entries must be greater than 0".into(),
            ));
        }
        if self.append_entries_max_bytes == 0 {
            return Err(BrokerRaftError::InvalidConfig(
                "raft.append_entries_max_bytes must be greater than 0".into(),
            ));
        }
        if self.install_snapshot_chunk_bytes == 0 {
            return Err(BrokerRaftError::InvalidConfig(
                "raft.install_snapshot_chunk_bytes must be greater than 0".into(),
            ));
        }
        if self.client_batch_max_entries == 0 {
            return Err(BrokerRaftError::InvalidConfig(
                "raft.client_batch_max_entries must be greater than 0".into(),
            ));
        }
        if self.client_pipeline_max_batches == 0 {
            return Err(BrokerRaftError::InvalidConfig(
                "raft.client_pipeline_max_batches must be greater than 0".into(),
            ));
        }
        if self.client_batch_max_pending == 0 {
            return Err(BrokerRaftError::InvalidConfig(
                "raft.client_batch_max_pending must be greater than 0".into(),
            ));
        }
        if self.client_response_cache_max_entries == 0 {
            return Err(BrokerRaftError::InvalidConfig(
                "raft.client_response_cache_max_entries must be greater than 0".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RaftPeerConfig {
    pub id: String,
    pub addr: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RaftProgressSnapshot {
    pub node_id: String,
    pub role: String,
    pub is_leader: bool,
    pub is_leader_ready: bool,
    pub leader_id: Option<String>,
    pub leader_addr: Option<String>,
    pub leader_quorum_age_ms: Option<u64>,
    pub leader_quorum_timeout_ms: u64,
    pub current_term: u64,
    pub commit_index: u64,
    pub last_applied: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
    pub membership_joint: bool,
    pub membership: RaftMembership,
    pub peers: Vec<RaftPeerProgressSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RaftPeerProgressSnapshot {
    pub id: String,
    pub addr: String,
    pub is_self: bool,
    pub voter: bool,
    pub staged_learner: bool,
    pub membership_role: String,
    pub next_index: Option<u64>,
    pub match_index: Option<u64>,
    pub lag: Option<u64>,
    pub caught_up: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RaftTelemetrySnapshot {
    pub append_progress_updates_total: u64,
    pub append_conflict_repairs_total: u64,
    pub append_conflict_clamps_total: u64,
    pub append_invalid_success_responses_total: u64,
    pub install_snapshot_chunks_total: u64,
    pub install_snapshot_bytes_total: u64,
    pub install_snapshot_progress_updates_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "state",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum RaftMembership {
    Simple {
        peers: Vec<RaftPeerConfig>,
    },
    Joint {
        old_peers: Vec<RaftPeerConfig>,
        new_peers: Vec<RaftPeerConfig>,
    },
}

impl RaftMembership {
    pub fn from_simple(peers: Vec<RaftPeerConfig>) -> Self {
        crate::routine_id!("ddl-routine-broker-raft-membership-simple-1");
        Self::Simple {
            peers: normalize_peers(peers),
        }
    }

    pub fn active_peers(&self) -> Vec<RaftPeerConfig> {
        crate::routine_id!("ddl-routine-broker-raft-membership-active-1");
        match self {
            Self::Simple { peers } => normalize_peers(peers.clone()),
            Self::Joint {
                old_peers,
                new_peers,
            } => normalize_peers(old_peers.iter().chain(new_peers.iter()).cloned().collect()),
        }
    }

    pub fn contains_id(&self, node_id: &str) -> bool {
        crate::routine_id!("ddl-routine-broker-raft-membership-contains-1");
        self.active_peers().iter().any(|peer| peer.id == node_id)
    }

    pub fn cluster_size(&self) -> usize {
        crate::routine_id!("ddl-routine-broker-raft-membership-cluster-size-1");
        self.active_peers().len()
    }

    pub fn quorum_size(&self) -> usize {
        crate::routine_id!("ddl-routine-broker-raft-membership-quorum-size-1");
        match self {
            Self::Simple { peers } => quorum_for(peers.len()),
            Self::Joint {
                old_peers,
                new_peers,
            } => quorum_for(old_peers.len()).max(quorum_for(new_peers.len())),
        }
    }

    pub fn is_joint(&self) -> bool {
        crate::routine_id!("ddl-routine-broker-raft-membership-is-joint-1");
        matches!(self, Self::Joint { .. })
    }

    pub fn quorum_met(&self, ack_ids: &BTreeSet<String>) -> bool {
        crate::routine_id!("ddl-routine-broker-raft-membership-quorum-met-1");
        match self {
            Self::Simple { peers } => {
                !peers.is_empty() && peer_votes(peers, ack_ids) >= quorum_for(peers.len())
            }
            Self::Joint {
                old_peers,
                new_peers,
            } => {
                !old_peers.is_empty()
                    && !new_peers.is_empty()
                    && peer_votes(old_peers, ack_ids) >= quorum_for(old_peers.len())
                    && peer_votes(new_peers, ack_ids) >= quorum_for(new_peers.len())
            }
        }
    }
}

fn quorum_for(cluster_size: usize) -> usize {
    crate::routine_id!("ddl-routine-broker-raft-quorum-for-1");
    if cluster_size == 0 {
        0
    } else {
        (cluster_size / 2) + 1
    }
}

fn normalize_peers(peers: Vec<RaftPeerConfig>) -> Vec<RaftPeerConfig> {
    crate::routine_id!("ddl-routine-broker-raft-normalize-peers-1");
    let mut by_id = BTreeMap::new();
    for peer in peers {
        by_id.insert(peer.id.clone(), peer);
    }
    by_id.into_values().collect()
}

fn peer_votes(peers: &[RaftPeerConfig], ack_ids: &BTreeSet<String>) -> usize {
    crate::routine_id!("ddl-routine-broker-raft-peer-votes-1");
    peers
        .iter()
        .filter(|peer| ack_ids.contains(&peer.id))
        .count()
}

fn validate_membership_peers(
    peers: Vec<RaftPeerConfig>,
) -> Result<Vec<RaftPeerConfig>, BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-validate-membership-peers-1");
    if peers.len() < 3 {
        return Err(BrokerRaftError::InvalidConfig(
            "raft membership requires at least 3 peers for failover".into(),
        ));
    }
    if peers.len() % 2 == 0 {
        return Err(BrokerRaftError::InvalidConfig(
            "raft membership should be odd-sized, e.g. 3 or 5".into(),
        ));
    }

    let mut ids = BTreeSet::new();
    let mut addrs = BTreeSet::new();
    let mut normalized = Vec::with_capacity(peers.len());
    for mut peer in peers {
        peer.id = peer.id.trim().to_string();
        peer.addr = peer.addr.trim().to_string();
        if peer.id.is_empty() {
            return Err(BrokerRaftError::InvalidConfig(
                "raft peer id cannot be empty".into(),
            ));
        }
        if peer.addr.is_empty() {
            return Err(BrokerRaftError::InvalidConfig(format!(
                "raft peer `{}` has an empty addr",
                peer.id
            )));
        }
        if !ids.insert(peer.id.clone()) {
            return Err(BrokerRaftError::InvalidConfig(format!(
                "raft peer id `{}` appears more than once",
                peer.id
            )));
        }
        if !addrs.insert(peer.addr.clone()) {
            return Err(BrokerRaftError::InvalidConfig(format!(
                "raft peer addr `{}` appears more than once",
                peer.addr
            )));
        }
        normalized.push(peer);
    }
    let normalized = normalize_peers(normalized);
    validate_raft_client_id_prefixes(&normalized)?;
    Ok(normalized)
}

fn validate_raft_client_id_prefixes(peers: &[RaftPeerConfig]) -> Result<(), BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-validate-client-prefixes-1");
    let mut by_prefix = BTreeMap::<u64, String>::new();
    for peer in peers {
        let prefix = raft_client_id_prefix(&peer.id);
        if let Some(previous) = by_prefix.get(&prefix) {
            if previous == &peer.id {
                continue;
            }
            return Err(BrokerRaftError::InvalidConfig(format!(
                "raft peer ids `{previous}` and `{}` derive the same client-id prefix {prefix}; rename one peer id",
                peer.id
            )));
        }
        by_prefix.insert(prefix, peer.id.clone());
    }
    Ok(())
}

fn raft_client_id_for_node(node_id: &str, sequence: u64) -> ClientId {
    crate::routine_id!("ddl-routine-broker-raft-client-id-for-node-1");
    let sequence = (sequence & RAFT_CLIENT_ID_SEQUENCE_MASK).max(1);
    (raft_client_id_prefix(node_id) << RAFT_CLIENT_ID_PREFIX_SHIFT) | sequence
}

fn raft_client_id_prefix(node_id: &str) -> u64 {
    crate::routine_id!("ddl-routine-broker-raft-client-id-prefix-1");
    (stable_node_jitter(node_id, 0) & 0xffff).max(1)
}

fn validate_staged_learner_peers(
    learners: Vec<RaftPeerConfig>,
    active_peers: &[RaftPeerConfig],
) -> Result<Vec<RaftPeerConfig>, BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-validate-staged-learners-1");
    let active_ids = active_peers
        .iter()
        .map(|peer| peer.id.clone())
        .collect::<BTreeSet<_>>();
    let active_addrs = active_peers
        .iter()
        .map(|peer| peer.addr.clone())
        .collect::<BTreeSet<_>>();
    let mut ids = BTreeSet::new();
    let mut addrs = BTreeSet::new();
    let mut normalized = Vec::new();
    for mut peer in learners {
        peer.id = peer.id.trim().to_string();
        peer.addr = peer.addr.trim().to_string();
        if peer.id.trim().is_empty() {
            return Err(BrokerRaftError::InvalidConfig(
                "raft learner id cannot be empty".into(),
            ));
        }
        if peer.addr.trim().is_empty() {
            return Err(BrokerRaftError::InvalidConfig(format!(
                "raft learner `{}` addr cannot be empty",
                peer.id
            )));
        }
        if active_ids.contains(&peer.id) {
            return Err(BrokerRaftError::InvalidConfig(format!(
                "raft learner `{}` is already an active voter",
                peer.id
            )));
        }
        if active_addrs.contains(&peer.addr) {
            return Err(BrokerRaftError::InvalidConfig(format!(
                "raft learner addr `{}` is already used by an active voter",
                peer.addr
            )));
        }
        if !ids.insert(peer.id.clone()) {
            return Err(BrokerRaftError::InvalidConfig(format!(
                "raft learner id `{}` appears more than once",
                peer.id
            )));
        }
        if !addrs.insert(peer.addr.clone()) {
            return Err(BrokerRaftError::InvalidConfig(format!(
                "raft learner addr `{}` appears more than once",
                peer.addr
            )));
        }
        normalized.push(peer);
    }
    let normalized = normalize_peers(normalized);
    let mut combined = active_peers.to_vec();
    combined.extend(normalized.clone());
    validate_raft_client_id_prefixes(&combined)?;
    Ok(normalized)
}

fn validate_raft_membership(membership: RaftMembership) -> Result<RaftMembership, BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-validate-membership-1");
    match membership {
        RaftMembership::Simple { peers } => Ok(RaftMembership::Simple {
            peers: validate_membership_peers(peers)?,
        }),
        RaftMembership::Joint {
            old_peers,
            new_peers,
        } => {
            let old_peers = validate_membership_peers(old_peers)?;
            let new_peers = validate_membership_peers(new_peers)?;
            let mut joint_peers = old_peers.clone();
            joint_peers.extend(new_peers.clone());
            validate_raft_client_id_prefixes(&joint_peers)?;
            Ok(RaftMembership::Joint {
                old_peers,
                new_peers,
            })
        }
    }
}

fn validate_raft_command(command: &RaftCommand) -> Result<(), BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-validate-command-1");
    match command {
        RaftCommand::SetMembership { membership } => {
            validate_raft_membership(membership.clone())?;
        }
        RaftCommand::SetStagedLearners { learners } => {
            validate_staged_learner_peers(learners.clone(), &[])?;
        }
        RaftCommand::ClientRequestWithIdentity {
            request_id,
            request_fingerprint,
            ..
        } => {
            if request_id.is_empty() {
                return Err(BrokerRaftError::InvalidAppendEntries(
                    "client request identity has empty request id".into(),
                ));
            }
            if request_fingerprint.is_empty() {
                return Err(BrokerRaftError::InvalidAppendEntries(
                    "client request identity has empty request fingerprint".into(),
                ));
            }
        }
        RaftCommand::Noop | RaftCommand::ClientRequest { .. } | RaftCommand::DropClient { .. } => {}
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum BrokerRaftError {
    #[error("raft storage I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("raft storage JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid raft config: {0}")]
    InvalidConfig(String),
    #[error("cannot compact through index {through_index}; latest snapshot only covers {snapshot_index}")]
    CompactionAheadOfSnapshot {
        through_index: u64,
        snapshot_index: u64,
    },
    #[error("cannot compact raft log before a snapshot has been written")]
    NoSnapshot,
    #[error("raft node is not leader; leader={leader_id:?} addr={leader_addr:?}")]
    NotLeader {
        leader_id: Option<String>,
        leader_addr: Option<String>,
    },
    #[error("raft quorum unavailable for log index {index}; votes={votes} quorum={quorum}")]
    QuorumUnavailable {
        index: u64,
        votes: usize,
        quorum: usize,
    },
    #[error("raft snapshot at index {index} is missing payload checksum")]
    SnapshotChecksumMissing { index: u64 },
    #[error(
        "raft snapshot payload checksum mismatch at index {index}; expected={expected} actual={actual}"
    )]
    SnapshotChecksumMismatch {
        index: u64,
        expected: String,
        actual: String,
    },
    #[error("raft broker snapshot error: {0}")]
    BrokerSnapshot(String),
    #[error("invalid raft append entries: {0}")]
    InvalidAppendEntries(String),
    #[error("invalid persisted raft log: {0}")]
    InvalidLog(String),
    #[error("raft learner `{peer_id}` did not catch up to index {target_index} before promotion")]
    LearnerCatchUpFailed { peer_id: String, target_index: u64 },
    #[error("raft leader client request queue full; pending={pending} limit={limit}")]
    ClientQueueFull { pending: usize, limit: usize },
    #[error("raft request id `{request_id}` was reused with a different request payload")]
    IdempotencyKeyConflict { request_id: String },
    #[error("raft RPC error: {0}")]
    Rpc(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RaftCommand {
    Noop,
    ClientRequest {
        client_id: ClientId,
        request: Request,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        grant: Option<RaftGrantPlan>,
    },
    ClientRequestWithIdentity {
        client_id: ClientId,
        request: Request,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        grant: Option<RaftGrantPlan>,
        request_id: String,
        request_fingerprint: String,
    },
    DropClient {
        client_id: ClientId,
    },
    SetMembership {
        membership: RaftMembership,
    },
    SetStagedLearners {
        learners: Vec<RaftPeerConfig>,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RaftGrantPlan {
    pub lock_uuid: Option<String>,
    pub fencing_seed: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
enum RaftRpc {
    PreVote {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth_token: Option<String>,
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
    },
    RequestVote {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth_token: Option<String>,
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
    },
    AppendEntries {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth_token: Option<String>,
        term: u64,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<RaftLogEntry>,
        leader_commit: u64,
    },
    InstallSnapshot {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth_token: Option<String>,
        term: u64,
        leader_id: String,
        last_included_index: u64,
        last_included_term: u64,
        payload_sha256: Option<String>,
        offset: u64,
        done: bool,
        data: String,
    },
    ProxyRequest {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth_token: Option<String>,
        request: Request,
        request_uuid: String,
        wait_ms: u64,
        is_acquire: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
enum RaftRpcResponse {
    PreVote {
        term: u64,
        vote_granted: bool,
    },
    RequestVote {
        term: u64,
        vote_granted: bool,
    },
    AppendEntries {
        term: u64,
        success: bool,
        match_index: u64,
        conflict_index: Option<u64>,
        conflict_term: Option<u64>,
    },
    InstallSnapshot {
        term: u64,
        success: bool,
        last_included_index: u64,
    },
    ProxyResponse {
        term: u64,
        response: Option<Response>,
        error: Option<String>,
    },
    Error {
        term: u64,
        error: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
struct RaftRuntimeState {
    current_term: u64,
    voted_for: Option<String>,
    role: RaftRole,
    leader_id: Option<String>,
    commit_index: u64,
    last_applied: u64,
    election_deadline: Instant,
    leader_progress: BTreeMap<String, RaftPeerProgress>,
    staged_learners: BTreeMap<String, RaftPeerConfig>,
    membership: RaftMembership,
}

impl RaftRuntimeState {
    #[cfg(test)]
    fn hard_state(&self) -> RaftHardState {
        crate::routine_id!("ddl-routine-broker-raft-runtime-hard-state-1");
        RaftHardState {
            current_term: self.current_term,
            voted_for: self.voted_for.clone(),
            commit_index: self.commit_index,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct RaftPeerProgress {
    next_index: u64,
    match_index: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct RaftPeerReplicationOutcome {
    contacted: bool,
    target_reached: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RaftAppendReport {
    success: bool,
    match_index: u64,
    conflict_index: Option<u64>,
    conflict_term: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RaftHardState {
    current_term: u64,
    voted_for: Option<String>,
    commit_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RaftLogEntry {
    pub index: u64,
    pub term: u64,
    pub created_at_ms: u64,
    pub command: RaftCommand,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RaftSnapshotMetadata {
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub created_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RaftSnapshotFile {
    metadata: RaftSnapshotMetadata,
    payload: serde_json::Value,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RaftLearnersFile {
    learners: Vec<RaftPeerConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftCompactionReport {
    pub compacted_through_index: u64,
    pub compacted_entries: usize,
    pub retained_entries: usize,
}

#[derive(Default)]
struct BrokerRaftTelemetry {
    append_progress_updates_total: AtomicU64,
    append_conflict_repairs_total: AtomicU64,
    append_conflict_clamps_total: AtomicU64,
    append_invalid_success_responses_total: AtomicU64,
    install_snapshot_chunks_total: AtomicU64,
    install_snapshot_bytes_total: AtomicU64,
    install_snapshot_progress_updates_total: AtomicU64,
}

#[derive(Debug)]
struct RaftLogState {
    last_index: u64,
    last_term: u64,
    hard_state: RaftHardState,
    latest_snapshot: Option<RaftSnapshotMetadata>,
    retained_log_entries: Vec<RaftLogEntry>,
    term_by_index: BTreeMap<u64, u64>,
    first_index_by_term: BTreeMap<u64, u64>,
    last_index_by_term: BTreeMap<u64, u64>,
}

#[derive(Debug)]
struct RaftMaintenanceState {
    last_snapshot_at: Instant,
    leader_quorum_observed_at: Instant,
}

#[derive(Debug, Default)]
struct PostCommitFanoutState {
    active: bool,
    pending: bool,
}

#[derive(Debug)]
struct PendingSnapshotTransfer {
    path: PathBuf,
    bytes_written: u64,
    updated_at_ms: u64,
}

struct PendingClientRequest {
    client_id: ClientId,
    request: Request,
    request_id: Option<String>,
    request_fingerprint: Option<String>,
    result_tx: oneshot::Sender<Result<u64, ClientRequestBatchError>>,
}

#[derive(Default)]
struct ClientRequestBatchState {
    pending: VecDeque<PendingClientRequest>,
    driver_active: bool,
}

#[derive(Debug, Clone)]
struct CachedClientResponse {
    request_fingerprint: String,
    applied: bool,
    response: Option<Response>,
}

#[derive(Default)]
struct ClientResponseCacheState {
    entries: BTreeMap<String, CachedClientResponse>,
    order: VecDeque<String>,
}

#[derive(Debug, Clone, Default)]
struct LeaderPeerHintCache {
    leader_id: Option<String>,
    leader_peer: Option<RaftPeerConfig>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RaftElectionLoopAction {
    Heartbeat,
    StartElection,
    Sleep(Duration),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClientResponseSnapshotEntry {
    request_id: String,
    request_fingerprint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    response: Option<Response>,
}

enum CachedClientResponseLookup {
    Missing,
    Pending,
    Completed(Response),
}

#[derive(Debug, Clone)]
enum ClientRequestBatchError {
    NotLeader {
        leader_id: Option<String>,
        leader_addr: Option<String>,
    },
    QuorumUnavailable {
        index: u64,
        votes: usize,
        quorum: usize,
    },
    Other(String),
}

impl ClientRequestBatchError {
    fn from_broker_error(err: BrokerRaftError) -> Self {
        crate::routine_id!("ddl-routine-broker-raft-client-batch-error-from-1");
        match err {
            BrokerRaftError::NotLeader {
                leader_id,
                leader_addr,
            } => Self::NotLeader {
                leader_id,
                leader_addr,
            },
            BrokerRaftError::QuorumUnavailable {
                index,
                votes,
                quorum,
            } => Self::QuorumUnavailable {
                index,
                votes,
                quorum,
            },
            other => Self::Other(other.to_string()),
        }
    }

    fn into_broker_error(self) -> BrokerRaftError {
        crate::routine_id!("ddl-routine-broker-raft-client-batch-error-into-1");
        match self {
            Self::NotLeader {
                leader_id,
                leader_addr,
            } => BrokerRaftError::NotLeader {
                leader_id,
                leader_addr,
            },
            Self::QuorumUnavailable {
                index,
                votes,
                quorum,
            } => BrokerRaftError::QuorumUnavailable {
                index,
                votes,
                quorum,
            },
            Self::Other(error) => BrokerRaftError::Rpc(error),
        }
    }
}

#[derive(Debug)]
pub struct RaftLogStore {
    data_dir: PathBuf,
    log_path: PathBuf,
    snapshot_path: PathBuf,
    hard_state_path: PathBuf,
    sync_log: bool,
    state: Mutex<RaftLogState>,
}

impl RaftLogStore {
    pub fn open(data_dir: impl Into<PathBuf>) -> Result<Self, BrokerRaftError> {
        Self::open_with_sync_log(data_dir, true)
    }

    pub fn open_with_sync_log(
        data_dir: impl Into<PathBuf>,
        sync_log: bool,
    ) -> Result<Self, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-log-open-1");
        let data_dir = data_dir.into();
        fs::create_dir_all(&data_dir)?;
        cleanup_orphaned_snapshot_part_files(&data_dir)?;
        let log_path = data_dir.join(LOG_FILE);
        let snapshot_path = data_dir.join(SNAPSHOT_FILE);
        let hard_state_path = data_dir.join(HARD_STATE_FILE);
        let latest_snapshot = read_snapshot_metadata(&snapshot_path)?;
        let hard_state = read_hard_state(&hard_state_path)?;
        let entries = read_log_entries_with_snapshot(&log_path, latest_snapshot.as_ref())?;
        let (last_index, last_term) = entries
            .last()
            .map(|entry| (entry.index, entry.term))
            .or_else(|| {
                latest_snapshot
                    .as_ref()
                    .map(|s| (s.last_included_index, s.last_included_term))
            })
            .unwrap_or((0, 0));

        let (term_by_index, first_index_by_term, last_index_by_term) =
            term_indexes_from_entries(&entries);

        Ok(Self {
            data_dir,
            log_path,
            snapshot_path,
            hard_state_path,
            sync_log,
            state: Mutex::new(RaftLogState {
                last_index,
                last_term,
                hard_state,
                latest_snapshot,
                retained_log_entries: entries.clone(),
                term_by_index,
                first_index_by_term,
                last_index_by_term,
            }),
        })
    }

    pub fn append(&self, term: u64, command: RaftCommand) -> Result<RaftLogEntry, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-log-append-1");
        let mut entries = self.append_batch(term, vec![command])?;
        entries.pop().ok_or_else(|| {
            BrokerRaftError::Rpc("raft log append produced no entry for one command".into())
        })
    }

    pub fn append_batch(
        &self,
        term: u64,
        commands: Vec<RaftCommand>,
    ) -> Result<Vec<RaftLogEntry>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-log-append-batch-1");
        if commands.is_empty() {
            return Ok(Vec::new());
        }
        for command in &commands {
            validate_raft_command(command)?;
        }
        let mut state = self.state.lock();
        if term == 0 {
            return Err(BrokerRaftError::InvalidAppendEntries(
                "local append term must be >= 1".into(),
            ));
        }
        if term < state.last_term {
            return Err(BrokerRaftError::InvalidAppendEntries(format!(
                "local append term {term} is older than last persisted term {}",
                state.last_term
            )));
        }
        let created_at_ms = unix_ms();
        let entries = commands
            .into_iter()
            .enumerate()
            .map(|(offset, command)| RaftLogEntry {
                index: state.last_index.saturating_add(offset as u64 + 1),
                term,
                created_at_ms,
                command,
            })
            .collect::<Vec<_>>();
        append_log_entries(&self.log_path, &entries, self.sync_log)?;

        if let Some(last) = entries.last() {
            state.last_index = last.index;
            state.last_term = last.term;
        }
        for entry in &entries {
            state.term_by_index.insert(entry.index, entry.term);
            state
                .first_index_by_term
                .entry(entry.term)
                .or_insert(entry.index);
            state.last_index_by_term.insert(entry.term, entry.index);
        }
        state.retained_log_entries.extend(entries.iter().cloned());
        Ok(entries)
    }

    pub fn last_index(&self) -> u64 {
        crate::routine_id!("ddl-routine-broker-raft-log-last-index-1");
        self.state.lock().last_index
    }

    pub fn last_term(&self) -> u64 {
        crate::routine_id!("ddl-routine-broker-raft-log-last-term-1");
        self.state.lock().last_term
    }

    pub fn latest_snapshot(&self) -> Option<RaftSnapshotMetadata> {
        crate::routine_id!("ddl-routine-broker-raft-latest-snapshot-1");
        self.state.lock().latest_snapshot.clone()
    }

    pub fn retained_entries_len(&self) -> usize {
        crate::routine_id!("ddl-routine-broker-raft-retained-entries-len-1");
        self.state.lock().retained_log_entries.len()
    }

    fn latest_snapshot_file(&self) -> Result<Option<RaftSnapshotFile>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-latest-snapshot-file-1");
        let _state = self.state.lock();
        read_snapshot_file(&self.snapshot_path)
    }

    pub fn read_entries(&self) -> Result<Vec<RaftLogEntry>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-read-entries-1");
        let state = self.state.lock();
        read_log_entries_with_snapshot(&self.log_path, state.latest_snapshot.as_ref())
    }

    pub fn entries_from(&self, index: u64) -> Result<Vec<RaftLogEntry>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-entries-from-1");
        let state = self.state.lock();
        Ok(entries_from_cached(&state.retained_log_entries, index))
    }

    pub fn entries_from_limited(
        &self,
        index: u64,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Vec<RaftLogEntry>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-entries-from-limited-1");
        let state = self.state.lock();
        entries_from_limited_cached(&state.retained_log_entries, index, max_entries, max_bytes)
    }

    pub fn entries_range(
        &self,
        start_index: u64,
        end_index: u64,
    ) -> Result<Vec<RaftLogEntry>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-entries-range-1");
        if start_index > end_index {
            return Ok(Vec::new());
        }
        let state = self.state.lock();
        let entries = entries_range_cached(&state.retained_log_entries, start_index, end_index);
        if entries.last().map(|entry| entry.index) != Some(end_index) {
            return Err(BrokerRaftError::InvalidLog(format!(
                "log is missing committed entry range {}..={}",
                start_index, end_index
            )));
        }
        Ok(entries)
    }

    pub fn prev_term_and_entries_from_limited(
        &self,
        next_index: u64,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Option<(u64, Vec<RaftLogEntry>)>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-prev-term-and-limited-entries-1");
        let state = self.state.lock();
        prev_term_and_entries_limited_cached(
            &state.retained_log_entries,
            state.latest_snapshot.as_ref(),
            state.last_index,
            state.last_term,
            next_index,
            max_entries,
            max_bytes,
        )
    }

    pub fn term_at(&self, index: u64) -> Result<Option<u64>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-term-at-1");
        let state = self.state.lock();
        Ok(term_at_index(&state, index))
    }

    pub fn last_index_for_term(&self, term: u64) -> Result<Option<u64>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-last-index-for-term-1");
        let state = self.state.lock();
        Ok(last_index_for_term(&state, term))
    }

    pub fn log_len_bytes(&self) -> Result<u64, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-log-len-bytes-1");
        let _state = self.state.lock();
        match fs::metadata(&self.log_path) {
            Ok(metadata) => Ok(metadata.len()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(err) => Err(err.into()),
        }
    }

    fn read_hard_state(&self) -> Result<RaftHardState, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-read-hard-state-1");
        let state = self.state.lock();
        Ok(state.hard_state.clone())
    }

    fn write_hard_state(&self, state: &RaftHardState) -> Result<(), BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-write-hard-state-1");
        let mut log_state = self.state.lock();
        if log_state.hard_state == *state {
            return Ok(());
        }
        write_pretty_json_atomic(&self.hard_state_path, state)?;
        log_state.hard_state = state.clone();
        Ok(())
    }

    pub fn replace_all(&self, entries: &[RaftLogEntry]) -> Result<(), BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-replace-all-1");
        let mut state = self.state.lock();
        validate_log_entries_for_snapshot(entries, state.latest_snapshot.as_ref())?;
        rewrite_log(&self.log_path, entries, self.sync_log)?;
        if let Some(last) = entries.last() {
            state.last_index = last.index;
            state.last_term = last.term;
        } else if let Some(snapshot) = state.latest_snapshot.clone() {
            state.last_index = snapshot.last_included_index;
            state.last_term = snapshot.last_included_term;
        } else {
            state.last_index = 0;
            state.last_term = 0;
        }
        state.retained_log_entries = entries.to_vec();
        let (term_by_index, first_index_by_term, last_index_by_term) =
            term_indexes_from_entries(entries);
        state.term_by_index = term_by_index;
        state.first_index_by_term = first_index_by_term;
        state.last_index_by_term = last_index_by_term;
        Ok(())
    }

    fn append_entries_from_leader(
        &self,
        prev_log_index: u64,
        prev_log_term: u64,
        leader_term: u64,
        local_commit_index: u64,
        entries: Vec<RaftLogEntry>,
    ) -> Result<RaftAppendReport, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-append-from-leader-1");
        validate_append_entries_shape(prev_log_index, prev_log_term, leader_term, &entries)?;
        let mut state = self.state.lock();
        let snapshot_index = state
            .latest_snapshot
            .as_ref()
            .map(|snapshot| snapshot.last_included_index)
            .unwrap_or(0);
        let local_last_index = state.last_index;

        if prev_log_index > local_last_index {
            return Ok(RaftAppendReport {
                success: false,
                match_index: local_last_index,
                conflict_index: Some(local_last_index.saturating_add(1)),
                conflict_term: None,
            });
        }

        match term_at_index(&state, prev_log_index) {
            Some(term) if term == prev_log_term => {}
            Some(term) => {
                return Ok(RaftAppendReport {
                    success: false,
                    match_index: local_last_index.min(prev_log_index.saturating_sub(1)),
                    conflict_index: first_index_for_term(&state, term),
                    conflict_term: Some(term),
                });
            }
            None => {
                return Ok(RaftAppendReport {
                    success: false,
                    match_index: local_last_index.min(prev_log_index.saturating_sub(1)),
                    conflict_index: Some(snapshot_index.saturating_add(1)),
                    conflict_term: None,
                });
            }
        }

        let incoming = entries
            .into_iter()
            .filter(|entry| entry.index > snapshot_index && entry.index > prev_log_index)
            .collect::<Vec<_>>();
        let match_index = incoming
            .last()
            .map(|entry| entry.index)
            .unwrap_or(prev_log_index);
        let mut append_from = None;
        let mut rewrite_from = None;
        for (pos, entry) in incoming.iter().enumerate() {
            match state.term_by_index.get(&entry.index).copied() {
                Some(local_term) if local_term == entry.term => {}
                Some(_) => {
                    rewrite_from = Some(pos);
                    break;
                }
                None if entry.index <= state.last_index => {
                    rewrite_from = Some(pos);
                    break;
                }
                None => {
                    append_from = Some(pos);
                    break;
                }
            }
        }

        if let Some(pos) = rewrite_from {
            let rewrite_index = incoming[pos].index;
            if rewrite_index <= local_commit_index {
                return Err(BrokerRaftError::InvalidAppendEntries(format!(
                    "refusing to rewrite committed follower log entry at index {rewrite_index}; local commitIndex is {local_commit_index}"
                )));
            }
            let mut local = state
                .retained_log_entries
                .iter()
                .take_while(|entry| entry.index < rewrite_index)
                .cloned()
                .collect::<Vec<_>>();
            local.extend(incoming[pos..].iter().cloned());
            validate_log_entries_for_snapshot(&local, state.latest_snapshot.as_ref())?;
            rewrite_log(&self.log_path, &local, self.sync_log)?;
            if let Some(last) = local.last() {
                state.last_index = last.index;
                state.last_term = last.term;
            } else if let Some(snapshot) = state.latest_snapshot.clone() {
                state.last_index = snapshot.last_included_index;
                state.last_term = snapshot.last_included_term;
            } else {
                state.last_index = 0;
                state.last_term = 0;
            }
            state.retained_log_entries = local.clone();
            let (term_by_index, first_index_by_term, last_index_by_term) =
                term_indexes_from_entries(&local);
            state.term_by_index = term_by_index;
            state.first_index_by_term = first_index_by_term;
            state.last_index_by_term = last_index_by_term;
        } else if let Some(pos) = append_from {
            let append_only = &incoming[pos..];
            if append_only
                .first()
                .is_some_and(|entry| entry.index <= local_commit_index)
            {
                let append_index = append_only[0].index;
                return Err(BrokerRaftError::InvalidAppendEntries(format!(
                    "refusing to fill missing committed follower log entry at index {append_index}; local commitIndex is {local_commit_index}"
                )));
            }
            append_log_entries(&self.log_path, append_only, self.sync_log)?;
            if let Some(last) = append_only.last() {
                state.last_index = last.index;
                state.last_term = last.term;
            }
            for entry in append_only {
                state.term_by_index.insert(entry.index, entry.term);
                state
                    .first_index_by_term
                    .entry(entry.term)
                    .or_insert(entry.index);
                state.last_index_by_term.insert(entry.term, entry.index);
            }
            state
                .retained_log_entries
                .extend(append_only.iter().cloned());
        }

        Ok(RaftAppendReport {
            success: true,
            match_index,
            conflict_index: None,
            conflict_term: None,
        })
    }

    pub fn write_snapshot(
        &self,
        last_included_index: u64,
        last_included_term: u64,
        payload: serde_json::Value,
    ) -> Result<RaftSnapshotMetadata, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-write-snapshot-1");
        let mut state = self.state.lock();
        let payload_sha256 = snapshot_payload_sha256(&payload)?;
        let metadata = RaftSnapshotMetadata {
            last_included_index,
            last_included_term,
            created_at_ms: unix_ms(),
            payload_sha256: Some(payload_sha256),
        };
        let snapshot = RaftSnapshotFile {
            metadata: metadata.clone(),
            payload,
        };
        write_pretty_json_atomic(&self.snapshot_path, &snapshot)?;

        state.latest_snapshot = Some(metadata.clone());
        if state.last_index <= metadata.last_included_index {
            state.last_index = metadata.last_included_index;
            state.last_term = metadata.last_included_term;
        }
        Ok(metadata)
    }

    fn install_snapshot_from_leader(
        &self,
        last_included_index: u64,
        last_included_term: u64,
        payload_sha256: Option<String>,
        payload: serde_json::Value,
    ) -> Result<RaftSnapshotMetadata, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-install-snapshot-log-1");
        let mut state = self.state.lock();
        let verified_payload_sha256 =
            verify_snapshot_payload_checksum(last_included_index, &payload, payload_sha256)?;
        if let Some(existing) = state
            .latest_snapshot
            .as_ref()
            .filter(|snapshot| snapshot.last_included_index >= last_included_index)
        {
            return Ok(existing.clone());
        }

        let entries = state.retained_log_entries.clone();
        let retain_suffix = term_at_index(&state, last_included_index) == Some(last_included_term);
        let retained: Vec<RaftLogEntry> = if retain_suffix {
            entries
                .into_iter()
                .filter(|entry| entry.index > last_included_index)
                .collect()
        } else {
            Vec::new()
        };
        let metadata = RaftSnapshotMetadata {
            last_included_index,
            last_included_term,
            created_at_ms: unix_ms(),
            payload_sha256: Some(verified_payload_sha256),
        };
        let snapshot = RaftSnapshotFile {
            metadata: metadata.clone(),
            payload,
        };
        validate_log_entries_for_snapshot(&retained, Some(&metadata))?;
        write_pretty_json_atomic(&self.snapshot_path, &snapshot)?;
        rewrite_log(&self.log_path, &retained, self.sync_log)?;

        state.latest_snapshot = Some(metadata.clone());
        if let Some(last) = retained.last() {
            state.last_index = last.index;
            state.last_term = last.term;
        } else {
            state.last_index = metadata.last_included_index;
            state.last_term = metadata.last_included_term;
        }
        state.retained_log_entries = retained.clone();
        let (term_by_index, first_index_by_term, last_index_by_term) =
            term_indexes_from_entries(&retained);
        state.term_by_index = term_by_index;
        state.first_index_by_term = first_index_by_term;
        state.last_index_by_term = last_index_by_term;
        Ok(metadata)
    }

    pub fn compact_to_latest_snapshot(&self) -> Result<RaftCompactionReport, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-compact-latest-1");
        let snapshot = self.latest_snapshot().ok_or(BrokerRaftError::NoSnapshot)?;
        self.compact_through(snapshot.last_included_index)
    }

    pub fn compact_through(
        &self,
        through_index: u64,
    ) -> Result<RaftCompactionReport, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-compact-through-1");
        let mut state = self.state.lock();
        let snapshot_index = state
            .latest_snapshot
            .as_ref()
            .map(|s| s.last_included_index)
            .ok_or(BrokerRaftError::NoSnapshot)?;

        if through_index > snapshot_index {
            return Err(BrokerRaftError::CompactionAheadOfSnapshot {
                through_index,
                snapshot_index,
            });
        }

        let entries = state.retained_log_entries.clone();
        let before = entries.len();
        let retained: Vec<RaftLogEntry> = entries
            .into_iter()
            .filter(|entry| entry.index > through_index)
            .collect();
        validate_log_entries_for_snapshot(&retained, state.latest_snapshot.as_ref())?;
        rewrite_log(&self.log_path, &retained, self.sync_log)?;

        if let Some(last) = retained.last() {
            state.last_index = last.index;
            state.last_term = last.term;
        } else if let Some(snapshot) = state.latest_snapshot.clone() {
            state.last_index = snapshot.last_included_index;
            state.last_term = snapshot.last_included_term;
        } else {
            state.last_index = 0;
            state.last_term = 0;
        }
        state.retained_log_entries = retained.clone();
        let (term_by_index, first_index_by_term, last_index_by_term) =
            term_indexes_from_entries(&retained);
        state.term_by_index = term_by_index;
        state.first_index_by_term = first_index_by_term;
        state.last_index_by_term = last_index_by_term;

        Ok(RaftCompactionReport {
            compacted_through_index: through_index,
            compacted_entries: before.saturating_sub(retained.len()),
            retained_entries: retained.len(),
        })
    }
}

#[derive(Clone)]
pub struct BrokerRaft {
    broker: Broker,
    config: BrokerRaftConfig,
    log: Arc<RaftLogStore>,
    runtime: Arc<Mutex<RaftRuntimeState>>,
    maintenance: Arc<Mutex<RaftMaintenanceState>>,
    commit_lock: Arc<tokio::sync::Mutex<()>>,
    post_commit_fanout: Arc<Mutex<PostCommitFanoutState>>,
    rpc_connections: Arc<Mutex<BTreeMap<String, Arc<AsyncMutex<RaftRpcConnection>>>>>,
    snapshot_transfers: Arc<Mutex<BTreeMap<String, PendingSnapshotTransfer>>>,
    client_request_batch: Arc<Mutex<ClientRequestBatchState>>,
    client_response_cache: Arc<Mutex<ClientResponseCacheState>>,
    next_client_sequence: Arc<AtomicU64>,
    cached_role: Arc<AtomicU8>,
    cached_term: Arc<AtomicU64>,
    cached_leader_peer: Arc<RwLock<LeaderPeerHintCache>>,
    leader_progress_generation: Arc<AtomicU64>,
    telemetry: Arc<BrokerRaftTelemetry>,
    client_batch_notify: Arc<Notify>,
    #[cfg(test)]
    client_batch_waiting: Arc<Notify>,
}

impl BrokerRaft {
    pub fn open(config: BrokerRaftConfig) -> Result<Self, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-open-1");
        config.validate()?;
        let log_store = RaftLogStore::open_with_sync_log(&config.data_dir, config.sync_log)?;
        let mut hard_state = log_store.read_hard_state()?;
        let snapshot_file = log_store.latest_snapshot_file()?;
        let mut recovered_membership = RaftMembership::from_simple(config.peers.clone());
        let mut snapshot_staged_learners = None;
        if let Some(snapshot_file) = &snapshot_file {
            if let Some(membership) = membership_from_snapshot_payload(&snapshot_file.payload)? {
                recovered_membership = membership;
            }
            snapshot_staged_learners =
                staged_learners_from_snapshot_payload(&snapshot_file.payload)?;
        }
        let staged_learners = if let Some(learners) = snapshot_staged_learners.clone() {
            learners
        } else {
            read_staged_learners(
                &config.data_dir.join(LEARNERS_FILE),
                &recovered_membership.active_peers(),
            )?
        };
        let snapshot_index = log_store
            .latest_snapshot()
            .map(|snapshot| snapshot.last_included_index)
            .unwrap_or(0);
        let last_index = log_store.last_index();
        if hard_state.commit_index > last_index {
            error!(
                target: "lmx::raft",
                node_id = %config.node_id,
                durable_commit_index = hard_state.commit_index,
                last_index,
                "raft startup rejected hard-state commit index beyond available log/snapshot boundary",
            );
            return Err(BrokerRaftError::InvalidLog(format!(
                "durable commit index {} is ahead of available log/snapshot boundary {}",
                hard_state.commit_index, last_index
            )));
        }
        let current_term = hard_state.current_term.max(log_store.last_term());
        let commit_index = hard_state.commit_index.max(snapshot_index);
        let mut normalized = hard_state.clone();
        normalized.current_term = current_term;
        normalized.commit_index = commit_index;
        if normalized.current_term != hard_state.current_term {
            normalized.voted_for = None;
        }
        if normalized != hard_state {
            log_store.write_hard_state(&normalized)?;
            hard_state = normalized;
        }

        let log = Arc::new(log_store);
        let now = Instant::now();
        let election_deadline = now
            .checked_add(config.election_timeout_min)
            .unwrap_or(now + Duration::from_millis(300));
        let client_response_cache = Arc::new(Mutex::new(ClientResponseCacheState::default()));
        let cache_for_observer = Arc::clone(&client_response_cache);
        let broker = Broker::with_response_observer(
            config.broker.clone(),
            Arc::new(move |response| {
                observe_client_response_cache(&cache_for_observer, response);
            }),
        );
        let raft = Self {
            broker,
            runtime: Arc::new(Mutex::new(RaftRuntimeState {
                current_term: hard_state.current_term,
                voted_for: hard_state.voted_for,
                role: RaftRole::Follower,
                leader_id: None,
                commit_index: hard_state.commit_index,
                last_applied: snapshot_index.min(hard_state.commit_index),
                election_deadline,
                leader_progress: BTreeMap::new(),
                staged_learners: staged_learners
                    .into_iter()
                    .map(|peer| (peer.id.clone(), peer))
                    .collect(),
                membership: recovered_membership,
            })),
            maintenance: Arc::new(Mutex::new(RaftMaintenanceState {
                last_snapshot_at: now,
                leader_quorum_observed_at: now,
            })),
            commit_lock: Arc::new(tokio::sync::Mutex::new(())),
            post_commit_fanout: Arc::new(Mutex::new(PostCommitFanoutState::default())),
            rpc_connections: Arc::new(Mutex::new(BTreeMap::new())),
            snapshot_transfers: Arc::new(Mutex::new(BTreeMap::new())),
            client_request_batch: Arc::new(Mutex::new(ClientRequestBatchState::default())),
            client_response_cache,
            next_client_sequence: Arc::new(AtomicU64::new(last_index.saturating_add(1).max(1))),
            cached_role: Arc::new(AtomicU8::new(RAFT_ROLE_CACHE_FOLLOWER)),
            cached_term: Arc::new(AtomicU64::new(hard_state.current_term)),
            cached_leader_peer: Arc::new(RwLock::new(LeaderPeerHintCache::default())),
            leader_progress_generation: Arc::new(AtomicU64::new(0)),
            telemetry: Arc::new(BrokerRaftTelemetry::default()),
            client_batch_notify: Arc::new(Notify::new()),
            #[cfg(test)]
            client_batch_waiting: Arc::new(Notify::new()),
            config,
            log,
        };
        if let Some(snapshot_file) = snapshot_file {
            raft.broker
                .install_raft_snapshot(&snapshot_file.payload)
                .map_err(BrokerRaftError::BrokerSnapshot)?;
            if let Some(membership) = membership_from_snapshot_payload(&snapshot_file.payload)? {
                raft.apply_membership(membership)?;
            }
            if let Some(learners) = staged_learners_from_snapshot_payload(&snapshot_file.payload)? {
                raft.apply_staged_learners(learners)?;
            }
            raft.restore_client_response_cache(client_responses_from_snapshot_payload(
                &snapshot_file.payload,
            )?)?;
        }
        raft.apply_committed()?;
        raft.clear_removed_vote_for_current_membership()?;
        Ok(raft)
    }

    pub fn config(&self) -> &BrokerRaftConfig {
        crate::routine_id!("ddl-routine-broker-raft-config-1");
        &self.config
    }

    pub fn broker(&self) -> &Broker {
        crate::routine_id!("ddl-routine-broker-raft-inner-broker-1");
        &self.broker
    }

    pub fn log(&self) -> &RaftLogStore {
        crate::routine_id!("ddl-routine-broker-raft-log-1");
        &self.log
    }

    fn role_cache_value(role: RaftRole) -> u8 {
        crate::routine_id!("ddl-routine-broker-raft-role-cache-value-1");
        match role {
            RaftRole::Follower => RAFT_ROLE_CACHE_FOLLOWER,
            RaftRole::Candidate => RAFT_ROLE_CACHE_CANDIDATE,
            RaftRole::Leader => RAFT_ROLE_CACHE_LEADER,
        }
    }

    fn publish_role_cache(&self, role: RaftRole, term: u64) {
        crate::routine_id!("ddl-routine-broker-raft-publish-role-cache-1");
        self.cached_term.store(term, Ordering::Release);
        self.cached_role
            .store(Self::role_cache_value(role), Ordering::Release);
    }

    fn publish_runtime_role_cache(&self, runtime: &RaftRuntimeState) {
        crate::routine_id!("ddl-routine-broker-raft-publish-runtime-role-cache-1");
        self.publish_role_cache(runtime.role, runtime.current_term);
    }

    fn leader_peer_hint_from_runtime(runtime: &RaftRuntimeState) -> LeaderPeerHintCache {
        crate::routine_id!("ddl-routine-broker-raft-leader-peer-hint-from-runtime-1");
        let leader_id = runtime.leader_id.clone();
        let leader_peer = leader_id.as_deref().and_then(|id| {
            runtime
                .membership
                .active_peers()
                .into_iter()
                .find(|peer| peer.id == id)
        });
        LeaderPeerHintCache {
            leader_id,
            leader_peer,
        }
    }

    fn publish_leader_peer_hint_cache(&self, runtime: &RaftRuntimeState) -> LeaderPeerHintCache {
        crate::routine_id!("ddl-routine-broker-raft-publish-leader-peer-hint-cache-1");
        let hint = Self::leader_peer_hint_from_runtime(runtime);
        *self.cached_leader_peer.write() = hint.clone();
        hint
    }

    fn note_leader_progress_changed(&self) {
        crate::routine_id!("ddl-routine-broker-raft-progress-changed-1");
        self.leader_progress_generation
            .fetch_add(1, Ordering::AcqRel);
    }

    fn leader_progress_generation(&self) -> u64 {
        crate::routine_id!("ddl-routine-broker-raft-progress-generation-1");
        self.leader_progress_generation.load(Ordering::Acquire)
    }

    fn election_loop_action_at(&self, now: Instant) -> RaftElectionLoopAction {
        crate::routine_id!("ddl-routine-broker-raft-election-loop-action-1");
        let runtime = self.runtime.lock();
        self.publish_runtime_role_cache(&runtime);
        if runtime.role == RaftRole::Leader {
            return RaftElectionLoopAction::Heartbeat;
        }
        if now >= runtime.election_deadline {
            return RaftElectionLoopAction::StartElection;
        }
        RaftElectionLoopAction::Sleep(runtime.election_deadline.duration_since(now))
    }

    pub fn is_leader(&self) -> bool {
        crate::routine_id!("ddl-routine-broker-raft-is-leader-1");
        if let Some(runtime) = self.runtime.try_lock() {
            let is_leader = runtime.role == RaftRole::Leader;
            self.publish_runtime_role_cache(&runtime);
            return is_leader;
        }
        self.cached_role.load(Ordering::Acquire) == RAFT_ROLE_CACHE_LEADER
    }

    fn is_leader_in_term(&self, term: u64) -> bool {
        crate::routine_id!("ddl-routine-broker-raft-is-leader-term-1");
        if let Some(runtime) = self.runtime.try_lock() {
            let is_leader_in_term =
                runtime.role == RaftRole::Leader && runtime.current_term == term;
            self.publish_runtime_role_cache(&runtime);
            return is_leader_in_term;
        }
        self.cached_role.load(Ordering::Acquire) == RAFT_ROLE_CACHE_LEADER
            && self.cached_term.load(Ordering::Acquire) == term
    }

    pub fn is_leader_ready(&self) -> bool {
        crate::routine_id!("ddl-routine-broker-raft-is-leader-ready-1");
        let (is_leader, self_is_quorum) = {
            let runtime = self.runtime.lock();
            let self_ack = BTreeSet::from([self.config.node_id.clone()]);
            (
                runtime.role == RaftRole::Leader,
                runtime.membership.quorum_met(&self_ack),
            )
        };
        if !is_leader {
            return false;
        }
        if self_is_quorum {
            return true;
        }
        self.maintenance.lock().leader_quorum_observed_at.elapsed()
            < self.config.election_timeout_min
    }

    async fn ensure_leader_ready_async(&self) -> Result<(), BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-ensure-leader-ready-async-1");
        if self.is_leader_ready() {
            return Ok(());
        }
        let should_step_down = {
            let runtime = self.runtime.lock();
            let self_ack = BTreeSet::from([self.config.node_id.clone()]);
            runtime.role == RaftRole::Leader
                && !runtime.membership.quorum_met(&self_ack)
                && self.maintenance.lock().leader_quorum_observed_at.elapsed()
                    >= self.config.election_timeout_min
        };
        if should_step_down {
            let term = self.runtime.lock().current_term;
            self.step_down_blocking(term, None).await;
        }
        Err(BrokerRaftError::NotLeader {
            leader_id: self.leader_id(),
            leader_addr: self.leader_addr(),
        })
    }

    pub fn leader_id(&self) -> Option<String> {
        crate::routine_id!("ddl-routine-broker-raft-leader-id-1");
        self.runtime.lock().leader_id.clone()
    }

    fn leader_peer_hint(&self) -> (Option<String>, Option<RaftPeerConfig>) {
        crate::routine_id!("ddl-routine-broker-raft-leader-peer-hint-1");
        let hint = if let Some(runtime) = self.runtime.try_lock() {
            self.publish_leader_peer_hint_cache(&runtime)
        } else {
            self.cached_leader_peer.read().clone()
        };
        (hint.leader_id, hint.leader_peer)
    }

    pub fn leader_addr(&self) -> Option<String> {
        crate::routine_id!("ddl-routine-broker-raft-leader-addr-1");
        self.leader_peer_hint().1.map(|peer| peer.addr)
    }

    pub fn membership(&self) -> RaftMembership {
        crate::routine_id!("ddl-routine-broker-raft-membership-1");
        self.runtime.lock().membership.clone()
    }

    pub fn active_peers(&self) -> Vec<RaftPeerConfig> {
        crate::routine_id!("ddl-routine-broker-raft-active-peers-1");
        self.runtime.lock().membership.active_peers()
    }

    pub fn active_cluster_size(&self) -> usize {
        crate::routine_id!("ddl-routine-broker-raft-active-cluster-size-1");
        self.runtime.lock().membership.cluster_size()
    }

    pub fn active_quorum_size(&self) -> usize {
        crate::routine_id!("ddl-routine-broker-raft-active-quorum-size-1");
        self.runtime.lock().membership.quorum_size()
    }

    pub fn membership_is_joint(&self) -> bool {
        crate::routine_id!("ddl-routine-broker-raft-membership-is-joint-api-1");
        self.runtime.lock().membership.is_joint()
    }

    pub fn current_term(&self) -> u64 {
        crate::routine_id!("ddl-routine-broker-raft-current-term-api-1");
        self.runtime.lock().current_term
    }

    pub fn commit_index(&self) -> u64 {
        crate::routine_id!("ddl-routine-broker-raft-commit-index-api-1");
        self.runtime.lock().commit_index
    }

    pub fn last_applied(&self) -> u64 {
        crate::routine_id!("ddl-routine-broker-raft-last-applied-api-1");
        self.runtime.lock().last_applied
    }

    pub fn progress_snapshot(&self) -> RaftProgressSnapshot {
        crate::routine_id!("ddl-routine-broker-raft-progress-snapshot-1");
        let last_log_index = self.log.last_index();
        let last_log_term = self.log.last_term();
        let (
            role,
            is_leader,
            leader_id,
            current_term,
            commit_index,
            last_applied,
            membership,
            leader_progress,
            staged_learners,
        ) = {
            let runtime = self.runtime.lock();
            let is_leader = runtime.role == RaftRole::Leader;
            (
                raft_role_name(runtime.role),
                is_leader,
                runtime.leader_id.clone(),
                runtime.current_term,
                runtime.commit_index,
                runtime.last_applied,
                runtime.membership.clone(),
                runtime.leader_progress.clone(),
                runtime.staged_learners.clone(),
            )
        };
        let leader_quorum_age_ms = is_leader.then(|| {
            self.maintenance
                .lock()
                .leader_quorum_observed_at
                .elapsed()
                .as_millis() as u64
        });
        let is_leader_ready = self.is_leader_ready();
        let mut peers_by_id = membership
            .active_peers()
            .into_iter()
            .map(|peer| (peer.id.clone(), peer))
            .collect::<BTreeMap<_, _>>();
        peers_by_id.extend(
            staged_learners
                .iter()
                .map(|(id, peer)| (id.clone(), peer.clone())),
        );
        for peer_id in leader_progress.keys() {
            peers_by_id
                .entry(peer_id.clone())
                .or_insert_with(|| RaftPeerConfig {
                    id: peer_id.clone(),
                    addr: String::new(),
                });
        }
        peers_by_id
            .entry(self.config.node_id.clone())
            .or_insert_with(|| RaftPeerConfig {
                id: self.config.node_id.clone(),
                addr: self
                    .config
                    .advertise_addr
                    .clone()
                    .or_else(|| {
                        self.config
                            .bind_addr
                            .as_ref()
                            .map(std::string::ToString::to_string)
                    })
                    .unwrap_or_default(),
            });

        let peers = peers_by_id
            .into_iter()
            .map(|(id, peer)| {
                let is_self = id == self.config.node_id;
                let staged_learner = staged_learners.contains_key(&id);
                let progress = if is_self {
                    Some(RaftPeerProgress {
                        next_index: last_log_index.saturating_add(1),
                        match_index: last_log_index,
                    })
                } else {
                    leader_progress.get(&id).copied()
                };
                let match_index = progress.map(|progress| progress.match_index);
                let next_index = progress.map(|progress| progress.next_index);
                let lag = match_index.map(|match_index| last_log_index.saturating_sub(match_index));
                let caught_up = match_index.map(|match_index| match_index >= last_log_index);
                RaftPeerProgressSnapshot {
                    id: id.clone(),
                    addr: peer.addr,
                    is_self,
                    voter: !staged_learner && membership.contains_id(&id),
                    staged_learner,
                    membership_role: membership_role_name(&membership, &id, staged_learner),
                    next_index,
                    match_index,
                    lag,
                    caught_up,
                }
            })
            .collect::<Vec<_>>();

        RaftProgressSnapshot {
            node_id: self.config.node_id.clone(),
            role,
            is_leader,
            is_leader_ready,
            leader_id: leader_id.clone(),
            leader_addr: leader_id
                .as_ref()
                .and_then(|id| peers.iter().find(|peer| &peer.id == id))
                .map(|peer| peer.addr.clone()),
            leader_quorum_age_ms,
            leader_quorum_timeout_ms: self.config.election_timeout_min.as_millis() as u64,
            current_term,
            commit_index,
            last_applied,
            last_log_index,
            last_log_term,
            membership_joint: membership.is_joint(),
            membership,
            peers,
        }
    }

    pub fn telemetry_snapshot(&self) -> RaftTelemetrySnapshot {
        crate::routine_id!("ddl-routine-broker-raft-telemetry-snapshot-1");
        RaftTelemetrySnapshot {
            append_progress_updates_total: self
                .telemetry
                .append_progress_updates_total
                .load(Ordering::Relaxed),
            append_conflict_repairs_total: self
                .telemetry
                .append_conflict_repairs_total
                .load(Ordering::Relaxed),
            append_conflict_clamps_total: self
                .telemetry
                .append_conflict_clamps_total
                .load(Ordering::Relaxed),
            append_invalid_success_responses_total: self
                .telemetry
                .append_invalid_success_responses_total
                .load(Ordering::Relaxed),
            install_snapshot_chunks_total: self
                .telemetry
                .install_snapshot_chunks_total
                .load(Ordering::Relaxed),
            install_snapshot_bytes_total: self
                .telemetry
                .install_snapshot_bytes_total
                .load(Ordering::Relaxed),
            install_snapshot_progress_updates_total: self
                .telemetry
                .install_snapshot_progress_updates_total
                .load(Ordering::Relaxed),
        }
    }

    pub fn raft_metrics_text(&self) -> String {
        crate::routine_id!("ddl-routine-broker-raft-metrics-text-1");
        let snapshot = self.telemetry_snapshot();
        format!(
            concat!(
                "# HELP dd_rust_network_mutex_raft_append_progress_updates_total Leader-side AppendEntries progress changes applied to peer nextIndex or matchIndex.\n",
                "# TYPE dd_rust_network_mutex_raft_append_progress_updates_total counter\n",
                "dd_rust_network_mutex_raft_append_progress_updates_total {}\n",
                "# HELP dd_rust_network_mutex_raft_append_conflict_repairs_total Leader-side AppendEntries conflict hints processed for follower catch-up repair.\n",
                "# TYPE dd_rust_network_mutex_raft_append_conflict_repairs_total counter\n",
                "dd_rust_network_mutex_raft_append_conflict_repairs_total {}\n",
                "# HELP dd_rust_network_mutex_raft_append_conflict_clamps_total Conflict repairs clamped because a stale hint would rewind nextIndex below known matchIndex plus one.\n",
                "# TYPE dd_rust_network_mutex_raft_append_conflict_clamps_total counter\n",
                "dd_rust_network_mutex_raft_append_conflict_clamps_total {}\n",
                "# HELP dd_rust_network_mutex_raft_append_invalid_success_responses_total AppendEntries success responses rejected because the reported matchIndex could not cover the matched previous entry or sent batch.\n",
                "# TYPE dd_rust_network_mutex_raft_append_invalid_success_responses_total counter\n",
                "dd_rust_network_mutex_raft_append_invalid_success_responses_total {}\n",
                "# HELP dd_rust_network_mutex_raft_install_snapshot_chunks_total Leader-side InstallSnapshot chunk RPCs attempted for lagging peer catch-up.\n",
                "# TYPE dd_rust_network_mutex_raft_install_snapshot_chunks_total counter\n",
                "dd_rust_network_mutex_raft_install_snapshot_chunks_total {}\n",
                "# HELP dd_rust_network_mutex_raft_install_snapshot_bytes_total Raw snapshot payload bytes attempted in leader-side InstallSnapshot chunks.\n",
                "# TYPE dd_rust_network_mutex_raft_install_snapshot_bytes_total counter\n",
                "dd_rust_network_mutex_raft_install_snapshot_bytes_total {}\n",
                "# HELP dd_rust_network_mutex_raft_install_snapshot_progress_updates_total Leader-side peer progress changes applied after InstallSnapshot acknowledgement.\n",
                "# TYPE dd_rust_network_mutex_raft_install_snapshot_progress_updates_total counter\n",
                "dd_rust_network_mutex_raft_install_snapshot_progress_updates_total {}\n",
            ),
            snapshot.append_progress_updates_total,
            snapshot.append_conflict_repairs_total,
            snapshot.append_conflict_clamps_total,
            snapshot.append_invalid_success_responses_total,
            snapshot.install_snapshot_chunks_total,
            snapshot.install_snapshot_bytes_total,
            snapshot.install_snapshot_progress_updates_total,
        )
    }

    fn quorum_met(&self, ack_ids: &BTreeSet<String>) -> bool {
        crate::routine_id!("ddl-routine-broker-raft-quorum-met-1");
        self.runtime.lock().membership.quorum_met(ack_ids)
    }

    pub async fn spawn_raft_tasks(&self) -> Result<Vec<JoinHandle<()>>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-spawn-tasks-1");
        let bind_addr = self.config.bind_addr.ok_or_else(|| {
            BrokerRaftError::InvalidConfig("raft.bind_addr is required when Raft is enabled".into())
        })?;
        let listener = TcpListener::bind(bind_addr).await?;
        info!(
            target: "lmx::raft",
            node_id = %self.config.node_id,
            %bind_addr,
            quorum = self.active_quorum_size(),
            "raft RPC listener bound",
        );

        let accept_node = self.clone();
        let accept_task = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer)) => {
                        if let Err(err) = stream.set_nodelay(true) {
                            debug!(
                                target: "lmx::raft",
                                %peer,
                                error = %err,
                                "failed to enable TCP_NODELAY on accepted raft RPC socket",
                            );
                        }
                        let node = accept_node.clone();
                        tokio::spawn(async move {
                            if let Err(err) = node.handle_rpc_stream(stream).await {
                                warn!(target: "lmx::raft", %peer, error=%err, "raft RPC failed");
                            }
                        });
                    }
                    Err(err) => {
                        error!(target: "lmx::raft", error=%err, "raft accept failed");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        });

        let election_node = self.clone();
        let election_task = tokio::spawn(async move {
            election_node.election_loop().await;
        });

        let maintenance_node = self.clone();
        let maintenance_task = tokio::spawn(async move {
            maintenance_node.maintenance_loop().await;
        });

        Ok(vec![accept_task, election_task, maintenance_task])
    }

    pub async fn spawn_raft_tasks_into(
        &self,
        tasks: &mut JoinSet<()>,
    ) -> Result<(), BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-spawn-tasks-into-1");
        let bind_addr = self.config.bind_addr.ok_or_else(|| {
            BrokerRaftError::InvalidConfig("raft.bind_addr is required when Raft is enabled".into())
        })?;
        let listener = TcpListener::bind(bind_addr).await?;
        info!(
            target: "lmx::raft",
            node_id = %self.config.node_id,
            %bind_addr,
            quorum = self.active_quorum_size(),
            "raft RPC listener bound",
        );

        let accept_node = self.clone();
        tasks.spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer)) => {
                        if let Err(err) = stream.set_nodelay(true) {
                            debug!(
                                target: "lmx::raft",
                                %peer,
                                error = %err,
                                "failed to enable TCP_NODELAY on accepted raft RPC socket",
                            );
                        }
                        let node = accept_node.clone();
                        tokio::spawn(async move {
                            if let Err(err) = node.handle_rpc_stream(stream).await {
                                warn!(target: "lmx::raft", %peer, error=%err, "raft RPC failed");
                            }
                        });
                    }
                    Err(err) => {
                        error!(target: "lmx::raft", error=%err, "raft accept failed");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        });

        let election_node = self.clone();
        tasks.spawn(async move {
            election_node.election_loop().await;
        });

        let maintenance_node = self.clone();
        tasks.spawn(async move {
            maintenance_node.maintenance_loop().await;
        });

        Ok(())
    }

    pub fn register_client(
        &self,
    ) -> (
        ClientId,
        tokio::sync::mpsc::UnboundedReceiver<crate::protocol::Response>,
    ) {
        crate::routine_id!("ddl-routine-broker-raft-register-client-1");
        self.broker
            .register_client_with_id(self.next_raft_client_id())
    }

    fn next_raft_client_id(&self) -> ClientId {
        crate::routine_id!("ddl-routine-broker-raft-next-client-id-1");
        let sequence = self
            .next_client_sequence
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        raft_client_id_for_node(&self.config.node_id, sequence)
    }

    /// Append the request to the leader log, replicate it to a quorum, and
    /// only then apply it to the in-process broker.
    pub async fn handle_request(
        &self,
        client: ClientId,
        request: Request,
    ) -> Result<u64, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-handle-request-1");
        self.ensure_leader_ready_async().await?;
        self.enqueue_client_request(client, request, None, None)
            .await
    }

    async fn enqueue_client_request(
        &self,
        client_id: ClientId,
        request: Request,
        request_id: Option<String>,
        request_fingerprint: Option<String>,
    ) -> Result<u64, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-enqueue-client-request-1");
        let (result_tx, result_rx) = oneshot::channel();
        let (start_driver, notify_full_batch) = {
            let mut state = self.client_request_batch.lock();
            let max_pending = self.config.client_batch_max_pending.max(1);
            if state.pending.len() >= max_pending {
                return Err(BrokerRaftError::ClientQueueFull {
                    pending: state.pending.len(),
                    limit: max_pending,
                });
            }
            state.pending.push_back(PendingClientRequest {
                client_id,
                request,
                request_id,
                request_fingerprint,
                result_tx,
            });
            let notify_full_batch =
                state.pending.len() >= self.config.client_batch_max_entries.max(1);
            if state.driver_active {
                (false, notify_full_batch)
            } else {
                state.driver_active = true;
                (true, notify_full_batch)
            }
        };
        if notify_full_batch {
            self.client_batch_notify.notify_one();
        }
        if start_driver {
            let node = self.clone();
            tokio::spawn(async move {
                node.drive_client_request_batches().await;
            });
        }
        match result_rx.await {
            Ok(Ok(index)) => Ok(index),
            Ok(Err(error)) => Err(error.into_broker_error()),
            Err(_) => Err(BrokerRaftError::Rpc(
                "raft client request batch driver stopped before replying".into(),
            )),
        }
    }

    async fn drive_client_request_batches(&self) {
        crate::routine_id!("ddl-routine-broker-raft-drive-client-batches-1");
        loop {
            let pending_len = self.client_request_batch.lock().pending.len();
            let max_entries = self.config.client_batch_max_entries.max(1);
            if pending_len > 0
                && pending_len < max_entries
                && max_entries > 1
                && !self.config.client_batch_max_delay.is_zero()
            {
                let notified = self.client_batch_notify.notified();
                tokio::pin!(notified);
                let pending_len = self.client_request_batch.lock().pending.len();
                if pending_len > 0 && pending_len < max_entries {
                    #[cfg(test)]
                    self.client_batch_waiting.notify_waiters();
                    tokio::select! {
                        _ = tokio::time::sleep(self.config.client_batch_max_delay) => {}
                        _ = &mut notified => {}
                    }
                }
            }

            let pipeline = self.take_client_request_pipeline();
            if pipeline.is_empty() {
                let mut state = self.client_request_batch.lock();
                if state.pending.is_empty() {
                    state.driver_active = false;
                    return;
                }
                continue;
            }

            let result = {
                let commands = pipeline
                    .iter()
                    .map(|pending| {
                        match (
                            pending.request_id.clone(),
                            pending.request_fingerprint.clone(),
                        ) {
                            (Some(request_id), Some(request_fingerprint)) => {
                                RaftCommand::ClientRequestWithIdentity {
                                    client_id: pending.client_id,
                                    request: pending.request.clone(),
                                    grant: None,
                                    request_id,
                                    request_fingerprint,
                                }
                            }
                            _ => RaftCommand::ClientRequest {
                                client_id: pending.client_id,
                                request: pending.request.clone(),
                                grant: None,
                            },
                        }
                    })
                    .collect::<Vec<_>>();
                let _commit_guard = self.commit_lock.lock().await;
                self.append_replicate_commit_apply_client_batch(commands)
                    .await
            };
            match result {
                Ok(indexes) => {
                    for (pending, index) in pipeline.into_iter().zip(indexes) {
                        let _ = pending.result_tx.send(Ok(index));
                    }
                }
                Err(err) => {
                    let error = ClientRequestBatchError::from_broker_error(err);
                    for pending in pipeline {
                        let _ = pending.result_tx.send(Err(error.clone()));
                    }
                }
            }
        }
    }

    fn take_client_request_pipeline(&self) -> Vec<PendingClientRequest> {
        crate::routine_id!("ddl-routine-broker-raft-take-client-pipeline-1");
        let mut state = self.client_request_batch.lock();
        let max_entries = self.config.client_batch_max_entries.max(1);
        let max_batches = self.config.client_pipeline_max_batches.max(1);
        let take = state
            .pending
            .len()
            .min(max_entries.saturating_mul(max_batches));
        let mut batch = Vec::with_capacity(take);
        for _ in 0..take {
            if let Some(pending) = state.pending.pop_front() {
                batch.push(pending);
            }
        }
        batch
    }

    pub async fn drop_client(&self, client: ClientId) -> Result<u64, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-drop-client-1");
        self.ensure_leader_ready_async().await?;
        let _commit_guard = self.commit_lock.lock().await;
        self.append_replicate_commit_apply(RaftCommand::DropClient { client_id: client })
            .await
    }

    pub async fn change_membership(
        &self,
        peers: Vec<RaftPeerConfig>,
    ) -> Result<u64, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-change-membership-1");
        let new_peers = validate_membership_peers(peers)?;
        self.ensure_leader_ready_async().await?;
        let _commit_guard = self.commit_lock.lock().await;
        let current_membership = self.membership();
        if let RaftMembership::Joint {
            old_peers,
            new_peers: joint_new_peers,
        } = current_membership
        {
            if new_peers != joint_new_peers {
                return Err(BrokerRaftError::InvalidConfig(
                    "cannot start a different raft membership change while joint consensus is active; post the joint config's new peers to finish the current change".into(),
                ));
            }
            let old_ids = old_peers
                .iter()
                .map(|peer| peer.id.clone())
                .collect::<BTreeSet<_>>();
            let final_index = self
                .append_replicate_commit_apply(RaftCommand::SetMembership {
                    membership: RaftMembership::from_simple(joint_new_peers),
                })
                .await?;
            self.catch_up_new_voters(&old_ids, final_index).await?;
            return Ok(final_index);
        }
        let old_peers = self.active_peers();
        self.catch_up_new_membership_peers(&old_peers, &new_peers)
            .await?;
        let old_ids = old_peers
            .iter()
            .map(|peer| peer.id.clone())
            .collect::<BTreeSet<_>>();
        let joint = RaftMembership::Joint {
            old_peers,
            new_peers: new_peers.clone(),
        };
        self.append_replicate_commit_apply_with_membership(
            RaftCommand::SetMembership {
                membership: joint.clone(),
            },
            Some(joint),
        )
        .await?;
        let final_index = self
            .append_replicate_commit_apply(RaftCommand::SetMembership {
                membership: RaftMembership::from_simple(new_peers),
            })
            .await?;
        self.catch_up_new_voters(&old_ids, final_index).await?;
        Ok(final_index)
    }

    pub fn staged_learners(&self) -> Vec<RaftPeerConfig> {
        crate::routine_id!("ddl-routine-broker-raft-staged-learners-1");
        self.runtime
            .lock()
            .staged_learners
            .values()
            .cloned()
            .collect()
    }

    pub async fn stage_learners(
        &self,
        peers: Vec<RaftPeerConfig>,
    ) -> Result<Vec<RaftPeerConfig>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-stage-learners-1");
        self.ensure_leader_ready_async().await?;
        if peers.is_empty() {
            return Err(BrokerRaftError::InvalidConfig(
                "raft learner staging requires at least one peer".into(),
            ));
        }
        let _commit_guard = self.commit_lock.lock().await;
        if self.membership_is_joint() {
            return Err(BrokerRaftError::InvalidConfig(
                "cannot stage raft learners while joint consensus is active".into(),
            ));
        }
        let active_peers = self.active_peers();
        let mut learners_by_id = self
            .runtime
            .lock()
            .staged_learners
            .clone()
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        for peer in validate_staged_learner_peers(peers, &active_peers)? {
            learners_by_id.insert(peer.id.clone(), peer);
        }
        let learners = validate_staged_learner_peers(
            learners_by_id.values().cloned().collect(),
            &active_peers,
        )?;
        let target_index = self
            .append_replicate_commit_apply(RaftCommand::SetStagedLearners {
                learners: learners.clone(),
            })
            .await?;
        if target_index > 0 {
            for peer in learners.iter().cloned() {
                if let Err(err) = self.catch_up_learner_peer(peer.clone(), target_index).await {
                    warn!(
                        target: "lmx::raft",
                        node_id = %self.config.node_id,
                        peer = %peer.id,
                        target_index,
                        error = %err,
                        "staged learner catch-up did not complete",
                    );
                }
            }
        }
        Ok(self.staged_learners())
    }

    pub async fn remove_staged_learners(
        &self,
        ids: Vec<String>,
    ) -> Result<Vec<RaftPeerConfig>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-remove-staged-learners-1");
        self.ensure_leader_ready_async().await?;
        if ids.is_empty() {
            return Err(BrokerRaftError::InvalidConfig(
                "raft learner removal requires at least one id".into(),
            ));
        }
        let _commit_guard = self.commit_lock.lock().await;
        if self.membership_is_joint() {
            return Err(BrokerRaftError::InvalidConfig(
                "cannot remove raft learners while joint consensus is active".into(),
            ));
        }
        let ids = ids
            .into_iter()
            .map(|id| id.trim().to_string())
            .collect::<BTreeSet<_>>();
        let active_peers = self.active_peers();
        let learners = validate_staged_learner_peers(
            self.runtime
                .lock()
                .staged_learners
                .values()
                .filter(|peer| !ids.contains(&peer.id))
                .cloned()
                .collect(),
            &active_peers,
        )?;
        self.append_replicate_commit_apply(RaftCommand::SetStagedLearners {
            learners: learners.clone(),
        })
        .await?;
        Ok(self.staged_learners())
    }

    fn persist_staged_learners_for_active_peers(
        &self,
        learners: &[RaftPeerConfig],
        active_peers: &[RaftPeerConfig],
    ) -> Result<(), BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-persist-staged-learners-active-1");
        let learners = validate_staged_learner_peers(learners.to_vec(), active_peers)?;
        write_staged_learners(
            &self.log.data_dir,
            &self.log.data_dir.join(LEARNERS_FILE),
            &learners,
        )
    }

    fn apply_staged_learners(&self, learners: Vec<RaftPeerConfig>) -> Result<(), BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-apply-staged-learners-1");
        let active_peers = self.active_peers();
        let active_ids = active_peers
            .iter()
            .map(|peer| peer.id.clone())
            .collect::<BTreeSet<_>>();
        let learners = validate_staged_learner_peers(learners, &active_peers)?;
        let learner_ids = learners
            .iter()
            .map(|peer| peer.id.clone())
            .collect::<BTreeSet<_>>();
        let initial_next_index = self
            .log
            .latest_snapshot()
            .map(|snapshot| snapshot.last_included_index.saturating_add(1))
            .unwrap_or(1);
        self.persist_staged_learners_for_active_peers(&learners, &active_peers)?;
        {
            let mut runtime = self.runtime.lock();
            let before_progress = runtime.leader_progress.clone();
            runtime.staged_learners = learners
                .iter()
                .cloned()
                .map(|peer| (peer.id.clone(), peer))
                .collect();
            runtime
                .leader_progress
                .retain(|peer_id, _| active_ids.contains(peer_id) || learner_ids.contains(peer_id));
            if runtime.role == RaftRole::Leader {
                for peer in &learners {
                    runtime
                        .leader_progress
                        .entry(peer.id.clone())
                        .or_insert(RaftPeerProgress {
                            next_index: initial_next_index,
                            match_index: 0,
                        });
                }
            }
            if runtime.leader_progress != before_progress {
                self.note_leader_progress_changed();
            }
        }
        self.rpc_connections
            .lock()
            .retain(|peer_id, _| active_ids.contains(peer_id) || learner_ids.contains(peer_id));
        Ok(())
    }

    async fn catch_up_new_membership_peers(
        &self,
        old_peers: &[RaftPeerConfig],
        new_peers: &[RaftPeerConfig],
    ) -> Result<(), BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-catch-up-new-membership-peers-1");
        let old_ids: BTreeSet<String> = old_peers.iter().map(|peer| peer.id.clone()).collect();
        let learners = new_peers
            .iter()
            .filter(|peer| peer.id != self.config.node_id && !old_ids.contains(&peer.id))
            .cloned()
            .collect::<Vec<_>>();
        if learners.is_empty() {
            return Ok(());
        }
        let target_index = self.log.last_index();
        if target_index == 0 {
            return Ok(());
        }
        let learner_ids = learners
            .iter()
            .map(|peer| peer.id.clone())
            .collect::<Vec<_>>();
        let initial_next_index = self
            .log
            .latest_snapshot()
            .map(|snapshot| snapshot.last_included_index.saturating_add(1))
            .unwrap_or(1);
        {
            let mut runtime = self.runtime.lock();
            let before_progress = runtime.leader_progress.clone();
            for peer in &learners {
                runtime
                    .staged_learners
                    .insert(peer.id.clone(), peer.clone());
                runtime
                    .leader_progress
                    .entry(peer.id.clone())
                    .or_insert(RaftPeerProgress {
                        next_index: initial_next_index,
                        match_index: 0,
                    });
            }
            if runtime.leader_progress != before_progress {
                self.note_leader_progress_changed();
            }
        }
        let mut catchup_tasks = JoinSet::new();
        for peer in learners {
            let node = self.clone();
            catchup_tasks
                .spawn(async move { node.catch_up_learner_peer(peer, target_index).await });
        }
        while let Some(result) = catchup_tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    catchup_tasks.abort_all();
                    self.discard_staged_learners(&learner_ids, &old_ids);
                    return Err(err);
                }
                Err(err) => {
                    catchup_tasks.abort_all();
                    self.discard_staged_learners(&learner_ids, &old_ids);
                    return Err(BrokerRaftError::Rpc(format!(
                        "raft learner catch-up task failed: {err}"
                    )));
                }
            }
        }
        Ok(())
    }

    async fn catch_up_learner_peer(
        &self,
        peer: RaftPeerConfig,
        target_index: u64,
    ) -> Result<(), BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-catch-up-learner-peer-1");
        let timeout = self
            .config
            .election_timeout_max
            .saturating_mul(4)
            .max(Duration::from_secs(2));
        let deadline = deadline_after(timeout);
        loop {
            if !self.is_leader() {
                return Err(BrokerRaftError::NotLeader {
                    leader_id: self.leader_id(),
                    leader_addr: self.leader_addr(),
                });
            }
            if self
                .runtime
                .lock()
                .leader_progress
                .get(&peer.id)
                .is_some_and(|progress| progress.match_index >= target_index)
            {
                return Ok(());
            }
            let (term, leader_commit) = {
                let runtime = self.runtime.lock();
                (runtime.current_term, runtime.commit_index)
            };
            let before_progress = self.leader_progress_generation();
            if self
                .replicate_to_peer(peer.clone(), term, leader_commit, Some(target_index))
                .await?
                .target_reached
            {
                return Ok(());
            }
            let progress_changed = self.leader_progress_generation() != before_progress;
            if tokio::time::Instant::now() >= deadline {
                return Err(BrokerRaftError::LearnerCatchUpFailed {
                    peer_id: peer.id,
                    target_index,
                });
            }
            if progress_changed {
                tokio::task::yield_now().await;
                continue;
            }
            tokio::time::sleep(
                self.config
                    .heartbeat_interval
                    .max(Duration::from_millis(25)),
            )
            .await;
        }
    }

    async fn catch_up_new_voters(
        &self,
        old_ids: &BTreeSet<String>,
        target_index: u64,
    ) -> Result<(), BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-new-voter-catch-up-1");
        let new_voters = self
            .active_peers()
            .into_iter()
            .filter(|peer| peer.id != self.config.node_id && !old_ids.contains(&peer.id))
            .collect::<Vec<_>>();
        let mut catchup_tasks = JoinSet::new();
        for peer in new_voters {
            let node = self.clone();
            catchup_tasks
                .spawn(async move { node.catch_up_learner_peer(peer, target_index).await });
        }
        while let Some(result) = catchup_tasks.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    catchup_tasks.abort_all();
                    return Err(err);
                }
                Err(err) => {
                    catchup_tasks.abort_all();
                    return Err(BrokerRaftError::Rpc(format!(
                        "raft promoted-voter catch-up task failed: {err}"
                    )));
                }
            }
        }
        Ok(())
    }

    fn discard_staged_learners(&self, learner_ids: &[String], old_ids: &BTreeSet<String>) {
        crate::routine_id!("ddl-routine-broker-raft-discard-staged-learners-1");
        let learner_ids = learner_ids.iter().cloned().collect::<BTreeSet<_>>();
        {
            let mut runtime = self.runtime.lock();
            let before_progress = runtime.leader_progress.clone();
            runtime
                .leader_progress
                .retain(|peer_id, _| old_ids.contains(peer_id) || !learner_ids.contains(peer_id));
            runtime
                .staged_learners
                .retain(|peer_id, _| old_ids.contains(peer_id) || !learner_ids.contains(peer_id));
            if runtime.leader_progress != before_progress {
                self.note_leader_progress_changed();
            }
        }
        self.rpc_connections
            .lock()
            .retain(|peer_id, _| old_ids.contains(peer_id) || !learner_ids.contains(peer_id));
    }

    async fn append_replicate_commit_apply(
        &self,
        command: RaftCommand,
    ) -> Result<u64, BrokerRaftError> {
        self.append_replicate_commit_apply_with_membership(command, None)
            .await
    }

    async fn append_replicate_commit_apply_with_membership(
        &self,
        command: RaftCommand,
        commit_membership: Option<RaftMembership>,
    ) -> Result<u64, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-append-replicate-commit-1");
        self.ensure_leader_ready_async().await?;
        let term = self.runtime.lock().current_term;
        let next_index = self.log.last_index().saturating_add(1);
        let command = command_with_deterministic_grant(command, next_index);
        let mut entries = self
            .append_local_batch_blocking(term, vec![command])
            .await?;
        let entry = entries.pop().ok_or_else(|| {
            BrokerRaftError::Rpc("raft log append produced no entry for one command".into())
        })?;
        let acks = match commit_membership.as_ref() {
            Some(membership) => {
                self.replicate_until_quorum_for_membership(entry.index, membership)
                    .await?
            }
            None => self.replicate_until_quorum(entry.index).await?,
        };
        let quorum = commit_membership
            .as_ref()
            .map(RaftMembership::quorum_size)
            .unwrap_or_else(|| self.active_quorum_size());
        let quorum_met = commit_membership
            .as_ref()
            .map(|membership| membership.quorum_met(&acks))
            .unwrap_or_else(|| self.quorum_met(&acks));
        if !quorum_met {
            self.step_down_after_proposal_quorum_failure(term, entry.index, acks.len(), quorum)
                .await;
            return Err(BrokerRaftError::QuorumUnavailable {
                index: entry.index,
                votes: acks.len(),
                quorum,
            });
        }
        if !self
            .commit_leader_index_in_term_with_membership_blocking(
                entry.index,
                term,
                true,
                commit_membership.clone(),
            )
            .await?
        {
            return Err(BrokerRaftError::NotLeader {
                leader_id: self.leader_id(),
                leader_addr: self.leader_addr(),
            });
        }
        self.spawn_post_commit_heartbeat();
        Ok(entry.index)
    }

    async fn append_replicate_commit_apply_client_batch(
        &self,
        commands: Vec<RaftCommand>,
    ) -> Result<Vec<u64>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-append-replicate-client-batch-1");
        if commands.is_empty() {
            return Ok(Vec::new());
        }
        self.ensure_leader_ready_async().await?;
        let term = self.runtime.lock().current_term;
        let first_index = self.log.last_index().saturating_add(1);
        let commands = commands
            .into_iter()
            .enumerate()
            .map(|(idx, command)| {
                command_with_deterministic_grant(command, first_index.saturating_add(idx as u64))
            })
            .collect::<Vec<_>>();
        let entries = self.append_local_batch_blocking(term, commands).await?;
        let Some(last_entry) = entries.last() else {
            return Ok(Vec::new());
        };
        let target_index = last_entry.index;
        let acks = self.replicate_until_quorum(target_index).await?;
        let quorum = self.active_quorum_size();
        if !self.quorum_met(&acks) {
            self.step_down_after_proposal_quorum_failure(term, target_index, acks.len(), quorum)
                .await;
            return Err(BrokerRaftError::QuorumUnavailable {
                index: target_index,
                votes: acks.len(),
                quorum,
            });
        }
        if !self
            .commit_leader_index_in_term_blocking(target_index, term, true)
            .await?
        {
            return Err(BrokerRaftError::NotLeader {
                leader_id: self.leader_id(),
                leader_addr: self.leader_addr(),
            });
        }
        self.spawn_post_commit_heartbeat();
        Ok(entries.into_iter().map(|entry| entry.index).collect())
    }

    async fn append_local_batch_blocking(
        &self,
        term: u64,
        commands: Vec<RaftCommand>,
    ) -> Result<Vec<RaftLogEntry>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-append-local-batch-blocking-1");
        let log = self.log.clone();
        tokio::task::spawn_blocking(move || log.append_batch(term, commands))
            .await
            .map_err(|err| BrokerRaftError::Rpc(format!("raft log append task failed: {err}")))?
    }

    async fn step_down_after_proposal_quorum_failure(
        &self,
        term: u64,
        index: u64,
        votes: usize,
        quorum: usize,
    ) {
        crate::routine_id!("ddl-routine-broker-raft-stepdown-proposal-quorum-failure-1");
        if !self.is_leader_in_term(term) {
            return;
        }
        warn!(
            target: "lmx::raft",
            node_id = %self.config.node_id,
            term,
            index,
            votes,
            quorum,
            "raft leader stepping down after failing to commit client proposal"
        );
        self.step_down_blocking(term, None).await;
    }

    fn cached_client_response(
        &self,
        request_id: &str,
        request_fingerprint: &str,
    ) -> Result<CachedClientResponseLookup, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-cached-client-response-1");
        let cache = self.client_response_cache.lock();
        let Some(cached) = cache.entries.get(request_id) else {
            return Ok(CachedClientResponseLookup::Missing);
        };
        if cached.request_fingerprint != request_fingerprint {
            return Err(BrokerRaftError::IdempotencyKeyConflict {
                request_id: request_id.to_string(),
            });
        }
        Ok(match &cached.response {
            Some(response) => CachedClientResponseLookup::Completed(response.clone()),
            None => CachedClientResponseLookup::Pending,
        })
    }

    fn begin_client_request_apply(
        &self,
        request_id: &str,
        request_fingerprint: &str,
    ) -> Result<bool, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-begin-client-request-apply-1");
        let limit = self.config.client_response_cache_max_entries.max(1);
        let mut cache = self.client_response_cache.lock();
        if let Some(cached) = cache.entries.get_mut(request_id) {
            if cached.request_fingerprint != request_fingerprint {
                return Err(BrokerRaftError::IdempotencyKeyConflict {
                    request_id: request_id.to_string(),
                });
            }
            if cached.applied {
                return Ok(false);
            }
            cached.applied = true;
            return Ok(true);
        }
        cache.order.push_back(request_id.to_string());
        cache.entries.insert(
            request_id.to_string(),
            CachedClientResponse {
                request_fingerprint: request_fingerprint.to_string(),
                applied: true,
                response: None,
            },
        );
        trim_client_response_cache(&mut cache, limit);
        Ok(true)
    }

    fn reserve_client_request_id(
        &self,
        request_id: &str,
        request_fingerprint: &str,
    ) -> Result<bool, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-reserve-client-request-id-1");
        let limit = self.config.client_response_cache_max_entries.max(1);
        let mut cache = self.client_response_cache.lock();
        if let Some(cached) = cache.entries.get(request_id) {
            if cached.request_fingerprint != request_fingerprint {
                return Err(BrokerRaftError::IdempotencyKeyConflict {
                    request_id: request_id.to_string(),
                });
            }
            return Ok(false);
        }
        cache.order.push_back(request_id.to_string());
        cache.entries.insert(
            request_id.to_string(),
            CachedClientResponse {
                request_fingerprint: request_fingerprint.to_string(),
                applied: false,
                response: None,
            },
        );
        trim_client_response_cache(&mut cache, limit);
        Ok(true)
    }

    fn release_unapplied_client_request_id(&self, request_id: &str, request_fingerprint: &str) {
        crate::routine_id!("ddl-routine-broker-raft-release-unapplied-request-id-1");
        let mut cache = self.client_response_cache.lock();
        let should_remove = cache.entries.get(request_id).is_some_and(|cached| {
            !cached.applied && cached.request_fingerprint == request_fingerprint
        });
        if should_remove {
            cache.entries.remove(request_id);
            cache.order.retain(|existing| existing != request_id);
        }
    }

    fn remember_client_response(
        &self,
        request_id: &str,
        request_fingerprint: String,
        response: Response,
    ) {
        crate::routine_id!("ddl-routine-broker-raft-remember-client-response-1");
        let limit = self.config.client_response_cache_max_entries.max(1);
        let mut cache = self.client_response_cache.lock();
        if cache.entries.contains_key(request_id) {
            cache.order.retain(|existing| existing != request_id);
        }
        cache.order.push_back(request_id.to_string());
        cache.entries.insert(
            request_id.to_string(),
            CachedClientResponse {
                request_fingerprint,
                applied: true,
                response: Some(response),
            },
        );
        trim_client_response_cache(&mut cache, limit);
    }

    fn client_response_snapshot_entries(&self) -> Vec<ClientResponseSnapshotEntry> {
        crate::routine_id!("ddl-routine-broker-raft-client-response-snapshot-entries-1");
        let cache = self.client_response_cache.lock();
        cache
            .order
            .iter()
            .filter_map(|request_id| {
                cache
                    .entries
                    .get(request_id)
                    .filter(|cached| cached.applied)
                    .map(|cached| ClientResponseSnapshotEntry {
                        request_id: request_id.clone(),
                        request_fingerprint: cached.request_fingerprint.clone(),
                        response: cached.response.clone(),
                    })
            })
            .collect()
    }

    fn restore_client_response_cache(
        &self,
        entries: Vec<ClientResponseSnapshotEntry>,
    ) -> Result<(), BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-restore-client-response-cache-1");
        let limit = self.config.client_response_cache_max_entries.max(1);
        let mut cache = self.client_response_cache.lock();
        cache.entries.clear();
        cache.order.clear();
        for entry in entries {
            if entry.request_id.is_empty() || entry.request_fingerprint.is_empty() {
                return Err(BrokerRaftError::InvalidLog(
                    "snapshot client response entry has empty request id or fingerprint".into(),
                ));
            }
            if cache.entries.contains_key(&entry.request_id) {
                return Err(BrokerRaftError::InvalidLog(format!(
                    "snapshot client response entry duplicates request id `{}`",
                    entry.request_id
                )));
            }
            cache.order.push_back(entry.request_id.clone());
            cache.entries.insert(
                entry.request_id,
                CachedClientResponse {
                    request_fingerprint: entry.request_fingerprint,
                    applied: true,
                    response: entry.response,
                },
            );
            trim_client_response_cache(&mut cache, limit);
        }
        Ok(())
    }

    pub async fn run_ephemeral(
        &self,
        request: Request,
        request_uuid: &str,
        wait: Duration,
        is_acquire: bool,
    ) -> Result<Option<Response>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-run-ephemeral-1");
        let request = request_for_ephemeral_wait(request, wait, is_acquire);
        let request_fingerprint = request_fingerprint(&request)?;
        let fail_fast_acquire = request_is_fail_fast_acquire(&request);
        if !self.is_leader() {
            let (leader_id, leader_peer) = self.leader_peer_hint();
            if leader_id.as_deref() == Some(self.config.node_id.as_str()) {
                return Err(BrokerRaftError::NotLeader {
                    leader_id,
                    leader_addr: None,
                });
            }
            let leader_peer = leader_peer.ok_or_else(|| BrokerRaftError::NotLeader {
                leader_id: leader_id.clone(),
                leader_addr: None,
            })?;
            let rpc = RaftRpc::ProxyRequest {
                auth_token: None,
                request,
                request_uuid: request_uuid.to_string(),
                wait_ms: wait.as_millis() as u64,
                is_acquire,
            };
            let timeout = wait.max(Duration::from_secs(2));
            let response = self.send_rpc_to_peer(&leader_peer, rpc, timeout).await?;
            return match response {
                RaftRpcResponse::ProxyResponse {
                    response,
                    error: None,
                    ..
                } => Ok(response),
                RaftRpcResponse::ProxyResponse {
                    error: Some(error), ..
                } => Err(BrokerRaftError::Rpc(error)),
                other => Err(BrokerRaftError::Rpc(format!(
                    "unexpected proxy response: {other:?}"
                ))),
            };
        }
        self.ensure_leader_ready_async().await?;
        match self.cached_client_response(request_uuid, &request_fingerprint)? {
            CachedClientResponseLookup::Completed(response) => return Ok(Some(response)),
            CachedClientResponseLookup::Pending => return Ok(None),
            CachedClientResponseLookup::Missing => {}
        }
        if !self.reserve_client_request_id(request_uuid, &request_fingerprint)? {
            return match self.cached_client_response(request_uuid, &request_fingerprint)? {
                CachedClientResponseLookup::Completed(response) => Ok(Some(response)),
                CachedClientResponseLookup::Pending | CachedClientResponseLookup::Missing => {
                    Ok(None)
                }
            };
        }
        let (client_id, mut rx) = self
            .broker
            .register_client_with_id(self.next_raft_client_id());
        let result = self
            .enqueue_client_request(
                client_id,
                request,
                Some(request_uuid.to_string()),
                Some(request_fingerprint.clone()),
            )
            .await;
        if let Err(err) = result {
            self.release_unapplied_client_request_id(request_uuid, &request_fingerprint);
            self.broker.drop_client(client_id);
            return Err(err);
        }
        let outcome = wait_for_response(&mut rx, request_uuid, wait, is_acquire).await;
        if let Some(lock_uuid) = outcome.as_ref().and_then(granted_lock_uuid) {
            self.broker.detach_lock_from_client(client_id, &lock_uuid);
            self.broker.drop_client(client_id);
        } else if is_acquire {
            if fail_fast_acquire {
                self.broker.drop_client(client_id);
            } else if let Err(err) = self.drop_client(client_id).await {
                warn!(
                    target: "lmx::raft",
                    node_id = %self.config.node_id,
                    client_id,
                    error = %err,
                    "failed to replicate timed-out ephemeral client cleanup",
                );
                self.broker.drop_client(client_id);
            }
        } else {
            self.broker.drop_client(client_id);
        }
        if let Some(response) = outcome.clone() {
            self.remember_client_response(request_uuid, request_fingerprint, response);
        }
        Ok(outcome)
    }

    async fn handle_rpc_stream(&self, stream: TcpStream) -> Result<(), BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-handle-rpc-stream-1");
        let mut reader = TokioBufReader::new(stream);
        loop {
            let line = match read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes()).await
            {
                Ok(line) => line,
                Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(err) => return Err(err.into()),
            };
            let rpc: RaftRpc = serde_json::from_str(line.trim())?;
            let response = self.handle_rpc(rpc).await;
            let body = serde_json::to_vec(&response)?;
            let stream = reader.get_mut();
            stream.write_all(&body).await?;
            stream.write_all(b"\n").await?;
            stream.flush().await?;
        }
    }

    async fn handle_rpc(&self, rpc: RaftRpc) -> RaftRpcResponse {
        crate::routine_id!("ddl-routine-broker-raft-handle-rpc-1");
        if !self.peer_rpc_authorized(&rpc) {
            return RaftRpcResponse::Error {
                term: self.runtime.lock().current_term,
                error: "unauthorized raft RPC".into(),
            };
        }
        match rpc {
            RaftRpc::PreVote {
                auth_token: _,
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => self.handle_pre_vote(term, candidate_id, last_log_index, last_log_term),
            RaftRpc::RequestVote {
                auth_token: _,
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                self.handle_request_vote_rpc(term, candidate_id, last_log_index, last_log_term)
                    .await
            }
            RaftRpc::AppendEntries {
                auth_token: _,
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                self.handle_append_entries_rpc(
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                )
                .await
            }
            RaftRpc::InstallSnapshot {
                auth_token: _,
                term,
                leader_id,
                last_included_index,
                last_included_term,
                payload_sha256,
                offset,
                done,
                data,
            } => {
                self.handle_install_snapshot_rpc(
                    term,
                    leader_id,
                    last_included_index,
                    last_included_term,
                    payload_sha256,
                    offset,
                    done,
                    data,
                )
                .await
            }
            RaftRpc::ProxyRequest {
                auth_token: _,
                request,
                request_uuid,
                wait_ms,
                is_acquire,
            } => {
                let term = self.runtime.lock().current_term;
                if !self.is_leader() {
                    return RaftRpcResponse::ProxyResponse {
                        term,
                        response: None,
                        error: Some(
                            BrokerRaftError::NotLeader {
                                leader_id: self.leader_id(),
                                leader_addr: self.leader_addr(),
                            }
                            .to_string(),
                        ),
                    };
                }
                match self
                    .run_ephemeral(
                        request,
                        &request_uuid,
                        Duration::from_millis(wait_ms),
                        is_acquire,
                    )
                    .await
                {
                    Ok(response) => RaftRpcResponse::ProxyResponse {
                        term,
                        response,
                        error: None,
                    },
                    Err(err) => RaftRpcResponse::ProxyResponse {
                        term,
                        response: None,
                        error: Some(err.to_string()),
                    },
                }
            }
        }
    }

    fn peer_rpc_authorized(&self, rpc: &RaftRpc) -> bool {
        crate::routine_id!("ddl-routine-broker-raft-peer-rpc-authorized-1");
        let Some(expected) = self.config.peer_token.as_deref() else {
            return true;
        };
        peer_rpc_auth_token(rpc).is_some_and(|actual| constant_time_eq(actual, expected))
    }

    fn with_peer_auth(&self, mut rpc: RaftRpc) -> RaftRpc {
        crate::routine_id!("ddl-routine-broker-raft-with-peer-auth-1");
        let token = self.config.peer_token.clone();
        match &mut rpc {
            RaftRpc::PreVote { auth_token, .. }
            | RaftRpc::RequestVote { auth_token, .. }
            | RaftRpc::AppendEntries { auth_token, .. }
            | RaftRpc::InstallSnapshot { auth_token, .. }
            | RaftRpc::ProxyRequest { auth_token, .. } => {
                *auth_token = token;
            }
        }
        rpc
    }

    fn handle_pre_vote(
        &self,
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
    ) -> RaftRpcResponse {
        crate::routine_id!("ddl-routine-broker-raft-handle-pre-vote-1");
        let local_last_term = self.log.last_term();
        let local_last_index = self.log.last_index();
        let runtime = self.runtime.lock();
        let local_is_voter = runtime.membership.contains_id(&self.config.node_id);
        let candidate_is_voter = runtime.membership.contains_id(&candidate_id);
        if !local_is_voter || !candidate_is_voter {
            return RaftRpcResponse::PreVote {
                term: runtime.current_term,
                vote_granted: false,
            };
        }
        if term < runtime.current_term {
            return RaftRpcResponse::PreVote {
                term: runtime.current_term,
                vote_granted: false,
            };
        }
        let leader_fresh = Instant::now() < runtime.election_deadline;
        if runtime
            .leader_id
            .as_ref()
            .is_some_and(|leader_id| leader_id != &candidate_id && leader_fresh)
        {
            return RaftRpcResponse::PreVote {
                term: runtime.current_term,
                vote_granted: false,
            };
        }
        let log_is_fresh = last_log_term > local_last_term
            || (last_log_term == local_last_term && last_log_index >= local_last_index);
        RaftRpcResponse::PreVote {
            term: runtime.current_term,
            vote_granted: log_is_fresh,
        }
    }

    fn handle_request_vote(
        &self,
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
    ) -> RaftRpcResponse {
        crate::routine_id!("ddl-routine-broker-raft-handle-vote-1");
        let local_last_term = self.log.last_term();
        let local_last_index = self.log.last_index();
        let mut runtime = self.runtime.lock();
        let local_is_voter = runtime.membership.contains_id(&self.config.node_id);
        let candidate_is_voter = runtime.membership.contains_id(&candidate_id);
        if !local_is_voter || !candidate_is_voter {
            return RaftRpcResponse::RequestVote {
                term: runtime.current_term,
                vote_granted: false,
            };
        }
        if term < runtime.current_term {
            return RaftRpcResponse::RequestVote {
                term: runtime.current_term,
                vote_granted: false,
            };
        }

        if runtime.leader_id.as_ref().is_some_and(|leader_id| {
            leader_id != &candidate_id && Instant::now() < runtime.election_deadline
        }) {
            return RaftRpcResponse::RequestVote {
                term: runtime.current_term,
                vote_granted: false,
            };
        }

        let response_term = runtime.current_term.max(term);
        let log_is_fresh = last_log_term > local_last_term
            || (last_log_term == local_last_term && last_log_index >= local_last_index);
        let can_vote = if term > runtime.current_term {
            true
        } else {
            runtime
                .voted_for
                .as_ref()
                .is_none_or(|voted_for| voted_for == &candidate_id)
        };
        let granted = can_vote && log_is_fresh;
        let next_voted_for = if granted {
            Some(candidate_id.clone())
        } else if term > runtime.current_term {
            None
        } else {
            runtime.voted_for.clone()
        };
        if term > runtime.current_term || granted {
            if term > runtime.current_term {
                self.publish_role_cache(RaftRole::Follower, term);
            }
            let state = RaftHardState {
                current_term: response_term,
                voted_for: next_voted_for.clone(),
                commit_index: runtime.commit_index,
            };
            if let Err(err) = self.log.write_hard_state(&state) {
                return RaftRpcResponse::Error {
                    term: response_term,
                    error: err.to_string(),
                };
            }
        }
        if term > runtime.current_term {
            runtime.current_term = term;
            runtime.role = RaftRole::Follower;
            runtime.leader_id = None;
        }
        if granted {
            runtime.election_deadline = self.next_election_deadline();
        }
        runtime.voted_for = next_voted_for;
        self.publish_leader_peer_hint_cache(&runtime);
        RaftRpcResponse::RequestVote {
            term: response_term,
            vote_granted: granted,
        }
    }

    async fn handle_request_vote_rpc(
        &self,
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
    ) -> RaftRpcResponse {
        crate::routine_id!("ddl-routine-broker-raft-handle-vote-rpc-1");
        let node = self.clone();
        match tokio::task::spawn_blocking(move || {
            node.handle_request_vote(term, candidate_id, last_log_index, last_log_term)
        })
        .await
        {
            Ok(response) => response,
            Err(err) => RaftRpcResponse::Error {
                term: self.runtime.lock().current_term,
                error: format!("raft request vote task failed: {err}"),
            },
        }
    }

    fn handle_append_entries(
        &self,
        term: u64,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<RaftLogEntry>,
        leader_commit: u64,
    ) -> RaftRpcResponse {
        crate::routine_id!("ddl-routine-broker-raft-handle-append-1");
        let local_commit_index = match self.prepare_append_entries(term, &leader_id) {
            Ok(local_commit_index) => local_commit_index,
            Err(response) => return response,
        };

        let append_report = match self.log.append_entries_from_leader(
            prev_log_index,
            prev_log_term,
            term,
            local_commit_index,
            entries,
        ) {
            Ok(report) => report,
            Err(err) => {
                return RaftRpcResponse::Error {
                    term: self.runtime.lock().current_term,
                    error: err.to_string(),
                };
            }
        };
        self.finish_append_entries(term, &leader_id, append_report, leader_commit)
    }

    async fn handle_append_entries_rpc(
        &self,
        term: u64,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<RaftLogEntry>,
        leader_commit: u64,
    ) -> RaftRpcResponse {
        crate::routine_id!("ddl-routine-broker-raft-handle-append-rpc-1");
        let node = self.clone();
        match tokio::task::spawn_blocking(move || {
            node.handle_append_entries(
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            )
        })
        .await
        {
            Ok(response) => response,
            Err(err) => RaftRpcResponse::Error {
                term: self.runtime.lock().current_term,
                error: format!("raft follower append task failed: {err}"),
            },
        }
    }

    fn prepare_append_entries(&self, term: u64, leader_id: &str) -> Result<u64, RaftRpcResponse> {
        crate::routine_id!("ddl-routine-broker-raft-prepare-append-1");
        let mut runtime = self.runtime.lock();
        if !runtime.membership.contains_id(leader_id) {
            return Err(RaftRpcResponse::AppendEntries {
                term: runtime.current_term,
                success: false,
                match_index: self.log.last_index(),
                conflict_index: None,
                conflict_term: None,
            });
        }
        if term < runtime.current_term {
            return Err(RaftRpcResponse::AppendEntries {
                term: runtime.current_term,
                success: false,
                match_index: self.log.last_index(),
                conflict_index: None,
                conflict_term: None,
            });
        }
        if term > runtime.current_term {
            self.publish_role_cache(RaftRole::Follower, term);
            let state = RaftHardState {
                current_term: term,
                voted_for: None,
                commit_index: runtime.commit_index,
            };
            if let Err(err) = self.log.write_hard_state(&state) {
                return Err(RaftRpcResponse::Error {
                    term,
                    error: err.to_string(),
                });
            }
            runtime.current_term = term;
            runtime.voted_for = None;
            runtime.leader_id = None;
        }
        if runtime
            .leader_id
            .as_ref()
            .is_some_and(|known_leader_id| known_leader_id != leader_id)
        {
            return Err(RaftRpcResponse::AppendEntries {
                term: runtime.current_term,
                success: false,
                match_index: self.log.last_index(),
                conflict_index: None,
                conflict_term: None,
            });
        }
        self.publish_role_cache(RaftRole::Follower, runtime.current_term);
        runtime.role = RaftRole::Follower;
        runtime.leader_id = Some(leader_id.to_string());
        runtime.election_deadline = self.next_election_deadline();
        self.publish_leader_peer_hint_cache(&runtime);
        Ok(runtime.commit_index)
    }

    fn finish_append_entries(
        &self,
        term: u64,
        leader_id: &str,
        append_report: RaftAppendReport,
        leader_commit: u64,
    ) -> RaftRpcResponse {
        crate::routine_id!("ddl-routine-broker-raft-finish-append-1");
        let response_term = {
            let runtime = self.runtime.lock();
            if runtime.current_term != term
                || runtime.role != RaftRole::Follower
                || runtime
                    .leader_id
                    .as_ref()
                    .is_none_or(|known_leader_id| known_leader_id != leader_id)
            {
                return RaftRpcResponse::AppendEntries {
                    term: runtime.current_term,
                    success: false,
                    match_index: append_report.match_index,
                    conflict_index: append_report.conflict_index,
                    conflict_term: append_report.conflict_term,
                };
            }
            runtime.current_term
        };
        if !append_report.success {
            return RaftRpcResponse::AppendEntries {
                term: response_term,
                success: false,
                match_index: append_report.match_index,
                conflict_index: append_report.conflict_index,
                conflict_term: append_report.conflict_term,
            };
        }

        {
            let mut runtime = self.runtime.lock();
            if runtime.current_term != term
                || runtime.role != RaftRole::Follower
                || runtime
                    .leader_id
                    .as_ref()
                    .is_none_or(|known_leader_id| known_leader_id != leader_id)
            {
                return RaftRpcResponse::AppendEntries {
                    term: runtime.current_term,
                    success: false,
                    match_index: append_report.match_index,
                    conflict_index: None,
                    conflict_term: None,
                };
            }
            let next_commit = runtime
                .commit_index
                .max(leader_commit.min(append_report.match_index));
            if next_commit != runtime.commit_index {
                let state = RaftHardState {
                    current_term: runtime.current_term,
                    voted_for: runtime.voted_for.clone(),
                    commit_index: next_commit,
                };
                if let Err(err) = self.log.write_hard_state(&state) {
                    return RaftRpcResponse::Error {
                        term: runtime.current_term,
                        error: err.to_string(),
                    };
                }
                runtime.commit_index = next_commit;
            }
        }
        if let Err(err) = self.apply_committed() {
            return RaftRpcResponse::Error {
                term: self.runtime.lock().current_term,
                error: err.to_string(),
            };
        }
        if let Err(err) = self.snapshot_and_compact_if_needed(false) {
            return RaftRpcResponse::Error {
                term: self.runtime.lock().current_term,
                error: err.to_string(),
            };
        }

        RaftRpcResponse::AppendEntries {
            term: self.runtime.lock().current_term,
            success: true,
            match_index: append_report.match_index,
            conflict_index: None,
            conflict_term: None,
        }
    }

    fn handle_install_snapshot(
        &self,
        term: u64,
        leader_id: String,
        last_included_index: u64,
        last_included_term: u64,
        payload_sha256: Option<String>,
        offset: u64,
        done: bool,
        data: String,
    ) -> RaftRpcResponse {
        crate::routine_id!("ddl-routine-broker-raft-handle-install-snapshot-1");
        {
            let mut runtime = self.runtime.lock();
            if !runtime.membership.contains_id(&leader_id) {
                return RaftRpcResponse::InstallSnapshot {
                    term: runtime.current_term,
                    success: false,
                    last_included_index: self
                        .log
                        .latest_snapshot()
                        .map(|snapshot| snapshot.last_included_index)
                        .unwrap_or(0),
                };
            }
            if term < runtime.current_term {
                return RaftRpcResponse::InstallSnapshot {
                    term: runtime.current_term,
                    success: false,
                    last_included_index: self
                        .log
                        .latest_snapshot()
                        .map(|snapshot| snapshot.last_included_index)
                        .unwrap_or(0),
                };
            }
            if term > runtime.current_term {
                self.publish_role_cache(RaftRole::Follower, term);
                let state = RaftHardState {
                    current_term: term,
                    voted_for: None,
                    commit_index: runtime.commit_index,
                };
                if let Err(err) = self.log.write_hard_state(&state) {
                    return RaftRpcResponse::Error {
                        term,
                        error: err.to_string(),
                    };
                }
                runtime.current_term = term;
                runtime.voted_for = None;
                runtime.leader_id = None;
            }
            if runtime
                .leader_id
                .as_ref()
                .is_some_and(|known_leader_id| known_leader_id != &leader_id)
            {
                return RaftRpcResponse::InstallSnapshot {
                    term: runtime.current_term,
                    success: false,
                    last_included_index: self
                        .log
                        .latest_snapshot()
                        .map(|snapshot| snapshot.last_included_index)
                        .unwrap_or(0),
                };
            }
            self.publish_role_cache(RaftRole::Follower, runtime.current_term);
            runtime.role = RaftRole::Follower;
            runtime.leader_id = Some(leader_id.clone());
            runtime.election_deadline = self.next_election_deadline();
            self.publish_leader_peer_hint_cache(&runtime);
        }

        let current_snapshot = self.log.latest_snapshot();
        if let Some(snapshot) = current_snapshot
            .as_ref()
            .filter(|snapshot| snapshot.last_included_index >= last_included_index)
        {
            if snapshot.last_included_index == last_included_index
                && snapshot.last_included_term != last_included_term
            {
                return RaftRpcResponse::Error {
                    term: self.runtime.lock().current_term,
                    error: format!(
                        "local snapshot term {} at index {} conflicts with InstallSnapshot term {}",
                        snapshot.last_included_term,
                        snapshot.last_included_index,
                        last_included_term
                    ),
                };
            }
            let should_apply_snapshot = {
                let runtime = self.runtime.lock();
                runtime.commit_index < snapshot.last_included_index
                    || runtime.last_applied < snapshot.last_included_index
            };
            if should_apply_snapshot {
                match self.log.latest_snapshot_file() {
                    Ok(Some(snapshot_file))
                        if snapshot_file.metadata.last_included_index
                            == snapshot.last_included_index
                            && snapshot_file.metadata.last_included_term
                                == snapshot.last_included_term =>
                    {
                        return self.finish_installed_snapshot_payload(
                            term,
                            &leader_id,
                            snapshot_file.metadata.last_included_index,
                            &snapshot_file.payload,
                        );
                    }
                    Ok(_) => {
                        return RaftRpcResponse::Error {
                            term: self.runtime.lock().current_term,
                            error: "local snapshot metadata changed while handling InstallSnapshot"
                                .into(),
                        };
                    }
                    Err(err) => {
                        return RaftRpcResponse::Error {
                            term: self.runtime.lock().current_term,
                            error: err.to_string(),
                        };
                    }
                }
            }
            if let Some(checksum) = payload_sha256.as_deref() {
                self.discard_snapshot_transfer(
                    &leader_id,
                    last_included_index,
                    last_included_term,
                    checksum,
                );
            }
            return RaftRpcResponse::InstallSnapshot {
                term: self.runtime.lock().current_term,
                success: true,
                last_included_index: snapshot.last_included_index,
            };
        }

        let expected_checksum = match payload_sha256.clone() {
            Some(checksum) => checksum,
            None => {
                return RaftRpcResponse::Error {
                    term: self.runtime.lock().current_term,
                    error: BrokerRaftError::SnapshotChecksumMissing {
                        index: last_included_index,
                    }
                    .to_string(),
                };
            }
        };
        let current_snapshot_index = current_snapshot
            .as_ref()
            .map(|snapshot| snapshot.last_included_index)
            .unwrap_or(0);
        let chunk = match decode_snapshot_chunk(&data) {
            Ok(chunk) => chunk,
            Err(err) => {
                self.discard_snapshot_transfer(
                    &leader_id,
                    last_included_index,
                    last_included_term,
                    &expected_checksum,
                );
                return RaftRpcResponse::Error {
                    term: self.runtime.lock().current_term,
                    error: err.to_string(),
                };
            }
        };
        let payload_path = match self.stage_snapshot_chunk(
            &leader_id,
            last_included_index,
            last_included_term,
            &expected_checksum,
            offset,
            done,
            chunk,
        ) {
            Ok(path) => path,
            Err(err) => {
                return RaftRpcResponse::Error {
                    term: self.runtime.lock().current_term,
                    error: err.to_string(),
                };
            }
        };
        let Some(payload_path) = payload_path else {
            return RaftRpcResponse::InstallSnapshot {
                term: self.runtime.lock().current_term,
                success: true,
                last_included_index: current_snapshot_index,
            };
        };
        if let Err(err) = verify_snapshot_payload_file_checksum(
            last_included_index,
            &payload_path,
            payload_sha256.clone(),
        ) {
            let _ = fs::remove_file(&payload_path);
            return RaftRpcResponse::Error {
                term: self.runtime.lock().current_term,
                error: err.to_string(),
            };
        }
        let payload: serde_json::Value = match File::open(&payload_path).and_then(|file| {
            serde_json::from_reader(file).map_err(|err| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string())
            })
        }) {
            Ok(payload) => {
                let _ = fs::remove_file(&payload_path);
                payload
            }
            Err(err) => {
                let _ = fs::remove_file(&payload_path);
                return RaftRpcResponse::Error {
                    term: self.runtime.lock().current_term,
                    error: err.to_string(),
                };
            }
        };
        if let Err(err) = membership_from_snapshot_payload(&payload) {
            return RaftRpcResponse::Error {
                term: self.runtime.lock().current_term,
                error: err.to_string(),
            };
        }
        if let Err(err) = staged_learners_from_snapshot_payload(&payload) {
            return RaftRpcResponse::Error {
                term: self.runtime.lock().current_term,
                error: err.to_string(),
            };
        }
        if let Err(err) = client_responses_from_snapshot_payload(&payload) {
            return RaftRpcResponse::Error {
                term: self.runtime.lock().current_term,
                error: err.to_string(),
            };
        }
        if let Err(err) = Broker::validate_raft_snapshot_payload(&payload) {
            return RaftRpcResponse::Error {
                term: self.runtime.lock().current_term,
                error: err,
            };
        }
        let mut installed_index = current_snapshot_index;
        if last_included_index > current_snapshot_index {
            if let Err(response) = self.ensure_current_snapshot_sender(term, &leader_id) {
                return response;
            }
            let installed = match self.log.install_snapshot_from_leader(
                last_included_index,
                last_included_term,
                payload_sha256.clone(),
                payload.clone(),
            ) {
                Ok(snapshot) => snapshot,
                Err(err) => {
                    return RaftRpcResponse::Error {
                        term: self.runtime.lock().current_term,
                        error: err.to_string(),
                    };
                }
            };
            installed_index = installed.last_included_index;
        }
        self.finish_installed_snapshot_payload(term, &leader_id, installed_index, &payload)
    }

    async fn handle_install_snapshot_rpc(
        &self,
        term: u64,
        leader_id: String,
        last_included_index: u64,
        last_included_term: u64,
        payload_sha256: Option<String>,
        offset: u64,
        done: bool,
        data: String,
    ) -> RaftRpcResponse {
        crate::routine_id!("ddl-routine-broker-raft-handle-install-snapshot-rpc-1");
        let node = self.clone();
        match tokio::task::spawn_blocking(move || {
            node.handle_install_snapshot(
                term,
                leader_id,
                last_included_index,
                last_included_term,
                payload_sha256,
                offset,
                done,
                data,
            )
        })
        .await
        {
            Ok(response) => response,
            Err(err) => RaftRpcResponse::Error {
                term: self.runtime.lock().current_term,
                error: format!("raft install snapshot task failed: {err}"),
            },
        }
    }

    fn ensure_current_snapshot_sender(
        &self,
        term: u64,
        leader_id: &str,
    ) -> Result<u64, RaftRpcResponse> {
        crate::routine_id!("ddl-routine-broker-raft-current-snapshot-sender-1");
        let current_snapshot_index = self
            .log
            .latest_snapshot()
            .map(|snapshot| snapshot.last_included_index)
            .unwrap_or(0);
        let runtime = self.runtime.lock();
        if runtime.current_term != term
            || runtime.role != RaftRole::Follower
            || runtime
                .leader_id
                .as_ref()
                .is_none_or(|known_leader_id| known_leader_id != leader_id)
        {
            return Err(RaftRpcResponse::InstallSnapshot {
                term: runtime.current_term,
                success: false,
                last_included_index: current_snapshot_index,
            });
        }
        Ok(runtime.current_term)
    }

    fn finish_installed_snapshot_payload(
        &self,
        term: u64,
        leader_id: &str,
        installed_index: u64,
        payload: &serde_json::Value,
    ) -> RaftRpcResponse {
        crate::routine_id!("ddl-routine-broker-raft-finish-install-snapshot-1");
        let response_term = match self.ensure_current_snapshot_sender(term, leader_id) {
            Ok(response_term) => response_term,
            Err(response) => return response,
        };
        if let Err(err) = self.apply_installed_snapshot_payload(installed_index, payload) {
            return RaftRpcResponse::Error {
                term: self.runtime.lock().current_term,
                error: err.to_string(),
            };
        }

        RaftRpcResponse::InstallSnapshot {
            term: response_term,
            success: true,
            last_included_index: installed_index,
        }
    }

    fn apply_installed_snapshot_payload(
        &self,
        installed_index: u64,
        payload: &serde_json::Value,
    ) -> Result<(), BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-apply-installed-snapshot-1");
        Broker::validate_raft_snapshot_payload(payload).map_err(BrokerRaftError::BrokerSnapshot)?;
        let snapshot_membership = membership_from_snapshot_payload(payload)?;
        let snapshot_learners = staged_learners_from_snapshot_payload(payload)?;
        let snapshot_client_responses = client_responses_from_snapshot_payload(payload)?;
        let (hard_state, should_apply) = {
            let runtime = self.runtime.lock();
            let next_commit = runtime.commit_index.max(installed_index);
            let should_apply = runtime.last_applied < installed_index;
            let hard_state = if next_commit != runtime.commit_index {
                Some(RaftHardState {
                    current_term: runtime.current_term,
                    voted_for: runtime.voted_for.clone(),
                    commit_index: next_commit,
                })
            } else {
                None
            };
            (hard_state, should_apply)
        };
        if let Some(state) = hard_state {
            debug!(
                target: "lmx::raft",
                node_id = %self.config.node_id,
                snapshot_index = installed_index,
                durable_commit_index = state.commit_index,
                "persisting hard state before applying installed snapshot",
            );
            self.log.write_hard_state(&state)?;
        }
        if should_apply {
            if let Some(membership) = snapshot_membership {
                debug!(
                    target: "lmx::raft",
                    node_id = %self.config.node_id,
                    snapshot_index = installed_index,
                    cluster_size = membership.cluster_size(),
                    quorum = membership.quorum_size(),
                    "applying installed snapshot membership side effects",
                );
                self.apply_membership(membership)?;
            }
            if let Some(learners) = snapshot_learners {
                debug!(
                    target: "lmx::raft",
                    node_id = %self.config.node_id,
                    snapshot_index = installed_index,
                    learner_count = learners.len(),
                    "applying installed snapshot staged learner side effects",
                );
                self.apply_staged_learners(learners)?;
            }
            self.restore_client_response_cache(snapshot_client_responses)?;
            debug!(
                target: "lmx::raft",
                node_id = %self.config.node_id,
                snapshot_index = installed_index,
                "installing broker state from raft snapshot",
            );
            self.broker
                .install_raft_snapshot(payload)
                .map_err(BrokerRaftError::BrokerSnapshot)?;
        }
        {
            let mut runtime = self.runtime.lock();
            runtime.commit_index = runtime.commit_index.max(installed_index);
            runtime.last_applied = runtime.last_applied.max(installed_index);
            info!(
                target: "lmx::raft",
                node_id = %self.config.node_id,
                snapshot_index = installed_index,
                commit_index = runtime.commit_index,
                last_applied = runtime.last_applied,
                applied = should_apply,
                "raft snapshot apply boundary completed",
            );
        }
        Ok(())
    }

    async fn election_loop(&self) {
        crate::routine_id!("ddl-routine-broker-raft-election-loop-1");
        loop {
            match self.election_loop_action_at(Instant::now()) {
                RaftElectionLoopAction::Heartbeat => {
                    if let Ok(_commit_guard) = self.commit_lock.try_lock() {
                        let _ = self.replicate_log_once(None).await;
                    }
                    tokio::time::sleep(self.config.heartbeat_interval).await;
                }
                RaftElectionLoopAction::StartElection => {
                    if let Err(err) = self.start_election().await {
                        warn!(
                            target: "lmx::raft",
                            node_id = %self.config.node_id,
                            error = %err,
                            "raft election failed",
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                RaftElectionLoopAction::Sleep(delay) => {
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    async fn maintenance_loop(&self) {
        crate::routine_id!("ddl-routine-broker-raft-maintenance-loop-1");
        let interval = self.config.snapshot_interval.max(Duration::from_secs(1));
        loop {
            tokio::time::sleep(interval).await;
            if let Err(err) = self.snapshot_and_compact_if_needed_blocking(true).await {
                warn!(
                    target: "lmx::raft",
                    node_id = %self.config.node_id,
                    error = %err,
                    "raft log maintenance failed",
                );
            }
        }
    }

    async fn start_election(&self) -> Result<(), BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-start-election-1");
        if !self.run_pre_vote().await? {
            return Ok(());
        }
        let Some(term) = self.begin_candidate_election_blocking().await? else {
            return Ok(());
        };
        let mut votes = BTreeSet::new();
        votes.insert(self.config.node_id.clone());
        let last_log_index = self.log.last_index();
        let last_log_term = self.log.last_term();
        let remote_peers = self.remote_peers();
        let mut vote_tasks = JoinSet::new();
        for peer in remote_peers.clone() {
            let node = self.clone();
            let peer_id = peer.id.clone();
            let rpc = RaftRpc::RequestVote {
                auth_token: None,
                term,
                candidate_id: self.config.node_id.clone(),
                last_log_index,
                last_log_term,
            };
            let timeout = self.config.election_timeout_min;
            vote_tasks.spawn(async move {
                let response = node.send_rpc_to_peer(&peer, rpc, timeout).await;
                (peer_id, response)
            });
        }
        while !self.quorum_met(&votes) {
            let Some(result) = vote_tasks.join_next().await else {
                break;
            };
            match result {
                Ok((peer_id, Ok(response))) => match response {
                    RaftRpcResponse::RequestVote {
                        term: peer_term,
                        vote_granted,
                    } => {
                        if peer_term > term {
                            vote_tasks.abort_all();
                            self.step_down_blocking(peer_term, None).await;
                            return Ok(());
                        }
                        if vote_granted {
                            votes.insert(peer_id);
                        }
                    }
                    RaftRpcResponse::Error {
                        term: peer_term, ..
                    } if peer_term > term => {
                        vote_tasks.abort_all();
                        self.step_down_blocking(peer_term, None).await;
                        return Ok(());
                    }
                    _ => {}
                },
                Ok((peer_id, Err(err))) => {
                    debug!(
                        target: "lmx::raft",
                        node_id = %self.config.node_id,
                        peer = %peer_id,
                        error = %err,
                        "vote request failed",
                    );
                }
                Err(err) => {
                    debug!(
                        target: "lmx::raft",
                        node_id = %self.config.node_id,
                        error = %err,
                        "vote request task failed",
                    );
                }
            }
            let still_candidate = {
                let runtime = self.runtime.lock();
                runtime.current_term == term && runtime.role == RaftRole::Candidate
            };
            if !still_candidate {
                vote_tasks.abort_all();
                return Ok(());
            }
        }
        vote_tasks.abort_all();

        let mut elected = false;
        if self.quorum_met(&votes) {
            let next_index = self.log.last_index().saturating_add(1);
            let mut runtime = self.runtime.lock();
            if runtime.current_term == term && runtime.role == RaftRole::Candidate {
                runtime.role = RaftRole::Leader;
                runtime.leader_id = Some(self.config.node_id.clone());
                let staged_learners = runtime.staged_learners.clone();
                runtime.leader_progress = remote_peers
                    .into_iter()
                    .chain(staged_learners.into_values())
                    .map(|peer| {
                        (
                            peer.id,
                            RaftPeerProgress {
                                next_index,
                                match_index: 0,
                            },
                        )
                    })
                    .collect();
                self.note_leader_progress_changed();
                info!(
                    target: "lmx::raft",
                    node_id = %self.config.node_id,
                    term,
                    votes = votes.len(),
                    quorum = runtime.membership.quorum_size(),
                    "raft leader elected",
                );
                elected = true;
                self.publish_runtime_role_cache(&runtime);
                self.publish_leader_peer_hint_cache(&runtime);
            }
        }
        if elected {
            self.note_leader_quorum_observed();
            self.append_leader_noop(term).await?;
        }
        Ok(())
    }

    fn begin_candidate_election(&self) -> Result<Option<u64>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-begin-candidate-election-1");
        let mut runtime = self.runtime.lock();
        if !runtime.membership.contains_id(&self.config.node_id) {
            return Ok(None);
        }
        let term = runtime.current_term.saturating_add(1);
        let hard_state = RaftHardState {
            current_term: term,
            voted_for: Some(self.config.node_id.clone()),
            commit_index: runtime.commit_index,
        };
        self.publish_role_cache(RaftRole::Candidate, term);
        self.log.write_hard_state(&hard_state)?;
        runtime.role = RaftRole::Candidate;
        runtime.current_term = term;
        runtime.voted_for = Some(self.config.node_id.clone());
        runtime.leader_id = None;
        runtime.election_deadline = self.next_election_deadline();
        self.publish_leader_peer_hint_cache(&runtime);
        Ok(Some(term))
    }

    async fn begin_candidate_election_blocking(&self) -> Result<Option<u64>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-begin-candidate-election-blocking-1");
        let node = self.clone();
        tokio::task::spawn_blocking(move || node.begin_candidate_election())
            .await
            .map_err(|err| BrokerRaftError::Rpc(format!("raft election task failed: {err}")))?
    }

    async fn run_pre_vote(&self) -> Result<bool, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-run-pre-vote-1");
        let (current_term, pre_vote_term) = {
            let runtime = self.runtime.lock();
            if !runtime.membership.contains_id(&self.config.node_id) {
                return Ok(false);
            }
            (runtime.current_term, runtime.current_term.saturating_add(1))
        };
        let mut votes = BTreeSet::new();
        votes.insert(self.config.node_id.clone());
        if self.quorum_met(&votes) {
            return Ok(true);
        }
        let last_log_index = self.log.last_index();
        let last_log_term = self.log.last_term();
        let mut vote_tasks = JoinSet::new();
        for peer in self.remote_peers() {
            let node = self.clone();
            let peer_id = peer.id.clone();
            let rpc = RaftRpc::PreVote {
                auth_token: None,
                term: pre_vote_term,
                candidate_id: self.config.node_id.clone(),
                last_log_index,
                last_log_term,
            };
            let timeout = self.config.election_timeout_min;
            vote_tasks.spawn(async move {
                let response = node.send_rpc_to_peer(&peer, rpc, timeout).await;
                (peer_id, response)
            });
        }
        while !self.quorum_met(&votes) {
            let Some(result) = vote_tasks.join_next().await else {
                break;
            };
            match result {
                Ok((peer_id, Ok(response))) => match response {
                    RaftRpcResponse::PreVote {
                        term: peer_term,
                        vote_granted,
                    } => {
                        if peer_term > current_term {
                            vote_tasks.abort_all();
                            self.step_down_blocking(peer_term, None).await;
                            return Ok(false);
                        }
                        if vote_granted {
                            votes.insert(peer_id);
                        }
                    }
                    RaftRpcResponse::Error {
                        term: peer_term, ..
                    } if peer_term > current_term => {
                        vote_tasks.abort_all();
                        self.step_down_blocking(peer_term, None).await;
                        return Ok(false);
                    }
                    _ => {}
                },
                Ok((peer_id, Err(err))) => {
                    debug!(
                        target: "lmx::raft",
                        node_id = %self.config.node_id,
                        peer = %peer_id,
                        error = %err,
                        "pre-vote request failed",
                    );
                }
                Err(err) => {
                    debug!(
                        target: "lmx::raft",
                        node_id = %self.config.node_id,
                        error = %err,
                        "pre-vote request task failed",
                    );
                }
            }
            let still_same_term = self.runtime.lock().current_term == current_term;
            if !still_same_term {
                vote_tasks.abort_all();
                return Ok(false);
            }
        }
        vote_tasks.abort_all();
        Ok(self.quorum_met(&votes))
    }

    async fn append_leader_noop(&self, term: u64) -> Result<(), BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-append-leader-noop-1");
        let _commit_guard = self.commit_lock.lock().await;
        let still_leader = {
            let runtime = self.runtime.lock();
            runtime.current_term == term && runtime.role == RaftRole::Leader
        };
        if !still_leader {
            return Ok(());
        }
        self.append_replicate_commit_apply(RaftCommand::Noop)
            .await
            .map(|_| ())
    }

    async fn replicate_log_once(
        &self,
        target_index: Option<u64>,
    ) -> Result<BTreeSet<String>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-replicate-once-1");
        let mut acks = BTreeSet::new();
        acks.insert(self.config.node_id.clone());
        let (term, leader_commit, remote_peers) = {
            let runtime = self.runtime.lock();
            self.publish_runtime_role_cache(&runtime);
            if runtime.role != RaftRole::Leader {
                return Ok(acks);
            }
            let remote_peers = runtime
                .membership
                .active_peers()
                .into_iter()
                .filter(|peer| peer.id != self.config.node_id)
                .collect::<Vec<_>>();
            (runtime.current_term, runtime.commit_index, remote_peers)
        };
        let mut tasks = JoinSet::new();
        for peer in remote_peers {
            let node = self.clone();
            let peer_id = peer.id.clone();
            tasks.spawn(async move {
                node.replicate_to_peer(peer, term, leader_commit, target_index)
                    .await
                    .map(|outcome| (peer_id, outcome))
            });
        }

        let mut active_acks = acks.clone();
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok((peer_id, outcome))) => {
                    if outcome.contacted {
                        active_acks.insert(peer_id.clone());
                    }
                    if outcome.target_reached {
                        acks.insert(peer_id);
                    }
                }
                Ok(Err(err)) => {
                    tasks.abort_all();
                    return Err(err);
                }
                Err(err) => {
                    debug!(
                        target: "lmx::raft",
                        node_id = %self.config.node_id,
                        error = %err,
                        "append entries task failed",
                    );
                }
            }
            if !self.is_leader() {
                tasks.abort_all();
                return Ok(acks);
            }
            if target_index.is_some() && self.quorum_met(&acks) {
                self.note_leader_quorum_observed();
                self.advance_leader_commit_from_progress_blocking().await?;
                tasks.detach_all();
                return Ok(acks);
            }
        }
        self.advance_leader_commit_from_progress_blocking().await?;
        self.check_leader_quorum(&active_acks).await;
        Ok(acks)
    }

    async fn replicate_log_once_for_membership(
        &self,
        target_index: u64,
        membership: &RaftMembership,
    ) -> Result<BTreeSet<String>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-replicate-once-membership-1");
        let mut acks = BTreeSet::new();
        acks.insert(self.config.node_id.clone());
        let (term, leader_commit) = {
            let runtime = self.runtime.lock();
            if runtime.role != RaftRole::Leader {
                return Ok(acks);
            }
            (runtime.current_term, runtime.commit_index)
        };
        let mut tasks = JoinSet::new();
        for peer in membership
            .active_peers()
            .into_iter()
            .filter(|peer| peer.id != self.config.node_id)
        {
            let node = self.clone();
            let peer_id = peer.id.clone();
            tasks.spawn(async move {
                node.replicate_to_peer(peer, term, leader_commit, Some(target_index))
                    .await
                    .map(|outcome| (peer_id, outcome))
            });
        }

        let mut active_acks = acks.clone();
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok((peer_id, outcome))) => {
                    if outcome.contacted {
                        active_acks.insert(peer_id.clone());
                    }
                    if outcome.target_reached {
                        acks.insert(peer_id);
                    }
                }
                Ok(Err(err)) => {
                    tasks.abort_all();
                    return Err(err);
                }
                Err(err) => {
                    debug!(
                        target: "lmx::raft",
                        node_id = %self.config.node_id,
                        error = %err,
                        "membership-scoped append entries task failed",
                    );
                }
            }
            if !self.is_leader() {
                tasks.abort_all();
                return Ok(acks);
            }
            if membership.quorum_met(&acks) {
                self.note_leader_quorum_observed();
                tasks.detach_all();
                return Ok(acks);
            }
        }
        self.check_leader_quorum(&active_acks).await;
        Ok(acks)
    }

    fn note_leader_quorum_observed(&self) {
        crate::routine_id!("ddl-routine-broker-raft-note-leader-quorum-1");
        self.maintenance.lock().leader_quorum_observed_at = Instant::now();
    }

    async fn check_leader_quorum(&self, acks: &BTreeSet<String>) {
        crate::routine_id!("ddl-routine-broker-raft-check-leader-quorum-1");
        if self.quorum_met(acks) {
            self.note_leader_quorum_observed();
            return;
        }
        let elapsed = self.maintenance.lock().leader_quorum_observed_at.elapsed();
        if elapsed < self.config.election_timeout_min {
            return;
        }
        let term = {
            let runtime = self.runtime.lock();
            if runtime.role != RaftRole::Leader {
                return;
            }
            runtime.current_term
        };
        warn!(
            target: "lmx::raft",
            node_id = %self.config.node_id,
            votes = acks.len(),
            quorum = self.active_quorum_size(),
            elapsed_ms = elapsed.as_millis() as u64,
            "raft leader stepping down after failing to observe quorum"
        );
        self.step_down_blocking(term, None).await;
    }

    fn advance_leader_commit_from_progress(&self) -> Result<Option<u64>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-advance-leader-commit-1");
        let last_index = self.log.last_index();
        let (term, current_commit, membership, matched) = {
            let runtime = self.runtime.lock();
            if runtime.role != RaftRole::Leader {
                return Ok(None);
            }
            let mut matched = Vec::with_capacity(runtime.leader_progress.len().saturating_add(1));
            matched.push((self.config.node_id.clone(), last_index));
            matched.extend(
                runtime
                    .leader_progress
                    .iter()
                    .map(|(peer_id, progress)| (peer_id.clone(), progress.match_index)),
            );
            (
                runtime.current_term,
                runtime.commit_index,
                runtime.membership.clone(),
                matched,
            )
        };
        let mut candidates = matched
            .iter()
            .map(|(_, match_index)| *match_index)
            .filter(|match_index| *match_index > current_commit && *match_index <= last_index)
            .collect::<Vec<_>>();
        candidates.sort_unstable();
        candidates.dedup();
        candidates.reverse();

        for candidate in candidates {
            if self.log.term_at(candidate)? != Some(term) {
                continue;
            }
            let ack_ids = matched
                .iter()
                .filter(|(_, match_index)| *match_index >= candidate)
                .map(|(peer_id, _)| peer_id.clone())
                .collect::<BTreeSet<_>>();
            if !membership.quorum_met(&ack_ids) {
                continue;
            }
            return if self.commit_leader_index_in_term(candidate, term, false)? {
                Ok(Some(candidate))
            } else {
                Ok(None)
            };
        }
        Ok(None)
    }

    async fn advance_leader_commit_from_progress_blocking(
        &self,
    ) -> Result<Option<u64>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-advance-leader-commit-blocking-1");
        let node = self.clone();
        tokio::task::spawn_blocking(move || node.advance_leader_commit_from_progress())
            .await
            .map_err(|err| BrokerRaftError::Rpc(format!("raft leader commit task failed: {err}")))?
    }

    fn commit_leader_index_in_term(
        &self,
        index: u64,
        term: u64,
        compact_after_apply: bool,
    ) -> Result<bool, BrokerRaftError> {
        self.commit_leader_index_in_term_with_membership(index, term, compact_after_apply, None)
    }

    async fn commit_leader_index_in_term_blocking(
        &self,
        index: u64,
        term: u64,
        compact_after_apply: bool,
    ) -> Result<bool, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-commit-leader-index-blocking-1");
        self.commit_leader_index_in_term_with_membership_blocking(
            index,
            term,
            compact_after_apply,
            None,
        )
        .await
    }

    async fn commit_leader_index_in_term_with_membership_blocking(
        &self,
        index: u64,
        term: u64,
        compact_after_apply: bool,
        commit_membership: Option<RaftMembership>,
    ) -> Result<bool, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-commit-leader-index-membership-blocking-1");
        let node = self.clone();
        tokio::task::spawn_blocking(move || {
            node.commit_leader_index_in_term_with_membership(
                index,
                term,
                compact_after_apply,
                commit_membership.as_ref(),
            )
        })
        .await
        .map_err(|err| BrokerRaftError::Rpc(format!("raft leader commit task failed: {err}")))?
    }

    fn commit_leader_index_in_term_with_membership(
        &self,
        index: u64,
        term: u64,
        compact_after_apply: bool,
        commit_membership: Option<&RaftMembership>,
    ) -> Result<bool, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-commit-leader-index-1");
        if self.log.term_at(index)? != Some(term) {
            return Ok(false);
        }
        let local_match_index = self.log.last_index();
        let hard_state = {
            let runtime = self.runtime.lock();
            if runtime.role != RaftRole::Leader || runtime.current_term != term {
                return Ok(false);
            }
            if index <= runtime.commit_index {
                None
            } else {
                let membership = commit_membership
                    .cloned()
                    .unwrap_or_else(|| runtime.membership.clone());
                let ack_ids = matched_ids_for_index(
                    &self.config.node_id,
                    local_match_index,
                    &runtime.leader_progress,
                    index,
                );
                if !membership.quorum_met(&ack_ids) {
                    return Ok(false);
                }
                Some(RaftHardState {
                    current_term: runtime.current_term,
                    voted_for: runtime.voted_for.clone(),
                    commit_index: index,
                })
            }
        };
        if let Some(hard_state) = hard_state {
            self.log.write_hard_state(&hard_state)?;
            {
                let mut runtime = self.runtime.lock();
                if runtime.role != RaftRole::Leader || runtime.current_term != term {
                    return Ok(false);
                }
                let membership = commit_membership
                    .cloned()
                    .unwrap_or_else(|| runtime.membership.clone());
                let ack_ids = matched_ids_for_index(
                    &self.config.node_id,
                    self.log.last_index(),
                    &runtime.leader_progress,
                    index,
                );
                if !membership.quorum_met(&ack_ids) {
                    return Ok(false);
                }
                runtime.commit_index = runtime.commit_index.max(index);
            }
        }
        self.apply_committed()?;
        if compact_after_apply {
            self.snapshot_and_compact_if_needed(false)?;
        }
        Ok(true)
    }

    fn spawn_post_commit_heartbeat(&self) {
        crate::routine_id!("ddl-routine-broker-raft-post-commit-heartbeat-1");
        if !self.request_post_commit_fanout() {
            return;
        }
        let node = self.clone();
        tokio::spawn(async move {
            node.run_post_commit_fanout().await;
        });
    }

    fn request_post_commit_fanout(&self) -> bool {
        crate::routine_id!("ddl-routine-broker-raft-request-post-commit-fanout-1");
        let mut state = self.post_commit_fanout.lock();
        state.pending = true;
        if state.active {
            false
        } else {
            state.active = true;
            true
        }
    }

    fn take_post_commit_fanout_round(&self) -> bool {
        crate::routine_id!("ddl-routine-broker-raft-take-post-commit-fanout-1");
        let mut state = self.post_commit_fanout.lock();
        if state.pending {
            state.pending = false;
            true
        } else {
            state.active = false;
            false
        }
    }

    async fn run_post_commit_fanout(&self) {
        crate::routine_id!("ddl-routine-broker-raft-run-post-commit-fanout-1");
        while self.take_post_commit_fanout_round() {
            if let Err(err) = self.replicate_log_once(None).await {
                debug!(
                    target: "lmx::raft",
                    node_id = %self.config.node_id,
                    error = %err,
                    "post-commit AppendEntries fan-out failed",
                );
            }
        }
    }

    async fn replicate_to_peer(
        &self,
        peer: RaftPeerConfig,
        term: u64,
        leader_commit: u64,
        target_index: Option<u64>,
    ) -> Result<RaftPeerReplicationOutcome, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-replicate-peer-1");
        let next_index = {
            let mut runtime = self.runtime.lock();
            if runtime.role != RaftRole::Leader || runtime.current_term != term {
                return Ok(RaftPeerReplicationOutcome::default());
            }
            let fallback = self.initial_replication_next_index();
            let inserted = !runtime.leader_progress.contains_key(&peer.id);
            let next_index = runtime
                .leader_progress
                .entry(peer.id.clone())
                .or_insert(RaftPeerProgress {
                    next_index: fallback,
                    match_index: 0,
                })
                .next_index
                .max(1);
            if inserted {
                self.note_leader_progress_changed();
            }
            next_index
        };
        let prev_log_index = next_index.saturating_sub(1);
        let Some((prev_log_term, entries)) = self.log.prev_term_and_entries_from_limited(
            next_index,
            self.config.append_entries_max_entries,
            self.config.append_entries_max_bytes,
        )?
        else {
            debug!(
                target: "lmx::raft",
                node_id = %self.config.node_id,
                peer = %peer.id,
                next_index,
                prev_log_index,
                "cannot replicate incremental entries before local snapshot boundary",
            );
            return self
                .install_snapshot_to_peer(peer, term, target_index)
                .await;
        };
        if entries
            .first()
            .is_some_and(|entry| entry.index != next_index)
        {
            debug!(
                target: "lmx::raft",
                node_id = %self.config.node_id,
                peer = %peer.id,
                next_index,
                first_retained_index = entries.first().map(|entry| entry.index).unwrap_or(0),
                "replicating snapshot because retained log suffix does not cover nextIndex",
            );
            return self
                .install_snapshot_to_peer(peer, term, target_index)
                .await;
        };
        let sent_match_index = entries
            .last()
            .map(|entry| entry.index)
            .unwrap_or(prev_log_index);
        let sent_entries_count = entries.len();
        let rpc = RaftRpc::AppendEntries {
            auth_token: None,
            term,
            leader_id: self.config.node_id.clone(),
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        };
        let timeout = self
            .config
            .election_timeout_min
            .max(self.config.heartbeat_interval.saturating_mul(2))
            .max(Duration::from_millis(250));
        match self.send_rpc_to_peer(&peer, rpc, timeout).await {
            Ok(RaftRpcResponse::AppendEntries {
                term: peer_term,
                success,
                match_index,
                conflict_index,
                conflict_term,
                ..
            }) => {
                if peer_term > term {
                    self.step_down_blocking(peer_term, None).await;
                    return Ok(RaftPeerReplicationOutcome::default());
                }
                if success {
                    if match_index < prev_log_index
                        || (sent_entries_count > 0 && match_index < sent_match_index)
                    {
                        self.telemetry
                            .append_invalid_success_responses_total
                            .fetch_add(1, Ordering::Relaxed);
                        debug!(
                            target: "lmx::raft",
                            node_id = %self.config.node_id,
                            peer = %peer.id,
                            prev_log_index,
                            sent_match_index,
                            sent_entries_count,
                            reported_match_index = match_index,
                            target_index = ?target_index,
                            "raft append success response underreported matched log boundary",
                        );
                        return Ok(RaftPeerReplicationOutcome {
                            contacted: true,
                            target_reached: false,
                        });
                    }
                    let acknowledged_match_index = match_index.min(sent_match_index);
                    let mut runtime = self.runtime.lock();
                    if runtime.role != RaftRole::Leader || runtime.current_term != term {
                        return Ok(RaftPeerReplicationOutcome::default());
                    }
                    let progress = runtime.leader_progress.entry(peer.id.clone()).or_insert(
                        RaftPeerProgress {
                            next_index: acknowledged_match_index.saturating_add(1),
                            match_index: 0,
                        },
                    );
                    let before = *progress;
                    progress.match_index = progress.match_index.max(acknowledged_match_index);
                    progress.next_index = progress
                        .next_index
                        .max(progress.match_index.saturating_add(1));
                    if *progress != before {
                        self.note_leader_progress_changed();
                        self.telemetry
                            .append_progress_updates_total
                            .fetch_add(1, Ordering::Relaxed);
                        debug!(
                            target: "lmx::raft",
                            node_id = %self.config.node_id,
                            peer = %peer.id,
                            prev_match_index = before.match_index,
                            prev_next_index = before.next_index,
                            match_index = progress.match_index,
                            next_index = progress.next_index,
                            acknowledged_match_index,
                            sent_match_index,
                            target_index = ?target_index,
                            "raft append progress advanced",
                        );
                    }
                    Ok(RaftPeerReplicationOutcome {
                        contacted: true,
                        target_reached: target_index
                            .is_none_or(|target| progress.match_index >= target),
                    })
                } else {
                    let repaired_next_index =
                        self.next_index_after_conflict(conflict_term, conflict_index, next_index)?;
                    let mut runtime = self.runtime.lock();
                    if runtime.role != RaftRole::Leader || runtime.current_term != term {
                        return Ok(RaftPeerReplicationOutcome::default());
                    }
                    let progress = runtime.leader_progress.entry(peer.id.clone()).or_insert(
                        RaftPeerProgress {
                            next_index: repaired_next_index,
                            match_index: 0,
                        },
                    );
                    let before = *progress;
                    let min_safe_next_index = progress.match_index.saturating_add(1).max(1);
                    let clamped_next_index = repaired_next_index.max(min_safe_next_index);
                    let clamped = clamped_next_index != repaired_next_index;
                    self.telemetry
                        .append_conflict_repairs_total
                        .fetch_add(1, Ordering::Relaxed);
                    if clamped {
                        self.telemetry
                            .append_conflict_clamps_total
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    progress.next_index = clamped_next_index;
                    if *progress != before {
                        self.note_leader_progress_changed();
                        self.telemetry
                            .append_progress_updates_total
                            .fetch_add(1, Ordering::Relaxed);
                        debug!(
                            target: "lmx::raft",
                            node_id = %self.config.node_id,
                            peer = %peer.id,
                            prev_match_index = before.match_index,
                            prev_next_index = before.next_index,
                            match_index = progress.match_index,
                            next_index = progress.next_index,
                            repaired_next_index,
                            clamped,
                            conflict_index = ?conflict_index,
                            conflict_term = ?conflict_term,
                            "raft append conflict repaired peer nextIndex",
                        );
                    } else if clamped {
                        debug!(
                            target: "lmx::raft",
                            node_id = %self.config.node_id,
                            peer = %peer.id,
                            match_index = progress.match_index,
                            next_index = progress.next_index,
                            repaired_next_index,
                            conflict_index = ?conflict_index,
                            conflict_term = ?conflict_term,
                            "raft append conflict ignored stale nextIndex rewind",
                        );
                    }
                    Ok(RaftPeerReplicationOutcome {
                        contacted: true,
                        target_reached: false,
                    })
                }
            }
            Ok(RaftRpcResponse::Error {
                term: peer_term,
                error,
            }) => {
                if peer_term > term {
                    self.step_down_blocking(peer_term, None).await;
                }
                debug!(
                    target: "lmx::raft",
                    node_id = %self.config.node_id,
                    peer = %peer.id,
                    error,
                    "append entries rejected",
                );
                Ok(RaftPeerReplicationOutcome::default())
            }
            Ok(_) => Ok(RaftPeerReplicationOutcome::default()),
            Err(err) => {
                debug!(
                    target: "lmx::raft",
                    node_id = %self.config.node_id,
                    peer = %peer.id,
                    error = %err,
                    "append entries failed",
                );
                Ok(RaftPeerReplicationOutcome::default())
            }
        }
    }

    fn initial_replication_next_index(&self) -> u64 {
        crate::routine_id!("ddl-routine-broker-raft-initial-replication-next-index-1");
        self.log
            .latest_snapshot()
            .map(|snapshot| snapshot.last_included_index.saturating_add(1))
            .unwrap_or(1)
    }

    async fn install_snapshot_to_peer(
        &self,
        peer: RaftPeerConfig,
        term: u64,
        target_index: Option<u64>,
    ) -> Result<RaftPeerReplicationOutcome, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-install-snapshot-peer-1");
        if !self.is_leader_in_term(term) {
            return Ok(RaftPeerReplicationOutcome::default());
        }
        let Some(snapshot) = self.log.latest_snapshot_file()? else {
            debug!(
                target: "lmx::raft",
                node_id = %self.config.node_id,
                peer = %peer.id,
                "cannot install snapshot because no local snapshot exists",
            );
            return Ok(RaftPeerReplicationOutcome::default());
        };
        let last_included_index = snapshot.metadata.last_included_index;
        let payload_sha256 = match snapshot.metadata.payload_sha256.clone() {
            Some(checksum) => checksum,
            None => snapshot_payload_sha256(&snapshot.payload)?,
        };
        let payload_bytes = serde_json::to_vec(&snapshot.payload)?;
        let timeout = self
            .config
            .election_timeout_min
            .max(self.config.heartbeat_interval.saturating_mul(2))
            .max(Duration::from_millis(250));
        let chunk_size = self.config.install_snapshot_chunk_bytes.max(1);
        let mut offset = 0usize;
        loop {
            if !self.is_leader_in_term(term) {
                return Ok(RaftPeerReplicationOutcome::default());
            }
            let end = if payload_bytes.is_empty() {
                0
            } else {
                offset.saturating_add(chunk_size).min(payload_bytes.len())
            };
            let chunk_len = end.saturating_sub(offset);
            let done = end >= payload_bytes.len();
            let data = BASE64.encode(&payload_bytes[offset..end]);
            let rpc = RaftRpc::InstallSnapshot {
                auth_token: None,
                term,
                leader_id: self.config.node_id.clone(),
                last_included_index,
                last_included_term: snapshot.metadata.last_included_term,
                payload_sha256: Some(payload_sha256.clone()),
                offset: offset as u64,
                done,
                data,
            };
            self.telemetry
                .install_snapshot_chunks_total
                .fetch_add(1, Ordering::Relaxed);
            self.telemetry
                .install_snapshot_bytes_total
                .fetch_add(chunk_len as u64, Ordering::Relaxed);
            match self.send_rpc_to_peer(&peer, rpc, timeout).await {
                Ok(RaftRpcResponse::InstallSnapshot {
                    term: peer_term,
                    success,
                    last_included_index: installed_index,
                }) => {
                    if peer_term > term {
                        self.step_down_blocking(peer_term, None).await;
                        return Ok(RaftPeerReplicationOutcome::default());
                    }
                    if !self.is_leader_in_term(term) {
                        return Ok(RaftPeerReplicationOutcome::default());
                    }
                    if !success {
                        return Ok(RaftPeerReplicationOutcome {
                            contacted: true,
                            target_reached: false,
                        });
                    }
                    if installed_index >= last_included_index {
                        let acknowledged_index = last_included_index;
                        let mut runtime = self.runtime.lock();
                        if runtime.role != RaftRole::Leader || runtime.current_term != term {
                            return Ok(RaftPeerReplicationOutcome::default());
                        }
                        let progress = runtime.leader_progress.entry(peer.id.clone()).or_insert(
                            RaftPeerProgress {
                                next_index: acknowledged_index.saturating_add(1),
                                match_index: 0,
                            },
                        );
                        let before = *progress;
                        progress.match_index = progress.match_index.max(acknowledged_index);
                        progress.next_index = progress
                            .next_index
                            .max(progress.match_index.saturating_add(1));
                        if *progress != before {
                            self.note_leader_progress_changed();
                            self.telemetry
                                .install_snapshot_progress_updates_total
                                .fetch_add(1, Ordering::Relaxed);
                            debug!(
                                target: "lmx::raft",
                                node_id = %self.config.node_id,
                                peer = %peer.id,
                                prev_match_index = before.match_index,
                                prev_next_index = before.next_index,
                                match_index = progress.match_index,
                                next_index = progress.next_index,
                                last_included_index,
                                target_index = ?target_index,
                                "raft install snapshot progress advanced",
                            );
                        }
                        return Ok(RaftPeerReplicationOutcome {
                            contacted: true,
                            target_reached: target_index
                                .is_none_or(|target| progress.match_index >= target),
                        });
                    }
                    if done {
                        return Ok(RaftPeerReplicationOutcome {
                            contacted: true,
                            target_reached: false,
                        });
                    }
                }
                Ok(RaftRpcResponse::Error {
                    term: peer_term,
                    error,
                }) => {
                    if peer_term > term {
                        self.step_down_blocking(peer_term, None).await;
                    }
                    debug!(
                        target: "lmx::raft",
                        node_id = %self.config.node_id,
                        peer = %peer.id,
                        error,
                        "install snapshot rejected",
                    );
                    return Ok(RaftPeerReplicationOutcome::default());
                }
                Ok(_) => return Ok(RaftPeerReplicationOutcome::default()),
                Err(err) => {
                    debug!(
                        target: "lmx::raft",
                        node_id = %self.config.node_id,
                        peer = %peer.id,
                        error = %err,
                        "install snapshot failed",
                    );
                    return Ok(RaftPeerReplicationOutcome::default());
                }
            }
            if done {
                return Ok(RaftPeerReplicationOutcome::default());
            }
            offset = end;
            if payload_bytes.is_empty() {
                return Ok(RaftPeerReplicationOutcome::default());
            }
        }
    }

    async fn replicate_until_quorum(
        &self,
        target_index: u64,
    ) -> Result<BTreeSet<String>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-replicate-until-quorum-1");
        let timeout = self
            .config
            .election_timeout_max
            .saturating_mul(2)
            .max(Duration::from_millis(500));
        let deadline = deadline_after(timeout);
        let mut best_acks = BTreeSet::new();
        loop {
            let before_progress = self.leader_progress_generation();
            let acks = self.replicate_log_once(Some(target_index)).await?;
            let progress_changed = self.leader_progress_generation() != before_progress;
            if acks.len() > best_acks.len() {
                best_acks = acks.clone();
            }
            if self.quorum_met(&acks) || !self.is_leader() {
                return Ok(acks);
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(best_acks);
            }
            if progress_changed {
                tokio::task::yield_now().await;
                continue;
            }
            tokio::time::sleep(
                self.config
                    .heartbeat_interval
                    .max(Duration::from_millis(25)),
            )
            .await;
        }
    }

    async fn replicate_until_quorum_for_membership(
        &self,
        target_index: u64,
        membership: &RaftMembership,
    ) -> Result<BTreeSet<String>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-replicate-until-membership-1");
        let timeout = self
            .config
            .election_timeout_max
            .saturating_mul(2)
            .max(Duration::from_millis(500));
        let deadline = deadline_after(timeout);
        let mut best_acks = BTreeSet::new();
        loop {
            let before_progress = self.leader_progress_generation();
            let acks = self
                .replicate_log_once_for_membership(target_index, membership)
                .await?;
            let progress_changed = self.leader_progress_generation() != before_progress;
            if acks.len() > best_acks.len() {
                best_acks = acks.clone();
            }
            if membership.quorum_met(&acks) || !self.is_leader() {
                return Ok(acks);
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(best_acks);
            }
            if progress_changed {
                tokio::task::yield_now().await;
                continue;
            }
            tokio::time::sleep(
                self.config
                    .heartbeat_interval
                    .max(Duration::from_millis(25)),
            )
            .await;
        }
    }

    fn apply_committed(&self) -> Result<(), BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-apply-committed-1");
        let (start_index, commit_index) = {
            let runtime = self.runtime.lock();
            (runtime.last_applied.saturating_add(1), runtime.commit_index)
        };
        if start_index > commit_index {
            return Ok(());
        }
        let entries = self.log.entries_range(start_index, commit_index)?;
        for entry in entries {
            match entry.command.clone() {
                RaftCommand::Noop => {}
                RaftCommand::ClientRequest {
                    client_id,
                    request,
                    grant,
                } => {
                    self.apply_client_request(client_id, request, grant);
                }
                RaftCommand::ClientRequestWithIdentity {
                    client_id,
                    request,
                    grant,
                    request_id,
                    request_fingerprint,
                } => {
                    if self.begin_client_request_apply(&request_id, &request_fingerprint)? {
                        self.apply_client_request(client_id, request, grant);
                    }
                }
                RaftCommand::DropClient { client_id } => {
                    self.broker.drop_client(client_id);
                }
                RaftCommand::SetMembership { membership } => {
                    self.apply_membership(membership)?;
                }
                RaftCommand::SetStagedLearners { learners } => {
                    self.apply_staged_learners(learners)?;
                }
            }
            self.runtime.lock().last_applied = entry.index;
        }
        Ok(())
    }

    fn apply_client_request(
        &self,
        client_id: ClientId,
        request: Request,
        grant: Option<RaftGrantPlan>,
    ) {
        crate::routine_id!("ddl-routine-broker-raft-apply-client-request-1");
        let grant_lock_uuid = match &request {
            Request::Lock { uuid, .. } => grant
                .as_ref()
                .and_then(|grant| grant.lock_uuid.clone())
                .or_else(|| Some(uuid.clone())),
            _ => None,
        };
        let overrides = GrantOverrides {
            lock_uuid: grant
                .as_ref()
                .and_then(|grant| grant.lock_uuid.clone())
                .or(grant_lock_uuid),
            fencing_seed: grant.and_then(|grant| grant.fencing_seed),
        };
        self.broker
            .handle_request_with_grant_overrides(client_id, request, overrides);
    }

    fn clear_removed_vote_for_current_membership(&self) -> Result<(), BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-clear-removed-vote-1");
        let hard_state = {
            let runtime = self.runtime.lock();
            let Some(voted_for) = runtime.voted_for.as_ref() else {
                return Ok(());
            };
            if runtime.membership.contains_id(voted_for) {
                return Ok(());
            }
            RaftHardState {
                current_term: runtime.current_term,
                voted_for: None,
                commit_index: runtime.commit_index,
            }
        };
        self.log.write_hard_state(&hard_state)?;
        {
            let mut runtime = self.runtime.lock();
            let active_ids = runtime
                .membership
                .active_peers()
                .into_iter()
                .map(|peer| peer.id)
                .collect::<BTreeSet<_>>();
            if runtime
                .voted_for
                .as_ref()
                .is_some_and(|peer_id| !active_ids.contains(peer_id))
            {
                runtime.voted_for = None;
            }
        }
        Ok(())
    }

    fn snapshot_and_compact_if_needed(&self, _periodic: bool) -> Result<(), BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-snapshot-compact-needed-1");
        let (commit_index, last_applied, current_term) = {
            let runtime = self.runtime.lock();
            (
                runtime.commit_index,
                runtime.last_applied,
                runtime.current_term,
            )
        };
        if commit_index == 0 || last_applied < commit_index {
            return Ok(());
        }

        let bytes = self.log.log_len_bytes()?;
        let elapsed = self.maintenance.lock().last_snapshot_at.elapsed();
        let retained_entries = self.log.retained_entries_len();
        if retained_entries == 0 {
            return Ok(());
        }
        let latest_snapshot_index = self
            .log
            .latest_snapshot()
            .map(|snapshot| snapshot.last_included_index)
            .unwrap_or(0);

        let threshold_reached = (self.config.snapshot_max_log_entries > 0
            && retained_entries as u64 >= self.config.snapshot_max_log_entries)
            || (self.config.snapshot_max_log_bytes > 0
                && bytes >= self.config.snapshot_max_log_bytes);
        let cadence_due = elapsed >= self.config.snapshot_interval;
        if !threshold_reached && !cadence_due {
            return Ok(());
        }

        let compact_through = commit_index.saturating_sub(self.config.trailing_log_entries);
        if compact_through == 0 {
            return Ok(());
        }
        if compact_through <= latest_snapshot_index {
            self.maintenance.lock().last_snapshot_at = Instant::now();
            return Ok(());
        }

        let snapshot_term = self.log.term_at(commit_index)?.ok_or_else(|| {
            BrokerRaftError::InvalidLog(format!(
                "committed index {commit_index} is missing from retained log and latest snapshot"
            ))
        })?;
        if snapshot_term > current_term {
            return Err(BrokerRaftError::InvalidLog(format!(
                "committed index {commit_index} has future term {snapshot_term} above current term {current_term}"
            )));
        }
        let metrics = self.broker.metrics();
        let broker_snapshot = self
            .broker
            .snapshot_for_raft()
            .map_err(BrokerRaftError::BrokerSnapshot)?;
        let payload = serde_json::json!({
            "nodeId": self.config.node_id,
            "note": "Broker state snapshot including active holders, queued waiters, fencing counters, TTL deadlines, and membership.",
            "membership": self.membership(),
            "stagedLearners": self.staged_learners(),
            "clientResponses": self.client_response_snapshot_entries(),
            "broker": broker_snapshot,
            "metrics": {
                "keys": metrics.keys,
                "holders": metrics.holders,
                "waiters": metrics.waiters,
                "clients": metrics.clients,
                "pendingDeadlines": metrics.pending_deadlines,
                "ttlEvictionsTotal": metrics.ttl_evictions_total,
                "maxConcurrencyCap": metrics.max_concurrency_cap,
                "concurrencyCapClampsTotal": metrics.concurrency_cap_clamps_total,
                "fencingWatermark": metrics.fencing_watermark,
                "idleKeysPrunedTotal": metrics.idle_keys_pruned_total,
            },
        });
        let snapshot = self
            .log
            .write_snapshot(commit_index, snapshot_term, payload)?;
        let report = self.log.compact_through(compact_through)?;
        self.maintenance.lock().last_snapshot_at = Instant::now();
        info!(
            target: "lmx::raft",
            node_id = %self.config.node_id,
            snapshot_index = snapshot.last_included_index,
            compacted_through = report.compacted_through_index,
            compacted_entries = report.compacted_entries,
            retained_entries = report.retained_entries,
            log_bytes = bytes,
            "raft log compacted",
        );
        Ok(())
    }

    async fn snapshot_and_compact_if_needed_blocking(
        &self,
        periodic: bool,
    ) -> Result<(), BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-snapshot-compact-needed-blocking-1");
        let node = self.clone();
        tokio::task::spawn_blocking(move || node.snapshot_and_compact_if_needed(periodic))
            .await
            .map_err(|err| {
                BrokerRaftError::Rpc(format!("raft log maintenance task failed: {err}"))
            })?
    }

    fn stage_snapshot_chunk(
        &self,
        leader_id: &str,
        last_included_index: u64,
        last_included_term: u64,
        payload_sha256: &str,
        offset: u64,
        done: bool,
        chunk: Vec<u8>,
    ) -> Result<Option<PathBuf>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-stage-snapshot-chunk-1");
        let now_ms = unix_ms();
        self.cleanup_stale_snapshot_transfers(now_ms);
        let key = snapshot_transfer_key(
            leader_id,
            last_included_index,
            last_included_term,
            payload_sha256,
        );
        let path = self.snapshot_transfer_path(&key);
        let mut transfers = self.snapshot_transfers.lock();
        if offset == 0 {
            let _ = fs::remove_file(&path);
            transfers.insert(
                key.clone(),
                PendingSnapshotTransfer {
                    path: path.clone(),
                    bytes_written: 0,
                    updated_at_ms: now_ms,
                },
            );
        }
        let Some(pending) = transfers.get_mut(&key) else {
            return Err(BrokerRaftError::Rpc(format!(
                "snapshot chunk for index {last_included_index} started at offset {offset} without offset 0"
            )));
        };
        let expected_offset = pending.bytes_written;
        let chunk_len = chunk.len() as u64;
        if offset < expected_offset {
            let duplicate_end = offset.saturating_add(chunk_len);
            if duplicate_end <= expected_offset && !done {
                pending.updated_at_ms = unix_ms();
                return Ok(None);
            }
        }
        if expected_offset != offset {
            let stale_path = transfers.remove(&key).map(|pending| pending.path);
            drop(transfers);
            if let Some(path) = stale_path {
                let _ = fs::remove_file(path);
            }
            return Err(BrokerRaftError::Rpc(format!(
                "snapshot chunk offset mismatch for index {last_included_index}; expected={} got={offset}",
                expected_offset
            )));
        }
        {
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&pending.path)?;
            file.write_all(&chunk)?;
            if done {
                file.sync_data()?;
            }
        }
        pending.bytes_written = pending.bytes_written.saturating_add(chunk_len);
        pending.updated_at_ms = unix_ms();
        if done {
            Ok(transfers.remove(&key).map(|pending| pending.path))
        } else {
            Ok(None)
        }
    }

    fn snapshot_transfer_path(&self, key: &str) -> PathBuf {
        crate::routine_id!("ddl-routine-broker-raft-snapshot-transfer-path-1");
        self.log.data_dir.join(format!(
            "{}{}{}",
            SNAPSHOT_PART_FILE_PREFIX,
            sha256_hex(key.as_bytes()),
            SNAPSHOT_PART_FILE_SUFFIX
        ))
    }

    fn discard_snapshot_transfer(
        &self,
        leader_id: &str,
        last_included_index: u64,
        last_included_term: u64,
        payload_sha256: &str,
    ) {
        crate::routine_id!("ddl-routine-broker-raft-discard-snapshot-transfer-1");
        let path = self
            .snapshot_transfers
            .lock()
            .remove(&snapshot_transfer_key(
                leader_id,
                last_included_index,
                last_included_term,
                payload_sha256,
            ))
            .map(|pending| pending.path);
        if let Some(path) = path {
            let _ = fs::remove_file(path);
        }
    }

    fn cleanup_stale_snapshot_transfers(&self, now_ms: u64) -> usize {
        crate::routine_id!("ddl-routine-broker-raft-cleanup-stale-snapshot-transfers-1");
        let paths = {
            let mut transfers = self.snapshot_transfers.lock();
            let stale_keys = transfers
                .iter()
                .filter_map(|(key, pending)| {
                    if now_ms.saturating_sub(pending.updated_at_ms) >= SNAPSHOT_TRANSFER_STALE_MS {
                        Some(key.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            stale_keys
                .into_iter()
                .filter_map(|key| transfers.remove(&key).map(|pending| pending.path))
                .collect::<Vec<_>>()
        };
        let removed = paths.len();
        for path in paths {
            if let Err(err) = fs::remove_file(&path) {
                if err.kind() != std::io::ErrorKind::NotFound {
                    debug!(
                        target: "lmx::raft",
                        node_id = %self.config.node_id,
                        path = %path.display(),
                        error = %err,
                        "failed to remove stale staged snapshot transfer",
                    );
                }
            }
        }
        removed
    }

    async fn send_rpc_to_peer(
        &self,
        peer: &RaftPeerConfig,
        rpc: RaftRpc,
        timeout: Duration,
    ) -> Result<RaftRpcResponse, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-send-rpc-peer-1");
        let rpc = self.with_peer_auth(rpc);
        let connection = {
            let mut connections = self.rpc_connections.lock();
            connections
                .entry(peer.id.clone())
                .or_insert_with(|| Arc::new(AsyncMutex::new(RaftRpcConnection::default())))
                .clone()
        };
        let mut connection = connection.lock().await;
        let mut call = RaftRpcConnectionCall::new(&mut connection);
        call.call(&peer.addr, rpc, timeout).await
    }

    fn apply_membership(&self, membership: RaftMembership) -> Result<(), BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-apply-membership-1");
        let membership = validate_raft_membership(membership)?;
        let active_peers = membership.active_peers();
        let active_ids: BTreeSet<String> =
            active_peers.iter().map(|peer| peer.id.clone()).collect();
        let next_index = self.log.last_index().saturating_add(1);
        let self_is_active = active_ids.contains(&self.config.node_id);
        let durable_commit_index = self.log.read_hard_state()?.commit_index.max(
            self.log
                .latest_snapshot()
                .map_or(0, |snapshot| snapshot.last_included_index),
        );
        let (hard_state, staged_learners) = {
            let runtime = self.runtime.lock();
            let staged_learners = if self_is_active {
                runtime
                    .staged_learners
                    .values()
                    .filter(|peer| !active_ids.contains(&peer.id))
                    .cloned()
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            let hard_state = if !self_is_active
                || runtime
                    .voted_for
                    .as_ref()
                    .is_some_and(|peer_id| !active_ids.contains(peer_id))
            {
                Some(RaftHardState {
                    current_term: runtime.current_term,
                    voted_for: None,
                    commit_index: runtime.commit_index.max(durable_commit_index),
                })
            } else {
                None
            };
            (hard_state, staged_learners)
        };
        self.persist_staged_learners_for_active_peers(&staged_learners, &active_peers)?;
        if !self_is_active {
            let term = hard_state
                .as_ref()
                .map(|state| state.current_term)
                .unwrap_or_else(|| self.current_term());
            self.publish_role_cache(RaftRole::Follower, term);
        }
        if let Some(state) = &hard_state {
            self.log.write_hard_state(state)?;
        }
        {
            let mut runtime = self.runtime.lock();
            let before_progress = runtime.leader_progress.clone();
            runtime.membership = membership;
            runtime.leader_progress.retain(|peer_id, _| {
                active_ids.contains(peer_id) && peer_id != &self.config.node_id
            });
            runtime
                .staged_learners
                .retain(|peer_id, _| !active_ids.contains(peer_id));
            if runtime
                .leader_id
                .as_ref()
                .is_some_and(|peer_id| !active_ids.contains(peer_id))
            {
                runtime.leader_id = None;
            }
            if runtime
                .voted_for
                .as_ref()
                .is_some_and(|peer_id| !active_ids.contains(peer_id))
            {
                runtime.voted_for = None;
            }
            if runtime.role == RaftRole::Leader {
                for peer in active_peers
                    .iter()
                    .filter(|peer| peer.id != self.config.node_id)
                {
                    runtime
                        .leader_progress
                        .entry(peer.id.clone())
                        .or_insert(RaftPeerProgress {
                            next_index,
                            match_index: 0,
                        });
                }
            }
            if !self_is_active {
                self.publish_role_cache(RaftRole::Follower, runtime.current_term);
                runtime.role = RaftRole::Follower;
                runtime.leader_id = None;
                runtime.voted_for = None;
                runtime.leader_progress.clear();
                runtime.staged_learners.clear();
            } else {
                self.publish_runtime_role_cache(&runtime);
            }
            if runtime.leader_progress != before_progress {
                self.note_leader_progress_changed();
            }
            self.publish_leader_peer_hint_cache(&runtime);
        }
        self.retain_rpc_connections_for_active_peers(self_is_active, &active_peers);
        Ok(())
    }

    fn retain_rpc_connections_for_active_peers(
        &self,
        self_is_active: bool,
        active_peers: &[RaftPeerConfig],
    ) {
        crate::routine_id!("ddl-routine-broker-raft-retain-rpc-connections-1");
        let active_addrs = active_peers
            .iter()
            .map(|peer| (peer.id.clone(), peer.addr.clone()))
            .collect::<BTreeMap<_, _>>();
        self.rpc_connections.lock().retain(|peer_id, connection| {
            if !self_is_active || peer_id == &self.config.node_id {
                return false;
            }
            let Some(expected_addr) = active_addrs.get(peer_id) else {
                return false;
            };
            match connection.try_lock() {
                Ok(connection) => connection.addr.is_empty() || connection.addr == *expected_addr,
                Err(_) => false,
            }
        });
    }

    fn step_down(&self, term: u64, leader_id: Option<String>) {
        crate::routine_id!("ddl-routine-broker-raft-step-down-1");
        self.publish_role_cache(RaftRole::Follower, term);
        let mut runtime = self.runtime.lock();
        if term >= runtime.current_term {
            let state = RaftHardState {
                current_term: term,
                voted_for: None,
                commit_index: runtime.commit_index,
            };
            if let Err(err) = self.log.write_hard_state(&state) {
                warn!(
                    target: "lmx::raft",
                    node_id = %self.config.node_id,
                    error = %err,
                    "failed to persist raft hard state after step down",
                );
                return;
            }
            runtime.current_term = term;
            runtime.role = RaftRole::Follower;
            runtime.voted_for = None;
            runtime.leader_id = leader_id;
            runtime.election_deadline = self.next_election_deadline();
            self.publish_leader_peer_hint_cache(&runtime);
        } else {
            self.publish_runtime_role_cache(&runtime);
            self.publish_leader_peer_hint_cache(&runtime);
        }
    }

    async fn step_down_blocking(&self, term: u64, leader_id: Option<String>) {
        crate::routine_id!("ddl-routine-broker-raft-step-down-blocking-1");
        let node = self.clone();
        if let Err(err) = tokio::task::spawn_blocking(move || node.step_down(term, leader_id)).await
        {
            warn!(
                target: "lmx::raft",
                node_id = %self.config.node_id,
                error = %err,
                "raft step-down task failed",
            );
        }
    }

    fn remote_peers(&self) -> Vec<RaftPeerConfig> {
        crate::routine_id!("ddl-routine-broker-raft-remote-peers-1");
        self.active_peers()
            .into_iter()
            .filter(|peer| peer.id != self.config.node_id)
            .collect()
    }

    fn next_index_after_conflict(
        &self,
        conflict_term: Option<u64>,
        conflict_index: Option<u64>,
        current_next_index: u64,
    ) -> Result<u64, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-next-index-conflict-1");
        let retained_floor = self.initial_replication_next_index().max(1);
        if let Some(term) = conflict_term {
            if let Some(last_index) = self.log.last_index_for_term(term)? {
                return Ok(last_index.saturating_add(1).max(retained_floor));
            }
        }
        Ok(conflict_index
            .unwrap_or_else(|| current_next_index.saturating_sub(1))
            .max(retained_floor))
    }

    fn next_election_deadline(&self) -> Instant {
        crate::routine_id!("ddl-routine-broker-raft-election-deadline-1");
        let min = self.config.election_timeout_min;
        let max = self.config.election_timeout_max;
        let spread = max.saturating_sub(min);
        let jitter_ms = if spread.is_zero() {
            0
        } else {
            let span = spread.as_millis().max(1) as u64;
            stable_node_jitter(&self.config.node_id, unix_ms()) % span
        };
        Instant::now()
            .checked_add(min + Duration::from_millis(jitter_ms))
            .unwrap_or_else(|| Instant::now() + max)
    }
}

fn matched_ids_for_index(
    local_id: &str,
    local_match_index: u64,
    progress: &BTreeMap<String, RaftPeerProgress>,
    index: u64,
) -> BTreeSet<String> {
    crate::routine_id!("ddl-routine-broker-raft-matched-ids-for-index-1");
    let mut ids = BTreeSet::new();
    if local_match_index >= index {
        ids.insert(local_id.to_string());
    }
    ids.extend(
        progress
            .iter()
            .filter(|(_, progress)| progress.match_index >= index)
            .map(|(peer_id, _)| peer_id.clone()),
    );
    ids
}

fn unix_ms() -> u64 {
    crate::routine_id!("ddl-routine-broker-raft-unix-ms-1");
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn peer_rpc_auth_token(rpc: &RaftRpc) -> Option<&str> {
    crate::routine_id!("ddl-routine-broker-raft-peer-rpc-auth-token-1");
    match rpc {
        RaftRpc::PreVote { auth_token, .. }
        | RaftRpc::RequestVote { auth_token, .. }
        | RaftRpc::AppendEntries { auth_token, .. }
        | RaftRpc::InstallSnapshot { auth_token, .. }
        | RaftRpc::ProxyRequest { auth_token, .. } => auth_token.as_deref(),
    }
}

fn constant_time_eq(actual: &str, expected: &str) -> bool {
    crate::routine_id!("ddl-routine-broker-raft-constant-time-eq-1");
    let actual = actual.as_bytes();
    let expected = expected.as_bytes();
    let mut diff = actual.len() ^ expected.len();
    for idx in 0..actual.len().max(expected.len()) {
        let a = actual.get(idx).copied().unwrap_or(0);
        let b = expected.get(idx).copied().unwrap_or(0);
        diff |= (a ^ b) as usize;
    }
    diff == 0
}

fn raft_role_name(role: RaftRole) -> String {
    crate::routine_id!("ddl-routine-broker-raft-role-name-1");
    match role {
        RaftRole::Follower => "follower",
        RaftRole::Candidate => "candidate",
        RaftRole::Leader => "leader",
    }
    .into()
}

fn membership_role_name(
    membership: &RaftMembership,
    peer_id: &str,
    staged_learner: bool,
) -> String {
    crate::routine_id!("ddl-routine-broker-raft-membership-role-name-1");
    if staged_learner {
        return "stagedLearner".into();
    }
    match membership {
        RaftMembership::Simple { peers } => {
            if peers.iter().any(|peer| peer.id == peer_id) {
                "voter".into()
            } else {
                "unknown".into()
            }
        }
        RaftMembership::Joint {
            old_peers,
            new_peers,
        } => {
            let in_old = old_peers.iter().any(|peer| peer.id == peer_id);
            let in_new = new_peers.iter().any(|peer| peer.id == peer_id);
            match (in_old, in_new) {
                (true, true) => "jointOldNew",
                (true, false) => "jointOld",
                (false, true) => "jointNew",
                (false, false) => "unknown",
            }
            .into()
        }
    }
}

fn stable_node_jitter(node_id: &str, tick: u64) -> u64 {
    crate::routine_id!("ddl-routine-broker-raft-jitter-1");
    let mut h = tick.wrapping_mul(1_099_511_628_211);
    for b in node_id.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(1_099_511_628_211);
    }
    h
}

#[derive(Debug, Default)]
struct RaftRpcConnection {
    addr: String,
    reader: Option<TokioBufReader<TcpStream>>,
}

struct RaftRpcConnectionCall<'a> {
    connection: &'a mut RaftRpcConnection,
    completed: bool,
}

impl<'a> RaftRpcConnectionCall<'a> {
    fn new(connection: &'a mut RaftRpcConnection) -> Self {
        crate::routine_id!("ddl-routine-broker-raft-rpc-conn-call-guard-new-1");
        Self {
            connection,
            completed: false,
        }
    }

    async fn call(
        &mut self,
        addr: &str,
        rpc: RaftRpc,
        timeout: Duration,
    ) -> Result<RaftRpcResponse, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-rpc-conn-call-guard-call-1");
        let result = self.connection.call(addr, rpc, timeout).await;
        self.completed = true;
        result
    }
}

impl Drop for RaftRpcConnectionCall<'_> {
    fn drop(&mut self) {
        crate::routine_id!("ddl-routine-broker-raft-rpc-conn-call-guard-drop-1");
        if !self.completed {
            self.connection.reset();
        }
    }
}

impl RaftRpcConnection {
    async fn call(
        &mut self,
        addr: &str,
        rpc: RaftRpc,
        timeout: Duration,
    ) -> Result<RaftRpcResponse, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-rpc-conn-call-1");
        let body = serde_json::to_vec(&rpc)?;
        let timeout = timeout.max(Duration::from_millis(50));
        let mut last_error = None;

        for _attempt in 0..2 {
            if self.reader.is_none() || self.addr != addr {
                self.reset();
                if let Err(err) = self.connect(addr).await {
                    last_error = Some(err);
                    continue;
                }
            }

            let result = tokio::time::timeout(timeout, self.call_connected(&body)).await;
            match result {
                Ok(Ok(response)) => return Ok(response),
                Ok(Err(err)) => {
                    self.reset();
                    last_error = Some(err);
                }
                Err(err) => {
                    self.reset();
                    last_error = Some(BrokerRaftError::Rpc(err.to_string()));
                }
            }
        }

        Err(last_error.unwrap_or_else(|| BrokerRaftError::Rpc("raft RPC failed".into())))
    }

    async fn connect(&mut self, addr: &str) -> Result<(), BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-rpc-conn-connect-1");
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        self.addr = addr.to_string();
        self.reader = Some(TokioBufReader::new(stream));
        Ok(())
    }

    async fn call_connected(&mut self, body: &[u8]) -> Result<RaftRpcResponse, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-rpc-conn-connected-1");
        let reader = self
            .reader
            .as_mut()
            .ok_or_else(|| BrokerRaftError::Rpc("raft RPC connection is not open".into()))?;
        let stream = reader.get_mut();
        stream.write_all(body).await?;
        stream.write_all(b"\n").await?;
        stream.flush().await?;

        let line = read_raft_frame_bounded(reader, raft_rpc_max_frame_bytes()).await?;
        Ok(serde_json::from_str(line.trim())?)
    }

    fn reset(&mut self) {
        crate::routine_id!("ddl-routine-broker-raft-rpc-conn-reset-1");
        self.reader = None;
    }
}

fn deadline_after(timeout: Duration) -> tokio::time::Instant {
    crate::routine_id!("ddl-routine-broker-raft-deadline-after-1");
    let now = tokio::time::Instant::now();
    now.checked_add(timeout)
        .unwrap_or_else(|| now + Duration::from_secs(365 * 24 * 60 * 60))
}

fn raft_rpc_max_frame_bytes() -> usize {
    crate::routine_id!("ddl-routine-broker-raft-max-frame-bytes-1");
    std::env::var("LMX_RAFT_MAX_FRAME_BYTES")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_RAFT_RPC_MAX_FRAME_BYTES)
}

fn membership_from_snapshot_payload(
    payload: &serde_json::Value,
) -> Result<Option<RaftMembership>, BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-membership-from-snapshot-1");
    let Some(value) = payload.get("membership") else {
        return Ok(None);
    };
    let membership: RaftMembership = serde_json::from_value(value.clone())?;
    Ok(Some(validate_raft_membership(membership)?))
}

fn staged_learners_from_snapshot_payload(
    payload: &serde_json::Value,
) -> Result<Option<Vec<RaftPeerConfig>>, BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-learners-from-snapshot-1");
    let Some(value) = payload.get("stagedLearners") else {
        return Ok(None);
    };
    let learners: Vec<RaftPeerConfig> = serde_json::from_value(value.clone())?;
    let active_peers = match membership_from_snapshot_payload(payload)? {
        Some(membership) => membership.active_peers(),
        None => Vec::new(),
    };
    Ok(Some(validate_staged_learner_peers(
        learners,
        &active_peers,
    )?))
}

fn client_responses_from_snapshot_payload(
    payload: &serde_json::Value,
) -> Result<Vec<ClientResponseSnapshotEntry>, BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-client-responses-from-snapshot-1");
    let Some(value) = payload.get("clientResponses") else {
        return Ok(Vec::new());
    };
    serde_json::from_value(value.clone()).map_err(BrokerRaftError::from)
}

async fn read_raft_frame_bounded<R>(reader: &mut R, max_bytes: usize) -> std::io::Result<String>
where
    R: AsyncBufRead + Unpin,
{
    crate::routine_id!("ddl-routine-broker-raft-read-frame-bounded-1");
    let mut buf = Vec::new();
    loop {
        let chunk = reader.fill_buf().await?;
        if chunk.is_empty() {
            if buf.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "raft RPC frame ended before any bytes were read",
                ));
            }
            break;
        }
        if let Some(idx) = chunk.iter().position(|byte| *byte == b'\n') {
            let take = idx + 1;
            if buf.len().saturating_add(take) > max_bytes {
                reader.consume(take);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("raft RPC frame exceeds {max_bytes} bytes"),
                ));
            }
            buf.extend_from_slice(&chunk[..take]);
            reader.consume(take);
            break;
        }

        let take = chunk.len();
        if buf.len().saturating_add(take) > max_bytes {
            reader.consume(take);
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("raft RPC frame exceeds {max_bytes} bytes"),
            ));
        }
        buf.extend_from_slice(chunk);
        reader.consume(take);
    }

    if buf.ends_with(b"\n") {
        buf.pop();
        if buf.ends_with(b"\r") {
            buf.pop();
        }
    }
    String::from_utf8(buf).map_err(|err| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("raft RPC frame is not valid UTF-8: {err}"),
        )
    })
}

async fn wait_for_response(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Response>,
    request_uuid: &str,
    wait: Duration,
    keep_polling_until_definitive: bool,
) -> Option<Response> {
    crate::routine_id!("ddl-routine-broker-raft-wait-response-1");
    let timeout = if wait.is_zero() {
        Duration::from_millis(50)
    } else {
        wait
    };
    let deadline = deadline_after(timeout);
    let mut last_match: Option<Response> = None;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let res = if remaining.is_zero() {
            rx.try_recv().ok()
        } else {
            tokio::time::timeout(remaining, rx.recv())
                .await
                .ok()
                .flatten()
        };
        match res {
            Some(msg) if msg.correlation_uuid() == request_uuid => {
                let definitive = matches!(
                    &msg,
                    Response::Lock { acquired: true, .. }
                        | Response::CompositeLock { acquired: true, .. }
                        | Response::RegisterReadResult { granted: true, .. }
                        | Response::RegisterWriteResult { granted: true, .. }
                        | Response::Unlock { .. }
                        | Response::EndReadResult { .. }
                        | Response::EndWriteResult { .. }
                        | Response::LockInfo { .. }
                        | Response::LsResult { .. }
                        | Response::Error { .. }
                );
                last_match = Some(msg);
                if definitive || !keep_polling_until_definitive {
                    return last_match;
                }
            }
            Some(_) => continue,
            None => return last_match,
        }
    }
}

fn command_with_deterministic_grant(command: RaftCommand, index: u64) -> RaftCommand {
    crate::routine_id!("ddl-routine-broker-raft-command-deterministic-grant-1");
    match command {
        RaftCommand::ClientRequest {
            client_id,
            request,
            grant,
        } => {
            let grant = grant.or_else(|| deterministic_grant_plan(&request, index));
            RaftCommand::ClientRequest {
                client_id,
                request,
                grant,
            }
        }
        RaftCommand::ClientRequestWithIdentity {
            client_id,
            request,
            grant,
            request_id,
            request_fingerprint,
        } => {
            let grant = grant.or_else(|| deterministic_grant_plan(&request, index));
            RaftCommand::ClientRequestWithIdentity {
                client_id,
                request,
                grant,
                request_id,
                request_fingerprint,
            }
        }
        other => other,
    }
}

fn request_for_ephemeral_wait(mut request: Request, wait: Duration, is_acquire: bool) -> Request {
    crate::routine_id!("ddl-routine-broker-raft-request-ephemeral-wait-1");
    if is_acquire && wait.is_zero() {
        if let Request::Lock { wait, .. } = &mut request {
            *wait = Some(false);
        }
    }
    request
}

fn request_fingerprint(request: &Request) -> Result<String, BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-request-fingerprint-1");
    serde_json::to_string(request)
        .map(|serialized| sha256_hex(serialized.as_bytes()))
        .map_err(BrokerRaftError::from)
}

fn observe_client_response_cache(
    cache: &Arc<Mutex<ClientResponseCacheState>>,
    response: &Response,
) {
    crate::routine_id!("ddl-routine-broker-raft-observe-client-response-cache-1");
    let request_id = response.correlation_uuid();
    let mut cache = cache.lock();
    if let Some(cached) = cache.entries.get_mut(request_id) {
        cached.applied = true;
        cached.response = Some(response.clone());
        cache.order.retain(|existing| existing != request_id);
        cache.order.push_back(request_id.to_string());
    }
}

fn trim_client_response_cache(cache: &mut ClientResponseCacheState, limit: usize) {
    crate::routine_id!("ddl-routine-broker-raft-trim-client-response-cache-1");
    while cache.entries.len() > limit {
        let Some(oldest) = cache.order.pop_front() else {
            break;
        };
        cache.entries.remove(&oldest);
    }
}

fn request_is_fail_fast_acquire(request: &Request) -> bool {
    crate::routine_id!("ddl-routine-broker-raft-request-fail-fast-acquire-1");
    matches!(
        request,
        Request::Lock {
            wait: Some(false),
            ..
        }
    )
}

fn deterministic_grant_plan(request: &Request, index: u64) -> Option<RaftGrantPlan> {
    crate::routine_id!("ddl-routine-broker-raft-deterministic-grant-plan-1");
    if !request_can_grant(request) {
        return None;
    }
    Some(RaftGrantPlan {
        lock_uuid: Some(deterministic_lock_uuid(index)),
        fencing_seed: Some(deterministic_fencing_seed(index)),
    })
}

fn request_can_grant(request: &Request) -> bool {
    crate::routine_id!("ddl-routine-broker-raft-request-can-grant-1");
    matches!(
        request,
        Request::Lock { .. } | Request::RegisterRead { .. } | Request::RegisterWrite { .. }
    )
}

fn deterministic_lock_uuid(index: u64) -> String {
    crate::routine_id!("ddl-routine-broker-raft-deterministic-lock-uuid-1");
    format!("raft-{index:020}")
}

fn deterministic_fencing_seed(index: u64) -> u64 {
    crate::routine_id!("ddl-routine-broker-raft-deterministic-fencing-seed-1");
    RAFT_FENCING_TOKEN_BASE
        .saturating_add(index.saturating_mul(MAX_COMPOSITE_KEYS.saturating_add(1) as u64))
}

fn granted_lock_uuid(resp: &Response) -> Option<String> {
    crate::routine_id!("ddl-routine-broker-raft-granted-lock-uuid-1");
    match resp {
        Response::Lock {
            acquired: true,
            lock_uuid: Some(u),
            ..
        }
        | Response::CompositeLock {
            acquired: true,
            lock_uuid: Some(u),
            ..
        }
        | Response::RegisterReadResult {
            granted: true,
            lock_uuid: Some(u),
            ..
        }
        | Response::RegisterWriteResult {
            granted: true,
            lock_uuid: Some(u),
            ..
        } => Some(u.clone()),
        _ => None,
    }
}

fn read_snapshot_metadata(path: &Path) -> Result<Option<RaftSnapshotMetadata>, BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-read-snapshot-meta-1");
    Ok(read_snapshot_file(path)?.map(|snapshot| snapshot.metadata))
}

fn read_snapshot_file(path: &Path) -> Result<Option<RaftSnapshotFile>, BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-read-snapshot-file-1");
    if !path.exists() {
        return Ok(None);
    }
    let file = File::open(path)?;
    let snapshot: RaftSnapshotFile = serde_json::from_reader(file)?;
    if let Some(expected) = snapshot.metadata.payload_sha256.as_deref() {
        verify_snapshot_payload_checksum(
            snapshot.metadata.last_included_index,
            &snapshot.payload,
            Some(expected.to_string()),
        )?;
    }
    Ok(Some(snapshot))
}

fn snapshot_payload_sha256(payload: &serde_json::Value) -> Result<String, BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-snapshot-sha256-1");
    let bytes = serde_json::to_vec(payload)?;
    Ok(sha256_hex(&bytes))
}

fn verify_snapshot_payload_checksum(
    index: u64,
    payload: &serde_json::Value,
    expected: Option<String>,
) -> Result<String, BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-verify-snapshot-sha256-1");
    let expected = expected.ok_or(BrokerRaftError::SnapshotChecksumMissing { index })?;
    let actual = snapshot_payload_sha256(payload)?;
    if actual != expected {
        return Err(BrokerRaftError::SnapshotChecksumMismatch {
            index,
            expected,
            actual,
        });
    }
    Ok(actual)
}

fn verify_snapshot_payload_file_checksum(
    index: u64,
    path: &Path,
    expected: Option<String>,
) -> Result<String, BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-verify-snapshot-file-sha256-1");
    let expected = expected.ok_or(BrokerRaftError::SnapshotChecksumMissing { index })?;
    let actual = snapshot_payload_file_sha256(path)?;
    if actual != expected {
        return Err(BrokerRaftError::SnapshotChecksumMismatch {
            index,
            expected,
            actual,
        });
    }
    Ok(actual)
}

fn snapshot_payload_file_sha256(path: &Path) -> Result<String, BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-snapshot-file-sha256-1");
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex_encode(&hasher.finalize()))
}

fn decode_snapshot_chunk(data: &str) -> Result<Vec<u8>, BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-decode-snapshot-chunk-1");
    BASE64
        .decode(data.as_bytes())
        .map_err(|err| BrokerRaftError::Rpc(format!("invalid snapshot chunk encoding: {err}")))
}

fn snapshot_transfer_key(
    leader_id: &str,
    last_included_index: u64,
    last_included_term: u64,
    payload_sha256: &str,
) -> String {
    crate::routine_id!("ddl-routine-broker-raft-snapshot-transfer-key-1");
    format!("{leader_id}:{last_included_index}:{last_included_term}:{payload_sha256}")
}

fn sha256_hex(bytes: &[u8]) -> String {
    crate::routine_id!("ddl-routine-broker-raft-sha256-hex-1");
    let digest = Sha256::digest(bytes);
    hex_encode(&digest)
}

fn hex_encode(bytes: &[u8]) -> String {
    crate::routine_id!("ddl-routine-broker-raft-hex-encode-1");
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn read_hard_state(path: &Path) -> Result<RaftHardState, BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-read-hard-state-file-1");
    if !path.exists() {
        return Ok(RaftHardState::default());
    }
    let file = File::open(path)?;
    Ok(serde_json::from_reader(file)?)
}

fn read_staged_learners(
    path: &Path,
    active_peers: &[RaftPeerConfig],
) -> Result<Vec<RaftPeerConfig>, BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-read-staged-learners-1");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = File::open(path)?;
    let learners_file: RaftLearnersFile = serde_json::from_reader(file)?;
    validate_staged_learner_peers(learners_file.learners, active_peers)
}

fn write_staged_learners(
    data_dir: &Path,
    path: &Path,
    learners: &[RaftPeerConfig],
) -> Result<(), BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-write-staged-learners-1");
    if learners.is_empty() {
        match fs::remove_file(path) {
            Ok(()) => sync_dir(data_dir)?,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
        return Ok(());
    }
    write_pretty_json_atomic(
        path,
        &RaftLearnersFile {
            learners: learners.to_vec(),
        },
    )
}

fn read_log_entries(path: &Path) -> Result<Vec<RaftLogEntry>, BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-read-log-entries-1");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut entries = Vec::new();
    for line in reader.lines() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        entries.push(serde_json::from_str(line)?);
    }
    validate_persisted_log_entries(&entries)?;
    Ok(entries)
}

fn read_log_entries_with_snapshot(
    path: &Path,
    latest_snapshot: Option<&RaftSnapshotMetadata>,
) -> Result<Vec<RaftLogEntry>, BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-read-log-with-snapshot-1");
    let entries = read_log_entries(path)?;
    reconcile_persisted_log_with_snapshot(entries, latest_snapshot)
}

fn entries_from_cached(entries: &[RaftLogEntry], index: u64) -> Vec<RaftLogEntry> {
    crate::routine_id!("ddl-routine-broker-raft-entries-from-cached-1");
    let index = index.max(1);
    entries
        .iter()
        .filter(|entry| entry.index >= index)
        .cloned()
        .collect()
}

fn entries_from_limited_cached(
    entries: &[RaftLogEntry],
    index: u64,
    max_entries: usize,
    max_bytes: usize,
) -> Result<Vec<RaftLogEntry>, BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-entries-limited-cached-1");
    let index = index.max(1);
    let max_entries = max_entries.max(1);
    let max_bytes = max_bytes.max(1);
    let mut selected = Vec::new();
    let mut selected_bytes = 0usize;

    for entry in entries.iter().filter(|entry| entry.index >= index) {
        let entry_bytes = serde_json::to_vec(entry)?.len();
        if !selected.is_empty() && selected_bytes.saturating_add(entry_bytes) > max_bytes {
            break;
        }
        selected_bytes = selected_bytes.saturating_add(entry_bytes);
        selected.push(entry.clone());
        if selected.len() >= max_entries {
            break;
        }
    }
    Ok(selected)
}

fn entries_range_cached(
    entries: &[RaftLogEntry],
    start_index: u64,
    end_index: u64,
) -> Vec<RaftLogEntry> {
    crate::routine_id!("ddl-routine-broker-raft-entries-range-cached-1");
    if start_index > end_index {
        return Vec::new();
    }
    let start_index = start_index.max(1);
    entries
        .iter()
        .filter(|entry| entry.index >= start_index && entry.index <= end_index)
        .cloned()
        .collect()
}

fn prev_term_and_entries_limited_cached(
    entries: &[RaftLogEntry],
    latest_snapshot: Option<&RaftSnapshotMetadata>,
    last_index: u64,
    last_term: u64,
    next_index: u64,
    max_entries: usize,
    max_bytes: usize,
) -> Result<Option<(u64, Vec<RaftLogEntry>)>, BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-prev-term-limited-cached-1");
    let next_index = next_index.max(1);
    let prev_log_index = next_index.saturating_sub(1);
    if prev_log_index == last_index {
        return Ok(Some((last_term, Vec::new())));
    }
    let prev_log_term = if prev_log_index == 0 {
        Some(0)
    } else {
        entries
            .iter()
            .find(|entry| entry.index == prev_log_index)
            .map(|entry| entry.term)
            .or_else(|| {
                latest_snapshot
                    .filter(|snapshot| snapshot.last_included_index == prev_log_index)
                    .map(|snapshot| snapshot.last_included_term)
            })
    };
    let Some(prev_log_term) = prev_log_term else {
        return Ok(None);
    };
    Ok(Some((
        prev_log_term,
        entries_from_limited_cached(entries, next_index, max_entries, max_bytes)?,
    )))
}

fn validate_log_entries_for_snapshot(
    entries: &[RaftLogEntry],
    latest_snapshot: Option<&RaftSnapshotMetadata>,
) -> Result<(), BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-validate-log-for-snapshot-1");
    validate_persisted_log_entries(entries)?;
    validate_snapshot_log_boundary(entries, latest_snapshot)
}

fn validate_persisted_log_entries(entries: &[RaftLogEntry]) -> Result<(), BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-validate-log-entries-1");
    let mut previous: Option<&RaftLogEntry> = None;
    for entry in entries {
        if entry.index == 0 {
            return Err(BrokerRaftError::InvalidLog(
                "entry index must be >= 1".into(),
            ));
        }
        if entry.term == 0 {
            return Err(BrokerRaftError::InvalidLog(format!(
                "entry index {} has term 0",
                entry.index
            )));
        }
        if let Some(prev) = previous {
            let expected_index = prev.index.checked_add(1).ok_or_else(|| {
                BrokerRaftError::InvalidLog(format!(
                    "entry index {} cannot be followed by another entry",
                    prev.index
                ))
            })?;
            if entry.index != expected_index {
                return Err(BrokerRaftError::InvalidLog(format!(
                    "entry index {} is not contiguous after index {}; expected {}",
                    entry.index, prev.index, expected_index
                )));
            }
            if entry.term < prev.term {
                return Err(BrokerRaftError::InvalidLog(format!(
                    "entry index {} has term {} older than previous term {}",
                    entry.index, entry.term, prev.term
                )));
            }
        }
        if let Err(err) = validate_raft_command(&entry.command) {
            return Err(BrokerRaftError::InvalidLog(format!(
                "entry index {} has invalid command: {err}",
                entry.index
            )));
        }
        previous = Some(entry);
    }
    Ok(())
}

fn reconcile_persisted_log_with_snapshot(
    entries: Vec<RaftLogEntry>,
    latest_snapshot: Option<&RaftSnapshotMetadata>,
) -> Result<Vec<RaftLogEntry>, BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-reconcile-log-snapshot-1");
    let Some(snapshot) = latest_snapshot else {
        validate_snapshot_log_boundary(&entries, None)?;
        return Ok(entries);
    };
    let Some(first) = entries.first() else {
        return Ok(entries);
    };
    let Some(last) = entries.last() else {
        return Ok(entries);
    };
    let snapshot_index = snapshot.last_included_index;
    if last.index < snapshot_index {
        return Ok(Vec::new());
    }
    let max_first_after_snapshot = snapshot_index.checked_add(1).ok_or_else(|| {
        BrokerRaftError::InvalidLog(format!(
            "snapshot index {} cannot be followed by retained log entries",
            snapshot_index
        ))
    })?;
    if first.index > max_first_after_snapshot {
        return Err(BrokerRaftError::InvalidLog(format!(
            "retained log starts at index {} but snapshot only covers through {}; missing index {}",
            first.index, snapshot_index, max_first_after_snapshot
        )));
    }
    if first.index <= snapshot_index {
        let boundary_matches = entries.iter().any(|entry| {
            entry.index == snapshot_index && entry.term == snapshot.last_included_term
        });
        if !boundary_matches {
            return Ok(Vec::new());
        }
    }
    validate_snapshot_log_boundary(&entries, Some(snapshot))?;
    Ok(entries)
}

fn validate_snapshot_log_boundary(
    entries: &[RaftLogEntry],
    latest_snapshot: Option<&RaftSnapshotMetadata>,
) -> Result<(), BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-validate-snapshot-log-boundary-1");
    let Some(first) = entries.first() else {
        return Ok(());
    };
    let Some(last) = entries.last() else {
        return Ok(());
    };
    let Some(snapshot) = latest_snapshot else {
        if first.index != 1 {
            return Err(BrokerRaftError::InvalidLog(format!(
                "log starts at index {} without a snapshot covering indexes before it",
                first.index
            )));
        }
        return Ok(());
    };

    let snapshot_index = snapshot.last_included_index;
    let snapshot_term = snapshot.last_included_term;
    if last.index < snapshot_index {
        return Err(BrokerRaftError::InvalidLog(format!(
            "retained log ends at index {} before snapshot index {}",
            last.index, snapshot_index
        )));
    }
    let max_first_after_snapshot = snapshot_index.checked_add(1).ok_or_else(|| {
        BrokerRaftError::InvalidLog(format!(
            "snapshot index {} cannot be followed by retained log entries",
            snapshot_index
        ))
    })?;
    if first.index > max_first_after_snapshot {
        return Err(BrokerRaftError::InvalidLog(format!(
            "retained log starts at index {} but snapshot only covers through {}; missing index {}",
            first.index, snapshot_index, max_first_after_snapshot
        )));
    }
    if first.index <= snapshot_index {
        let Some(snapshot_entry) = entries.iter().find(|entry| entry.index == snapshot_index)
        else {
            return Err(BrokerRaftError::InvalidLog(format!(
                "retained log crosses snapshot index {} but does not contain it",
                snapshot_index
            )));
        };
        if snapshot_entry.term != snapshot_term {
            return Err(BrokerRaftError::InvalidLog(format!(
                "retained log term {} at snapshot index {} does not match snapshot term {}",
                snapshot_entry.term, snapshot_index, snapshot_term
            )));
        }
    }
    Ok(())
}

fn term_at_index(state: &RaftLogState, index: u64) -> Option<u64> {
    crate::routine_id!("ddl-routine-broker-raft-term-at-index-1");
    if index == 0 {
        return Some(0);
    }
    if let Some(term) = state.term_by_index.get(&index).copied() {
        return Some(term);
    }
    if let Some(snapshot) = &state.latest_snapshot {
        if snapshot.last_included_index == index {
            return Some(snapshot.last_included_term);
        }
        if index < snapshot.last_included_index {
            return None;
        }
    }
    None
}

fn validate_append_entries_shape(
    prev_log_index: u64,
    mut prev_log_term: u64,
    leader_term: u64,
    entries: &[RaftLogEntry],
) -> Result<(), BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-validate-append-shape-1");
    let Some(mut expected_index) = prev_log_index.checked_add(1) else {
        if entries.is_empty() {
            return Ok(());
        }
        return Err(BrokerRaftError::InvalidAppendEntries(
            "prevLogIndex is too large for non-empty entries".into(),
        ));
    };
    for entry in entries {
        if entry.index == 0 {
            return Err(BrokerRaftError::InvalidAppendEntries(
                "entry index must be >= 1".into(),
            ));
        }
        if entry.index != expected_index {
            return Err(BrokerRaftError::InvalidAppendEntries(format!(
                "entry index {} is not contiguous after prevLogIndex {}; expected {}",
                entry.index, prev_log_index, expected_index
            )));
        }
        if entry.term == 0 {
            return Err(BrokerRaftError::InvalidAppendEntries(format!(
                "entry index {} has term 0",
                entry.index
            )));
        }
        if entry.term < prev_log_term {
            return Err(BrokerRaftError::InvalidAppendEntries(format!(
                "entry index {} has term {} older than previous term {}",
                entry.index, entry.term, prev_log_term
            )));
        }
        if entry.term > leader_term {
            return Err(BrokerRaftError::InvalidAppendEntries(format!(
                "entry index {} has term {} greater than leader term {}",
                entry.index, entry.term, leader_term
            )));
        }
        if let Err(err) = validate_raft_command(&entry.command) {
            return Err(BrokerRaftError::InvalidAppendEntries(format!(
                "entry index {} has invalid command: {err}",
                entry.index
            )));
        }
        prev_log_term = entry.term;
        expected_index = expected_index.saturating_add(1);
    }
    Ok(())
}

fn first_index_for_term(state: &RaftLogState, term: u64) -> Option<u64> {
    crate::routine_id!("ddl-routine-broker-raft-first-index-for-term-1");
    let snapshot_match = state
        .latest_snapshot
        .as_ref()
        .filter(|snapshot| snapshot.last_included_term == term)
        .map(|snapshot| snapshot.last_included_index);
    state
        .first_index_by_term
        .get(&term)
        .copied()
        .or(snapshot_match)
}

fn term_indexes_from_entries(
    entries: &[RaftLogEntry],
) -> (BTreeMap<u64, u64>, BTreeMap<u64, u64>, BTreeMap<u64, u64>) {
    crate::routine_id!("ddl-routine-broker-raft-term-indexes-from-entries-1");
    let mut term_by_index = BTreeMap::new();
    let mut first_index_by_term = BTreeMap::new();
    let mut last_index_by_term = BTreeMap::new();
    for entry in entries {
        term_by_index.insert(entry.index, entry.term);
        first_index_by_term.entry(entry.term).or_insert(entry.index);
        last_index_by_term.insert(entry.term, entry.index);
    }
    (term_by_index, first_index_by_term, last_index_by_term)
}

fn last_index_for_term(state: &RaftLogState, term: u64) -> Option<u64> {
    crate::routine_id!("ddl-routine-broker-raft-last-index-for-term-1");
    state.last_index_by_term.get(&term).copied().or_else(|| {
        state
            .latest_snapshot
            .as_ref()
            .filter(|snapshot| snapshot.last_included_term == term)
            .map(|snapshot| snapshot.last_included_index)
    })
}

fn append_log_entries(
    path: &Path,
    entries: &[RaftLogEntry],
    sync_log: bool,
) -> Result<(), BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-append-log-entries-1");
    if entries.is_empty() {
        return Ok(());
    }
    let created = match fs::metadata(path) {
        Ok(_) => false,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => true,
        Err(err) => return Err(err.into()),
    };
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let mut writer = BufWriter::new(file);
    for entry in entries {
        serde_json::to_writer(&mut writer, entry)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    let file = writer.into_inner().map_err(|err| err.into_error())?;
    if sync_log {
        file.sync_data()?;
    }
    drop(file);
    if created && sync_log {
        if let Some(parent) = path.parent() {
            sync_dir(parent)?;
        }
    }
    Ok(())
}

fn cleanup_orphaned_snapshot_part_files(data_dir: &Path) -> Result<usize, BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-cleanup-orphaned-snapshot-parts-1");
    let entries = match fs::read_dir(data_dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(err.into()),
    };
    let mut removed = 0usize;
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !is_snapshot_part_file_name(name) {
            continue;
        }
        if !entry.file_type()?.is_file() {
            continue;
        }
        match fs::remove_file(entry.path()) {
            Ok(()) => removed += 1,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
    }
    if removed > 0 {
        sync_dir(data_dir)?;
    }
    Ok(removed)
}

fn is_snapshot_part_file_name(name: &str) -> bool {
    crate::routine_id!("ddl-routine-broker-raft-is-snapshot-part-file-1");
    name.starts_with(SNAPSHOT_PART_FILE_PREFIX) && name.ends_with(SNAPSHOT_PART_FILE_SUFFIX)
}

fn rewrite_log(
    path: &Path,
    entries: &[RaftLogEntry],
    sync_log: bool,
) -> Result<(), BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-rewrite-log-1");
    let tmp = path.with_extension("ndjson.tmp");
    {
        let file = File::create(&tmp)?;
        let mut writer = BufWriter::new(file);
        for entry in entries {
            serde_json::to_writer(&mut writer, entry)?;
            writer.write_all(b"\n")?;
        }
        writer.flush()?;
        let file = writer.into_inner().map_err(|err| err.into_error())?;
        if sync_log {
            file.sync_all()?;
        }
    }
    rename_and_maybe_sync_parent(&tmp, path, sync_log)
}

fn write_pretty_json_atomic<T: Serialize + ?Sized>(
    path: &Path,
    value: &T,
) -> Result<(), BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-write-json-atomic-1");
    let tmp = path.with_extension("json.tmp");
    {
        let file = File::create(&tmp)?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer_pretty(&mut writer, value)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        let file = writer.into_inner().map_err(|err| err.into_error())?;
        file.sync_all()?;
    }
    rename_and_sync_parent(&tmp, path)
}

fn rename_and_sync_parent(tmp: &Path, path: &Path) -> Result<(), BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-rename-sync-parent-1");
    rename_and_maybe_sync_parent(tmp, path, true)
}

fn rename_and_maybe_sync_parent(
    tmp: &Path,
    path: &Path,
    sync_parent: bool,
) -> Result<(), BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-rename-maybe-sync-parent-1");
    fs::rename(tmp, path)?;
    if sync_parent {
        if let Some(parent) = path.parent() {
            sync_dir(parent)?;
        }
    }
    Ok(())
}

fn sync_dir(path: &Path) -> Result<(), BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-sync-dir-1");
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use serde_json::json;
    use uuid::Uuid;

    fn temp_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("live-mutex-rs-{name}-{}", Uuid::new_v4()))
    }

    fn write_raw_log(dir: &Path, entries: &[RaftLogEntry]) {
        fs::create_dir_all(dir).expect("create raw log dir");
        let mut file = File::create(dir.join(LOG_FILE)).expect("create raw log");
        for entry in entries {
            serde_json::to_writer(&mut file, entry).expect("write raw log entry");
            file.write_all(b"\n").expect("write raw log newline");
        }
        file.sync_all().expect("sync raw log");
    }

    fn append_raw_log_line(dir: &Path, line: &str) {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(LOG_FILE))
            .expect("open raw log for append");
        file.write_all(line.as_bytes())
            .expect("append raw log line");
        file.write_all(b"\n").expect("append raw log newline");
        file.sync_all().expect("sync raw appended log");
    }

    fn noop_entry(index: u64, term: u64) -> RaftLogEntry {
        RaftLogEntry {
            index,
            term,
            created_at_ms: 10 + index,
            command: RaftCommand::Noop,
        }
    }

    fn test_raft_config(data_dir: PathBuf) -> BrokerRaftConfig {
        let mut cfg = BrokerRaftConfig::default();
        cfg.enabled = true;
        cfg.node_id = "n1".into();
        cfg.data_dir = data_dir;
        cfg.peers = vec![
            RaftPeerConfig {
                id: "n1".into(),
                addr: "127.0.0.1:7980".into(),
            },
            RaftPeerConfig {
                id: "n2".into(),
                addr: "127.0.0.1:7981".into(),
            },
            RaftPeerConfig {
                id: "n3".into(),
                addr: "127.0.0.1:7982".into(),
            },
        ];
        cfg
    }

    fn colliding_peer_id_for_prefix(target_id: &str) -> String {
        crate::routine_id!("ddl-routine-broker-raft-test-colliding-peer-id-1");
        let target_prefix = raft_client_id_prefix(target_id);
        for i in 0..200_000 {
            let candidate = format!("prefix-collision-{i}");
            if candidate != target_id && raft_client_id_prefix(&candidate) == target_prefix {
                return candidate;
            }
        }
        panic!("could not find deterministic client-id prefix collision for `{target_id}`");
    }

    fn idle_snapshot_payload() -> serde_json::Value {
        json!({
            "nodeId": "test",
            "note": "idle test snapshot",
            "metrics": {
                "keys": 0,
                "holders": 0,
                "waiters": 0,
                "clients": 0,
                "pendingDeadlines": 0,
                "ttlEvictionsTotal": 3,
                "maxConcurrencyCap": 1000,
                "concurrencyCapClampsTotal": 5,
                "fencingWatermark": 8,
                "idleKeysPrunedTotal": 13
            }
        })
    }

    fn payload_checksum(payload: &serde_json::Value) -> String {
        snapshot_payload_sha256(payload).expect("snapshot checksum")
    }

    fn single_lock_request(uuid: &str, key: &str) -> Request {
        Request::Lock {
            uuid: uuid.into(),
            key: Some(key.into()),
            keys: None,
            pid: None,
            ttl: None,
            max: None,
            force: false,
            retry_count: 0,
            keep_locks_after_death: false,
            wait: Some(false),
        }
    }

    fn snapshot_part_files(dir: &Path) -> Vec<PathBuf> {
        let mut files = fs::read_dir(dir)
            .map(|entries| {
                entries
                    .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                    .filter(|path| {
                        path.file_name()
                            .and_then(|name| name.to_str())
                            .is_some_and(is_snapshot_part_file_name)
                            && path.is_file()
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        files.sort();
        files
    }

    fn snapshot_rpc_parts(payload: &serde_json::Value) -> (String, String) {
        let bytes = serde_json::to_vec(payload).expect("snapshot bytes");
        (sha256_hex(&bytes), BASE64.encode(bytes))
    }

    #[test]
    fn log_open_removes_orphaned_snapshot_part_files_without_touching_decoys() {
        let dir = temp_dir("raft-log-open-cleans-orphaned-snapshot-parts");
        fs::create_dir_all(&dir).expect("create raft dir");
        let orphan = dir.join(format!(
            "{}deadbeef{}",
            SNAPSHOT_PART_FILE_PREFIX, SNAPSHOT_PART_FILE_SUFFIX
        ));
        let wrong_suffix = dir.join(format!("{}deadbeef.tmp", SNAPSHOT_PART_FILE_PREFIX));
        let wrong_prefix = dir.join(format!("not-a-snapshot{}", SNAPSHOT_PART_FILE_SUFFIX));
        let matching_directory = dir.join(format!(
            "{}directory{}",
            SNAPSHOT_PART_FILE_PREFIX, SNAPSHOT_PART_FILE_SUFFIX
        ));
        fs::write(&orphan, b"partial snapshot").expect("write orphaned part");
        fs::write(&wrong_suffix, b"decoy").expect("write wrong suffix decoy");
        fs::write(&wrong_prefix, b"decoy").expect("write wrong prefix decoy");
        fs::create_dir(&matching_directory).expect("create matching directory decoy");

        let _store = RaftLogStore::open(&dir).expect("open raft log store");

        assert!(!orphan.exists());
        assert!(wrong_suffix.exists());
        assert!(wrong_prefix.exists());
        assert!(matching_directory.exists());
        assert!(snapshot_part_files(&dir).is_empty());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn broker_raft_open_applies_configured_log_sync_policy() {
        let dir = temp_dir("raft-log-sync-policy");
        let mut cfg = test_raft_config(dir.clone());
        cfg.sync_log = false;

        let raft = BrokerRaft::open(cfg).expect("open raft with async log policy");

        assert!(!raft.log.sync_log);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn post_commit_fanout_requests_coalesce_while_worker_is_active() {
        let dir = temp_dir("raft-post-commit-fanout-coalesce");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");

        assert!(raft.request_post_commit_fanout());
        assert!(!raft.request_post_commit_fanout());
        {
            let state = raft.post_commit_fanout.lock();
            assert!(state.active);
            assert!(state.pending);
        }

        assert!(raft.take_post_commit_fanout_round());
        {
            let state = raft.post_commit_fanout.lock();
            assert!(state.active);
            assert!(!state.pending);
        }

        assert!(!raft.request_post_commit_fanout());
        assert!(raft.take_post_commit_fanout_round());
        assert!(!raft.take_post_commit_fanout_round());
        {
            let state = raft.post_commit_fanout.lock();
            assert!(!state.active);
            assert!(!state.pending);
        }

        assert!(raft.request_post_commit_fanout());

        let _ = fs::remove_dir_all(dir);
    }

    async fn serve_append_entries_until_simple_membership(listener: TcpListener) -> Vec<Vec<u64>> {
        serve_append_entries_until_simple_membership_with_options(listener, false).await
    }

    async fn serve_append_entries_drop_first_simple_membership_ack(
        listener: TcpListener,
    ) -> Vec<Vec<u64>> {
        serve_append_entries_until_simple_membership_with_options(listener, true).await
    }

    async fn serve_one_append_after_peer_seen(
        listener: TcpListener,
        seen_tx: oneshot::Sender<()>,
        peer_seen_rx: oneshot::Receiver<()>,
    ) -> Vec<u64> {
        crate::routine_id!("ddl-routine-broker-raft-test-serve-one-append-after-peer-seen-1");
        let (stream, _) = listener.accept().await.expect("accept learner peer");
        let mut reader = TokioBufReader::new(stream);
        let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
            .await
            .expect("read learner append frame");
        let rpc: RaftRpc = serde_json::from_str(&line).expect("parse learner append frame");
        let (term, match_index, indexes) = match rpc {
            RaftRpc::AppendEntries {
                term,
                prev_log_index,
                entries,
                ..
            } => {
                let indexes = entries.iter().map(|entry| entry.index).collect::<Vec<_>>();
                let match_index = entries
                    .last()
                    .map(|entry| entry.index)
                    .unwrap_or(prev_log_index);
                (term, match_index, indexes)
            }
            other => panic!("unexpected rpc: {other:?}"),
        };
        seen_tx.send(()).expect("signal learner append seen");
        peer_seen_rx
            .await
            .expect("other learner should be contacted concurrently");
        let response = RaftRpcResponse::AppendEntries {
            term,
            success: true,
            match_index,
            conflict_index: None,
            conflict_term: None,
        };
        let body = serde_json::to_vec(&response).expect("serialize append response");
        reader
            .get_mut()
            .write_all(&body)
            .await
            .expect("write append response");
        reader
            .get_mut()
            .write_all(b"\n")
            .await
            .expect("write append newline");
        reader.get_mut().flush().await.expect("flush append");
        indexes
    }

    async fn serve_append_entries_until_simple_membership_with_options(
        listener: TcpListener,
        drop_first_simple_membership_ack: bool,
    ) -> Vec<Vec<u64>> {
        crate::routine_id!("ddl-routine-broker-raft-test-serve-append-until-simple-1");
        let mut observed = Vec::new();
        let mut accepted_any_connection = false;
        let mut dropped_simple_membership_ack = false;
        loop {
            let (stream, _) = if accepted_any_connection {
                match tokio::time::timeout(Duration::from_millis(500), listener.accept()).await {
                    Ok(Ok(accepted)) => accepted,
                    Ok(Err(err)) => panic!("accept append peer: {err}"),
                    Err(_) => break,
                }
            } else {
                listener.accept().await.expect("accept append peer")
            };
            accepted_any_connection = true;
            let mut reader = TokioBufReader::new(stream);
            loop {
                let read_frame = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes());
                let line = match tokio::time::timeout(Duration::from_secs(2), read_frame).await {
                    Ok(Ok(line)) => line,
                    Ok(Err(err))
                        if matches!(
                            err.kind(),
                            std::io::ErrorKind::UnexpectedEof
                                | std::io::ErrorKind::ConnectionReset
                                | std::io::ErrorKind::BrokenPipe
                        ) =>
                    {
                        break;
                    }
                    Ok(Err(err)) => panic!("read append frame: {err}"),
                    Err(_) => break,
                };
                let rpc: RaftRpc = serde_json::from_str(&line).expect("parse append frame");
                let (term, match_index, indexes, frame_saw_simple_membership) = match rpc {
                    RaftRpc::AppendEntries {
                        term,
                        prev_log_index,
                        entries,
                        ..
                    } => {
                        let indexes = entries.iter().map(|entry| entry.index).collect::<Vec<_>>();
                        let match_index = entries
                            .last()
                            .map(|entry| entry.index)
                            .unwrap_or(prev_log_index);
                        let frame_saw_simple_membership = entries.iter().any(|entry| {
                            matches!(
                                entry.command,
                                RaftCommand::SetMembership {
                                    membership: RaftMembership::Simple { .. }
                                }
                            )
                        });
                        (term, match_index, indexes, frame_saw_simple_membership)
                    }
                    other => panic!("unexpected rpc: {other:?}"),
                };
                if !indexes.is_empty() {
                    observed.push(indexes);
                }
                if frame_saw_simple_membership {
                    if drop_first_simple_membership_ack && !dropped_simple_membership_ack {
                        dropped_simple_membership_ack = true;
                        break;
                    }
                }
                let response = RaftRpcResponse::AppendEntries {
                    term,
                    success: true,
                    match_index,
                    conflict_index: None,
                    conflict_term: None,
                };
                let body = serde_json::to_vec(&response).expect("serialize append response");
                if reader.get_mut().write_all(&body).await.is_err()
                    || reader.get_mut().write_all(b"\n").await.is_err()
                    || reader.get_mut().flush().await.is_err()
                {
                    break;
                }
                if frame_saw_simple_membership {
                    break;
                }
            }
        }
        observed
    }

    async fn serve_append_entries_until_staged_learners(listener: TcpListener) -> Vec<Vec<u64>> {
        crate::routine_id!("ddl-routine-broker-raft-test-serve-append-until-learners-1");
        let (stream, _) = listener.accept().await.expect("accept append peer");
        let mut reader = TokioBufReader::new(stream);
        let mut observed = Vec::new();
        loop {
            let line = match read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes()).await
            {
                Ok(line) => line,
                Err(err)
                    if matches!(
                        err.kind(),
                        std::io::ErrorKind::UnexpectedEof
                            | std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::BrokenPipe
                    ) =>
                {
                    break;
                }
                Err(err) => panic!("read append frame: {err}"),
            };
            let rpc: RaftRpc = serde_json::from_str(&line).expect("parse append frame");
            let (term, match_index, indexes, saw_staged_learners) = match rpc {
                RaftRpc::AppendEntries {
                    term,
                    prev_log_index,
                    entries,
                    ..
                } => {
                    let indexes = entries.iter().map(|entry| entry.index).collect::<Vec<_>>();
                    let match_index = entries
                        .last()
                        .map(|entry| entry.index)
                        .unwrap_or(prev_log_index);
                    let saw_staged_learners = entries.iter().any(|entry| {
                        matches!(entry.command, RaftCommand::SetStagedLearners { .. })
                    });
                    (term, match_index, indexes, saw_staged_learners)
                }
                other => panic!("unexpected rpc: {other:?}"),
            };
            if !indexes.is_empty() {
                observed.push(indexes);
            }
            let response = RaftRpcResponse::AppendEntries {
                term,
                success: true,
                match_index,
                conflict_index: None,
                conflict_term: None,
            };
            let body = serde_json::to_vec(&response).expect("serialize append response");
            if reader.get_mut().write_all(&body).await.is_err()
                || reader.get_mut().write_all(b"\n").await.is_err()
                || reader.get_mut().flush().await.is_err()
            {
                break;
            }
            if saw_staged_learners {
                break;
            }
        }
        observed
    }

    async fn spawn_rejecting_append_peer(
        id: &str,
    ) -> (RaftPeerConfig, Arc<AtomicUsize>, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind rejecting append peer");
        let peer = RaftPeerConfig {
            id: id.into(),
            addr: listener
                .local_addr()
                .expect("rejecting append peer addr")
                .to_string(),
        };
        let requests = Arc::new(AtomicUsize::new(0));
        let requests_for_server = Arc::clone(&requests);
        let server = tokio::spawn(async move {
            let (stream, _) = listener
                .accept()
                .await
                .expect("accept rejecting append peer");
            let mut reader = TokioBufReader::new(stream);
            loop {
                let line = match tokio::time::timeout(
                    Duration::from_millis(100),
                    read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes()),
                )
                .await
                {
                    Ok(Ok(line)) => line,
                    Ok(Err(err))
                        if matches!(
                            err.kind(),
                            std::io::ErrorKind::UnexpectedEof
                                | std::io::ErrorKind::ConnectionReset
                                | std::io::ErrorKind::BrokenPipe
                        ) =>
                    {
                        break;
                    }
                    Ok(Err(err)) => panic!("read rejecting append frame: {err}"),
                    Err(_) => break,
                };
                let rpc: RaftRpc = serde_json::from_str(&line).expect("parse rejecting append");
                let term = match rpc {
                    RaftRpc::AppendEntries { term, .. } => term,
                    other => panic!("unexpected rejecting peer rpc: {other:?}"),
                };
                requests_for_server.fetch_add(1, Ordering::SeqCst);
                let response = RaftRpcResponse::AppendEntries {
                    term,
                    success: false,
                    match_index: 0,
                    conflict_index: Some(1),
                    conflict_term: None,
                };
                let body = serde_json::to_vec(&response).expect("serialize append rejection");
                if reader.get_mut().write_all(&body).await.is_err()
                    || reader.get_mut().write_all(b"\n").await.is_err()
                    || reader.get_mut().flush().await.is_err()
                {
                    break;
                }
            }
        });
        (peer, requests, server)
    }

    fn test_peer(id: &str, port: u16) -> RaftPeerConfig {
        RaftPeerConfig {
            id: id.into(),
            addr: format!("127.0.0.1:{port}"),
        }
    }

    fn five_test_peers() -> Vec<RaftPeerConfig> {
        vec![
            test_peer("n1", 7980),
            test_peer("n2", 7981),
            test_peer("n3", 7982),
            test_peer("n4", 7983),
            test_peer("n5", 7984),
        ]
    }

    fn commit_local_entry(raft: &BrokerRaft, term: u64, command: RaftCommand) -> u64 {
        let entry = raft.log.append(term, command).expect("append local entry");
        raft.log
            .write_hard_state(&RaftHardState {
                current_term: term,
                voted_for: None,
                commit_index: entry.index,
            })
            .expect("persist committed hard state");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = term;
            runtime.commit_index = entry.index;
        }
        raft.apply_committed().expect("apply committed local entry");
        entry.index
    }

    fn duplicate_peer_membership() -> RaftMembership {
        RaftMembership::Simple {
            peers: vec![
                test_peer("n1", 7980),
                test_peer("n1", 7981),
                test_peer("n3", 7982),
            ],
        }
    }

    fn even_peer_membership() -> RaftMembership {
        RaftMembership::Simple {
            peers: vec![
                test_peer("n1", 7980),
                test_peer("n2", 7981),
                test_peer("n3", 7982),
                test_peer("n4", 7983),
            ],
        }
    }

    #[test]
    fn quorum_is_derived_from_peer_count() {
        let mut cfg = BrokerRaftConfig::default();
        cfg.peers = vec![
            RaftPeerConfig {
                id: "n1".into(),
                addr: "127.0.0.1:7980".into(),
            },
            RaftPeerConfig {
                id: "n2".into(),
                addr: "127.0.0.1:7981".into(),
            },
            RaftPeerConfig {
                id: "n3".into(),
                addr: "127.0.0.1:7982".into(),
            },
        ];
        assert_eq!(cfg.cluster_size(), 3);
        assert_eq!(cfg.quorum_size(), 2);

        cfg.peers.push(RaftPeerConfig {
            id: "n4".into(),
            addr: "127.0.0.1:7983".into(),
        });
        cfg.peers.push(RaftPeerConfig {
            id: "n5".into(),
            addr: "127.0.0.1:7984".into(),
        });
        assert_eq!(cfg.cluster_size(), 5);
        assert_eq!(cfg.quorum_size(), 3);
    }

    #[test]
    fn membership_validation_rejects_duplicate_and_even_configs() {
        let duplicate = validate_membership_peers(vec![
            test_peer("n1", 7980),
            test_peer("n1", 7981),
            test_peer("n3", 7982),
        ]);
        assert!(matches!(duplicate, Err(BrokerRaftError::InvalidConfig(_))));

        let even = validate_membership_peers(vec![
            test_peer("n1", 7980),
            test_peer("n2", 7981),
            test_peer("n3", 7982),
            test_peer("n4", 7983),
        ]);
        assert!(matches!(even, Err(BrokerRaftError::InvalidConfig(_))));

        let duplicate_membership = validate_raft_membership(duplicate_peer_membership());
        assert!(matches!(
            duplicate_membership,
            Err(BrokerRaftError::InvalidConfig(_))
        ));

        let invalid_joint = validate_raft_membership(RaftMembership::Joint {
            old_peers: vec![
                test_peer("n1", 7980),
                test_peer("n2", 7981),
                test_peer("n3", 7982),
            ],
            new_peers: match even_peer_membership() {
                RaftMembership::Simple { peers } => peers,
                RaftMembership::Joint { .. } => unreachable!("test helper returns simple config"),
            },
        });
        assert!(matches!(
            invalid_joint,
            Err(BrokerRaftError::InvalidConfig(_))
        ));

        let colliding_peer_id = colliding_peer_id_for_prefix("n1");
        let invalid_joint_prefix = validate_raft_membership(RaftMembership::Joint {
            old_peers: vec![
                test_peer("n1", 7980),
                test_peer("n2", 7981),
                test_peer("n3", 7982),
            ],
            new_peers: vec![
                RaftPeerConfig {
                    id: colliding_peer_id,
                    addr: "127.0.0.1:7990".into(),
                },
                test_peer("n2", 7981),
                test_peer("n3", 7982),
            ],
        });
        assert!(matches!(
            invalid_joint_prefix,
            Err(BrokerRaftError::InvalidConfig(_))
        ));
    }

    #[test]
    fn joint_membership_requires_old_and_new_majorities() {
        let old_peers = vec![
            test_peer("n1", 7980),
            test_peer("n2", 7981),
            test_peer("n3", 7982),
        ];
        let new_peers = five_test_peers();
        let joint = RaftMembership::Joint {
            old_peers,
            new_peers,
        };

        let ack_ids = BTreeSet::from(["n1".to_string(), "n2".to_string(), "n4".to_string()]);
        assert!(joint.quorum_met(&ack_ids));

        let old_only = BTreeSet::from(["n1".to_string(), "n2".to_string()]);
        assert!(!joint.quorum_met(&old_only));

        let new_only = BTreeSet::from(["n3".to_string(), "n4".to_string(), "n5".to_string()]);
        assert!(!joint.quorum_met(&new_only));

        let simple = RaftMembership::from_simple(vec![
            test_peer("n1", 7980),
            test_peer("n2", 7981),
            test_peer("n3", 7982),
        ]);
        assert!(simple.quorum_met(&old_only));
    }

    #[test]
    fn committed_membership_entry_updates_active_quorum() {
        let dir = temp_dir("raft-committed-membership");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let entry = raft
            .log
            .append(
                1,
                RaftCommand::SetMembership {
                    membership: RaftMembership::from_simple(five_test_peers()),
                },
            )
            .expect("append membership entry");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
            runtime.commit_index = entry.index;
            runtime.role = RaftRole::Leader;
        }

        raft.apply_committed().expect("apply membership entry");

        assert_eq!(raft.active_cluster_size(), 5);
        assert_eq!(raft.active_quorum_size(), 3);
        assert!(raft.membership().contains_id("n5"));
        assert_eq!(raft.remote_peers().len(), 4);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn apply_membership_rejects_invalid_config_without_mutating_runtime() {
        let dir = temp_dir("raft-apply-invalid-membership");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let before = raft.membership();

        let err = raft
            .apply_membership(duplicate_peer_membership())
            .expect_err("duplicate membership must be rejected");
        assert!(matches!(err, BrokerRaftError::InvalidConfig(_)));
        assert_eq!(raft.membership(), before);
        assert_eq!(raft.active_cluster_size(), 3);
        assert_eq!(raft.active_quorum_size(), 2);
        assert!(raft.membership().contains_id("n2"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn apply_membership_clears_peer_state_when_local_node_is_removed() {
        let dir = temp_dir("raft-membership-removes-local-node");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.voted_for = Some("n1".into());
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 4,
                    match_index: 3,
                },
            );
            runtime.staged_learners.insert(
                "n4".into(),
                RaftPeerConfig {
                    id: "n4".into(),
                    addr: "127.0.0.1:7983".into(),
                },
            );
        }
        {
            let mut connections = raft.rpc_connections.lock();
            connections.insert(
                "n2".into(),
                Arc::new(AsyncMutex::new(RaftRpcConnection::default())),
            );
            connections.insert(
                "n4".into(),
                Arc::new(AsyncMutex::new(RaftRpcConnection::default())),
            );
        }

        raft.apply_membership(RaftMembership::from_simple(vec![
            test_peer("n2", 7981),
            test_peer("n3", 7982),
            test_peer("n4", 7983),
        ]))
        .expect("apply membership removing local node");

        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.role, RaftRole::Follower);
            assert_eq!(runtime.leader_id, None);
            assert_eq!(runtime.voted_for, None);
            assert!(runtime.leader_progress.is_empty());
            assert!(runtime.staged_learners.is_empty());
            assert!(!runtime.membership.contains_id("n1"));
        }
        assert!(raft.rpc_connections.lock().is_empty());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn apply_membership_clears_removed_leader_and_vote_when_local_node_stays_active() {
        let dir = temp_dir("raft-membership-removes-known-leader");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let initial_state = RaftHardState {
            current_term: 7,
            voted_for: Some("n3".into()),
            commit_index: 0,
        };
        raft.log
            .write_hard_state(&initial_state)
            .expect("write initial hard state");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 7;
            runtime.role = RaftRole::Follower;
            runtime.leader_id = Some("n3".into());
            runtime.voted_for = Some("n3".into());
        }
        {
            let mut connections = raft.rpc_connections.lock();
            connections.insert(
                "n2".into(),
                Arc::new(AsyncMutex::new(RaftRpcConnection::default())),
            );
            connections.insert(
                "n3".into(),
                Arc::new(AsyncMutex::new(RaftRpcConnection::default())),
            );
        }

        raft.apply_membership(RaftMembership::from_simple(vec![
            test_peer("n1", 7980),
            test_peer("n2", 7981),
            test_peer("n4", 7983),
        ]))
        .expect("apply membership removing known leader");

        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.role, RaftRole::Follower);
            assert_eq!(runtime.current_term, 7);
            assert_eq!(runtime.leader_id, None);
            assert_eq!(runtime.voted_for, None);
            assert!(runtime.membership.contains_id("n1"));
            assert!(!runtime.membership.contains_id("n3"));
            assert!(runtime.membership.contains_id("n4"));
        }
        let hard_state = raft.log.read_hard_state().expect("read hard state");
        assert_eq!(hard_state.current_term, 7);
        assert_eq!(hard_state.voted_for, None);
        assert_eq!(hard_state.commit_index, 0);
        {
            let connections = raft.rpc_connections.lock();
            assert!(connections.contains_key("n2"));
            assert!(!connections.contains_key("n3"));
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn apply_membership_drops_pooled_rpc_connection_when_peer_address_changes() {
        let dir = temp_dir("raft-membership-peer-address-change");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut connections = raft.rpc_connections.lock();
            connections.insert(
                "n2".into(),
                Arc::new(AsyncMutex::new(RaftRpcConnection {
                    addr: "127.0.0.1:7981".into(),
                    reader: None,
                })),
            );
            connections.insert(
                "n3".into(),
                Arc::new(AsyncMutex::new(RaftRpcConnection {
                    addr: "127.0.0.1:7982".into(),
                    reader: None,
                })),
            );
        }

        raft.apply_membership(RaftMembership::from_simple(vec![
            test_peer("n1", 7980),
            RaftPeerConfig {
                id: "n2".into(),
                addr: "127.0.0.1:8999".into(),
            },
            test_peer("n3", 7982),
        ]))
        .expect("apply membership with changed peer address");

        let connections = raft.rpc_connections.lock();
        assert!(
            !connections.contains_key("n2"),
            "pooled connection to n2's old address must be dropped"
        );
        assert!(
            connections.contains_key("n3"),
            "unchanged peer address should keep its pooled connection"
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn apply_membership_does_not_mutate_when_removed_vote_hard_state_write_fails() {
        let dir = temp_dir("raft-membership-removed-vote-hard-state-fails");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let before = raft.membership();
        let initial_state = RaftHardState {
            current_term: 7,
            voted_for: Some("n3".into()),
            commit_index: 0,
        };
        raft.log
            .write_hard_state(&initial_state)
            .expect("write initial hard state");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 7;
            runtime.role = RaftRole::Follower;
            runtime.leader_id = Some("n3".into());
            runtime.voted_for = Some("n3".into());
        }
        fs::create_dir_all(dir.join(HARD_STATE_FILE).with_extension("json.tmp"))
            .expect("block hard-state temp path");

        let err = raft
            .apply_membership(RaftMembership::from_simple(vec![
                test_peer("n1", 7980),
                test_peer("n2", 7981),
                test_peer("n4", 7983),
            ]))
            .expect_err("clearing removed vote should require durable hard-state write");

        assert!(matches!(err, BrokerRaftError::Io(_)));
        assert_eq!(raft.membership(), before);
        assert_eq!(
            raft.log.read_hard_state().expect("read hard state"),
            initial_state
        );
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.role, RaftRole::Follower);
            assert_eq!(runtime.current_term, 7);
            assert_eq!(runtime.leader_id.as_deref(), Some("n3"));
            assert_eq!(runtime.voted_for.as_deref(), Some("n3"));
            assert!(runtime.membership.contains_id("n3"));
            assert!(!runtime.membership.contains_id("n4"));
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn apply_staged_learners_does_not_mutate_runtime_when_sidecar_write_fails() {
        let dir = temp_dir("raft-stage-learners-sidecar-fails");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let learner = RaftPeerConfig {
            id: "n4".into(),
            addr: "127.0.0.1:7994".into(),
        };
        let blocked_tmp = dir.join(LEARNERS_FILE).with_extension("json.tmp");
        fs::create_dir_all(&blocked_tmp).expect("block learners temp file");

        let err = raft
            .apply_staged_learners(vec![learner])
            .expect_err("sidecar temp path should fail");

        assert!(matches!(err, BrokerRaftError::Io(_)));
        assert!(raft.staged_learners().is_empty());
        assert!(!raft.runtime.lock().leader_progress.contains_key("n4"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn apply_membership_does_not_mutate_runtime_when_sidecar_write_fails() {
        let dir = temp_dir("raft-membership-sidecar-fails");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let retained_learner = RaftPeerConfig {
            id: "n4".into(),
            addr: "127.0.0.1:7994".into(),
        };
        {
            let mut runtime = raft.runtime.lock();
            runtime
                .staged_learners
                .insert(retained_learner.id.clone(), retained_learner.clone());
        }
        let before_membership = raft.membership();
        let before_learners = raft.staged_learners();
        let blocked_tmp = dir.join(LEARNERS_FILE).with_extension("json.tmp");
        fs::create_dir_all(&blocked_tmp).expect("block learners temp file");

        let err = raft
            .apply_membership(RaftMembership::from_simple(vec![
                test_peer("n1", 7980),
                test_peer("n2", 7981),
                test_peer("n5", 7984),
            ]))
            .expect_err("sidecar temp path should fail");

        assert!(matches!(err, BrokerRaftError::Io(_)));
        assert_eq!(raft.membership(), before_membership);
        assert_eq!(raft.staged_learners(), before_learners);
        assert!(raft.membership().contains_id("n3"));
        assert!(!raft.membership().contains_id("n5"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn progress_snapshot_exposes_peer_lag_and_staged_learners() {
        let dir = temp_dir("raft-progress-snapshot");
        let cfg = test_raft_config(dir.clone());
        let leader_quorum_timeout_ms = cfg.election_timeout_min.as_millis() as u64;
        let raft = BrokerRaft::open(cfg).expect("open raft");
        raft.log.append(1, RaftCommand::Noop).expect("append one");
        raft.log.append(1, RaftCommand::Noop).expect("append two");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.commit_index = 1;
            runtime.last_applied = 1;
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 3,
                    match_index: 2,
                },
            );
            runtime.leader_progress.insert(
                "n4".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
            runtime.staged_learners.insert(
                "n4".into(),
                RaftPeerConfig {
                    id: "n4".into(),
                    addr: "127.0.0.1:7984".into(),
                },
            );
        }
        raft.note_leader_quorum_observed();

        let progress = raft.progress_snapshot();
        assert_eq!(progress.role, "leader");
        assert!(progress.is_leader);
        assert!(progress.is_leader_ready);
        assert_eq!(progress.leader_quorum_timeout_ms, leader_quorum_timeout_ms);
        assert!(progress
            .leader_quorum_age_ms
            .is_some_and(|age| age <= leader_quorum_timeout_ms));
        assert_eq!(progress.current_term, 1);
        assert_eq!(progress.commit_index, 1);
        assert_eq!(progress.last_applied, 1);
        let n1 = progress
            .peers
            .iter()
            .find(|peer| peer.id == "n1")
            .expect("self peer progress");
        assert_eq!(n1.match_index, Some(2));
        assert_eq!(n1.lag, Some(0));
        assert_eq!(n1.caught_up, Some(true));
        let n2 = progress
            .peers
            .iter()
            .find(|peer| peer.id == "n2")
            .expect("n2 peer progress");
        assert_eq!(n2.next_index, Some(3));
        assert_eq!(n2.match_index, Some(2));
        assert_eq!(n2.lag, Some(0));
        assert_eq!(n2.membership_role, "voter");
        let n4 = progress
            .peers
            .iter()
            .find(|peer| peer.id == "n4")
            .expect("staged learner progress");
        assert!(n4.staged_learner);
        assert!(!n4.voter);
        assert_eq!(n4.membership_role, "stagedLearner");
        assert_eq!(n4.lag, Some(2));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn progress_snapshot_marks_follower_not_leader_ready() {
        let dir = temp_dir("raft-progress-follower-readiness");
        let cfg = test_raft_config(dir.clone());
        let leader_quorum_timeout_ms = cfg.election_timeout_min.as_millis() as u64;
        let raft = BrokerRaft::open(cfg).expect("open raft");

        let progress = raft.progress_snapshot();

        assert_eq!(progress.role, "follower");
        assert!(!progress.is_leader);
        assert!(!progress.is_leader_ready);
        assert_eq!(progress.leader_quorum_age_ms, None);
        assert_eq!(progress.leader_quorum_timeout_ms, leader_quorum_timeout_ms);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn staged_learners_replay_across_reopen_and_can_be_removed() {
        let dir = temp_dir("raft-persistent-learners");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg.clone()).expect("open raft");
        let learner = RaftPeerConfig {
            id: "n4".into(),
            addr: "127.0.0.1:7984".into(),
        };
        commit_local_entry(
            &raft,
            1,
            RaftCommand::SetStagedLearners {
                learners: vec![learner.clone()],
            },
        );
        assert_eq!(raft.staged_learners(), vec![learner.clone()]);
        assert!(dir.join(LEARNERS_FILE).exists());

        let reopened = BrokerRaft::open(cfg.clone()).expect("reopen raft");
        assert_eq!(reopened.staged_learners(), vec![learner.clone()]);
        commit_local_entry(
            &reopened,
            1,
            RaftCommand::SetStagedLearners {
                learners: Vec::new(),
            },
        );
        assert!(reopened.staged_learners().is_empty());
        assert!(!dir.join(LEARNERS_FILE).exists());

        let reopened = BrokerRaft::open(cfg).expect("reopen after learner removal");
        assert!(reopened.staged_learners().is_empty());

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn stage_learners_appends_consensus_backed_command_and_catches_up_learner() {
        let dir = temp_dir("raft-stage-learners-replicated");
        let listener_n2 = TcpListener::bind("127.0.0.1:0").await.expect("bind n2");
        let unused_n3 = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind unused n3");
        let listener_n4 = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind n4 learner");
        let mut cfg = test_raft_config(dir.clone());
        cfg.peers = vec![
            RaftPeerConfig {
                id: "n1".into(),
                addr: "127.0.0.1:7980".into(),
            },
            RaftPeerConfig {
                id: "n2".into(),
                addr: listener_n2.local_addr().unwrap().to_string(),
            },
            RaftPeerConfig {
                id: "n3".into(),
                addr: unused_n3.local_addr().unwrap().to_string(),
            },
        ];
        let learner = RaftPeerConfig {
            id: "n4".into(),
            addr: listener_n4.local_addr().unwrap().to_string(),
        };
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
        }
        let server_n2 = tokio::spawn(serve_append_entries_until_staged_learners(listener_n2));
        let server_n4 = tokio::spawn(serve_append_entries_until_staged_learners(listener_n4));

        let learners = tokio::time::timeout(
            Duration::from_secs(2),
            raft.stage_learners(vec![learner.clone()]),
        )
        .await
        .expect("learner staging should not hang")
        .expect("stage learner through raft log");

        assert_eq!(learners, vec![learner]);
        let entries = raft.log.read_entries().expect("read raft log");
        assert!(matches!(
            entries.last().map(|entry| &entry.command),
            Some(RaftCommand::SetStagedLearners { .. })
        ));
        assert!(dir.join(LEARNERS_FILE).exists());
        assert_eq!(
            raft.runtime.lock().commit_index,
            entries.last().unwrap().index
        );
        assert_eq!(
            raft.runtime
                .lock()
                .leader_progress
                .get("n4")
                .map(|progress| progress.match_index),
            Some(entries.last().unwrap().index)
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), server_n2)
                .await
                .expect("n2 server should finish")
                .expect("n2 server"),
            vec![vec![1]]
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), server_n4)
                .await
                .expect("n4 server should finish")
                .expect("n4 server"),
            vec![vec![1]]
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn stage_learners_rejects_joint_membership_without_appending() {
        let dir = temp_dir("raft-stage-learners-joint-rejected");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.membership = RaftMembership::Joint {
                old_peers: vec![
                    test_peer("n1", 7980),
                    test_peer("n2", 7981),
                    test_peer("n3", 7982),
                ],
                new_peers: five_test_peers(),
            };
        }
        raft.note_leader_quorum_observed();

        let err = raft
            .stage_learners(vec![test_peer("n6", 7985)])
            .await
            .expect_err("learner staging must wait for final simple config");

        assert!(matches!(
            err,
            BrokerRaftError::InvalidConfig(message) if message.contains("joint consensus")
        ));
        assert!(raft.log.read_entries().expect("read entries").is_empty());
        assert!(raft.staged_learners().is_empty());

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn remove_staged_learners_rejects_joint_membership_without_appending() {
        let dir = temp_dir("raft-remove-learners-joint-rejected");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let learner = test_peer("n6", 7985);
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.membership = RaftMembership::Joint {
                old_peers: vec![
                    test_peer("n1", 7980),
                    test_peer("n2", 7981),
                    test_peer("n3", 7982),
                ],
                new_peers: five_test_peers(),
            };
            runtime
                .staged_learners
                .insert(learner.id.clone(), learner.clone());
        }
        raft.note_leader_quorum_observed();

        let err = raft
            .remove_staged_learners(vec![learner.id.clone()])
            .await
            .expect_err("learner removal must wait for final simple config");

        assert!(matches!(
            err,
            BrokerRaftError::InvalidConfig(message) if message.contains("joint consensus")
        ));
        assert!(raft.log.read_entries().expect("read entries").is_empty());
        assert_eq!(raft.staged_learners(), vec![learner]);

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn promoted_staged_learners_are_removed_from_persistent_file() {
        let dir = temp_dir("raft-promoted-learner-file-cleanup");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg.clone()).expect("open raft");
        commit_local_entry(
            &raft,
            1,
            RaftCommand::SetStagedLearners {
                learners: vec![test_peer("n4", 7983), test_peer("n5", 7984)],
            },
        );
        assert!(dir.join(LEARNERS_FILE).exists());

        commit_local_entry(
            &raft,
            1,
            RaftCommand::SetMembership {
                membership: RaftMembership::from_simple(five_test_peers()),
            },
        );
        assert!(raft.staged_learners().is_empty());
        assert!(!dir.join(LEARNERS_FILE).exists());

        let reopened = BrokerRaft::open(cfg).expect("reopen after promotion cleanup");
        assert!(reopened.staged_learners().is_empty());

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn learner_catchup_requires_current_leadership_even_if_progress_is_caught_up() {
        let dir = temp_dir("raft-learner-catchup-requires-leader");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let learner = test_peer("n4", 7983);
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.role = RaftRole::Follower;
            runtime.leader_id = Some("n2".into());
            runtime.leader_progress.insert(
                learner.id.clone(),
                RaftPeerProgress {
                    next_index: 6,
                    match_index: 5,
                },
            );
        }

        let err = raft
            .catch_up_learner_peer(learner, 5)
            .await
            .expect_err("stale progress must not satisfy catch-up after leader loss");

        assert!(matches!(err, BrokerRaftError::NotLeader { .. }));

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn new_membership_peer_catchup_runs_learners_concurrently() {
        let dir = temp_dir("raft-membership-learner-catchup-concurrent");
        let listener_n4 = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind learner n4");
        let addr_n4 = listener_n4.local_addr().expect("learner n4 addr");
        let listener_n5 = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind learner n5");
        let addr_n5 = listener_n5.local_addr().expect("learner n5 addr");

        let old_peers = vec![
            test_peer("n1", 7980),
            test_peer("n2", 7981),
            test_peer("n3", 7982),
        ];
        let learner_n4 = RaftPeerConfig {
            id: "n4".into(),
            addr: addr_n4.to_string(),
        };
        let learner_n5 = RaftPeerConfig {
            id: "n5".into(),
            addr: addr_n5.to_string(),
        };
        let new_peers = vec![
            old_peers[0].clone(),
            old_peers[1].clone(),
            old_peers[2].clone(),
            learner_n4.clone(),
            learner_n5.clone(),
        ];

        let mut cfg = test_raft_config(dir.clone());
        cfg.heartbeat_interval = Duration::from_millis(10);
        cfg.election_timeout_min = Duration::from_millis(250);
        cfg.election_timeout_max = Duration::from_millis(500);
        cfg.peers = old_peers.clone();
        let raft = BrokerRaft::open(cfg).expect("open raft");
        raft.log.append(2, RaftCommand::Noop).expect("append seed");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 2;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
        }
        raft.note_leader_quorum_observed();

        let (n4_seen_tx, n4_seen_rx) = oneshot::channel();
        let (n5_seen_tx, n5_seen_rx) = oneshot::channel();
        let server_n4 = tokio::spawn(async move {
            serve_one_append_after_peer_seen(listener_n4, n4_seen_tx, n5_seen_rx).await
        });
        let server_n5 = tokio::spawn(async move {
            serve_one_append_after_peer_seen(listener_n5, n5_seen_tx, n4_seen_rx).await
        });

        tokio::time::timeout(
            Duration::from_secs(1),
            raft.catch_up_new_membership_peers(&old_peers, &new_peers),
        )
        .await
        .expect("both learners should be contacted before either response is required")
        .expect("catch up new learners");
        assert_eq!(server_n4.await.expect("learner n4 server"), vec![1]);
        assert_eq!(server_n5.await.expect("learner n5 server"), vec![1]);
        {
            let runtime = raft.runtime.lock();
            assert_eq!(
                runtime
                    .leader_progress
                    .get(&learner_n4.id)
                    .map(|progress| progress.match_index),
                Some(1)
            );
            assert_eq!(
                runtime
                    .leader_progress
                    .get(&learner_n5.id)
                    .map(|progress| progress.match_index),
                Some(1)
            );
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn change_membership_catches_up_new_peers_before_promotion() {
        let dir = temp_dir("raft-membership-learner-catchup");
        let listener_n2 = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind old peer");
        let addr_n2 = listener_n2.local_addr().expect("old peer addr");
        let unused_n3 = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind unused peer");
        let addr_n3 = unused_n3.local_addr().expect("unused peer addr");
        drop(unused_n3);
        let listener_n4 = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind learner n4");
        let addr_n4 = listener_n4.local_addr().expect("learner n4 addr");
        let listener_n5 = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind learner n5");
        let addr_n5 = listener_n5.local_addr().expect("learner n5 addr");

        let mut cfg = test_raft_config(dir.clone());
        cfg.heartbeat_interval = Duration::from_millis(10);
        cfg.election_timeout_min = Duration::from_millis(250);
        cfg.election_timeout_max = Duration::from_millis(500);
        cfg.peers = vec![
            test_peer("n1", 7980),
            RaftPeerConfig {
                id: "n2".into(),
                addr: addr_n2.to_string(),
            },
            RaftPeerConfig {
                id: "n3".into(),
                addr: addr_n3.to_string(),
            },
        ];
        let raft = BrokerRaft::open(cfg).expect("open raft");
        for _ in 0..3 {
            raft.log.append(2, RaftCommand::Noop).expect("append seed");
        }
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 2;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
            runtime.leader_progress.insert(
                "n3".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
        }
        raft.note_leader_quorum_observed();

        let server_n2 = tokio::spawn(serve_append_entries_until_simple_membership(listener_n2));
        let server_n4 = tokio::spawn(serve_append_entries_until_simple_membership(listener_n4));
        let server_n5 = tokio::spawn(serve_append_entries_until_simple_membership(listener_n5));

        let new_peers = vec![
            test_peer("n1", 7980),
            RaftPeerConfig {
                id: "n2".into(),
                addr: addr_n2.to_string(),
            },
            RaftPeerConfig {
                id: "n3".into(),
                addr: addr_n3.to_string(),
            },
            RaftPeerConfig {
                id: "n4".into(),
                addr: addr_n4.to_string(),
            },
            RaftPeerConfig {
                id: "n5".into(),
                addr: addr_n5.to_string(),
            },
        ];

        let index = raft
            .change_membership(new_peers)
            .await
            .expect("change membership with staged learners");
        assert_eq!(index, 5);
        assert!(!raft.membership_is_joint());
        assert!(raft.membership().contains_id("n4"));
        assert!(raft.membership().contains_id("n5"));
        assert_eq!(raft.active_cluster_size(), 5);
        assert_eq!(raft.active_quorum_size(), 3);

        let n2_batches = server_n2.await.expect("old peer server");
        let n4_batches = server_n4.await.expect("learner n4 server");
        let n5_batches = server_n5.await.expect("learner n5 server");
        assert_eq!(n4_batches.first(), Some(&vec![1, 2, 3]));
        assert_eq!(n5_batches.first(), Some(&vec![1, 2, 3]));
        assert!(
            n4_batches.iter().flatten().any(|index| *index == 5),
            "n4 should receive final simple membership entry: {n4_batches:?}"
        );
        assert!(
            n5_batches.iter().flatten().any(|index| *index == 5),
            "n5 should receive final simple membership entry: {n5_batches:?}"
        );
        assert!(
            n2_batches.iter().flatten().any(|index| *index == 4),
            "old quorum peer should receive joint membership entry: {n2_batches:?}"
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn change_membership_retries_promoted_voter_after_lost_final_ack() {
        let dir = temp_dir("raft-membership-lost-final-ack");
        let listener_n2 = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind old peer");
        let addr_n2 = listener_n2.local_addr().expect("old peer addr");
        let unused_n3 = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind unused peer");
        let addr_n3 = unused_n3.local_addr().expect("unused peer addr");
        drop(unused_n3);
        let listener_n4 = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind learner n4");
        let addr_n4 = listener_n4.local_addr().expect("learner n4 addr");
        let listener_n5 = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind learner n5");
        let addr_n5 = listener_n5.local_addr().expect("learner n5 addr");

        let mut cfg = test_raft_config(dir.clone());
        cfg.heartbeat_interval = Duration::from_millis(10);
        cfg.election_timeout_min = Duration::from_millis(250);
        cfg.election_timeout_max = Duration::from_millis(500);
        cfg.peers = vec![
            test_peer("n1", 7980),
            RaftPeerConfig {
                id: "n2".into(),
                addr: addr_n2.to_string(),
            },
            RaftPeerConfig {
                id: "n3".into(),
                addr: addr_n3.to_string(),
            },
        ];
        let raft = BrokerRaft::open(cfg).expect("open raft");
        for _ in 0..3 {
            raft.log.append(2, RaftCommand::Noop).expect("append seed");
        }
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 2;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
            runtime.leader_progress.insert(
                "n3".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
        }
        raft.note_leader_quorum_observed();

        let server_n2 = tokio::spawn(serve_append_entries_until_simple_membership(listener_n2));
        let server_n4 = tokio::spawn(serve_append_entries_until_simple_membership(listener_n4));
        let server_n5 = tokio::spawn(serve_append_entries_drop_first_simple_membership_ack(
            listener_n5,
        ));

        let new_peers = vec![
            test_peer("n1", 7980),
            RaftPeerConfig {
                id: "n2".into(),
                addr: addr_n2.to_string(),
            },
            RaftPeerConfig {
                id: "n3".into(),
                addr: addr_n3.to_string(),
            },
            RaftPeerConfig {
                id: "n4".into(),
                addr: addr_n4.to_string(),
            },
            RaftPeerConfig {
                id: "n5".into(),
                addr: addr_n5.to_string(),
            },
        ];

        let index = raft
            .change_membership(new_peers)
            .await
            .expect("change membership should retry promoted voter catch-up");
        assert_eq!(index, 5);
        assert!(raft.membership().contains_id("n4"));
        assert!(raft.membership().contains_id("n5"));
        assert_eq!(
            raft.runtime
                .lock()
                .leader_progress
                .get("n5")
                .map(|progress| progress.match_index),
            Some(5)
        );

        let _ = server_n2.await.expect("old peer server");
        let _ = server_n4.await.expect("learner n4 server");
        let n5_batches = server_n5.await.expect("learner n5 server");
        let n5_final_entry_deliveries = n5_batches
            .iter()
            .filter(|batch| batch.iter().any(|index| *index == 5))
            .count();
        assert!(
            n5_final_entry_deliveries >= 2,
            "n5 should receive the final membership entry again after the lost ack: {n5_batches:?}"
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn change_membership_finishes_existing_joint_membership_with_matching_new_peers() {
        let dir = temp_dir("raft-membership-finish-existing-joint");
        let listener_n2 = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind old peer");
        let addr_n2 = listener_n2.local_addr().expect("old peer addr");
        let unused_n3 = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind unused peer");
        let addr_n3 = unused_n3.local_addr().expect("unused peer addr");
        drop(unused_n3);
        let listener_n4 = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind learner n4");
        let addr_n4 = listener_n4.local_addr().expect("learner n4 addr");
        let listener_n5 = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind learner n5");
        let addr_n5 = listener_n5.local_addr().expect("learner n5 addr");

        let old_peers = vec![
            test_peer("n1", 7980),
            RaftPeerConfig {
                id: "n2".into(),
                addr: addr_n2.to_string(),
            },
            RaftPeerConfig {
                id: "n3".into(),
                addr: addr_n3.to_string(),
            },
        ];
        let new_peers = vec![
            test_peer("n1", 7980),
            RaftPeerConfig {
                id: "n2".into(),
                addr: addr_n2.to_string(),
            },
            RaftPeerConfig {
                id: "n3".into(),
                addr: addr_n3.to_string(),
            },
            RaftPeerConfig {
                id: "n4".into(),
                addr: addr_n4.to_string(),
            },
            RaftPeerConfig {
                id: "n5".into(),
                addr: addr_n5.to_string(),
            },
        ];

        let mut cfg = test_raft_config(dir.clone());
        cfg.heartbeat_interval = Duration::from_millis(10);
        cfg.election_timeout_min = Duration::from_millis(250);
        cfg.election_timeout_max = Duration::from_millis(500);
        cfg.peers = old_peers.clone();
        let raft = BrokerRaft::open(cfg).expect("open raft");
        for _ in 0..3 {
            raft.log.append(2, RaftCommand::Noop).expect("append seed");
        }
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 2;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.membership = RaftMembership::Joint {
                old_peers: old_peers.clone(),
                new_peers: new_peers.clone(),
            };
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
            runtime.leader_progress.insert(
                "n3".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
        }
        raft.note_leader_quorum_observed();

        let server_n2 = tokio::spawn(serve_append_entries_until_simple_membership(listener_n2));
        let server_n4 = tokio::spawn(serve_append_entries_until_simple_membership(listener_n4));
        let server_n5 = tokio::spawn(serve_append_entries_until_simple_membership(listener_n5));

        let index = raft
            .change_membership(new_peers.clone())
            .await
            .expect("finish existing joint membership");
        assert_eq!(index, 4);
        assert_eq!(raft.membership(), RaftMembership::from_simple(new_peers));
        assert!(!raft.membership_is_joint());
        assert_eq!(raft.active_cluster_size(), 5);
        assert_eq!(raft.active_quorum_size(), 3);

        let membership_entries = raft
            .log
            .read_entries()
            .expect("read entries")
            .into_iter()
            .filter_map(|entry| match entry.command {
                RaftCommand::SetMembership { membership } => Some((entry.index, membership)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(membership_entries.len(), 1);
        assert!(matches!(
            membership_entries[0],
            (4, RaftMembership::Simple { .. })
        ));

        let n2_batches = server_n2.await.expect("old peer server");
        let n4_batches = server_n4.await.expect("learner n4 server");
        let n5_batches = server_n5.await.expect("learner n5 server");
        assert!(
            n2_batches.iter().flatten().any(|entry| *entry == 4),
            "old quorum peer should receive final simple membership entry: {n2_batches:?}"
        );
        assert!(
            n4_batches.iter().flatten().any(|entry| *entry == 4),
            "new voter n4 should receive final simple membership entry: {n4_batches:?}"
        );
        assert!(
            n5_batches.iter().flatten().any(|entry| *entry == 4),
            "new voter n5 should receive final simple membership entry: {n5_batches:?}"
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn change_membership_rejects_different_joint_membership_without_appending() {
        let dir = temp_dir("raft-membership-reject-different-joint");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let old_peers = vec![
            test_peer("n1", 7980),
            test_peer("n2", 7981),
            test_peer("n3", 7982),
        ];
        let joint_new_peers = five_test_peers();
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 2;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.membership = RaftMembership::Joint {
                old_peers,
                new_peers: joint_new_peers,
            };
        }
        raft.note_leader_quorum_observed();

        let different_new_peers = vec![
            test_peer("n1", 7980),
            test_peer("n2", 7981),
            test_peer("n3", 7982),
            test_peer("n4", 7983),
            test_peer("n6", 7985),
        ];
        let err = raft
            .change_membership(different_new_peers)
            .await
            .expect_err("different joint change should be rejected");

        assert!(matches!(
            err,
            BrokerRaftError::InvalidConfig(message)
                if message.contains("different raft membership change")
        ));
        assert!(raft.log.read_entries().expect("read entries").is_empty());
        assert!(raft.membership_is_joint());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn joint_membership_entry_requires_old_and_new_quorums_to_commit() {
        let dir = temp_dir("raft-joint-membership-requires-new-quorum");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let old_peers = vec![
            test_peer("n1", 7980),
            test_peer("n2", 7981),
            test_peer("n3", 7982),
        ];
        let new_peers = vec![
            test_peer("n1", 7980),
            test_peer("n4", 7983),
            test_peer("n5", 7984),
            test_peer("n6", 7985),
            test_peer("n7", 7986),
        ];
        let joint = RaftMembership::Joint {
            old_peers: old_peers.clone(),
            new_peers: new_peers.clone(),
        };
        raft.log
            .append(
                3,
                RaftCommand::SetMembership {
                    membership: joint.clone(),
                },
            )
            .expect("append joint membership entry");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.membership = RaftMembership::from_simple(old_peers.clone());
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 2,
                    match_index: 1,
                },
            );
            runtime.leader_progress.insert(
                "n4".into(),
                RaftPeerProgress {
                    next_index: 2,
                    match_index: 1,
                },
            );
        }

        let committed = raft
            .commit_leader_index_in_term_with_membership(1, 3, true, Some(&joint))
            .expect("joint commit should check both quorums");

        assert!(
            !committed,
            "old quorum n1+n2 and partial new quorum n1+n4 must not commit joint config"
        );
        assert_eq!(raft.membership(), RaftMembership::from_simple(old_peers));
        assert_eq!(
            raft.log.read_hard_state().expect("hard state").commit_index,
            0
        );
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.commit_index, 0);
            assert_eq!(runtime.last_applied, 0);
        }

        {
            raft.runtime.lock().leader_progress.insert(
                "n5".into(),
                RaftPeerProgress {
                    next_index: 2,
                    match_index: 1,
                },
            );
        }
        let committed = raft
            .commit_leader_index_in_term_with_membership(1, 3, true, Some(&joint))
            .expect("joint commit should succeed after new quorum");

        assert!(committed);
        assert_eq!(raft.membership(), joint);
        assert_eq!(
            raft.log.read_hard_state().expect("hard state").commit_index,
            1
        );
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.commit_index, 1);
            assert_eq!(runtime.last_applied, 1);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn snapshot_payload_restores_membership_on_open() {
        let dir = temp_dir("raft-snapshot-membership");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg.clone()).expect("open raft");
        let mut payload = idle_snapshot_payload();
        payload["membership"] =
            serde_json::to_value(RaftMembership::from_simple(five_test_peers())).unwrap();
        raft.log
            .write_snapshot(3, 1, payload)
            .expect("write membership snapshot");
        raft.log
            .write_hard_state(&RaftHardState {
                current_term: 1,
                voted_for: None,
                commit_index: 3,
            })
            .expect("write hard state");
        drop(raft);

        let reopened = BrokerRaft::open(cfg).expect("reopen raft");

        assert_eq!(reopened.active_cluster_size(), 5);
        assert_eq!(reopened.active_quorum_size(), 3);
        assert!(reopened.membership().contains_id("n5"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn open_clears_persisted_vote_for_peer_removed_from_recovered_membership() {
        let dir = temp_dir("raft-open-clears-removed-vote");
        let mut cfg = test_raft_config(dir.clone());
        cfg.peers = vec![
            test_peer("n1", 7980),
            test_peer("n2", 7981),
            test_peer("n4", 7983),
        ];
        let store = RaftLogStore::open(&dir).expect("open store");
        store
            .write_hard_state(&RaftHardState {
                current_term: 7,
                voted_for: Some("n3".into()),
                commit_index: 0,
            })
            .expect("write stale vote");
        drop(store);

        let raft = BrokerRaft::open(cfg).expect("open raft clears stale vote");

        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 7);
            assert_eq!(runtime.voted_for, None);
            assert!(!runtime.membership.contains_id("n3"));
            assert!(runtime.membership.contains_id("n4"));
        }
        let hard_state = raft.log.read_hard_state().expect("read hard state");
        assert_eq!(hard_state.current_term, 7);
        assert_eq!(hard_state.voted_for, None);
        assert_eq!(hard_state.commit_index, 0);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn open_preserves_persisted_vote_when_snapshot_membership_restores_peer() {
        let dir = temp_dir("raft-open-keeps-snapshot-restored-vote");
        let mut cfg = test_raft_config(dir.clone());
        cfg.peers = vec![
            test_peer("n1", 7980),
            test_peer("n2", 7981),
            test_peer("n4", 7983),
        ];
        let store = RaftLogStore::open(&dir).expect("open store");
        let mut payload = idle_snapshot_payload();
        payload["membership"] = serde_json::to_value(RaftMembership::from_simple(vec![
            test_peer("n1", 7980),
            test_peer("n2", 7981),
            test_peer("n3", 7982),
        ]))
        .unwrap();
        store
            .write_snapshot(3, 2, payload)
            .expect("write membership snapshot");
        store
            .write_hard_state(&RaftHardState {
                current_term: 7,
                voted_for: Some("n3".into()),
                commit_index: 3,
            })
            .expect("write vote restored by snapshot membership");
        drop(store);

        let raft = BrokerRaft::open(cfg).expect("open raft with snapshot membership");

        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 7);
            assert_eq!(runtime.voted_for.as_deref(), Some("n3"));
            assert!(runtime.membership.contains_id("n3"));
            assert!(!runtime.membership.contains_id("n4"));
        }
        let hard_state = raft.log.read_hard_state().expect("read hard state");
        assert_eq!(hard_state.current_term, 7);
        assert_eq!(hard_state.voted_for.as_deref(), Some("n3"));
        assert_eq!(hard_state.commit_index, 3);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn open_does_not_clear_removed_vote_in_memory_when_hard_state_write_fails() {
        let dir = temp_dir("raft-open-clears-removed-vote-hard-state-fails");
        let mut cfg = test_raft_config(dir.clone());
        cfg.peers = vec![
            test_peer("n1", 7980),
            test_peer("n2", 7981),
            test_peer("n4", 7983),
        ];
        let initial_state = RaftHardState {
            current_term: 7,
            voted_for: Some("n3".into()),
            commit_index: 0,
        };
        let store = RaftLogStore::open(&dir).expect("open store");
        store
            .write_hard_state(&initial_state)
            .expect("write stale vote");
        drop(store);
        fs::create_dir_all(dir.join(HARD_STATE_FILE).with_extension("json.tmp"))
            .expect("block hard-state temp path");

        let opened = BrokerRaft::open(cfg);

        assert!(matches!(opened, Err(BrokerRaftError::Io(_))));
        assert_eq!(
            read_hard_state(&dir.join(HARD_STATE_FILE)).expect("read hard state"),
            initial_state
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn snapshot_payload_restores_staged_learners_on_open() {
        let dir = temp_dir("raft-snapshot-learners");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg.clone()).expect("open raft");
        let learner = test_peer("n4", 7983);
        let mut payload = idle_snapshot_payload();
        payload["membership"] =
            serde_json::to_value(RaftMembership::from_simple(cfg.peers.clone())).unwrap();
        payload["stagedLearners"] = serde_json::to_value(vec![learner.clone()]).unwrap();
        raft.log
            .write_snapshot(7, 3, payload)
            .expect("write learner snapshot");

        let reopened = BrokerRaft::open(cfg).expect("reopen raft");
        assert_eq!(reopened.staged_learners(), vec![learner]);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn open_validates_learner_sidecar_against_snapshot_membership() {
        let dir = temp_dir("raft-sidecar-learners-use-snapshot-membership");
        let cfg = test_raft_config(dir.clone());
        let store = RaftLogStore::open(&dir).expect("open store");
        let mut payload = idle_snapshot_payload();
        payload["membership"] = serde_json::to_value(RaftMembership::from_simple(vec![
            test_peer("n1", 7980),
            test_peer("n2", 7981),
            test_peer("n4", 7983),
        ]))
        .unwrap();
        store
            .write_snapshot(7, 3, payload)
            .expect("write membership snapshot");
        store
            .write_hard_state(&RaftHardState {
                current_term: 3,
                voted_for: None,
                commit_index: 7,
            })
            .expect("write committed snapshot state");
        drop(store);
        let learner = test_peer("n3", 7982);
        write_staged_learners(
            &dir,
            &dir.join(LEARNERS_FILE),
            std::slice::from_ref(&learner),
        )
        .expect("write learner sidecar");

        let reopened = BrokerRaft::open(cfg).expect("reopen with snapshot membership");

        assert!(reopened.membership().contains_id("n4"));
        assert!(!reopened.membership().contains_id("n3"));
        assert_eq!(reopened.staged_learners(), vec![learner]);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn snapshot_payload_rejects_invalid_staged_learners_on_open() {
        let dir = temp_dir("raft-snapshot-invalid-learners");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg.clone()).expect("open raft");
        let mut payload = idle_snapshot_payload();
        payload["membership"] =
            serde_json::to_value(RaftMembership::from_simple(cfg.peers.clone())).unwrap();
        payload["stagedLearners"] = serde_json::to_value(vec![test_peer("n2", 7981)]).unwrap();
        raft.log
            .write_snapshot(7, 3, payload)
            .expect("write invalid learner snapshot");

        let reopened = BrokerRaft::open(cfg);
        assert!(matches!(reopened, Err(BrokerRaftError::InvalidConfig(_))));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn snapshot_payload_rejects_invalid_membership_on_open() {
        let dir = temp_dir("raft-snapshot-invalid-membership");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg.clone()).expect("open raft");
        let mut payload = idle_snapshot_payload();
        payload["membership"] = serde_json::to_value(duplicate_peer_membership()).unwrap();
        raft.log
            .write_snapshot(3, 1, payload)
            .expect("write invalid membership snapshot");
        raft.log
            .write_hard_state(&RaftHardState {
                current_term: 1,
                voted_for: None,
                commit_index: 3,
            })
            .expect("write hard state");
        drop(raft);

        let reopened = BrokerRaft::open(cfg);
        assert!(matches!(reopened, Err(BrokerRaftError::InvalidConfig(_))));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn compaction_keeps_entries_after_snapshot_index() {
        let dir = temp_dir("raft-compact");
        let store = RaftLogStore::open(&dir).expect("open store");

        for _ in 0..5 {
            store.append(1, RaftCommand::Noop).expect("append");
        }
        store
            .write_snapshot(3, 1, json!({ "state": "test" }))
            .expect("snapshot");

        let report = store.compact_to_latest_snapshot().expect("compact");
        assert_eq!(report.compacted_through_index, 3);
        assert_eq!(report.compacted_entries, 3);
        assert_eq!(report.retained_entries, 2);

        let remaining = store.read_entries().expect("read remaining");
        assert_eq!(
            remaining.iter().map(|e| e.index).collect::<Vec<_>>(),
            vec![4, 5]
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn cannot_compact_past_latest_snapshot() {
        let dir = temp_dir("raft-compact-ahead");
        let store = RaftLogStore::open(&dir).expect("open store");
        store.append(1, RaftCommand::Noop).expect("append");
        store
            .write_snapshot(1, 1, json!({ "state": "test" }))
            .expect("snapshot");

        let err = store.compact_through(2).expect_err("must reject");
        assert!(matches!(
            err,
            BrokerRaftError::CompactionAheadOfSnapshot {
                through_index: 2,
                snapshot_index: 1
            }
        ));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn append_entries_reports_conflict_term_and_first_index() {
        let dir = temp_dir("raft-conflict-report");
        let store = RaftLogStore::open(&dir).expect("open store");
        for term in [1, 1, 2, 2, 3] {
            store.append(term, RaftCommand::Noop).expect("append");
        }

        let report = store
            .append_entries_from_leader(4, 99, 3, 0, Vec::new())
            .expect("append entries check");

        assert!(!report.success);
        assert_eq!(report.conflict_term, Some(2));
        assert_eq!(report.conflict_index, Some(3));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn rejected_higher_term_append_entries_persists_term_before_reply() {
        let dir = temp_dir("raft-append-higher-term-persist");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        raft.log.append(1, RaftCommand::Noop).expect("append seed");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
            runtime.voted_for = Some("n1".into());
        }

        let response = raft.handle_append_entries(5, "n2".into(), 1, 99, Vec::new(), 0);
        assert!(matches!(
            response,
            RaftRpcResponse::AppendEntries {
                term: 5,
                success: false,
                conflict_term: Some(1),
                ..
            }
        ));

        let hard_state = raft.log.read_hard_state().expect("read hard state");
        assert_eq!(hard_state.current_term, 5);
        assert_eq!(hard_state.voted_for, None);
        assert_eq!(hard_state.commit_index, 0);
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 5);
            assert_eq!(runtime.voted_for, None);
            assert_eq!(runtime.role, RaftRole::Follower);
            assert_eq!(runtime.leader_id.as_deref(), Some("n2"));
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn stale_append_finish_does_not_advance_commit_after_term_change() {
        let dir = temp_dir("raft-append-finish-stale-term");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 6;
            runtime.role = RaftRole::Follower;
            runtime.leader_id = Some("n2".into());
            runtime.commit_index = 0;
        }

        let response = raft.finish_append_entries(
            5,
            "n2",
            RaftAppendReport {
                success: true,
                match_index: 2,
                conflict_index: None,
                conflict_term: None,
            },
            2,
        );

        assert!(matches!(
            response,
            RaftRpcResponse::AppendEntries {
                term: 6,
                success: false,
                match_index: 2,
                ..
            }
        ));
        assert_eq!(raft.runtime.lock().commit_index, 0);

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn rpc_append_entries_appends_entries_via_async_handler() {
        let dir = temp_dir("raft-rpc-append-async-handler");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
        }

        let response = raft
            .handle_rpc(RaftRpc::AppendEntries {
                auth_token: None,
                term: 2,
                leader_id: "n2".into(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![noop_entry(1, 2), noop_entry(2, 2)],
                leader_commit: 2,
            })
            .await;

        assert!(matches!(
            response,
            RaftRpcResponse::AppendEntries {
                term: 2,
                success: true,
                match_index: 2,
                ..
            }
        ));
        assert_eq!(
            raft.log
                .read_entries()
                .expect("read appended entries")
                .iter()
                .map(|entry| (entry.index, entry.term))
                .collect::<Vec<_>>(),
            vec![(1, 2), (2, 2)]
        );
        assert_eq!(
            raft.log.read_hard_state().expect("read hard state"),
            RaftHardState {
                current_term: 2,
                voted_for: None,
                commit_index: 2,
            }
        );
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 2);
            assert_eq!(runtime.commit_index, 2);
            assert_eq!(runtime.last_applied, 2);
            assert_eq!(runtime.leader_id.as_deref(), Some("n2"));
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn higher_term_append_entries_does_not_mutate_runtime_when_hard_state_write_fails() {
        let dir = temp_dir("raft-append-higher-term-hard-state-fails");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let (initial_state, initial_deadline) = {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 2;
            runtime.voted_for = Some("n1".into());
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.election_deadline = Instant::now() + Duration::from_secs(60);
            (runtime.hard_state(), runtime.election_deadline)
        };
        raft.log
            .write_hard_state(&initial_state)
            .expect("persist initial hard state");
        fs::create_dir_all(dir.join(HARD_STATE_FILE).with_extension("json.tmp"))
            .expect("block hard-state temp path");

        let response = raft.handle_append_entries(5, "n2".into(), 0, 0, Vec::new(), 0);

        assert!(matches!(response, RaftRpcResponse::Error { term: 5, .. }));
        assert_eq!(
            raft.log.read_hard_state().expect("read hard state"),
            initial_state
        );
        assert!(raft.log.read_entries().expect("entries").is_empty());
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 2);
            assert_eq!(runtime.voted_for.as_deref(), Some("n1"));
            assert_eq!(runtime.role, RaftRole::Leader);
            assert_eq!(runtime.leader_id.as_deref(), Some("n1"));
            assert_eq!(runtime.election_deadline, initial_deadline);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn append_entries_caps_follower_commit_at_matched_leader_index() {
        let dir = temp_dir("raft-append-commit-match-cap");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        raft.log.append(1, RaftCommand::Noop).expect("append seed");
        raft.log
            .append(2, RaftCommand::Noop)
            .expect("append divergent local tail");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 2;
            runtime.commit_index = 0;
            runtime.last_applied = 0;
        }

        let response = raft.handle_append_entries(3, "n2".into(), 1, 1, Vec::new(), 2);
        assert!(matches!(
            response,
            RaftRpcResponse::AppendEntries {
                term: 3,
                success: true,
                match_index: 1,
                ..
            }
        ));

        let hard_state = raft.log.read_hard_state().expect("read hard state");
        assert_eq!(hard_state.current_term, 3);
        assert_eq!(
            hard_state.commit_index, 1,
            "follower commitIndex is persisted before applying leaderCommit"
        );
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 3);
            assert_eq!(runtime.commit_index, 1);
            assert_eq!(runtime.last_applied, 1);
            assert_eq!(runtime.leader_id.as_deref(), Some("n2"));
        }
        assert_eq!(raft.log.last_index(), 2);
        assert_eq!(raft.log.last_term(), 2);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn append_entries_refuses_to_rewrite_committed_follower_prefix() {
        let dir = temp_dir("raft-append-committed-prefix-conflict");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        raft.log.append(1, RaftCommand::Noop).expect("append first");
        raft.log
            .append(1, RaftCommand::Noop)
            .expect("append committed second");
        let initial_state = RaftHardState {
            current_term: 1,
            voted_for: Some("n1".into()),
            commit_index: 2,
        };
        raft.log
            .write_hard_state(&initial_state)
            .expect("persist committed prefix");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
            runtime.voted_for = Some("n1".into());
            runtime.commit_index = 2;
            runtime.last_applied = 2;
        }

        let response = raft.handle_append_entries(
            2,
            "n2".into(),
            1,
            1,
            vec![RaftLogEntry {
                index: 2,
                term: 2,
                created_at_ms: 10,
                command: RaftCommand::Noop,
            }],
            2,
        );

        assert!(matches!(response, RaftRpcResponse::Error { term: 2, .. }));
        assert_eq!(
            raft.log
                .read_entries()
                .expect("entries")
                .iter()
                .map(|entry| (entry.index, entry.term))
                .collect::<Vec<_>>(),
            vec![(1, 1), (2, 1)]
        );
        let hard_state = raft.log.read_hard_state().expect("read hard state");
        assert_eq!(hard_state.current_term, 2);
        assert_eq!(hard_state.commit_index, 2);
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 2);
            assert_eq!(runtime.commit_index, 2);
            assert_eq!(runtime.last_applied, 2);
            assert_eq!(runtime.leader_id.as_deref(), Some("n2"));
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn raft_rpc_rejects_bad_peer_token_before_term_change() {
        let dir = temp_dir("raft-rpc-bad-peer-token");
        let mut cfg = test_raft_config(dir.clone());
        cfg.peer_token = Some("secret".into());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
            runtime.voted_for = Some("n1".into());
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
        }

        let response = raft
            .handle_rpc(RaftRpc::AppendEntries {
                auth_token: Some("wrong".into()),
                term: 9,
                leader_id: "n2".into(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Vec::new(),
                leader_commit: 0,
            })
            .await;
        assert!(matches!(
            response,
            RaftRpcResponse::Error {
                term: 1,
                ref error
            } if error == "unauthorized raft RPC"
        ));

        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 1);
            assert_eq!(runtime.voted_for.as_deref(), Some("n1"));
            assert_eq!(runtime.role, RaftRole::Leader);
            assert_eq!(runtime.leader_id.as_deref(), Some("n1"));
        }
        assert_eq!(raft.log.last_index(), 0);

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn raft_rpc_accepts_matching_peer_token() {
        let dir = temp_dir("raft-rpc-good-peer-token");
        let mut cfg = test_raft_config(dir.clone());
        cfg.peer_token = Some("secret".into());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
        }

        let response = raft
            .handle_rpc(RaftRpc::AppendEntries {
                auth_token: Some("secret".into()),
                term: 2,
                leader_id: "n2".into(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Vec::new(),
                leader_commit: 0,
            })
            .await;
        assert!(matches!(
            response,
            RaftRpcResponse::AppendEntries {
                term: 2,
                success: true,
                match_index: 0,
                ..
            }
        ));

        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 2);
            assert_eq!(runtime.role, RaftRole::Follower);
            assert_eq!(runtime.leader_id.as_deref(), Some("n2"));
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn pre_vote_does_not_advance_term_or_persist_vote() {
        let dir = temp_dir("raft-pre-vote-no-mutation");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let (initial_state, initial_deadline) = {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.voted_for = Some("n2".into());
            runtime.role = RaftRole::Follower;
            runtime.leader_id = None;
            runtime.election_deadline = Instant::now() + Duration::from_secs(60);
            (runtime.hard_state(), runtime.election_deadline)
        };
        raft.log
            .write_hard_state(&initial_state)
            .expect("persist initial hard state");

        let response = raft.handle_pre_vote(4, "n3".into(), 0, 0);
        assert!(matches!(
            response,
            RaftRpcResponse::PreVote {
                term: 3,
                vote_granted: true
            }
        ));

        assert_eq!(
            raft.log.read_hard_state().expect("read hard state"),
            initial_state
        );
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 3);
            assert_eq!(runtime.voted_for.as_deref(), Some("n2"));
            assert_eq!(runtime.role, RaftRole::Follower);
            assert_eq!(runtime.leader_id, None);
            assert_eq!(runtime.election_deadline, initial_deadline);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn pre_vote_rejects_stale_log_without_term_change() {
        let dir = temp_dir("raft-pre-vote-stale-log");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        raft.log.append(3, RaftCommand::Noop).expect("append seed");
        let initial_state = {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.voted_for = None;
            runtime.role = RaftRole::Follower;
            runtime.leader_id = None;
            runtime.hard_state()
        };
        raft.log
            .write_hard_state(&initial_state)
            .expect("persist initial hard state");

        let response = raft.handle_pre_vote(4, "n3".into(), 0, 0);
        assert!(matches!(
            response,
            RaftRpcResponse::PreVote {
                term: 3,
                vote_granted: false
            }
        ));
        assert_eq!(
            raft.log.read_hard_state().expect("read hard state"),
            initial_state
        );
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 3);
            assert_eq!(runtime.voted_for, None);
            assert_eq!(runtime.role, RaftRole::Follower);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn pre_vote_allows_candidate_after_known_leader_deadline_expires() {
        let dir = temp_dir("raft-pre-vote-expired-leader");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let initial_state = {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.voted_for = None;
            runtime.role = RaftRole::Follower;
            runtime.leader_id = Some("n2".into());
            runtime.election_deadline = Instant::now() - Duration::from_millis(1);
            runtime.hard_state()
        };
        raft.log
            .write_hard_state(&initial_state)
            .expect("persist initial hard state");

        let response = raft.handle_pre_vote(4, "n3".into(), 0, 0);
        assert!(matches!(
            response,
            RaftRpcResponse::PreVote {
                term: 3,
                vote_granted: true
            }
        ));
        assert_eq!(
            raft.log.read_hard_state().expect("read hard state"),
            initial_state
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn request_vote_from_unknown_candidate_does_not_advance_term() {
        let dir = temp_dir("raft-unknown-candidate-vote");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let (initial_state, initial_deadline) = {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.voted_for = Some("n1".into());
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.election_deadline = Instant::now() + Duration::from_secs(60);
            (runtime.hard_state(), runtime.election_deadline)
        };
        raft.log
            .write_hard_state(&initial_state)
            .expect("persist initial hard state");

        let response = raft.handle_request_vote(9, "ghost".into(), 0, 0);
        assert!(matches!(
            response,
            RaftRpcResponse::RequestVote {
                term: 3,
                vote_granted: false
            }
        ));

        let hard_state = raft.log.read_hard_state().expect("read hard state");
        assert_eq!(hard_state.current_term, 3);
        assert_eq!(hard_state.voted_for.as_deref(), Some("n1"));
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 3);
            assert_eq!(runtime.voted_for.as_deref(), Some("n1"));
            assert_eq!(runtime.role, RaftRole::Leader);
            assert_eq!(runtime.leader_id.as_deref(), Some("n1"));
            assert_eq!(runtime.election_deadline, initial_deadline);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn request_vote_from_same_term_candidate_rejected_after_known_leader() {
        let dir = temp_dir("raft-same-term-candidate-known-leader");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let (initial_state, initial_deadline) = {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.voted_for = None;
            runtime.role = RaftRole::Follower;
            runtime.leader_id = Some("n2".into());
            runtime.election_deadline = Instant::now() + Duration::from_secs(60);
            (runtime.hard_state(), runtime.election_deadline)
        };
        raft.log
            .write_hard_state(&initial_state)
            .expect("persist initial hard state");

        let response = raft.handle_request_vote(3, "n3".into(), 0, 0);
        assert!(matches!(
            response,
            RaftRpcResponse::RequestVote {
                term: 3,
                vote_granted: false
            }
        ));

        let hard_state = raft.log.read_hard_state().expect("read hard state");
        assert_eq!(hard_state.current_term, 3);
        assert_eq!(hard_state.voted_for, None);
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 3);
            assert_eq!(runtime.voted_for, None);
            assert_eq!(runtime.role, RaftRole::Follower);
            assert_eq!(runtime.leader_id.as_deref(), Some("n2"));
            assert_eq!(runtime.election_deadline, initial_deadline);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn request_vote_higher_term_candidate_rejected_after_fresh_known_leader() {
        let dir = temp_dir("raft-higher-term-candidate-fresh-known-leader");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let (initial_state, initial_deadline) = {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.voted_for = None;
            runtime.role = RaftRole::Follower;
            runtime.leader_id = Some("n2".into());
            runtime.election_deadline = Instant::now() + Duration::from_secs(60);
            (runtime.hard_state(), runtime.election_deadline)
        };
        raft.log
            .write_hard_state(&initial_state)
            .expect("persist initial hard state");

        let response = raft.handle_request_vote(4, "n3".into(), 0, 0);
        assert!(matches!(
            response,
            RaftRpcResponse::RequestVote {
                term: 3,
                vote_granted: false
            }
        ));

        assert_eq!(
            raft.log.read_hard_state().expect("read hard state"),
            initial_state
        );
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 3);
            assert_eq!(runtime.voted_for, None);
            assert_eq!(runtime.role, RaftRole::Follower);
            assert_eq!(runtime.leader_id.as_deref(), Some("n2"));
            assert_eq!(runtime.election_deadline, initial_deadline);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn request_vote_does_not_mutate_runtime_when_hard_state_write_fails() {
        let dir = temp_dir("raft-vote-hard-state-fails");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let (initial_state, initial_deadline) = {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.voted_for = Some("n1".into());
            runtime.role = RaftRole::Follower;
            runtime.leader_id = None;
            runtime.election_deadline = Instant::now() + Duration::from_secs(60);
            (runtime.hard_state(), runtime.election_deadline)
        };
        raft.log
            .write_hard_state(&initial_state)
            .expect("persist initial hard state");
        fs::create_dir_all(dir.join(HARD_STATE_FILE).with_extension("json.tmp"))
            .expect("block hard-state temp path");

        let response = raft.handle_request_vote(4, "n2".into(), 0, 0);

        assert!(matches!(response, RaftRpcResponse::Error { term: 4, .. }));
        assert_eq!(
            raft.log.read_hard_state().expect("read hard state"),
            initial_state
        );
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 3);
            assert_eq!(runtime.voted_for.as_deref(), Some("n1"));
            assert_eq!(runtime.role, RaftRole::Follower);
            assert_eq!(runtime.leader_id, None);
            assert_eq!(runtime.election_deadline, initial_deadline);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn rpc_request_vote_persists_vote_via_async_handler() {
        let dir = temp_dir("raft-rpc-request-vote-async-handler");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
            runtime.voted_for = None;
            runtime.role = RaftRole::Follower;
            runtime.leader_id = None;
            runtime.commit_index = 0;
        }

        let response = raft
            .handle_rpc(RaftRpc::RequestVote {
                auth_token: None,
                term: 2,
                candidate_id: "n2".into(),
                last_log_index: 0,
                last_log_term: 0,
            })
            .await;

        assert!(matches!(
            response,
            RaftRpcResponse::RequestVote {
                term: 2,
                vote_granted: true
            }
        ));
        assert_eq!(
            raft.log.read_hard_state().expect("read hard state"),
            RaftHardState {
                current_term: 2,
                voted_for: Some("n2".into()),
                commit_index: 0,
            }
        );
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 2);
            assert_eq!(runtime.voted_for.as_deref(), Some("n2"));
            assert_eq!(runtime.role, RaftRole::Follower);
            assert_eq!(runtime.leader_id, None);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn append_entries_from_unknown_leader_does_not_advance_term() {
        let dir = temp_dir("raft-unknown-leader-append");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        raft.log.append(1, RaftCommand::Noop).expect("append seed");
        let (initial_state, initial_deadline) = {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.voted_for = Some("n1".into());
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.election_deadline = Instant::now() + Duration::from_secs(60);
            (runtime.hard_state(), runtime.election_deadline)
        };
        raft.log
            .write_hard_state(&initial_state)
            .expect("persist initial hard state");

        let response = raft.handle_append_entries(9, "ghost".into(), 1, 1, Vec::new(), 0);
        assert!(matches!(
            response,
            RaftRpcResponse::AppendEntries {
                term: 3,
                success: false,
                match_index: 1,
                ..
            }
        ));

        let hard_state = raft.log.read_hard_state().expect("read hard state");
        assert_eq!(hard_state.current_term, 3);
        assert_eq!(hard_state.voted_for.as_deref(), Some("n1"));
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 3);
            assert_eq!(runtime.voted_for.as_deref(), Some("n1"));
            assert_eq!(runtime.role, RaftRole::Leader);
            assert_eq!(runtime.leader_id.as_deref(), Some("n1"));
            assert_eq!(runtime.election_deadline, initial_deadline);
        }
        assert_eq!(raft.log.last_index(), 1);
        assert_eq!(raft.log.last_term(), 1);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn append_entries_from_same_term_conflicting_leader_is_rejected() {
        let dir = temp_dir("raft-same-term-conflicting-leader-append");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        raft.log.append(1, RaftCommand::Noop).expect("append seed");
        let (initial_state, initial_deadline) = {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.voted_for = None;
            runtime.role = RaftRole::Follower;
            runtime.leader_id = Some("n2".into());
            runtime.election_deadline = Instant::now() + Duration::from_secs(60);
            (runtime.hard_state(), runtime.election_deadline)
        };
        raft.log
            .write_hard_state(&initial_state)
            .expect("persist initial hard state");

        let response = raft.handle_append_entries(3, "n3".into(), 1, 1, Vec::new(), 0);
        assert!(matches!(
            response,
            RaftRpcResponse::AppendEntries {
                term: 3,
                success: false,
                match_index: 1,
                ..
            }
        ));

        let hard_state = raft.log.read_hard_state().expect("read hard state");
        assert_eq!(hard_state.current_term, 3);
        assert_eq!(hard_state.voted_for, None);
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 3);
            assert_eq!(runtime.voted_for, None);
            assert_eq!(runtime.role, RaftRole::Follower);
            assert_eq!(runtime.leader_id.as_deref(), Some("n2"));
            assert_eq!(runtime.election_deadline, initial_deadline);
        }
        assert_eq!(raft.log.last_index(), 1);
        assert_eq!(raft.log.last_term(), 1);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn higher_term_append_entries_replaces_stale_known_leader() {
        let dir = temp_dir("raft-higher-term-new-leader-append");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        raft.log.append(1, RaftCommand::Noop).expect("append seed");
        let initial_state = {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.voted_for = Some("n2".into());
            runtime.role = RaftRole::Follower;
            runtime.leader_id = Some("n2".into());
            runtime.election_deadline = Instant::now() + Duration::from_secs(60);
            runtime.hard_state()
        };
        raft.log
            .write_hard_state(&initial_state)
            .expect("persist initial hard state");

        let response = raft.handle_append_entries(4, "n3".into(), 1, 1, Vec::new(), 0);
        assert!(matches!(
            response,
            RaftRpcResponse::AppendEntries {
                term: 4,
                success: true,
                match_index: 1,
                ..
            }
        ));

        let hard_state = raft.log.read_hard_state().expect("read hard state");
        assert_eq!(hard_state.current_term, 4);
        assert_eq!(hard_state.voted_for, None);
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 4);
            assert_eq!(runtime.voted_for, None);
            assert_eq!(runtime.role, RaftRole::Follower);
            assert_eq!(runtime.leader_id.as_deref(), Some("n3"));
        }
        assert_eq!(raft.log.last_index(), 1);
        assert_eq!(raft.log.last_term(), 1);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn append_entries_commit_advance_does_not_apply_when_hard_state_write_fails() {
        let dir = temp_dir("raft-append-commit-hard-state-fails");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        raft.log.append(1, RaftCommand::Noop).expect("append seed");
        let initial_state = RaftHardState {
            current_term: 3,
            voted_for: Some("n1".into()),
            commit_index: 0,
        };
        raft.log
            .write_hard_state(&initial_state)
            .expect("persist initial hard state");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.voted_for = Some("n1".into());
            runtime.commit_index = 0;
            runtime.last_applied = 0;
        }
        fs::create_dir_all(dir.join(HARD_STATE_FILE).with_extension("json.tmp"))
            .expect("block hard-state temp path");

        let response = raft.handle_append_entries(3, "n2".into(), 1, 1, Vec::new(), 1);

        assert!(matches!(response, RaftRpcResponse::Error { term: 3, .. }));
        assert_eq!(
            raft.log.read_hard_state().expect("read hard state"),
            initial_state
        );
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.commit_index, 0);
            assert_eq!(runtime.last_applied, 0);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn append_entries_rejects_malformed_non_contiguous_batches() {
        let dir = temp_dir("raft-append-malformed-gap");
        let store = RaftLogStore::open(&dir).expect("open store");
        store.append(1, RaftCommand::Noop).expect("append seed");

        let err = store
            .append_entries_from_leader(
                1,
                1,
                2,
                0,
                vec![RaftLogEntry {
                    index: 3,
                    term: 2,
                    created_at_ms: 10,
                    command: RaftCommand::Noop,
                }],
            )
            .expect_err("gap must be rejected");
        assert!(matches!(err, BrokerRaftError::InvalidAppendEntries(_)));
        assert_eq!(store.read_entries().expect("entries").len(), 1);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn append_entries_rejects_entries_newer_than_leader_term() {
        let dir = temp_dir("raft-append-malformed-term");
        let store = RaftLogStore::open(&dir).expect("open store");

        let err = store
            .append_entries_from_leader(
                0,
                0,
                2,
                0,
                vec![RaftLogEntry {
                    index: 1,
                    term: 3,
                    created_at_ms: 10,
                    command: RaftCommand::Noop,
                }],
            )
            .expect_err("future-term entry must be rejected");
        assert!(matches!(err, BrokerRaftError::InvalidAppendEntries(_)));
        assert!(store.read_entries().expect("entries").is_empty());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn append_entries_rejects_term_regression_inside_batch() {
        let dir = temp_dir("raft-append-term-regression");
        let store = RaftLogStore::open(&dir).expect("open store");
        store.append(3, RaftCommand::Noop).expect("append seed");

        let err = store
            .append_entries_from_leader(
                1,
                3,
                4,
                0,
                vec![RaftLogEntry {
                    index: 2,
                    term: 2,
                    created_at_ms: 10,
                    command: RaftCommand::Noop,
                }],
            )
            .expect_err("batch term regression must be rejected");

        assert!(matches!(err, BrokerRaftError::InvalidAppendEntries(_)));
        assert_eq!(
            store
                .read_entries()
                .expect("entries")
                .iter()
                .map(|entry| (entry.index, entry.term))
                .collect::<Vec<_>>(),
            vec![(1, 3)]
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn append_batch_rejects_local_term_regression() {
        let dir = temp_dir("raft-local-term-regression");
        let store = RaftLogStore::open(&dir).expect("open store");
        store.append(3, RaftCommand::Noop).expect("append seed");

        let err = store
            .append(2, RaftCommand::Noop)
            .expect_err("local term regression must be rejected");

        assert!(matches!(err, BrokerRaftError::InvalidAppendEntries(_)));
        assert_eq!(
            store
                .read_entries()
                .expect("entries")
                .iter()
                .map(|entry| (entry.index, entry.term))
                .collect::<Vec<_>>(),
            vec![(1, 3)]
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn append_batch_rejects_invalid_membership_command() {
        let dir = temp_dir("raft-local-invalid-membership");
        let store = RaftLogStore::open(&dir).expect("open store");

        let err = store
            .append(
                1,
                RaftCommand::SetMembership {
                    membership: duplicate_peer_membership(),
                },
            )
            .expect_err("invalid local membership command must be rejected");

        assert!(matches!(err, BrokerRaftError::InvalidConfig(_)));
        assert_eq!(store.last_index(), 0);
        assert!(store.read_entries().expect("entries").is_empty());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn append_entries_rejects_invalid_membership_command_before_writing() {
        let dir = temp_dir("raft-append-invalid-membership");
        let store = RaftLogStore::open(&dir).expect("open store");

        let err = store
            .append_entries_from_leader(
                0,
                0,
                1,
                0,
                vec![RaftLogEntry {
                    index: 1,
                    term: 1,
                    created_at_ms: 10,
                    command: RaftCommand::SetMembership {
                        membership: even_peer_membership(),
                    },
                }],
            )
            .expect_err("invalid replicated membership command must be rejected");

        assert!(matches!(err, BrokerRaftError::InvalidAppendEntries(_)));
        assert_eq!(store.last_index(), 0);
        assert!(store.read_entries().expect("entries").is_empty());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn persisted_log_rejects_gapped_indexes_on_open() {
        let dir = temp_dir("raft-log-gap-on-open");
        write_raw_log(
            &dir,
            &[
                RaftLogEntry {
                    index: 1,
                    term: 1,
                    created_at_ms: 10,
                    command: RaftCommand::Noop,
                },
                RaftLogEntry {
                    index: 3,
                    term: 1,
                    created_at_ms: 11,
                    command: RaftCommand::Noop,
                },
            ],
        );

        let err = RaftLogStore::open(&dir).expect_err("gapped log must be rejected");
        assert!(matches!(err, BrokerRaftError::InvalidLog(_)));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn persisted_log_rejects_decreasing_terms_on_open() {
        let dir = temp_dir("raft-log-term-regression-on-open");
        write_raw_log(
            &dir,
            &[
                RaftLogEntry {
                    index: 1,
                    term: 3,
                    created_at_ms: 10,
                    command: RaftCommand::Noop,
                },
                RaftLogEntry {
                    index: 2,
                    term: 2,
                    created_at_ms: 11,
                    command: RaftCommand::Noop,
                },
            ],
        );

        let err = RaftLogStore::open(&dir).expect_err("decreasing terms must be rejected");
        assert!(matches!(err, BrokerRaftError::InvalidLog(_)));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn persisted_log_rejects_invalid_membership_command_on_open() {
        let dir = temp_dir("raft-log-invalid-membership-on-open");
        write_raw_log(
            &dir,
            &[RaftLogEntry {
                index: 1,
                term: 1,
                created_at_ms: 10,
                command: RaftCommand::SetMembership {
                    membership: duplicate_peer_membership(),
                },
            }],
        );

        let err = RaftLogStore::open(&dir).expect_err("invalid membership must be rejected");
        assert!(matches!(err, BrokerRaftError::InvalidLog(_)));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn persisted_log_rejects_suffix_without_snapshot_on_open() {
        let dir = temp_dir("raft-log-suffix-without-snapshot");
        write_raw_log(&dir, &[noop_entry(2, 1)]);

        let err = RaftLogStore::open(&dir).expect_err("suffix without snapshot must be rejected");
        assert!(matches!(err, BrokerRaftError::InvalidLog(_)));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn persisted_log_rejects_suffix_gap_after_snapshot_on_open() {
        let dir = temp_dir("raft-log-snapshot-gap-on-open");
        let store = RaftLogStore::open(&dir).expect("open store");
        store
            .write_snapshot(6, 1, idle_snapshot_payload())
            .expect("write snapshot");
        drop(store);
        write_raw_log(&dir, &[noop_entry(8, 1)]);

        let err = RaftLogStore::open(&dir).expect_err("snapshot suffix gap must be rejected");
        assert!(matches!(err, BrokerRaftError::InvalidLog(_)));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn persisted_log_recovers_snapshot_term_mismatch_on_open() {
        let dir = temp_dir("raft-log-snapshot-term-mismatch-recovery");
        let store = RaftLogStore::open(&dir).expect("open store");
        store
            .write_snapshot(6, 2, idle_snapshot_payload())
            .expect("write snapshot");
        drop(store);
        write_raw_log(&dir, &[noop_entry(5, 1), noop_entry(6, 1)]);

        let reopened =
            RaftLogStore::open(&dir).expect("snapshot term mismatch should be recovered");
        assert!(reopened
            .read_entries()
            .expect("reconciled entries")
            .is_empty());
        assert_eq!(reopened.last_index(), 6);
        assert_eq!(reopened.last_term(), 2);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn persisted_log_discards_unmatched_suffix_after_snapshot_crash() {
        let dir = temp_dir("raft-log-snapshot-crash-recovery");
        write_raw_log(
            &dir,
            &[
                noop_entry(1, 1),
                noop_entry(2, 1),
                noop_entry(3, 2),
                noop_entry(4, 2),
            ],
        );
        let store = RaftLogStore::open(&dir).expect("open store");
        store
            .write_snapshot(3, 9, idle_snapshot_payload())
            .expect("write conflicting snapshot before simulated crash");
        drop(store);

        let reopened = RaftLogStore::open(&dir)
            .expect("snapshot install crash window should discard unprovable suffix");
        assert!(reopened.read_entries().expect("entries").is_empty());
        assert_eq!(reopened.last_index(), 3);
        assert_eq!(reopened.last_term(), 9);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn failed_snapshot_install_after_snapshot_rename_is_recoverable() {
        let dir = temp_dir("raft-log-snapshot-install-rewrite-failure");
        let store = RaftLogStore::open(&dir).expect("open store");
        for term in [1, 1, 2, 2] {
            store.append(term, RaftCommand::Noop).expect("append");
        }
        fs::create_dir_all(dir.join(LOG_FILE).with_extension("ndjson.tmp"))
            .expect("block log rewrite temp path");

        let payload = idle_snapshot_payload();
        let err = store
            .install_snapshot_from_leader(3, 9, Some(payload_checksum(&payload)), payload)
            .expect_err("blocked log rewrite should fail after snapshot rename");
        assert!(matches!(err, BrokerRaftError::Io(_)));
        assert_eq!(
            store
                .latest_snapshot()
                .map(|snapshot| snapshot.last_included_index),
            None,
            "in-memory state must not advance when the full install fails"
        );
        assert_eq!(store.last_index(), 4);
        assert_eq!(
            read_snapshot_file(&dir.join(SNAPSHOT_FILE))
                .expect("read snapshot file")
                .expect("snapshot was durably renamed")
                .metadata
                .last_included_index,
            3
        );
        drop(store);
        fs::remove_dir_all(dir.join(LOG_FILE).with_extension("ndjson.tmp"))
            .expect("unblock log rewrite temp path");

        let reopened =
            RaftLogStore::open(&dir).expect("restart should recover from snapshot/log mismatch");
        assert!(reopened.read_entries().expect("entries").is_empty());
        assert_eq!(reopened.last_index(), 3);
        assert_eq!(reopened.last_term(), 9);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn replace_all_rejects_suffix_gap_after_snapshot() {
        let dir = temp_dir("raft-replace-snapshot-gap");
        let store = RaftLogStore::open(&dir).expect("open store");
        store
            .write_snapshot(6, 1, idle_snapshot_payload())
            .expect("write snapshot");

        let err = store
            .replace_all(&[noop_entry(8, 1)])
            .expect_err("replace_all must preserve snapshot/log boundary");
        assert!(matches!(err, BrokerRaftError::InvalidLog(_)));
        assert!(store.read_entries().expect("entries").is_empty());
        assert_eq!(store.last_index(), 6);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn append_entries_truncates_conflicting_suffix_and_appends_leader_entries() {
        let dir = temp_dir("raft-conflict-repair");
        let store = RaftLogStore::open(&dir).expect("open store");
        for term in [1, 1, 3, 3] {
            store.append(term, RaftCommand::Noop).expect("append");
        }
        let leader_entries = vec![
            RaftLogEntry {
                index: 3,
                term: 2,
                created_at_ms: 10,
                command: RaftCommand::Noop,
            },
            RaftLogEntry {
                index: 4,
                term: 2,
                created_at_ms: 11,
                command: RaftCommand::Noop,
            },
        ];

        let report = store
            .append_entries_from_leader(2, 1, 2, 0, leader_entries)
            .expect("append entries repair");

        assert!(report.success);
        assert_eq!(report.match_index, 4);
        assert_eq!(
            store
                .read_entries()
                .expect("entries")
                .iter()
                .map(|entry| (entry.index, entry.term))
                .collect::<Vec<_>>(),
            vec![(1, 1), (2, 1), (3, 2), (4, 2)]
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn append_batch_assigns_contiguous_indexes_and_terms() {
        let dir = temp_dir("raft-append-batch");
        let store = RaftLogStore::open(&dir).expect("open store");
        store.append(1, RaftCommand::Noop).expect("seed append");

        let entries = store
            .append_batch(
                2,
                vec![RaftCommand::Noop, RaftCommand::Noop, RaftCommand::Noop],
            )
            .expect("append batch");

        assert_eq!(
            entries
                .iter()
                .map(|entry| (entry.index, entry.term))
                .collect::<Vec<_>>(),
            vec![(2, 2), (3, 2), (4, 2)]
        );
        assert_eq!(store.last_index(), 4);
        assert_eq!(store.last_term(), 2);
        assert_eq!(
            store
                .read_entries()
                .expect("entries")
                .iter()
                .map(|entry| entry.index)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );

        let large_batch = (0..128).map(|_| RaftCommand::Noop).collect::<Vec<_>>();
        store
            .append_batch(2, large_batch)
            .expect("append large buffered batch");
        drop(store);
        let reopened = RaftLogStore::open(&dir).expect("reopen buffered batch log");
        assert_eq!(reopened.last_index(), 132);
        assert_eq!(reopened.last_term(), 2);
        assert_eq!(
            reopened
                .read_entries()
                .expect("reopened entries")
                .iter()
                .map(|entry| entry.index)
                .collect::<Vec<_>>(),
            (1..=132).collect::<Vec<_>>()
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn local_append_creates_missing_log_file_through_append_helper() {
        let dir = temp_dir("raft-local-append-missing-log");
        let store = RaftLogStore::open(&dir).expect("open store");
        let log_path = dir.join(LOG_FILE);
        assert!(!log_path.exists());

        let entry = store.append(1, RaftCommand::Noop).expect("append");

        assert_eq!(entry.index, 1);
        assert!(log_path.exists());
        drop(store);
        let reopened = RaftLogStore::open(&dir).expect("reopen store");
        assert_eq!(
            reopened
                .read_entries()
                .expect("entries")
                .iter()
                .map(|entry| (entry.index, entry.term))
                .collect::<Vec<_>>(),
            vec![(1, 1)]
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn follower_append_entries_creates_missing_log_file_through_append_helper() {
        let dir = temp_dir("raft-follower-append-missing-log");
        let store = RaftLogStore::open(&dir).expect("open store");
        let log_path = dir.join(LOG_FILE);
        assert!(!log_path.exists());

        let report = store
            .append_entries_from_leader(
                0,
                0,
                3,
                0,
                vec![RaftLogEntry {
                    index: 1,
                    term: 3,
                    created_at_ms: 10,
                    command: RaftCommand::Noop,
                }],
            )
            .expect("append entries from leader");

        assert!(report.success);
        assert_eq!(report.match_index, 1);
        assert!(log_path.exists());
        drop(store);
        let reopened = RaftLogStore::open(&dir).expect("reopen store");
        assert_eq!(
            reopened
                .read_entries()
                .expect("entries")
                .iter()
                .map(|entry| (entry.index, entry.term))
                .collect::<Vec<_>>(),
            vec![(1, 3)]
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn append_entries_heartbeat_uses_retained_state_without_full_log_scan() {
        let dir = temp_dir("raft-append-heartbeat-no-scan");
        let store = RaftLogStore::open(&dir).expect("open store");
        store.append(1, RaftCommand::Noop).expect("append one");
        store.append(1, RaftCommand::Noop).expect("append two");
        append_raw_log_line(&dir, "{not valid json");

        let report = store
            .append_entries_from_leader(2, 1, 2, 0, Vec::new())
            .expect("heartbeat should use retained state");

        assert!(report.success);
        assert_eq!(report.match_index, 2);
        assert_eq!(store.last_index(), 2);
        assert!(
            store.read_entries().is_err(),
            "full log reads should still detect the malformed appended line"
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn append_entries_append_only_uses_retained_state_without_full_log_scan() {
        let dir = temp_dir("raft-append-only-no-scan");
        let store = RaftLogStore::open(&dir).expect("open store");
        store.append(1, RaftCommand::Noop).expect("append one");
        append_raw_log_line(&dir, "{not valid json");

        let report = store
            .append_entries_from_leader(
                1,
                1,
                2,
                0,
                vec![RaftLogEntry {
                    index: 2,
                    term: 2,
                    created_at_ms: 10,
                    command: RaftCommand::Noop,
                }],
            )
            .expect("append-only catch-up should not scan the whole log");

        assert!(report.success);
        assert_eq!(report.match_index, 2);
        assert_eq!(store.last_index(), 2);
        assert_eq!(store.term_at(2).expect("new appended term"), Some(2));
        assert!(
            store.read_entries().is_err(),
            "full log reads should still detect the malformed appended line"
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn append_entries_matching_entries_use_retained_state_without_full_log_scan() {
        let dir = temp_dir("raft-matching-append-no-scan");
        let store = RaftLogStore::open(&dir).expect("open store");
        store.append(1, RaftCommand::Noop).expect("append one");
        append_raw_log_line(&dir, "{not valid json");

        let report = store
            .append_entries_from_leader(
                0,
                0,
                1,
                0,
                vec![RaftLogEntry {
                    index: 1,
                    term: 1,
                    created_at_ms: 10,
                    command: RaftCommand::Noop,
                }],
            )
            .expect("matching entries should not scan the whole log");

        assert!(report.success);
        assert_eq!(report.match_index, 1);
        assert_eq!(store.last_index(), 1);
        assert!(
            store.read_entries().is_err(),
            "full log reads should still detect the malformed appended line"
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn entries_from_limited_respects_count_and_byte_caps() {
        let dir = temp_dir("raft-limited-entries");
        let store = RaftLogStore::open(&dir).expect("open store");
        for _ in 0..5 {
            store.append(1, RaftCommand::Noop).expect("append");
        }

        let by_count = store
            .entries_from_limited(2, 2, usize::MAX)
            .expect("limited by count");
        assert_eq!(
            by_count.iter().map(|entry| entry.index).collect::<Vec<_>>(),
            vec![2, 3]
        );

        let first_size = serde_json::to_vec(&store.read_entries().expect("entries")[0])
            .expect("serialize entry")
            .len();
        let by_bytes = store
            .entries_from_limited(1, 5, first_size)
            .expect("limited by bytes");
        assert_eq!(by_bytes.len(), 1);
        assert_eq!(by_bytes[0].index, 1);

        let oversized_first = store
            .entries_from_limited(1, 5, 1)
            .expect("first entry still makes progress");
        assert_eq!(oversized_first.len(), 1);
        assert_eq!(oversized_first[0].index, 1);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn entries_from_limited_stops_before_unneeded_tail() {
        let dir = temp_dir("raft-limited-early-stop");
        let store = RaftLogStore::open(&dir).expect("open store");
        for _ in 0..3 {
            store.append(1, RaftCommand::Noop).expect("append");
        }
        append_raw_log_line(&dir, "{not valid json");

        let limited = store
            .entries_from_limited(1, 1, usize::MAX)
            .expect("bounded read should not parse beyond first selected entry");
        assert_eq!(
            limited.iter().map(|entry| entry.index).collect::<Vec<_>>(),
            vec![1]
        );
        assert!(
            store.read_entries().is_err(),
            "full reads should still detect the malformed tail"
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn entries_range_stops_at_end_index_before_unneeded_tail() {
        let dir = temp_dir("raft-range-early-stop");
        let store = RaftLogStore::open(&dir).expect("open store");
        for _ in 0..3 {
            store.append(1, RaftCommand::Noop).expect("append");
        }
        append_raw_log_line(&dir, "{not valid json");

        let range = store
            .entries_range(1, 2)
            .expect("range read should stop at end index");
        assert_eq!(
            range.iter().map(|entry| entry.index).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(
            store.read_entries().is_err(),
            "full reads should still detect the malformed tail"
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn term_at_uses_retained_cache_without_full_log_scan() {
        let dir = temp_dir("raft-term-at-cache");
        let store = RaftLogStore::open(&dir).expect("open store");
        store.append(1, RaftCommand::Noop).expect("append one");
        store.append(2, RaftCommand::Noop).expect("append two");
        append_raw_log_line(&dir, "{not valid json");

        assert_eq!(
            store
                .term_at(2)
                .expect("term lookup should use retained cache"),
            Some(2)
        );
        assert!(
            store.read_entries().is_err(),
            "full reads should still detect the malformed tail"
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn prev_term_and_entries_limited_uses_retained_cache_without_full_log_scan() {
        let dir = temp_dir("raft-prev-term-cache");
        let store = RaftLogStore::open(&dir).expect("open store");
        for term in [1, 2, 2] {
            store.append(term, RaftCommand::Noop).expect("append");
        }
        append_raw_log_line(&dir, "{not valid json");

        let (prev_term, entries) = store
            .prev_term_and_entries_from_limited(2, 2, usize::MAX)
            .expect("cached prev term read")
            .expect("prev term should exist");
        assert_eq!(prev_term, 1);
        assert_eq!(
            entries
                .iter()
                .map(|entry| (entry.index, entry.term))
                .collect::<Vec<_>>(),
            vec![(2, 2), (3, 2)]
        );
        assert!(
            store.read_entries().is_err(),
            "full reads should still detect the malformed tail"
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn lagging_peer_catches_up_over_bounded_append_batches() {
        let dir = temp_dir("raft-bounded-catchup");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake peer");
        let peer_addr = listener.local_addr().expect("fake peer addr");
        let unused_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind unused peer addr");
        let unused_addr = unused_listener.local_addr().expect("unused peer addr");
        drop(unused_listener);

        let mut cfg = test_raft_config(dir.clone());
        cfg.heartbeat_interval = Duration::from_millis(10);
        cfg.election_timeout_min = Duration::from_millis(500);
        cfg.election_timeout_max = Duration::from_millis(1_000);
        cfg.append_entries_max_entries = 2;
        cfg.append_entries_max_bytes = usize::MAX;
        cfg.peers = vec![
            test_peer("n1", 7980),
            RaftPeerConfig {
                id: "n2".into(),
                addr: peer_addr.to_string(),
            },
            RaftPeerConfig {
                id: "n3".into(),
                addr: unused_addr.to_string(),
            },
        ];
        let raft = BrokerRaft::open(cfg).expect("open raft");
        for _ in 0..5 {
            raft.log.append(1, RaftCommand::Noop).expect("append");
        }
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
            runtime.leader_progress.insert(
                "n3".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
        }

        let batches = Arc::new(Mutex::new(Vec::<Vec<u64>>::new()));
        let batches_for_server = Arc::clone(&batches);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept fake peer");
            let mut reader = TokioBufReader::new(stream);
            for _ in 0..3 {
                let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                    .await
                    .expect("read append entries");
                let rpc: RaftRpc = serde_json::from_str(&line).expect("parse append entries");
                let (term, match_index, batch_indexes) = match rpc {
                    RaftRpc::AppendEntries {
                        term,
                        prev_log_index,
                        entries,
                        ..
                    } => {
                        let batch_indexes =
                            entries.iter().map(|entry| entry.index).collect::<Vec<_>>();
                        let match_index = entries
                            .last()
                            .map(|entry| entry.index)
                            .unwrap_or(prev_log_index);
                        (term, match_index, batch_indexes)
                    }
                    other => panic!("unexpected rpc: {other:?}"),
                };
                batches_for_server.lock().push(batch_indexes);
                let response = RaftRpcResponse::AppendEntries {
                    term,
                    success: true,
                    match_index,
                    conflict_index: None,
                    conflict_term: None,
                };
                let body = serde_json::to_vec(&response).expect("serialize append response");
                reader
                    .get_mut()
                    .write_all(&body)
                    .await
                    .expect("write append response");
                reader
                    .get_mut()
                    .write_all(b"\n")
                    .await
                    .expect("write append newline");
                reader
                    .get_mut()
                    .flush()
                    .await
                    .expect("flush append response");
            }
        });

        let acks = raft
            .replicate_until_quorum(5)
            .await
            .expect("replicate to bounded quorum");
        assert!(acks.contains("n1"));
        assert!(acks.contains("n2"));
        server.await.expect("fake peer server");
        assert_eq!(
            batches.lock().clone(),
            vec![vec![1, 2], vec![3, 4], vec![5]]
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn replicate_until_quorum_retries_immediately_after_progress_change() {
        let dir = temp_dir("raft-progress-retry-catchup");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake peer");
        let peer_addr = listener.local_addr().expect("fake peer addr");
        let unused_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind unused peer addr");
        let unused_addr = unused_listener.local_addr().expect("unused peer addr");
        drop(unused_listener);

        let mut cfg = test_raft_config(dir.clone());
        cfg.heartbeat_interval = Duration::from_secs(5);
        cfg.election_timeout_min = Duration::from_secs(10);
        cfg.election_timeout_max = Duration::from_secs(20);
        cfg.append_entries_max_entries = 2;
        cfg.append_entries_max_bytes = usize::MAX;
        cfg.peers = vec![
            test_peer("n1", 7980),
            RaftPeerConfig {
                id: "n2".into(),
                addr: peer_addr.to_string(),
            },
            RaftPeerConfig {
                id: "n3".into(),
                addr: unused_addr.to_string(),
            },
        ];
        let raft = BrokerRaft::open(cfg).expect("open raft");
        for _ in 0..5 {
            raft.log.append(1, RaftCommand::Noop).expect("append");
        }
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
            runtime.leader_progress.insert(
                "n3".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
        }

        let batches = Arc::new(Mutex::new(Vec::<Vec<u64>>::new()));
        let batches_for_server = Arc::clone(&batches);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept fake peer");
            let mut reader = TokioBufReader::new(stream);
            for _ in 0..3 {
                let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                    .await
                    .expect("read append entries");
                let rpc: RaftRpc = serde_json::from_str(&line).expect("parse append entries");
                let (term, match_index, batch_indexes) = match rpc {
                    RaftRpc::AppendEntries {
                        term,
                        prev_log_index,
                        entries,
                        ..
                    } => {
                        let batch_indexes =
                            entries.iter().map(|entry| entry.index).collect::<Vec<_>>();
                        let match_index = entries
                            .last()
                            .map(|entry| entry.index)
                            .unwrap_or(prev_log_index);
                        (term, match_index, batch_indexes)
                    }
                    other => panic!("unexpected rpc: {other:?}"),
                };
                batches_for_server.lock().push(batch_indexes);
                let response = RaftRpcResponse::AppendEntries {
                    term,
                    success: true,
                    match_index,
                    conflict_index: None,
                    conflict_term: None,
                };
                let body = serde_json::to_vec(&response).expect("serialize append response");
                reader
                    .get_mut()
                    .write_all(&body)
                    .await
                    .expect("write append response");
                reader
                    .get_mut()
                    .write_all(b"\n")
                    .await
                    .expect("write append newline");
                reader
                    .get_mut()
                    .flush()
                    .await
                    .expect("flush append response");
            }
        });

        let acks = tokio::time::timeout(Duration::from_secs(1), raft.replicate_until_quorum(5))
            .await
            .expect("progress changes should retry before heartbeat delay")
            .expect("replicate to bounded quorum");
        assert!(acks.contains("n1"));
        assert!(acks.contains("n2"));
        server.await.expect("fake peer server");
        assert_eq!(
            batches.lock().clone(),
            vec![vec![1, 2], vec![3, 4], vec![5]]
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn learner_catchup_retries_immediately_after_progress_change() {
        let dir = temp_dir("raft-learner-progress-retry-catchup");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake learner");
        let peer_addr = listener.local_addr().expect("fake learner addr");
        let mut cfg = test_raft_config(dir.clone());
        cfg.heartbeat_interval = Duration::from_secs(5);
        cfg.election_timeout_min = Duration::from_secs(10);
        cfg.election_timeout_max = Duration::from_secs(20);
        cfg.append_entries_max_entries = 2;
        cfg.append_entries_max_bytes = usize::MAX;
        let learner = RaftPeerConfig {
            id: "n4".into(),
            addr: peer_addr.to_string(),
        };
        let raft = BrokerRaft::open(cfg).expect("open raft");
        for _ in 0..5 {
            raft.log.append(1, RaftCommand::Noop).expect("append");
        }
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime
                .staged_learners
                .insert(learner.id.clone(), learner.clone());
            runtime.leader_progress.insert(
                learner.id.clone(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
        }

        let batches = Arc::new(Mutex::new(Vec::<Vec<u64>>::new()));
        let batches_for_server = Arc::clone(&batches);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept fake learner");
            let mut reader = TokioBufReader::new(stream);
            for _ in 0..3 {
                let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                    .await
                    .expect("read learner append entries");
                let rpc: RaftRpc = serde_json::from_str(&line).expect("parse append entries");
                let (term, match_index, batch_indexes) = match rpc {
                    RaftRpc::AppendEntries {
                        term,
                        prev_log_index,
                        entries,
                        ..
                    } => {
                        let batch_indexes =
                            entries.iter().map(|entry| entry.index).collect::<Vec<_>>();
                        let match_index = entries
                            .last()
                            .map(|entry| entry.index)
                            .unwrap_or(prev_log_index);
                        (term, match_index, batch_indexes)
                    }
                    other => panic!("unexpected rpc: {other:?}"),
                };
                batches_for_server.lock().push(batch_indexes);
                let response = RaftRpcResponse::AppendEntries {
                    term,
                    success: true,
                    match_index,
                    conflict_index: None,
                    conflict_term: None,
                };
                let body = serde_json::to_vec(&response).expect("serialize append response");
                reader
                    .get_mut()
                    .write_all(&body)
                    .await
                    .expect("write append response");
                reader
                    .get_mut()
                    .write_all(b"\n")
                    .await
                    .expect("write append newline");
                reader
                    .get_mut()
                    .flush()
                    .await
                    .expect("flush append response");
            }
        });

        tokio::time::timeout(
            Duration::from_secs(1),
            raft.catch_up_learner_peer(learner, 5),
        )
        .await
        .expect("progress changes should retry before learner heartbeat delay")
        .expect("catch up learner");
        server.await.expect("fake learner server");
        assert_eq!(
            batches.lock().clone(),
            vec![vec![1, 2], vec![3, 4], vec![5]]
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn leader_progress_generation_advances_when_append_ack_updates_progress() {
        let dir = temp_dir("raft-progress-generation-advances");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake peer");
        let peer_addr = listener.local_addr().expect("fake peer addr");
        let mut cfg = test_raft_config(dir.clone());
        cfg.peers = vec![
            test_peer("n1", 7980),
            RaftPeerConfig {
                id: "n2".into(),
                addr: peer_addr.to_string(),
            },
            test_peer("n3", 7982),
        ];
        let peer = cfg.peers[1].clone();
        let raft = BrokerRaft::open(cfg).expect("open raft");
        raft.log.append(1, RaftCommand::Noop).expect("append one");
        raft.log.append(1, RaftCommand::Noop).expect("append two");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
        }
        let before = raft.leader_progress_generation();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept fake peer");
            let mut reader = TokioBufReader::new(stream);
            let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                .await
                .expect("read append entries");
            let rpc: RaftRpc = serde_json::from_str(&line).expect("parse append entries");
            let term = match rpc {
                RaftRpc::AppendEntries {
                    term,
                    prev_log_index,
                    entries,
                    ..
                } => {
                    assert_eq!(prev_log_index, 0);
                    assert_eq!(
                        entries.iter().map(|entry| entry.index).collect::<Vec<_>>(),
                        vec![1, 2]
                    );
                    term
                }
                other => panic!("unexpected rpc: {other:?}"),
            };
            let body = serde_json::to_vec(&RaftRpcResponse::AppendEntries {
                term,
                success: true,
                match_index: 2,
                conflict_index: None,
                conflict_term: None,
            })
            .expect("serialize append response");
            reader
                .get_mut()
                .write_all(&body)
                .await
                .expect("write append response");
            reader
                .get_mut()
                .write_all(b"\n")
                .await
                .expect("write append newline");
            reader
                .get_mut()
                .flush()
                .await
                .expect("flush append response");
        });

        let outcome = raft
            .replicate_to_peer(peer, 1, 0, Some(2))
            .await
            .expect("replicate to peer");
        assert!(outcome.target_reached);
        assert!(raft.leader_progress_generation() > before);
        assert_eq!(raft.telemetry_snapshot().append_progress_updates_total, 1);
        server.await.expect("fake peer server");

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn leader_progress_generation_does_not_advance_for_duplicate_ack() {
        let dir = temp_dir("raft-progress-generation-duplicate-ack");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake peer");
        let peer_addr = listener.local_addr().expect("fake peer addr");
        let mut cfg = test_raft_config(dir.clone());
        cfg.peers = vec![
            test_peer("n1", 7980),
            RaftPeerConfig {
                id: "n2".into(),
                addr: peer_addr.to_string(),
            },
            test_peer("n3", 7982),
        ];
        let peer = cfg.peers[1].clone();
        let raft = BrokerRaft::open(cfg).expect("open raft");
        for _ in 0..5 {
            raft.log.append(1, RaftCommand::Noop).expect("append");
        }
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 6,
                    match_index: 5,
                },
            );
        }
        let before = raft.leader_progress_generation();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept fake peer");
            let mut reader = TokioBufReader::new(stream);
            let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                .await
                .expect("read append entries");
            let rpc: RaftRpc = serde_json::from_str(&line).expect("parse append entries");
            let term = match rpc {
                RaftRpc::AppendEntries {
                    term,
                    prev_log_index,
                    entries,
                    ..
                } => {
                    assert_eq!(prev_log_index, 5);
                    assert!(entries.is_empty());
                    term
                }
                other => panic!("unexpected rpc: {other:?}"),
            };
            let body = serde_json::to_vec(&RaftRpcResponse::AppendEntries {
                term,
                success: true,
                match_index: 5,
                conflict_index: None,
                conflict_term: None,
            })
            .expect("serialize append response");
            reader
                .get_mut()
                .write_all(&body)
                .await
                .expect("write append response");
            reader
                .get_mut()
                .write_all(b"\n")
                .await
                .expect("write append newline");
            reader
                .get_mut()
                .flush()
                .await
                .expect("flush append response");
        });

        let outcome = raft
            .replicate_to_peer(peer, 1, 0, Some(5))
            .await
            .expect("replicate duplicate ack");
        assert!(outcome.target_reached);
        assert_eq!(raft.leader_progress_generation(), before);
        server.await.expect("fake peer server");

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn append_conflict_does_not_rewind_next_index_below_known_match_index() {
        let dir = temp_dir("raft-conflict-does-not-rewind-known-match");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake peer");
        let peer_addr = listener.local_addr().expect("fake peer addr");
        let mut cfg = test_raft_config(dir.clone());
        cfg.peers = vec![
            test_peer("n1", 7980),
            RaftPeerConfig {
                id: "n2".into(),
                addr: peer_addr.to_string(),
            },
            test_peer("n3", 7982),
        ];
        let peer = cfg.peers[1].clone();
        let raft = BrokerRaft::open(cfg).expect("open raft");
        for _ in 0..5 {
            raft.log.append(1, RaftCommand::Noop).expect("append");
        }
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 6,
                    match_index: 5,
                },
            );
        }
        let before = raft.leader_progress_generation();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept fake peer");
            let mut reader = TokioBufReader::new(stream);
            let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                .await
                .expect("read append entries");
            let rpc: RaftRpc = serde_json::from_str(&line).expect("parse append entries");
            let term = match rpc {
                RaftRpc::AppendEntries {
                    term,
                    prev_log_index,
                    entries,
                    ..
                } => {
                    assert_eq!(prev_log_index, 5);
                    assert!(entries.is_empty());
                    term
                }
                other => panic!("unexpected rpc: {other:?}"),
            };
            let response = RaftRpcResponse::AppendEntries {
                term,
                success: false,
                match_index: 0,
                conflict_index: Some(1),
                conflict_term: None,
            };
            let body = serde_json::to_vec(&response).expect("serialize append response");
            reader
                .get_mut()
                .write_all(&body)
                .await
                .expect("write append response");
            reader
                .get_mut()
                .write_all(b"\n")
                .await
                .expect("write append newline");
            reader
                .get_mut()
                .flush()
                .await
                .expect("flush append response");
        });

        let outcome = raft
            .replicate_to_peer(peer, 1, 0, Some(5))
            .await
            .expect("replicate conflict");
        assert!(outcome.contacted);
        assert!(!outcome.target_reached);
        assert_eq!(raft.leader_progress_generation(), before);
        let telemetry = raft.telemetry_snapshot();
        assert_eq!(telemetry.append_progress_updates_total, 0);
        assert_eq!(telemetry.append_conflict_repairs_total, 1);
        assert_eq!(telemetry.append_conflict_clamps_total, 1);
        let metrics = raft.raft_metrics_text();
        assert!(metrics.contains("dd_rust_network_mutex_raft_append_conflict_repairs_total 1"));
        assert!(metrics.contains("dd_rust_network_mutex_raft_append_conflict_clamps_total 1"));
        server.await.expect("fake peer server");
        let progress = raft
            .runtime
            .lock()
            .leader_progress
            .get("n2")
            .copied()
            .expect("progress");
        assert_eq!(progress.match_index, 5);
        assert_eq!(progress.next_index, 6);

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn append_success_underreporting_prev_log_index_does_not_count_target_ack() {
        let dir = temp_dir("raft-append-success-underreports-prev");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake peer");
        let peer_addr = listener.local_addr().expect("fake peer addr");
        let mut cfg = test_raft_config(dir.clone());
        cfg.peers = vec![
            test_peer("n1", 7980),
            RaftPeerConfig {
                id: "n2".into(),
                addr: peer_addr.to_string(),
            },
            test_peer("n3", 7982),
        ];
        let peer = cfg.peers[1].clone();
        let raft = BrokerRaft::open(cfg).expect("open raft");
        for _ in 0..5 {
            raft.log.append(1, RaftCommand::Noop).expect("append");
        }
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 6,
                    match_index: 5,
                },
            );
        }
        let before = raft.leader_progress_generation();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept fake peer");
            let mut reader = TokioBufReader::new(stream);
            let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                .await
                .expect("read append entries");
            let rpc: RaftRpc = serde_json::from_str(&line).expect("parse append entries");
            let term = match rpc {
                RaftRpc::AppendEntries {
                    term,
                    prev_log_index,
                    entries,
                    ..
                } => {
                    assert_eq!(prev_log_index, 5);
                    assert!(entries.is_empty());
                    term
                }
                other => panic!("unexpected rpc: {other:?}"),
            };
            let response = RaftRpcResponse::AppendEntries {
                term,
                success: true,
                match_index: 0,
                conflict_index: None,
                conflict_term: None,
            };
            let body = serde_json::to_vec(&response).expect("serialize append response");
            reader
                .get_mut()
                .write_all(&body)
                .await
                .expect("write append response");
            reader
                .get_mut()
                .write_all(b"\n")
                .await
                .expect("write append newline");
            reader
                .get_mut()
                .flush()
                .await
                .expect("flush append response");
        });

        let outcome = raft
            .replicate_to_peer(peer, 1, 0, Some(5))
            .await
            .expect("replicate underreported success");
        assert!(outcome.contacted);
        assert!(
            !outcome.target_reached,
            "impossible success response must not count as a fresh target ack"
        );
        assert_eq!(raft.leader_progress_generation(), before);
        let telemetry = raft.telemetry_snapshot();
        assert_eq!(telemetry.append_progress_updates_total, 0);
        assert_eq!(telemetry.append_invalid_success_responses_total, 1);
        let metrics = raft.raft_metrics_text();
        assert!(
            metrics.contains("dd_rust_network_mutex_raft_append_invalid_success_responses_total 1")
        );
        server.await.expect("fake peer server");
        let progress = raft
            .runtime
            .lock()
            .leader_progress
            .get("n2")
            .copied()
            .expect("progress");
        assert_eq!(progress.match_index, 5);
        assert_eq!(progress.next_index, 6);

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn replicate_log_once_detaches_non_quorum_peer_after_quorum_return() {
        let dir = temp_dir("raft-quorum-detaches-slow-peer");
        let listener_n2 = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fast peer");
        let addr_n2 = listener_n2.local_addr().expect("fast peer addr");
        let listener_n3 = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind slow peer");
        let addr_n3 = listener_n3.local_addr().expect("slow peer addr");

        let mut cfg = test_raft_config(dir.clone());
        cfg.heartbeat_interval = Duration::from_millis(10);
        cfg.election_timeout_min = Duration::from_millis(50);
        cfg.election_timeout_max = Duration::from_millis(100);
        cfg.append_entries_max_entries = 16;
        cfg.append_entries_max_bytes = usize::MAX;
        cfg.peers = vec![
            test_peer("n1", 7980),
            RaftPeerConfig {
                id: "n2".into(),
                addr: addr_n2.to_string(),
            },
            RaftPeerConfig {
                id: "n3".into(),
                addr: addr_n3.to_string(),
            },
        ];
        let raft = BrokerRaft::open(cfg).expect("open raft");
        raft.log.append(1, RaftCommand::Noop).expect("append");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
            runtime.leader_progress.insert(
                "n3".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
        }

        let (n3_seen_tx, n3_seen_rx) = oneshot::channel();
        let (release_n3_tx, release_n3_rx) = oneshot::channel();
        let server_n2 = tokio::spawn(async move {
            let (stream, _) = listener_n2.accept().await.expect("accept fast peer");
            let mut reader = TokioBufReader::new(stream);
            let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                .await
                .expect("read fast append entries");
            let rpc: RaftRpc = serde_json::from_str(&line).expect("parse fast append entries");
            let (term, match_index) = match rpc {
                RaftRpc::AppendEntries {
                    term,
                    prev_log_index,
                    entries,
                    ..
                } => {
                    assert_eq!(prev_log_index, 0);
                    assert_eq!(
                        entries.iter().map(|entry| entry.index).collect::<Vec<_>>(),
                        vec![1]
                    );
                    (term, 1)
                }
                other => panic!("unexpected rpc: {other:?}"),
            };
            n3_seen_rx.await.expect("slow peer should receive append");
            let response = RaftRpcResponse::AppendEntries {
                term,
                success: true,
                match_index,
                conflict_index: None,
                conflict_term: None,
            };
            let body = serde_json::to_vec(&response).expect("serialize fast response");
            reader
                .get_mut()
                .write_all(&body)
                .await
                .expect("write fast response");
            reader
                .get_mut()
                .write_all(b"\n")
                .await
                .expect("write fast newline");
            reader.get_mut().flush().await.expect("flush fast response");
        });
        let server_n3 = tokio::spawn(async move {
            let (stream, _) = listener_n3.accept().await.expect("accept slow peer");
            let mut reader = TokioBufReader::new(stream);
            let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                .await
                .expect("read slow append entries");
            let rpc: RaftRpc = serde_json::from_str(&line).expect("parse slow append entries");
            let term = match rpc {
                RaftRpc::AppendEntries {
                    term,
                    prev_log_index,
                    entries,
                    ..
                } => {
                    assert_eq!(prev_log_index, 0);
                    assert_eq!(
                        entries.iter().map(|entry| entry.index).collect::<Vec<_>>(),
                        vec![1]
                    );
                    term
                }
                other => panic!("unexpected rpc: {other:?}"),
            };
            let _ = n3_seen_tx.send(());
            release_n3_rx.await.expect("release slow response");
            let response = RaftRpcResponse::AppendEntries {
                term,
                success: true,
                match_index: 1,
                conflict_index: None,
                conflict_term: None,
            };
            let body = serde_json::to_vec(&response).expect("serialize slow response");
            let _ = reader.get_mut().write_all(&body).await;
            let _ = reader.get_mut().write_all(b"\n").await;
            let _ = reader.get_mut().flush().await;
        });

        let acks = raft
            .replicate_log_once(Some(1))
            .await
            .expect("replicate to quorum");
        assert_eq!(acks, BTreeSet::from(["n1".to_string(), "n2".to_string()]));
        assert_eq!(
            raft.runtime
                .lock()
                .leader_progress
                .get("n3")
                .map(|progress| progress.match_index),
            Some(0),
            "slow peer should not have replied before quorum returned"
        );

        release_n3_tx.send(()).expect("release slow peer");
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if raft
                    .runtime
                    .lock()
                    .leader_progress
                    .get("n3")
                    .is_some_and(|progress| progress.match_index >= 1)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("detached slow peer replication should still update progress");

        server_n2.await.expect("fast peer server");
        server_n3.await.expect("slow peer server");
        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn append_local_batch_blocking_does_not_block_current_thread_runtime() {
        let dir = temp_dir("raft-append-blocking-pool");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let state_guard = raft.log.state.lock();

        let node = raft.clone();
        let append = tokio::spawn(async move {
            node.append_local_batch_blocking(1, vec![RaftCommand::Noop])
                .await
        });

        tokio::time::timeout(
            Duration::from_millis(100),
            tokio::time::sleep(Duration::from_millis(10)),
        )
        .await
        .expect("blocking append must not occupy the current-thread runtime");
        drop(state_guard);

        let entries = tokio::time::timeout(Duration::from_secs(1), append)
            .await
            .expect("append task should finish after releasing log lock")
            .expect("append task should not panic")
            .expect("append should succeed");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].index, 1);

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn snapshot_maintenance_blocking_does_not_block_current_thread_runtime() {
        let dir = temp_dir("raft-maintenance-blocking-pool");
        let mut cfg = test_raft_config(dir.clone());
        cfg.snapshot_max_log_entries = 1;
        cfg.snapshot_max_log_bytes = u64::MAX;
        cfg.trailing_log_entries = 0;
        let raft = BrokerRaft::open(cfg).expect("open raft");
        raft.log.append(1, RaftCommand::Noop).expect("append");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
            runtime.commit_index = 1;
            runtime.last_applied = 1;
        }
        let state_guard = raft.log.state.lock();

        let node = raft.clone();
        let maintenance =
            tokio::spawn(async move { node.snapshot_and_compact_if_needed_blocking(true).await });

        tokio::time::timeout(
            Duration::from_millis(100),
            tokio::time::sleep(Duration::from_millis(10)),
        )
        .await
        .expect("blocking maintenance must not occupy the current-thread runtime");
        drop(state_guard);

        tokio::time::timeout(Duration::from_secs(1), maintenance)
            .await
            .expect("maintenance task should finish after releasing log lock")
            .expect("maintenance task should not panic")
            .expect("maintenance should succeed");
        assert_eq!(
            raft.log
                .latest_snapshot()
                .expect("snapshot")
                .last_included_index,
            1
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn candidate_election_blocking_does_not_block_current_thread_runtime() {
        let dir = temp_dir("raft-election-blocking-pool");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 0;
            runtime.voted_for = None;
            runtime.role = RaftRole::Follower;
            runtime.leader_id = None;
        }
        let runtime_guard = raft.runtime.lock();

        let node = raft.clone();
        let election = tokio::spawn(async move { node.begin_candidate_election_blocking().await });

        tokio::time::timeout(
            Duration::from_millis(100),
            tokio::time::sleep(Duration::from_millis(10)),
        )
        .await
        .expect("blocking self-vote persistence must not occupy the current-thread runtime");
        drop(runtime_guard);

        let term = tokio::time::timeout(Duration::from_secs(1), election)
            .await
            .expect("election task should finish after releasing runtime lock")
            .expect("election task should not panic")
            .expect("election should persist self-vote");
        assert_eq!(term, Some(1));
        assert_eq!(
            raft.log.read_hard_state().expect("read hard state"),
            RaftHardState {
                current_term: 1,
                voted_for: Some("n1".into()),
                commit_index: 0,
            }
        );
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 1);
            assert_eq!(runtime.voted_for.as_deref(), Some("n1"));
            assert_eq!(runtime.role, RaftRole::Candidate);
            assert_eq!(runtime.leader_id, None);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn step_down_blocking_does_not_block_current_thread_runtime() {
        let dir = temp_dir("raft-stepdown-blocking-pool");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        for _ in 0..3 {
            raft.log.append(2, RaftCommand::Noop).expect("append");
        }
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 2;
            runtime.voted_for = Some("n1".into());
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.commit_index = 3;
        }
        let runtime_guard = raft.runtime.lock();

        let node = raft.clone();
        let step_down = tokio::spawn(async move { node.step_down_blocking(4, None).await });

        tokio::time::timeout(
            Duration::from_millis(100),
            tokio::time::sleep(Duration::from_millis(10)),
        )
        .await
        .expect("blocking step-down persistence must not occupy the current-thread runtime");
        drop(runtime_guard);

        tokio::time::timeout(Duration::from_secs(1), step_down)
            .await
            .expect("step-down task should finish after releasing runtime lock")
            .expect("step-down task should not panic");
        assert_eq!(
            raft.log.read_hard_state().expect("read hard state"),
            RaftHardState {
                current_term: 4,
                voted_for: None,
                commit_index: 3,
            }
        );
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 4);
            assert_eq!(runtime.voted_for, None);
            assert_eq!(runtime.role, RaftRole::Follower);
            assert_eq!(runtime.leader_id, None);
            assert_eq!(runtime.commit_index, 3);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn append_entries_response_match_index_is_capped_to_sent_batch() {
        let dir = temp_dir("raft-append-inflated-match-index");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake peer");
        let peer_addr = listener.local_addr().expect("fake peer addr");

        let mut cfg = test_raft_config(dir.clone());
        cfg.append_entries_max_entries = 2;
        cfg.append_entries_max_bytes = usize::MAX;
        let raft = BrokerRaft::open(cfg).expect("open raft");
        for _ in 0..5 {
            raft.log.append(1, RaftCommand::Noop).expect("append");
        }
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
        }

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept fake peer");
            let mut reader = TokioBufReader::new(stream);
            let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                .await
                .expect("read append entries");
            let rpc: RaftRpc = serde_json::from_str(&line).expect("parse append entries");
            let term = match rpc {
                RaftRpc::AppendEntries {
                    term,
                    prev_log_index,
                    entries,
                    ..
                } => {
                    assert_eq!(prev_log_index, 0);
                    assert_eq!(
                        entries.iter().map(|entry| entry.index).collect::<Vec<_>>(),
                        vec![1, 2]
                    );
                    term
                }
                other => panic!("unexpected rpc: {other:?}"),
            };
            let response = RaftRpcResponse::AppendEntries {
                term,
                success: true,
                match_index: 99,
                conflict_index: None,
                conflict_term: None,
            };
            let body = serde_json::to_vec(&response).expect("serialize append response");
            reader
                .get_mut()
                .write_all(&body)
                .await
                .expect("write append response");
            reader
                .get_mut()
                .write_all(b"\n")
                .await
                .expect("write append newline");
            reader
                .get_mut()
                .flush()
                .await
                .expect("flush append response");
        });

        let peer = RaftPeerConfig {
            id: "n2".into(),
            addr: peer_addr.to_string(),
        };
        let outcome = raft
            .replicate_to_peer(peer, 1, 0, Some(5))
            .await
            .expect("replicate to fake peer");
        assert!(
            !outcome.target_reached,
            "inflated follower matchIndex must not satisfy the target"
        );
        server.await.expect("fake peer server");
        let progress = raft
            .runtime
            .lock()
            .leader_progress
            .get("n2")
            .copied()
            .expect("progress");
        assert_eq!(progress.match_index, 2);
        assert_eq!(progress.next_index, 3);

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn append_entries_response_after_stepdown_does_not_update_progress() {
        let dir = temp_dir("raft-append-response-after-stepdown");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake peer");
        let peer_addr = listener.local_addr().expect("fake peer addr");

        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        for _ in 0..3 {
            raft.log.append(1, RaftCommand::Noop).expect("append");
        }
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
        }

        let (request_seen_tx, request_seen_rx) = oneshot::channel();
        let (reply_tx, reply_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept fake peer");
            let mut reader = TokioBufReader::new(stream);
            let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                .await
                .expect("read append entries");
            let rpc: RaftRpc = serde_json::from_str(&line).expect("parse append entries");
            let term = match rpc {
                RaftRpc::AppendEntries { term, entries, .. } => {
                    assert_eq!(
                        entries.iter().map(|entry| entry.index).collect::<Vec<_>>(),
                        vec![1, 2, 3]
                    );
                    term
                }
                other => panic!("unexpected rpc: {other:?}"),
            };
            request_seen_tx.send(()).expect("signal request seen");
            reply_rx.await.expect("wait for stepdown");
            let response = RaftRpcResponse::AppendEntries {
                term,
                success: true,
                match_index: 3,
                conflict_index: None,
                conflict_term: None,
            };
            let body = serde_json::to_vec(&response).expect("serialize append response");
            reader
                .get_mut()
                .write_all(&body)
                .await
                .expect("write append response");
            reader
                .get_mut()
                .write_all(b"\n")
                .await
                .expect("write append newline");
            reader
                .get_mut()
                .flush()
                .await
                .expect("flush append response");
        });

        let peer = RaftPeerConfig {
            id: "n2".into(),
            addr: peer_addr.to_string(),
        };
        let node = raft.clone();
        let replicate =
            tokio::spawn(async move { node.replicate_to_peer(peer, 1, 0, Some(3)).await });
        request_seen_rx
            .await
            .expect("append request should be sent");
        raft.step_down(2, None);
        reply_tx.send(()).expect("release fake peer response");
        let outcome = replicate
            .await
            .expect("replication task")
            .expect("replication result");

        assert!(!outcome.contacted);
        assert!(!outcome.target_reached);
        server.await.expect("fake peer server");
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 2);
            assert_eq!(runtime.role, RaftRole::Follower);
            let progress = runtime
                .leader_progress
                .get("n2")
                .copied()
                .expect("progress");
            assert_eq!(progress.match_index, 0);
            assert_eq!(progress.next_index, 1);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn replication_batch_read_stops_before_unneeded_tail() {
        let dir = temp_dir("raft-replication-early-stop");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake peer");
        let peer_addr = listener.local_addr().expect("fake peer addr");
        let unused_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind unused peer addr");
        let unused_addr = unused_listener.local_addr().expect("unused peer addr");
        drop(unused_listener);

        let mut cfg = test_raft_config(dir.clone());
        cfg.heartbeat_interval = Duration::from_millis(10);
        cfg.election_timeout_min = Duration::from_millis(50);
        cfg.election_timeout_max = Duration::from_millis(100);
        cfg.append_entries_max_entries = 1;
        cfg.append_entries_max_bytes = usize::MAX;
        cfg.peers = vec![
            test_peer("n1", 7980),
            RaftPeerConfig {
                id: "n2".into(),
                addr: peer_addr.to_string(),
            },
            RaftPeerConfig {
                id: "n3".into(),
                addr: unused_addr.to_string(),
            },
        ];
        let raft = BrokerRaft::open(cfg).expect("open raft");
        for _ in 0..3 {
            raft.log.append(1, RaftCommand::Noop).expect("append");
        }
        append_raw_log_line(&dir, "{not valid json");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
            runtime.leader_progress.insert(
                "n3".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
        }

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept fake peer");
            let mut reader = TokioBufReader::new(stream);
            let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                .await
                .expect("read append entries");
            let rpc: RaftRpc = serde_json::from_str(&line).expect("parse append entries");
            let term = match rpc {
                RaftRpc::AppendEntries {
                    term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    ..
                } => {
                    assert_eq!(prev_log_index, 0);
                    assert_eq!(prev_log_term, 0);
                    assert_eq!(
                        entries.iter().map(|entry| entry.index).collect::<Vec<_>>(),
                        vec![1]
                    );
                    term
                }
                other => panic!("unexpected rpc: {other:?}"),
            };
            let response = RaftRpcResponse::AppendEntries {
                term,
                success: true,
                match_index: 1,
                conflict_index: None,
                conflict_term: None,
            };
            let body = serde_json::to_vec(&response).expect("serialize append response");
            reader
                .get_mut()
                .write_all(&body)
                .await
                .expect("write append response");
            reader
                .get_mut()
                .write_all(b"\n")
                .await
                .expect("write append newline");
            reader
                .get_mut()
                .flush()
                .await
                .expect("flush append response");
        });

        let acks = raft
            .replicate_until_quorum(1)
            .await
            .expect("replicate first bounded entry");
        assert!(acks.contains("n1"));
        assert!(acks.contains("n2"));
        server.await.expect("fake peer server");

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn retained_snapshot_suffix_is_used_for_incremental_catchup() {
        let dir = temp_dir("raft-retained-suffix-catchup");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake peer");
        let peer_addr = listener.local_addr().expect("fake peer addr");
        let unused_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind unused peer addr");
        let unused_addr = unused_listener.local_addr().expect("unused peer addr");
        drop(unused_listener);

        let mut cfg = test_raft_config(dir.clone());
        cfg.heartbeat_interval = Duration::from_millis(10);
        cfg.election_timeout_min = Duration::from_millis(50);
        cfg.election_timeout_max = Duration::from_millis(100);
        cfg.append_entries_max_entries = 8;
        cfg.append_entries_max_bytes = usize::MAX;
        cfg.peers = vec![
            test_peer("n1", 7980),
            RaftPeerConfig {
                id: "n2".into(),
                addr: peer_addr.to_string(),
            },
            RaftPeerConfig {
                id: "n3".into(),
                addr: unused_addr.to_string(),
            },
        ];
        let raft = BrokerRaft::open(cfg).expect("open raft");
        for _ in 0..6 {
            raft.log.append(1, RaftCommand::Noop).expect("append");
        }
        raft.log
            .write_snapshot(6, 1, idle_snapshot_payload())
            .expect("write snapshot");
        raft.log
            .compact_through(3)
            .expect("retain entries covered by snapshot");
        assert_eq!(
            raft.log
                .read_entries()
                .expect("retained entries")
                .iter()
                .map(|entry| entry.index)
                .collect::<Vec<_>>(),
            vec![4, 5, 6]
        );
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 5,
                    match_index: 4,
                },
            );
            runtime.leader_progress.insert(
                "n3".into(),
                RaftPeerProgress {
                    next_index: 5,
                    match_index: 4,
                },
            );
        }

        let observed = Arc::new(Mutex::new(Vec::<u64>::new()));
        let observed_for_server = Arc::clone(&observed);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept fake peer");
            let mut reader = TokioBufReader::new(stream);
            let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                .await
                .expect("read append entries");
            let rpc: RaftRpc = serde_json::from_str(&line).expect("parse append entries");
            let (term, match_index) = match rpc {
                RaftRpc::AppendEntries {
                    term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    ..
                } => {
                    assert_eq!(prev_log_index, 4);
                    assert_eq!(prev_log_term, 1);
                    let indexes = entries.iter().map(|entry| entry.index).collect::<Vec<_>>();
                    assert_eq!(indexes, vec![5, 6]);
                    observed_for_server.lock().extend(indexes);
                    (term, 6)
                }
                RaftRpc::InstallSnapshot { .. } => {
                    panic!("retained suffix catch-up should not require InstallSnapshot")
                }
                other => panic!("unexpected rpc: {other:?}"),
            };
            let response = RaftRpcResponse::AppendEntries {
                term,
                success: true,
                match_index,
                conflict_index: None,
                conflict_term: None,
            };
            let body = serde_json::to_vec(&response).expect("serialize append response");
            reader
                .get_mut()
                .write_all(&body)
                .await
                .expect("write append response");
            reader
                .get_mut()
                .write_all(b"\n")
                .await
                .expect("write append newline");
            reader
                .get_mut()
                .flush()
                .await
                .expect("flush append response");
        });

        let acks = raft
            .replicate_until_quorum(6)
            .await
            .expect("replicate retained suffix");
        assert!(acks.contains("n1"));
        assert!(acks.contains("n2"));
        server.await.expect("fake peer server");
        assert_eq!(observed.lock().clone(), vec![5, 6]);

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn client_request_queue_limit_rejects_before_append() {
        let dir = temp_dir("raft-client-request-queue-limit");
        let mut cfg = test_raft_config(dir.clone());
        cfg.client_batch_max_pending = 1;
        let raft = BrokerRaft::open(cfg).expect("open raft");

        let (queued_client, _) = raft.register_client();
        let (result_tx, _result_rx) = oneshot::channel();
        {
            let mut state = raft.client_request_batch.lock();
            state.driver_active = true;
            state.pending.push_back(PendingClientRequest {
                client_id: queued_client,
                request: single_lock_request("queued-lock", "queued-key"),
                request_id: None,
                request_fingerprint: None,
                result_tx,
            });
        }

        let (overflow_client, _) = raft.register_client();
        let err = raft
            .enqueue_client_request(
                overflow_client,
                single_lock_request("overflow-lock", "overflow-key"),
                None,
                None,
            )
            .await
            .expect_err("full leader-local queue should reject new requests");
        assert!(matches!(
            err,
            BrokerRaftError::ClientQueueFull {
                pending: 1,
                limit: 1
            }
        ));
        assert_eq!(raft.log.last_index(), 0);
        assert_eq!(raft.client_request_batch.lock().pending.len(), 1);

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn concurrent_client_requests_share_one_replicated_batch() {
        let dir = temp_dir("raft-client-request-batch");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake peer");
        let peer_addr = listener.local_addr().expect("fake peer addr");
        let unused_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind unused peer addr");
        let unused_addr = unused_listener.local_addr().expect("unused peer addr");
        drop(unused_listener);

        let mut cfg = test_raft_config(dir.clone());
        cfg.heartbeat_interval = Duration::from_millis(10);
        cfg.election_timeout_min = Duration::from_millis(50);
        cfg.election_timeout_max = Duration::from_millis(100);
        cfg.append_entries_max_entries = 16;
        cfg.append_entries_max_bytes = usize::MAX;
        cfg.client_batch_max_entries = 8;
        cfg.client_batch_max_delay = Duration::from_millis(25);
        cfg.peers = vec![
            test_peer("n1", 7980),
            RaftPeerConfig {
                id: "n2".into(),
                addr: peer_addr.to_string(),
            },
            RaftPeerConfig {
                id: "n3".into(),
                addr: unused_addr.to_string(),
            },
        ];
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
            runtime.leader_progress.insert(
                "n3".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
        }
        raft.note_leader_quorum_observed();

        let observed_batches = Arc::new(Mutex::new(Vec::<Vec<u64>>::new()));
        let observed_for_server = Arc::clone(&observed_batches);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept fake peer");
            let mut reader = TokioBufReader::new(stream);
            let mut observed_entries = 0usize;
            while observed_entries < 4 {
                let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                    .await
                    .expect("read client batch append entries");
                let rpc: RaftRpc =
                    serde_json::from_str(&line).expect("parse client batch append entries");
                let (term, match_index, batch_indexes) = match rpc {
                    RaftRpc::AppendEntries {
                        term,
                        prev_log_index,
                        entries,
                        ..
                    } => {
                        let batch_indexes =
                            entries.iter().map(|entry| entry.index).collect::<Vec<_>>();
                        let match_index = entries
                            .last()
                            .map(|entry| entry.index)
                            .unwrap_or(prev_log_index);
                        (term, match_index, batch_indexes)
                    }
                    other => panic!("unexpected rpc: {other:?}"),
                };
                if !batch_indexes.is_empty() {
                    observed_entries += batch_indexes.len();
                    observed_for_server.lock().push(batch_indexes);
                }
                let response = RaftRpcResponse::AppendEntries {
                    term,
                    success: true,
                    match_index,
                    conflict_index: None,
                    conflict_term: None,
                };
                let body = serde_json::to_vec(&response).expect("serialize append response");
                reader
                    .get_mut()
                    .write_all(&body)
                    .await
                    .expect("write append response");
                reader
                    .get_mut()
                    .write_all(b"\n")
                    .await
                    .expect("write append newline");
                reader
                    .get_mut()
                    .flush()
                    .await
                    .expect("flush append response");
            }
        });

        let barrier = Arc::new(tokio::sync::Barrier::new(5));
        let mut handles = Vec::new();
        let mut response_receivers = Vec::new();
        for i in 0..4 {
            let uuid = format!("batch-lock-{i}");
            let key = format!("batch-key-{i}");
            let (client_id, rx) = raft.register_client();
            response_receivers.push((uuid.clone(), rx));
            let node = raft.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                node.handle_request(client_id, single_lock_request(&uuid, &key))
                    .await
            }));
        }
        barrier.wait().await;

        let mut indexes = Vec::new();
        for handle in handles {
            indexes.push(handle.await.expect("client task").expect("commit index"));
        }
        indexes.sort_unstable();
        assert_eq!(indexes, vec![1, 2, 3, 4]);
        server.await.expect("fake peer server");
        assert_eq!(observed_batches.lock().clone(), vec![vec![1, 2, 3, 4]]);
        for (uuid, mut rx) in response_receivers {
            let response = wait_for_response(&mut rx, &uuid, Duration::from_secs(1), true).await;
            assert!(matches!(
                response,
                Some(Response::Lock {
                    acquired: true,
                    lock_uuid: Some(_),
                    ..
                })
            ));
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn client_pipeline_drains_multiple_configured_batches_in_one_quorum_round() {
        let dir = temp_dir("raft-client-request-pipeline");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake peer");
        let peer_addr = listener.local_addr().expect("fake peer addr");
        let unused_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind unused peer addr");
        let unused_addr = unused_listener.local_addr().expect("unused peer addr");
        drop(unused_listener);

        let mut cfg = test_raft_config(dir.clone());
        cfg.heartbeat_interval = Duration::from_millis(10);
        cfg.election_timeout_min = Duration::from_millis(50);
        cfg.election_timeout_max = Duration::from_millis(100);
        cfg.append_entries_max_entries = 16;
        cfg.append_entries_max_bytes = usize::MAX;
        cfg.client_batch_max_entries = 2;
        cfg.client_pipeline_max_batches = 2;
        cfg.client_batch_max_delay = Duration::from_secs(5);
        cfg.peers = vec![
            test_peer("n1", 7980),
            RaftPeerConfig {
                id: "n2".into(),
                addr: peer_addr.to_string(),
            },
            RaftPeerConfig {
                id: "n3".into(),
                addr: unused_addr.to_string(),
            },
        ];
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
            runtime.leader_progress.insert(
                "n3".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
        }

        let observed_batches = Arc::new(Mutex::new(Vec::<Vec<u64>>::new()));
        let observed_for_server = Arc::clone(&observed_batches);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept fake peer");
            let mut reader = TokioBufReader::new(stream);
            let mut observed_entries = 0usize;
            while observed_entries < 4 {
                let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                    .await
                    .expect("read pipelined append entries");
                let rpc: RaftRpc =
                    serde_json::from_str(&line).expect("parse pipelined append entries");
                let (term, match_index, batch_indexes) = match rpc {
                    RaftRpc::AppendEntries {
                        term,
                        prev_log_index,
                        entries,
                        ..
                    } => {
                        let batch_indexes =
                            entries.iter().map(|entry| entry.index).collect::<Vec<_>>();
                        let match_index = entries
                            .last()
                            .map(|entry| entry.index)
                            .unwrap_or(prev_log_index);
                        (term, match_index, batch_indexes)
                    }
                    other => panic!("unexpected rpc: {other:?}"),
                };
                if !batch_indexes.is_empty() {
                    observed_entries += batch_indexes.len();
                    observed_for_server.lock().push(batch_indexes);
                }
                let response = RaftRpcResponse::AppendEntries {
                    term,
                    success: true,
                    match_index,
                    conflict_index: None,
                    conflict_term: None,
                };
                let body = serde_json::to_vec(&response).expect("serialize append response");
                reader
                    .get_mut()
                    .write_all(&body)
                    .await
                    .expect("write append response");
                reader
                    .get_mut()
                    .write_all(b"\n")
                    .await
                    .expect("write append newline");
                reader
                    .get_mut()
                    .flush()
                    .await
                    .expect("flush append response");
            }
        });

        let mut result_receivers = Vec::new();
        let mut response_receivers = Vec::new();
        {
            let mut state = raft.client_request_batch.lock();
            state.driver_active = true;
            for i in 0..4 {
                let uuid = format!("pipeline-lock-{i}");
                let key = format!("pipeline-key-{i}");
                let (client_id, rx) = raft.register_client();
                let (result_tx, result_rx) = oneshot::channel();
                response_receivers.push((uuid.clone(), rx));
                result_receivers.push(result_rx);
                state.pending.push_back(PendingClientRequest {
                    client_id,
                    request: single_lock_request(&uuid, &key),
                    request_id: None,
                    request_fingerprint: None,
                    result_tx,
                });
            }
        }

        tokio::time::timeout(Duration::from_secs(3), raft.drive_client_request_batches())
            .await
            .expect("pipelined batch driver should finish");

        let mut indexes = Vec::new();
        for result_rx in result_receivers {
            indexes.push(
                result_rx
                    .await
                    .expect("driver result")
                    .expect("committed index"),
            );
        }
        indexes.sort_unstable();
        assert_eq!(indexes, vec![1, 2, 3, 4]);
        server.await.expect("fake peer server");
        assert_eq!(observed_batches.lock().clone(), vec![vec![1, 2, 3, 4]]);
        assert!(!raft.client_request_batch.lock().driver_active);
        for (uuid, mut rx) in response_receivers {
            let response = wait_for_response(&mut rx, &uuid, Duration::from_secs(1), true).await;
            assert!(matches!(
                response,
                Some(Response::Lock {
                    acquired: true,
                    lock_uuid: Some(_),
                    ..
                })
            ));
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn client_batch_wakes_early_when_capacity_is_reached() {
        let dir = temp_dir("raft-client-request-batch-early-wake");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake peer");
        let peer_addr = listener.local_addr().expect("fake peer addr");
        let unused_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind unused peer addr");
        let unused_addr = unused_listener.local_addr().expect("unused peer addr");
        drop(unused_listener);

        let mut cfg = test_raft_config(dir.clone());
        cfg.heartbeat_interval = Duration::from_millis(10);
        cfg.election_timeout_min = Duration::from_millis(50);
        cfg.election_timeout_max = Duration::from_millis(100);
        cfg.append_entries_max_entries = 16;
        cfg.append_entries_max_bytes = usize::MAX;
        cfg.client_batch_max_entries = 4;
        cfg.client_batch_max_delay = Duration::from_secs(5);
        cfg.peers = vec![
            test_peer("n1", 7980),
            RaftPeerConfig {
                id: "n2".into(),
                addr: peer_addr.to_string(),
            },
            RaftPeerConfig {
                id: "n3".into(),
                addr: unused_addr.to_string(),
            },
        ];
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
            runtime.leader_progress.insert(
                "n3".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
        }

        let observed_batches = Arc::new(Mutex::new(Vec::<Vec<u64>>::new()));
        let observed_for_server = Arc::clone(&observed_batches);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept fake peer");
            let mut reader = TokioBufReader::new(stream);
            let mut observed_entries = 0usize;
            while observed_entries < 4 {
                let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                    .await
                    .expect("read early-wake append entries");
                let rpc: RaftRpc =
                    serde_json::from_str(&line).expect("parse early-wake append entries");
                let (term, match_index, batch_indexes) = match rpc {
                    RaftRpc::AppendEntries {
                        term,
                        prev_log_index,
                        entries,
                        ..
                    } => {
                        let batch_indexes =
                            entries.iter().map(|entry| entry.index).collect::<Vec<_>>();
                        let match_index = entries
                            .last()
                            .map(|entry| entry.index)
                            .unwrap_or(prev_log_index);
                        (term, match_index, batch_indexes)
                    }
                    other => panic!("unexpected rpc: {other:?}"),
                };
                if !batch_indexes.is_empty() {
                    observed_entries += batch_indexes.len();
                    observed_for_server.lock().push(batch_indexes);
                }
                let response = RaftRpcResponse::AppendEntries {
                    term,
                    success: true,
                    match_index,
                    conflict_index: None,
                    conflict_term: None,
                };
                let body = serde_json::to_vec(&response).expect("serialize append response");
                reader
                    .get_mut()
                    .write_all(&body)
                    .await
                    .expect("write append response");
                reader
                    .get_mut()
                    .write_all(b"\n")
                    .await
                    .expect("write append newline");
                reader
                    .get_mut()
                    .flush()
                    .await
                    .expect("flush append response");
            }
        });

        let mut handles = Vec::new();
        let waiting = raft.client_batch_waiting.notified();
        tokio::pin!(waiting);
        let (first_client, _) = raft.register_client();
        let first = raft.clone();
        handles.push(tokio::spawn(async move {
            first
                .handle_request(
                    first_client,
                    single_lock_request("early-batch-lock-0", "early-batch-key-0"),
                )
                .await
        }));
        tokio::time::timeout(Duration::from_secs(1), &mut waiting)
            .await
            .expect("batch driver should enter coalescing wait");
        for i in 1..4 {
            let uuid = format!("early-batch-lock-{i}");
            let key = format!("early-batch-key-{i}");
            let (client_id, _) = raft.register_client();
            let node = raft.clone();
            handles.push(tokio::spawn(async move {
                node.handle_request(client_id, single_lock_request(&uuid, &key))
                    .await
            }));
        }

        let indexes = tokio::time::timeout(Duration::from_secs(3), async move {
            let mut indexes = Vec::new();
            for handle in handles {
                indexes.push(handle.await.expect("client task").expect("commit index"));
            }
            indexes
        })
        .await
        .expect("full client batch should wake before the long coalescing delay");
        let mut indexes = indexes;
        indexes.sort_unstable();
        assert_eq!(indexes, vec![1, 2, 3, 4]);
        server.await.expect("fake peer server");
        assert_eq!(observed_batches.lock().clone(), vec![vec![1, 2, 3, 4]]);

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn no_wait_ephemeral_acquire_does_not_queue_or_append_drop_client() {
        let dir = temp_dir("raft-no-wait-ephemeral");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.membership = RaftMembership::from_simple(vec![test_peer("n1", 7980)]);
            runtime.leader_progress.clear();
        }

        let holder_uuid = "no-wait-holder";
        let holder = Request::Lock {
            uuid: holder_uuid.into(),
            key: Some("no-wait-key".into()),
            keys: None,
            pid: None,
            ttl: Some(5_000),
            max: None,
            force: false,
            retry_count: 0,
            keep_locks_after_death: false,
            wait: None,
        };
        let granted = raft
            .run_ephemeral(holder, holder_uuid, Duration::ZERO, true)
            .await
            .expect("holder request");
        assert!(matches!(
            granted,
            Some(Response::Lock { acquired: true, .. })
        ));
        assert_eq!(raft.broker.metrics().holders, 1);
        assert_eq!(raft.broker.metrics().waiters, 0);
        assert_eq!(raft.log.read_entries().expect("entries").len(), 1);

        let waiter_uuid = "no-wait-contended";
        let contended = Request::Lock {
            uuid: waiter_uuid.into(),
            key: Some("no-wait-key".into()),
            keys: None,
            pid: None,
            ttl: Some(5_000),
            max: None,
            force: false,
            retry_count: 0,
            keep_locks_after_death: false,
            wait: None,
        };
        let denied = raft
            .run_ephemeral(contended, waiter_uuid, Duration::ZERO, true)
            .await
            .expect("contended no-wait request");
        assert!(matches!(
            denied,
            Some(Response::Lock {
                acquired: false,
                ..
            })
        ));
        assert_eq!(raft.broker.metrics().holders, 1);
        assert_eq!(raft.broker.metrics().waiters, 0);

        let entries = raft.log.read_entries().expect("entries");
        assert_eq!(entries.len(), 2, "no cleanup DropClient entry expected");
        assert!(matches!(
            &entries[1].command,
            RaftCommand::ClientRequestWithIdentity {
                request: Request::Lock {
                    wait: Some(false),
                    ..
                },
                ..
            }
        ));

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn duplicate_ephemeral_request_id_returns_cached_response_without_second_append() {
        let dir = temp_dir("raft-duplicate-request-id-cache");
        let mut cfg = test_raft_config(dir.clone());
        cfg.client_response_cache_max_entries = 8;
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.membership = RaftMembership::from_simple(vec![test_peer("n1", 7980)]);
            runtime.leader_progress.clear();
        }
        raft.note_leader_quorum_observed();

        let request = single_lock_request("retry-request", "retry-key");
        let first = raft
            .run_ephemeral(request.clone(), "retry-request", Duration::ZERO, true)
            .await
            .expect("first request")
            .expect("first response");
        let second = raft
            .run_ephemeral(request, "retry-request", Duration::ZERO, true)
            .await
            .expect("cached request")
            .expect("cached response");

        assert_eq!(granted_lock_uuid(&first), granted_lock_uuid(&second));
        assert_eq!(raft.log.read_entries().expect("entries").len(), 1);

        let conflict = raft
            .run_ephemeral(
                single_lock_request("retry-request", "different-key"),
                "retry-request",
                Duration::ZERO,
                true,
            )
            .await
            .expect_err("same request id with different payload must conflict");
        assert!(matches!(
            conflict,
            BrokerRaftError::IdempotencyKeyConflict { .. }
        ));
        assert_eq!(raft.log.read_entries().expect("entries").len(), 1);

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn duplicate_request_id_while_batch_pending_does_not_append_again() {
        let dir = temp_dir("raft-duplicate-request-id-pending-batch");
        let mut cfg = test_raft_config(dir.clone());
        cfg.client_response_cache_max_entries = 8;
        cfg.client_batch_max_entries = 2;
        cfg.client_batch_max_delay = Duration::from_secs(60);
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.membership = RaftMembership::from_simple(vec![test_peer("n1", 7980)]);
            runtime.leader_progress.clear();
        }
        raft.note_leader_quorum_observed();

        let request = single_lock_request("pending-request", "pending-key");
        let node = raft.clone();
        let first_request = request.clone();
        let first = tokio::spawn(async move {
            node.run_ephemeral(first_request, "pending-request", Duration::ZERO, true)
                .await
        });
        tokio::time::timeout(Duration::from_secs(3), raft.client_batch_waiting.notified())
            .await
            .expect("batch driver should wait for coalescing delay");

        let duplicate = raft
            .run_ephemeral(request, "pending-request", Duration::ZERO, true)
            .await
            .expect("duplicate pending request");
        assert!(
            duplicate.is_none(),
            "duplicate retry should observe the in-flight request instead of appending"
        );
        assert!(
            raft.log.read_entries().expect("entries").is_empty(),
            "nothing should be appended while the first request is still batched"
        );

        raft.client_batch_notify.notify_one();
        let first_response = first
            .await
            .expect("first task")
            .expect("first request")
            .expect("first response");
        assert!(matches!(
            first_response,
            Response::Lock { acquired: true, .. }
        ));
        assert_eq!(raft.log.read_entries().expect("entries").len(), 1);

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn request_id_reservation_is_released_when_queue_full_before_append() {
        let dir = temp_dir("raft-request-id-queue-full-release");
        let mut cfg = test_raft_config(dir.clone());
        cfg.client_response_cache_max_entries = 8;
        cfg.client_batch_max_pending = 1;
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.membership = RaftMembership::from_simple(vec![test_peer("n1", 7980)]);
            runtime.leader_progress.clear();
        }
        raft.note_leader_quorum_observed();

        let (queued_client, _) = raft.register_client();
        let (result_tx, _result_rx) = oneshot::channel();
        {
            let mut state = raft.client_request_batch.lock();
            state.driver_active = true;
            state.pending.push_back(PendingClientRequest {
                client_id: queued_client,
                request: single_lock_request("held-queue-entry", "held-queue-key"),
                request_id: None,
                request_fingerprint: None,
                result_tx,
            });
        }

        let request = single_lock_request("retry-after-full", "retry-after-full-key");
        let err = raft
            .run_ephemeral(request.clone(), "retry-after-full", Duration::ZERO, true)
            .await
            .expect_err("queue-full request should fail before append");
        assert!(matches!(err, BrokerRaftError::ClientQueueFull { .. }));
        assert!(raft.log.read_entries().expect("entries").is_empty());

        {
            let mut state = raft.client_request_batch.lock();
            state.pending.clear();
            state.driver_active = false;
        }
        let retried = raft
            .run_ephemeral(request, "retry-after-full", Duration::ZERO, true)
            .await
            .expect("retry after queue space")
            .expect("retry response");
        assert!(matches!(retried, Response::Lock { acquired: true, .. }));
        assert_eq!(raft.log.read_entries().expect("entries").len(), 1);

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn committed_request_id_replays_after_reopen_without_duplicate_append() {
        let dir = temp_dir("raft-request-id-replay-cache");
        let mut cfg = test_raft_config(dir.clone());
        cfg.client_response_cache_max_entries = 8;
        let raft = BrokerRaft::open(cfg.clone()).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.membership = RaftMembership::from_simple(vec![test_peer("n1", 7980)]);
            runtime.leader_progress.clear();
        }
        raft.note_leader_quorum_observed();

        let request = single_lock_request("replay-request", "replay-key");
        let first = raft
            .run_ephemeral(request.clone(), "replay-request", Duration::ZERO, true)
            .await
            .expect("first request")
            .expect("first response");
        assert_eq!(
            raft.log.read_hard_state().expect("hard state").commit_index,
            1
        );
        assert_eq!(raft.log.read_entries().expect("entries").len(), 1);
        drop(raft);

        let reopened = BrokerRaft::open(cfg).expect("reopen raft");
        {
            let mut runtime = reopened.runtime.lock();
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.membership = RaftMembership::from_simple(vec![test_peer("n1", 7980)]);
            runtime.leader_progress.clear();
        }
        reopened.note_leader_quorum_observed();
        let second = reopened
            .run_ephemeral(request, "replay-request", Duration::ZERO, true)
            .await
            .expect("replayed cached request")
            .expect("cached response");

        assert_eq!(granted_lock_uuid(&first), granted_lock_uuid(&second));
        assert_eq!(reopened.log.read_entries().expect("entries").len(), 1);

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn snapshotted_request_id_cache_survives_compacted_log_reopen() {
        let dir = temp_dir("raft-request-id-snapshot-cache");
        let mut cfg = test_raft_config(dir.clone());
        cfg.client_response_cache_max_entries = 8;
        cfg.snapshot_max_log_entries = 0;
        cfg.snapshot_max_log_bytes = u64::MAX;
        cfg.trailing_log_entries = 0;
        let mut raft = BrokerRaft::open(cfg.clone()).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.membership = RaftMembership::from_simple(vec![test_peer("n1", 7980)]);
            runtime.leader_progress.clear();
        }
        raft.note_leader_quorum_observed();

        let request = single_lock_request("snap-request", "snap-key");
        let first = raft
            .run_ephemeral(request.clone(), "snap-request", Duration::ZERO, true)
            .await
            .expect("first request")
            .expect("first response");
        raft.runtime.lock().membership = RaftMembership::from_simple(cfg.peers.clone());
        raft.config.snapshot_max_log_entries = 1;
        raft.snapshot_and_compact_if_needed(false)
            .expect("snapshot committed request");
        assert_eq!(
            raft.log
                .latest_snapshot()
                .expect("snapshot")
                .last_included_index,
            1
        );
        assert!(raft
            .log
            .read_entries()
            .expect("retained entries")
            .is_empty());
        drop(raft);

        let reopened = BrokerRaft::open(cfg).expect("reopen raft");
        {
            let mut runtime = reopened.runtime.lock();
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.membership = RaftMembership::from_simple(vec![test_peer("n1", 7980)]);
            runtime.leader_progress.clear();
        }
        reopened.note_leader_quorum_observed();
        let second = reopened
            .run_ephemeral(request, "snap-request", Duration::ZERO, true)
            .await
            .expect("snapshotted cached request")
            .expect("cached response");

        assert_eq!(granted_lock_uuid(&first), granted_lock_uuid(&second));
        assert!(reopened.log.read_entries().expect("entries").is_empty());

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn follower_rejects_self_leader_hint_without_proxy_loop() {
        let dir = temp_dir("raft-self-leader-hint-proxy");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.role = RaftRole::Follower;
            runtime.leader_id = Some("n1".into());
        }

        let err = raft
            .run_ephemeral(
                single_lock_request("self-proxy-lock", "self-proxy-key"),
                "self-proxy-lock",
                Duration::ZERO,
                true,
            )
            .await
            .expect_err("follower must not proxy to itself");

        assert!(matches!(
            err,
            BrokerRaftError::NotLeader {
                leader_id: Some(ref leader_id),
                leader_addr: None,
            } if leader_id == "n1"
        ));
        assert!(raft.log.read_entries().expect("entries").is_empty());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn leader_peer_hint_uses_one_active_membership_snapshot() {
        let dir = temp_dir("raft-leader-peer-hint");
        let mut cfg = test_raft_config(dir.clone());
        cfg.peers = vec![
            test_peer("n1", 7980),
            test_peer("n2", 8123),
            test_peer("n3", 7982),
        ];
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 4;
            runtime.role = RaftRole::Follower;
            runtime.leader_id = Some("n2".into());
        }

        let (leader_id, leader_peer) = raft.leader_peer_hint();
        assert_eq!(leader_id.as_deref(), Some("n2"));
        assert_eq!(
            leader_peer.as_ref().map(|peer| peer.addr.as_str()),
            Some("127.0.0.1:8123")
        );
        assert_eq!(raft.leader_addr().as_deref(), Some("127.0.0.1:8123"));

        raft.runtime.lock().leader_id = Some("ghost".into());
        let (leader_id, leader_peer) = raft.leader_peer_hint();
        assert_eq!(leader_id.as_deref(), Some("ghost"));
        assert!(leader_peer.is_none());
        assert_eq!(raft.leader_addr(), None);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn leader_peer_hint_uses_cached_hint_when_runtime_lock_is_contended() {
        let dir = temp_dir("raft-leader-peer-hint-cache");
        let mut cfg = test_raft_config(dir.clone());
        cfg.peers = vec![
            test_peer("n1", 7980),
            test_peer("n2", 8123),
            test_peer("n3", 7982),
        ];
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 4;
            runtime.role = RaftRole::Follower;
            runtime.leader_id = Some("n2".into());
        }
        assert_eq!(raft.leader_addr().as_deref(), Some("127.0.0.1:8123"));

        let _runtime_guard = raft.runtime.lock();
        let (leader_id, leader_peer) = raft.leader_peer_hint();
        assert_eq!(leader_id.as_deref(), Some("n2"));
        assert_eq!(
            leader_peer.as_ref().map(|peer| peer.addr.as_str()),
            Some("127.0.0.1:8123")
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn leader_peer_hint_cache_updates_when_membership_address_changes() {
        let dir = temp_dir("raft-leader-peer-hint-cache-membership");
        let mut cfg = test_raft_config(dir.clone());
        cfg.peers = vec![
            test_peer("n1", 7980),
            test_peer("n2", 8123),
            test_peer("n3", 7982),
        ];
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 4;
            runtime.role = RaftRole::Follower;
            runtime.leader_id = Some("n2".into());
        }
        assert_eq!(raft.leader_addr().as_deref(), Some("127.0.0.1:8123"));

        raft.apply_membership(RaftMembership::from_simple(vec![
            test_peer("n1", 7980),
            test_peer("n2", 8133),
            test_peer("n3", 7982),
        ]))
        .expect("apply address change");

        let _runtime_guard = raft.runtime.lock();
        let (leader_id, leader_peer) = raft.leader_peer_hint();
        assert_eq!(leader_id.as_deref(), Some("n2"));
        assert_eq!(
            leader_peer.as_ref().map(|peer| peer.addr.as_str()),
            Some("127.0.0.1:8133")
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn election_loop_action_sleeps_until_follower_deadline() {
        let dir = temp_dir("raft-election-loop-action-sleep");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let now = Instant::now();
        {
            let mut runtime = raft.runtime.lock();
            runtime.role = RaftRole::Follower;
            runtime.election_deadline = now + Duration::from_millis(123);
        }

        assert_eq!(
            raft.election_loop_action_at(now),
            RaftElectionLoopAction::Sleep(Duration::from_millis(123))
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn election_loop_action_starts_election_after_deadline() {
        let dir = temp_dir("raft-election-loop-action-start");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let now = Instant::now();
        {
            let mut runtime = raft.runtime.lock();
            runtime.role = RaftRole::Follower;
            runtime.election_deadline = now - Duration::from_millis(1);
        }

        assert_eq!(
            raft.election_loop_action_at(now),
            RaftElectionLoopAction::StartElection
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn election_loop_action_heartbeats_for_leader() {
        let dir = temp_dir("raft-election-loop-action-heartbeat");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.election_deadline = Instant::now() - Duration::from_secs(1);
        }

        assert_eq!(
            raft.election_loop_action_at(Instant::now()),
            RaftElectionLoopAction::Heartbeat
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn raft_proxy_request_to_follower_is_not_reproxied() {
        let dir = temp_dir("raft-peer-proxy-follower-terminal");
        let mut cfg = test_raft_config(dir.clone());
        cfg.peers = vec![
            test_peer("n1", 7980),
            test_peer("n2", 7981),
            test_peer("n3", 7982),
        ];
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 4;
            runtime.role = RaftRole::Follower;
            runtime.leader_id = Some("n2".into());
        }

        let response = raft
            .handle_rpc(RaftRpc::ProxyRequest {
                auth_token: None,
                request: single_lock_request("peer-proxy-lock", "peer-proxy-key"),
                request_uuid: "peer-proxy-lock".into(),
                wait_ms: 0,
                is_acquire: true,
            })
            .await;

        assert!(matches!(
            response,
            RaftRpcResponse::ProxyResponse {
                term: 4,
                response: None,
                error: Some(ref error),
            } if error.contains("raft node is not leader")
        ));
        assert!(raft.rpc_connections.lock().is_empty());
        assert!(raft.log.read_entries().expect("entries").is_empty());

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn follower_proxy_reuses_pooled_leader_connection() {
        let dir = temp_dir("raft-follower-proxy-pool");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake leader");
        let leader_addr = listener.local_addr().expect("fake leader addr");
        let mut cfg = test_raft_config(dir.clone());
        cfg.peers = vec![
            test_peer("n1", 7980),
            RaftPeerConfig {
                id: "n2".into(),
                addr: leader_addr.to_string(),
            },
            test_peer("n3", 7982),
        ];
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 7;
            runtime.role = RaftRole::Follower;
            runtime.leader_id = Some("n2".into());
        }

        let accepted = Arc::new(AtomicUsize::new(0));
        let accepted_for_server = Arc::clone(&accepted);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept leader proxy");
            accepted_for_server.fetch_add(1, Ordering::SeqCst);
            let mut reader = TokioBufReader::new(stream);
            for i in 0..2 {
                let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                    .await
                    .expect("read proxied request");
                let rpc: RaftRpc = serde_json::from_str(&line).expect("parse proxied request");
                match rpc {
                    RaftRpc::ProxyRequest {
                        auth_token: None,
                        request_uuid,
                        is_acquire: false,
                        ..
                    } => assert_eq!(request_uuid, format!("proxy-pool-{i}")),
                    other => panic!("unexpected proxy rpc: {other:?}"),
                }
                let body = serde_json::to_vec(&RaftRpcResponse::ProxyResponse {
                    term: 7,
                    response: None,
                    error: None,
                })
                .expect("serialize proxy response");
                reader
                    .get_mut()
                    .write_all(&body)
                    .await
                    .expect("write proxy response");
                reader
                    .get_mut()
                    .write_all(b"\n")
                    .await
                    .expect("write proxy newline");
                reader
                    .get_mut()
                    .flush()
                    .await
                    .expect("flush proxy response");
            }
        });

        for i in 0..2 {
            let uuid = format!("proxy-pool-{i}");
            let response = raft
                .run_ephemeral(
                    Request::Unlock {
                        uuid: uuid.clone(),
                        key: Some("proxy-pool-key".into()),
                        keys: None,
                        lock_uuid: Some("proxy-pool-lock".into()),
                        force: false,
                    },
                    &uuid,
                    Duration::from_millis(10),
                    false,
                )
                .await
                .expect("proxied follower request");
            assert!(response.is_none());
        }

        server.await.expect("fake leader server");
        assert_eq!(accepted.load(Ordering::SeqCst), 1);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn raft_client_ids_are_namespaced_by_node_and_do_not_collide_on_drop() {
        let dir1 = temp_dir("raft-client-id-namespace-n1");
        let dir2 = temp_dir("raft-client-id-namespace-n2");
        let cfg1 = test_raft_config(dir1.clone());
        let mut cfg2 = test_raft_config(dir2.clone());
        cfg2.node_id = "n2".into();
        let raft1 = BrokerRaft::open(cfg1).expect("open n1 raft");
        let raft2 = BrokerRaft::open(cfg2).expect("open n2 raft");

        let (client1, _rx1) = raft1.register_client();
        let (client2, _rx2) = raft2.register_client();
        assert_ne!(client1, client2);
        assert_eq!(
            client1 >> RAFT_CLIENT_ID_PREFIX_SHIFT,
            raft_client_id_prefix("n1")
        );
        assert_eq!(
            client2 >> RAFT_CLIENT_ID_PREFIX_SHIFT,
            raft_client_id_prefix("n2")
        );
        assert_eq!(client1 & RAFT_CLIENT_ID_SEQUENCE_MASK, 1);
        assert_eq!(client2 & RAFT_CLIENT_ID_SEQUENCE_MASK, 1);

        raft1.broker.handle_request_with_grant_overrides(
            client1,
            single_lock_request("namespaced-client-lock", "namespaced-client-key"),
            GrantOverrides::default(),
        );
        assert_eq!(raft1.broker.metrics().holders, 1);

        raft1.broker.drop_client(client2);
        assert_eq!(
            raft1.broker.metrics().holders,
            1,
            "a DropClient from another Raft node namespace must not release this holder"
        );

        raft1.broker.drop_client(client1);
        assert_eq!(raft1.broker.metrics().holders, 0);

        let _ = fs::remove_dir_all(dir1);
        let _ = fs::remove_dir_all(dir2);
    }

    #[test]
    fn raft_client_id_sequence_resumes_after_reopen_above_log_tail() {
        let dir = temp_dir("raft-client-id-reopen");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg.clone()).expect("open raft");
        let (first_client, _first_rx) = raft.register_client();
        assert_eq!(first_client & RAFT_CLIENT_ID_SEQUENCE_MASK, 1);
        raft.log
            .append(
                1,
                RaftCommand::ClientRequest {
                    client_id: first_client,
                    request: single_lock_request("client-reopen-lock", "client-reopen-key"),
                    grant: None,
                },
            )
            .expect("append client request");
        drop(raft);

        let reopened = BrokerRaft::open(cfg).expect("reopen raft");
        let (second_client, _second_rx) = reopened.register_client();
        assert_ne!(second_client, first_client);
        assert_eq!(
            second_client >> RAFT_CLIENT_ID_PREFIX_SHIFT,
            first_client >> RAFT_CLIENT_ID_PREFIX_SHIFT
        );
        assert!(second_client & RAFT_CLIENT_ID_SEQUENCE_MASK > 1);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn install_snapshot_retains_suffix_only_when_snapshot_entry_matches() {
        let dir = temp_dir("raft-install-snapshot-retain");
        let store = RaftLogStore::open(&dir).expect("open store");
        for term in [1, 1, 2, 2] {
            store.append(term, RaftCommand::Noop).expect("append");
        }

        let payload = idle_snapshot_payload();
        let snapshot = store
            .install_snapshot_from_leader(2, 1, Some(payload_checksum(&payload)), payload)
            .expect("install matching snapshot");
        assert_eq!(snapshot.last_included_index, 2);
        assert_eq!(
            store
                .read_entries()
                .expect("entries")
                .iter()
                .map(|entry| (entry.index, entry.term))
                .collect::<Vec<_>>(),
            vec![(3, 2), (4, 2)]
        );
        assert_eq!(store.last_index(), 4);
        assert_eq!(store.last_term(), 2);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn install_snapshot_discards_conflicting_suffix() {
        let dir = temp_dir("raft-install-snapshot-discard");
        let store = RaftLogStore::open(&dir).expect("open store");
        for term in [1, 1, 2, 2] {
            store.append(term, RaftCommand::Noop).expect("append");
        }

        let payload = idle_snapshot_payload();
        store
            .install_snapshot_from_leader(3, 9, Some(payload_checksum(&payload)), payload)
            .expect("install conflicting snapshot");

        assert!(store.read_entries().expect("entries").is_empty());
        assert_eq!(store.last_index(), 3);
        assert_eq!(store.last_term(), 9);
        assert_eq!(
            store
                .latest_snapshot()
                .expect("snapshot")
                .last_included_index,
            3
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn snapshot_payload_checksum_is_persisted_and_verified_on_read() {
        let dir = temp_dir("raft-snapshot-checksum");
        let store = RaftLogStore::open(&dir).expect("open store");
        let payload = idle_snapshot_payload();
        let expected = payload_checksum(&payload);

        let metadata = store
            .write_snapshot(4, 2, payload)
            .expect("write checksummed snapshot");

        assert_eq!(metadata.payload_sha256.as_deref(), Some(expected.as_str()));
        let snapshot = read_snapshot_file(&store.snapshot_path)
            .expect("read checksummed snapshot")
            .expect("snapshot exists");
        assert_eq!(
            snapshot.metadata.payload_sha256.as_deref(),
            Some(expected.as_str())
        );

        let mut tampered = serde_json::to_value(&snapshot).expect("snapshot json value");
        tampered["payload"]["note"] = json!("tampered");
        fs::write(
            &store.snapshot_path,
            serde_json::to_vec_pretty(&tampered).expect("tampered snapshot bytes"),
        )
        .expect("write tampered snapshot");

        let err = read_snapshot_file(&store.snapshot_path)
            .expect_err("tampered snapshot must be rejected");
        assert!(matches!(
            err,
            BrokerRaftError::SnapshotChecksumMismatch { index: 4, .. }
        ));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn install_snapshot_from_unknown_leader_does_not_advance_term() {
        let dir = temp_dir("raft-unknown-leader-snapshot");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let (initial_state, initial_deadline) = {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.voted_for = Some("n1".into());
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.election_deadline = Instant::now() + Duration::from_secs(60);
            (runtime.hard_state(), runtime.election_deadline)
        };
        raft.log
            .write_hard_state(&initial_state)
            .expect("persist initial hard state");

        let payload = idle_snapshot_payload();
        let (checksum, data) = snapshot_rpc_parts(&payload);
        let response =
            raft.handle_install_snapshot(9, "ghost".into(), 7, 2, Some(checksum), 0, true, data);
        assert!(matches!(
            response,
            RaftRpcResponse::InstallSnapshot {
                term: 3,
                success: false,
                last_included_index: 0,
            }
        ));

        let hard_state = raft.log.read_hard_state().expect("read hard state");
        assert_eq!(hard_state.current_term, 3);
        assert_eq!(hard_state.voted_for.as_deref(), Some("n1"));
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 3);
            assert_eq!(runtime.voted_for.as_deref(), Some("n1"));
            assert_eq!(runtime.role, RaftRole::Leader);
            assert_eq!(runtime.leader_id.as_deref(), Some("n1"));
            assert_eq!(runtime.election_deadline, initial_deadline);
        }
        assert!(raft.log.latest_snapshot().is_none());
        assert_eq!(raft.log.last_index(), 0);
        assert!(snapshot_part_files(&dir).is_empty());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn install_snapshot_from_same_term_conflicting_leader_is_rejected() {
        let dir = temp_dir("raft-same-term-conflicting-leader-snapshot");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let (initial_state, initial_deadline) = {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.voted_for = None;
            runtime.role = RaftRole::Follower;
            runtime.leader_id = Some("n2".into());
            runtime.election_deadline = Instant::now() + Duration::from_secs(60);
            (runtime.hard_state(), runtime.election_deadline)
        };
        raft.log
            .write_hard_state(&initial_state)
            .expect("persist initial hard state");

        let payload = idle_snapshot_payload();
        let (checksum, data) = snapshot_rpc_parts(&payload);
        let response =
            raft.handle_install_snapshot(3, "n3".into(), 7, 2, Some(checksum), 0, true, data);
        assert!(matches!(
            response,
            RaftRpcResponse::InstallSnapshot {
                term: 3,
                success: false,
                last_included_index: 0,
            }
        ));

        let hard_state = raft.log.read_hard_state().expect("read hard state");
        assert_eq!(hard_state.current_term, 3);
        assert_eq!(hard_state.voted_for, None);
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 3);
            assert_eq!(runtime.voted_for, None);
            assert_eq!(runtime.role, RaftRole::Follower);
            assert_eq!(runtime.leader_id.as_deref(), Some("n2"));
            assert_eq!(runtime.election_deadline, initial_deadline);
        }
        assert!(raft.log.latest_snapshot().is_none());
        assert_eq!(raft.log.last_index(), 0);
        assert!(snapshot_part_files(&dir).is_empty());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn higher_term_install_snapshot_replaces_stale_known_leader() {
        let dir = temp_dir("raft-higher-term-new-leader-snapshot");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let initial_state = {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.voted_for = Some("n2".into());
            runtime.role = RaftRole::Follower;
            runtime.leader_id = Some("n2".into());
            runtime.election_deadline = Instant::now() + Duration::from_secs(60);
            runtime.hard_state()
        };
        raft.log
            .write_hard_state(&initial_state)
            .expect("persist initial hard state");

        let payload = idle_snapshot_payload();
        let (checksum, data) = snapshot_rpc_parts(&payload);
        let response =
            raft.handle_install_snapshot(4, "n3".into(), 7, 2, Some(checksum), 0, true, data);
        assert!(matches!(
            response,
            RaftRpcResponse::InstallSnapshot {
                term: 4,
                success: true,
                last_included_index: 7,
            }
        ));

        let hard_state = raft.log.read_hard_state().expect("read hard state");
        assert_eq!(hard_state.current_term, 4);
        assert_eq!(hard_state.voted_for, None);
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 4);
            assert_eq!(runtime.voted_for, None);
            assert_eq!(runtime.role, RaftRole::Follower);
            assert_eq!(runtime.leader_id.as_deref(), Some("n3"));
            assert_eq!(runtime.commit_index, 7);
            assert_eq!(runtime.last_applied, 7);
        }
        assert_eq!(raft.log.last_index(), 7);
        assert_eq!(raft.log.last_term(), 2);
        assert!(snapshot_part_files(&dir).is_empty());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn handle_install_snapshot_advances_runtime_and_resets_idle_broker_state() {
        let dir = temp_dir("raft-handle-install-snapshot");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
            runtime.commit_index = 1;
            runtime.last_applied = 1;
        }

        let payload = idle_snapshot_payload();
        let (checksum, data) = snapshot_rpc_parts(&payload);
        let response =
            raft.handle_install_snapshot(2, "n2".into(), 7, 2, Some(checksum), 0, true, data);

        assert!(matches!(
            response,
            RaftRpcResponse::InstallSnapshot {
                term: 2,
                success: true,
                last_included_index: 7,
            }
        ));
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 2);
            assert_eq!(runtime.commit_index, 7);
            assert_eq!(runtime.last_applied, 7);
            assert_eq!(runtime.role, RaftRole::Follower);
            assert_eq!(runtime.leader_id.as_deref(), Some("n2"));
        }
        assert_eq!(raft.log.last_index(), 7);
        assert_eq!(raft.log.last_term(), 2);
        assert_eq!(raft.broker.metrics().holders, 0);
        assert_eq!(raft.broker.metrics().waiters, 0);
        assert!(snapshot_part_files(&dir).is_empty());

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn rpc_install_snapshot_installs_via_async_handler() {
        let dir = temp_dir("raft-rpc-install-snapshot-async-handler");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
        }

        let payload = idle_snapshot_payload();
        let (checksum, data) = snapshot_rpc_parts(&payload);
        let response = raft
            .handle_rpc(RaftRpc::InstallSnapshot {
                auth_token: None,
                term: 2,
                leader_id: "n2".into(),
                last_included_index: 7,
                last_included_term: 2,
                payload_sha256: Some(checksum),
                offset: 0,
                done: true,
                data,
            })
            .await;

        assert!(matches!(
            response,
            RaftRpcResponse::InstallSnapshot {
                term: 2,
                success: true,
                last_included_index: 7,
            }
        ));
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 2);
            assert_eq!(runtime.commit_index, 7);
            assert_eq!(runtime.last_applied, 7);
            assert_eq!(runtime.role, RaftRole::Follower);
            assert_eq!(runtime.leader_id.as_deref(), Some("n2"));
        }
        assert_eq!(raft.log.last_index(), 7);
        assert_eq!(raft.log.last_term(), 2);
        assert!(snapshot_part_files(&dir).is_empty());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn stale_install_snapshot_finish_does_not_apply_after_term_change() {
        let dir = temp_dir("raft-install-snapshot-finish-stale-term");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 6;
            runtime.role = RaftRole::Follower;
            runtime.leader_id = Some("n2".into());
            runtime.commit_index = 0;
            runtime.last_applied = 0;
        }

        let payload = idle_snapshot_payload();
        let response = raft.finish_installed_snapshot_payload(5, "n2", 7, &payload);

        assert!(matches!(
            response,
            RaftRpcResponse::InstallSnapshot {
                term: 6,
                success: false,
                last_included_index: 0,
            }
        ));
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.commit_index, 0);
            assert_eq!(runtime.last_applied, 0);
        }
        assert_eq!(raft.broker.metrics().holders, 0);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn handle_install_snapshot_rejects_invalid_membership_payload() {
        let dir = temp_dir("raft-handle-install-snapshot-invalid-membership");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let mut payload = idle_snapshot_payload();
        payload["membership"] = serde_json::to_value(duplicate_peer_membership()).unwrap();
        let (checksum, data) = snapshot_rpc_parts(&payload);

        let response =
            raft.handle_install_snapshot(2, "n2".into(), 7, 2, Some(checksum), 0, true, data);

        assert!(
            matches!(response, RaftRpcResponse::Error { ref error, .. } if error.contains("appears more than once"))
        );
        assert!(raft.log.latest_snapshot().is_none());
        assert_eq!(raft.log.last_index(), 0);
        assert!(snapshot_part_files(&dir).is_empty());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn handle_install_snapshot_persists_higher_term_before_acknowledging_chunk() {
        let dir = temp_dir("raft-install-snapshot-higher-term-chunk-hard-state");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 2;
            runtime.voted_for = Some("n1".into());
        }
        raft.log
            .write_hard_state(&RaftHardState {
                current_term: 2,
                voted_for: Some("n1".into()),
                commit_index: 0,
            })
            .expect("write initial hard state");

        let payload = idle_snapshot_payload();
        let bytes = serde_json::to_vec(&payload).expect("snapshot bytes");
        let checksum = sha256_hex(&bytes);
        let split = bytes.len() / 2;

        let response = raft.handle_install_snapshot(
            5,
            "n2".into(),
            7,
            2,
            Some(checksum),
            0,
            false,
            BASE64.encode(&bytes[..split]),
        );

        assert!(matches!(
            response,
            RaftRpcResponse::InstallSnapshot {
                term: 5,
                success: true,
                last_included_index: 0,
            }
        ));
        let hard_state = raft.log.read_hard_state().expect("read hard state");
        assert_eq!(hard_state.current_term, 5);
        assert_eq!(hard_state.voted_for, None);
        assert_eq!(hard_state.commit_index, 0);
        assert!(raft.log.latest_snapshot().is_none());
        assert_eq!(raft.snapshot_transfers.lock().len(), 1);
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 5);
            assert_eq!(runtime.voted_for, None);
            assert_eq!(runtime.role, RaftRole::Follower);
            assert_eq!(runtime.leader_id.as_deref(), Some("n2"));
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn higher_term_install_snapshot_does_not_mutate_runtime_when_hard_state_write_fails() {
        let dir = temp_dir("raft-install-snapshot-hard-state-fails");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let (initial_state, initial_deadline) = {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 2;
            runtime.voted_for = Some("n1".into());
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.election_deadline = Instant::now() + Duration::from_secs(60);
            (runtime.hard_state(), runtime.election_deadline)
        };
        raft.log
            .write_hard_state(&initial_state)
            .expect("persist initial hard state");
        fs::create_dir_all(dir.join(HARD_STATE_FILE).with_extension("json.tmp"))
            .expect("block hard-state temp path");

        let payload = idle_snapshot_payload();
        let (checksum, data) = snapshot_rpc_parts(&payload);
        let response =
            raft.handle_install_snapshot(5, "n2".into(), 7, 2, Some(checksum), 0, false, data);

        assert!(matches!(response, RaftRpcResponse::Error { term: 5, .. }));
        assert_eq!(
            raft.log.read_hard_state().expect("read hard state"),
            initial_state
        );
        assert!(raft.log.latest_snapshot().is_none());
        assert!(raft.snapshot_transfers.lock().is_empty());
        assert!(snapshot_part_files(&dir).is_empty());
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 2);
            assert_eq!(runtime.voted_for.as_deref(), Some("n1"));
            assert_eq!(runtime.role, RaftRole::Leader);
            assert_eq!(runtime.leader_id.as_deref(), Some("n1"));
            assert_eq!(runtime.election_deadline, initial_deadline);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn final_install_snapshot_hard_state_failure_does_not_apply_broker_or_runtime() {
        let dir = temp_dir("raft-install-snapshot-commit-hard-state-fails");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let (client, _rx) = raft.broker.register_client();
        raft.broker
            .handle_request(client, single_lock_request("local-holder", "survivor"));
        assert_eq!(raft.broker.metrics().holders, 1);
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 2;
            runtime.role = RaftRole::Follower;
            runtime.leader_id = Some("n2".into());
            runtime.commit_index = 0;
            runtime.last_applied = 0;
        }
        let initial_state = RaftHardState {
            current_term: 2,
            voted_for: None,
            commit_index: 0,
        };
        raft.log
            .write_hard_state(&initial_state)
            .expect("persist initial hard state");
        let hard_state_tmp = dir.join(HARD_STATE_FILE).with_extension("json.tmp");
        fs::create_dir_all(&hard_state_tmp).expect("block hard-state temp path");

        let payload = idle_snapshot_payload();
        let (checksum, data) = snapshot_rpc_parts(&payload);
        let response = raft.handle_install_snapshot(
            2,
            "n2".into(),
            7,
            2,
            Some(checksum.clone()),
            0,
            true,
            data.clone(),
        );

        assert!(matches!(response, RaftRpcResponse::Error { term: 2, .. }));
        assert_eq!(
            raft.log.read_hard_state().expect("read hard state"),
            initial_state
        );
        assert_eq!(
            raft.log
                .latest_snapshot()
                .map(|snapshot| snapshot.last_included_index),
            Some(7),
            "the snapshot file may be durable even though commit hard-state persistence failed"
        );
        assert!(snapshot_part_files(&dir).is_empty());
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.commit_index, 0);
            assert_eq!(runtime.last_applied, 0);
        }
        assert_eq!(
            raft.broker.metrics().holders,
            1,
            "broker state must not observe the snapshot before commitIndex is durable"
        );

        fs::remove_dir_all(&hard_state_tmp).expect("unblock hard-state temp path");
        let retry =
            raft.handle_install_snapshot(2, "n2".into(), 7, 2, Some(checksum), 0, true, data);

        assert!(matches!(
            retry,
            RaftRpcResponse::InstallSnapshot {
                term: 2,
                success: true,
                last_included_index: 7
            }
        ));
        assert_eq!(
            raft.log.read_hard_state().expect("read hard state"),
            RaftHardState {
                current_term: 2,
                voted_for: None,
                commit_index: 7,
            }
        );
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.commit_index, 7);
            assert_eq!(runtime.last_applied, 7);
        }
        assert_eq!(raft.broker.metrics().holders, 0);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn final_install_snapshot_sidecar_failure_does_not_advance_runtime_apply_indexes() {
        let dir = temp_dir("raft-install-snapshot-sidecar-fails");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let (local_client, _local_rx) = raft.broker.register_client();
        raft.broker.handle_request(
            local_client,
            single_lock_request("survivor-lock", "survivor-key"),
        );
        assert_eq!(
            raft.broker
                .top_keys(1)
                .first()
                .map(|snapshot| snapshot.key.as_str()),
            Some("survivor-key")
        );
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 2;
            runtime.role = RaftRole::Follower;
            runtime.leader_id = Some("n2".into());
            runtime.commit_index = 0;
            runtime.last_applied = 0;
        }
        raft.log
            .write_hard_state(&RaftHardState {
                current_term: 2,
                voted_for: None,
                commit_index: 0,
            })
            .expect("persist initial hard state");
        let source = Broker::new(BrokerConfig::default());
        let (source_client, _source_rx) = source.register_client();
        source.handle_request(
            source_client,
            single_lock_request("incoming-lock", "incoming-key"),
        );
        let learner = test_peer("n4", 7983);
        let mut payload = json!({
            "nodeId": "snapshot-source",
            "broker": source.snapshot_for_raft().expect("broker snapshot"),
        });
        payload["stagedLearners"] = serde_json::to_value(vec![learner.clone()]).unwrap();
        let (checksum, data) = snapshot_rpc_parts(&payload);
        let learners_tmp = dir.join(LEARNERS_FILE).with_extension("json.tmp");
        fs::create_dir_all(&learners_tmp).expect("block learners temp path");

        let response = raft.handle_install_snapshot(
            2,
            "n2".into(),
            7,
            2,
            Some(checksum.clone()),
            0,
            true,
            data.clone(),
        );

        assert!(matches!(response, RaftRpcResponse::Error { term: 2, .. }));
        assert_eq!(
            raft.log
                .read_hard_state()
                .expect("read committed hard state")
                .commit_index,
            7,
            "commitIndex is durable before the broker observes the snapshot"
        );
        assert_eq!(
            raft.log
                .latest_snapshot()
                .map(|snapshot| snapshot.last_included_index),
            Some(7)
        );
        {
            let runtime = raft.runtime.lock();
            assert_eq!(
                runtime.commit_index, 0,
                "runtime commit_index must not move until snapshot sidecars apply"
            );
            assert_eq!(
                runtime.last_applied, 0,
                "runtime last_applied must not move until snapshot sidecars apply"
            );
        }
        assert!(raft.staged_learners().is_empty());
        assert_eq!(
            raft.broker
                .top_keys(1)
                .first()
                .map(|snapshot| snapshot.key.as_str()),
            Some("survivor-key"),
            "broker state must not observe the incoming snapshot before sidecars apply"
        );
        assert!(snapshot_part_files(&dir).is_empty());

        fs::remove_dir_all(&learners_tmp).expect("unblock learners temp path");
        let retry =
            raft.handle_install_snapshot(2, "n2".into(), 7, 2, Some(checksum), 0, true, data);

        assert!(matches!(
            retry,
            RaftRpcResponse::InstallSnapshot {
                term: 2,
                success: true,
                last_included_index: 7
            }
        ));
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.commit_index, 7);
            assert_eq!(runtime.last_applied, 7);
        }
        assert_eq!(raft.staged_learners(), vec![learner]);
        assert_eq!(
            raft.broker
                .top_keys(1)
                .first()
                .map(|snapshot| snapshot.key.as_str()),
            Some("incoming-key")
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn final_install_snapshot_membership_vote_clear_preserves_durable_commit_index() {
        let dir = temp_dir("raft-install-snapshot-membership-commit-preserved");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 2;
            runtime.voted_for = Some("n3".into());
            runtime.role = RaftRole::Follower;
            runtime.leader_id = Some("n2".into());
            runtime.commit_index = 0;
            runtime.last_applied = 0;
        }
        raft.log
            .write_hard_state(&RaftHardState {
                current_term: 2,
                voted_for: Some("n3".into()),
                commit_index: 0,
            })
            .expect("persist initial hard state");
        let mut payload = idle_snapshot_payload();
        payload["membership"] = serde_json::to_value(RaftMembership::from_simple(vec![
            test_peer("n1", 7980),
            test_peer("n2", 7981),
            test_peer("n4", 7983),
        ]))
        .unwrap();
        let (checksum, data) = snapshot_rpc_parts(&payload);

        let response =
            raft.handle_install_snapshot(2, "n2".into(), 7, 2, Some(checksum), 0, true, data);

        assert!(matches!(
            response,
            RaftRpcResponse::InstallSnapshot {
                term: 2,
                success: true,
                last_included_index: 7
            }
        ));
        assert_eq!(
            raft.log.read_hard_state().expect("read hard state"),
            RaftHardState {
                current_term: 2,
                voted_for: None,
                commit_index: 7,
            },
            "membership vote cleanup must not regress the durable snapshot commit index"
        );
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.voted_for, None);
            assert_eq!(runtime.commit_index, 7);
            assert_eq!(runtime.last_applied, 7);
            assert!(runtime.membership.contains_id("n4"));
            assert!(!runtime.membership.contains_id("n3"));
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn open_normalizes_commit_index_to_durable_snapshot_index() {
        let dir = temp_dir("raft-open-normalizes-snapshot-commit");
        let cfg = test_raft_config(dir.clone());
        let store = RaftLogStore::open(&dir).expect("open store");
        store
            .write_snapshot(7, 3, idle_snapshot_payload())
            .expect("write durable snapshot");
        store
            .write_hard_state(&RaftHardState {
                current_term: 3,
                voted_for: None,
                commit_index: 0,
            })
            .expect("write stale hard-state commit");
        drop(store);

        let raft = BrokerRaft::open(cfg).expect("open raft with durable snapshot");

        assert_eq!(
            raft.log.read_hard_state().expect("read hard state"),
            RaftHardState {
                current_term: 3,
                voted_for: None,
                commit_index: 7,
            }
        );
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.commit_index, 7);
            assert_eq!(runtime.last_applied, 7);
        }
        assert_eq!(raft.broker.metrics().holders, 0);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn open_rejects_hard_state_commit_index_past_available_log() {
        let dir = temp_dir("raft-open-hard-state-commit-past-log");
        let cfg = test_raft_config(dir.clone());
        let store = RaftLogStore::open(&dir).expect("open store");
        store
            .append(3, RaftCommand::Noop)
            .expect("append only available entry");
        let hard_state = RaftHardState {
            current_term: 3,
            voted_for: None,
            commit_index: 2,
        };
        store
            .write_hard_state(&hard_state)
            .expect("write corrupt hard-state commit index");
        drop(store);

        let err = match BrokerRaft::open(cfg) {
            Ok(_) => panic!("commit past log tail must be rejected"),
            Err(err) => err,
        };

        assert!(matches!(err, BrokerRaftError::InvalidLog(_)));
        assert_eq!(
            read_hard_state(&dir.join(HARD_STATE_FILE)).expect("read durable hard state"),
            hard_state,
            "startup must not silently lower a durable committed index"
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn handle_install_snapshot_installs_after_final_chunk() {
        let dir = temp_dir("raft-handle-install-snapshot-chunked");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let payload = idle_snapshot_payload();
        let bytes = serde_json::to_vec(&payload).expect("snapshot bytes");
        let checksum = sha256_hex(&bytes);
        let split = bytes.len() / 2;

        let first = raft.handle_install_snapshot(
            2,
            "n2".into(),
            7,
            2,
            Some(checksum.clone()),
            0,
            false,
            BASE64.encode(&bytes[..split]),
        );
        assert!(matches!(
            first,
            RaftRpcResponse::InstallSnapshot {
                term: 2,
                success: true,
                last_included_index: 0,
            }
        ));
        assert!(raft.log.latest_snapshot().is_none());
        let (staged_path, staged_len) = {
            let transfers = raft.snapshot_transfers.lock();
            assert_eq!(transfers.len(), 1);
            let pending = transfers.values().next().expect("pending transfer");
            (pending.path.clone(), pending.bytes_written)
        };
        assert_eq!(staged_len, split as u64);
        assert_eq!(
            fs::metadata(&staged_path).expect("staged file").len(),
            split as u64
        );
        assert_eq!(snapshot_part_files(&dir), vec![staged_path.clone()]);

        let second = raft.handle_install_snapshot(
            2,
            "n2".into(),
            7,
            2,
            Some(checksum),
            split as u64,
            true,
            BASE64.encode(&bytes[split..]),
        );
        assert!(matches!(
            second,
            RaftRpcResponse::InstallSnapshot {
                term: 2,
                success: true,
                last_included_index: 7,
            }
        ));
        assert_eq!(
            raft.log
                .latest_snapshot()
                .expect("installed snapshot")
                .last_included_index,
            7
        );
        assert_eq!(raft.broker.metrics().holders, 0);
        assert!(raft.snapshot_transfers.lock().is_empty());
        assert!(!staged_path.exists());
        assert!(snapshot_part_files(&dir).is_empty());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn handle_install_snapshot_cleans_stale_partial_transfer_before_new_chunk() {
        let dir = temp_dir("raft-handle-install-snapshot-stale-transfer");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let payload = idle_snapshot_payload();
        let bytes = serde_json::to_vec(&payload).expect("snapshot bytes");
        let checksum = sha256_hex(&bytes);
        let split = bytes.len() / 2;

        let first = raft.handle_install_snapshot(
            2,
            "n2".into(),
            7,
            2,
            Some(checksum.clone()),
            0,
            false,
            BASE64.encode(&bytes[..split]),
        );
        assert!(matches!(
            first,
            RaftRpcResponse::InstallSnapshot {
                term: 2,
                success: true,
                last_included_index: 0,
            }
        ));
        let old_path = {
            let mut transfers = raft.snapshot_transfers.lock();
            assert_eq!(transfers.len(), 1);
            let pending = transfers.values_mut().next().expect("pending transfer");
            pending.updated_at_ms = unix_ms().saturating_sub(SNAPSHOT_TRANSFER_STALE_MS + 1);
            pending.path.clone()
        };
        assert!(old_path.exists());

        let next = raft.handle_install_snapshot(
            2,
            "n2".into(),
            8,
            2,
            Some(checksum),
            0,
            false,
            BASE64.encode(&bytes[..split]),
        );
        assert!(matches!(
            next,
            RaftRpcResponse::InstallSnapshot {
                term: 2,
                success: true,
                last_included_index: 0,
            }
        ));
        assert!(!old_path.exists());
        let new_path = {
            let transfers = raft.snapshot_transfers.lock();
            assert_eq!(transfers.len(), 1);
            transfers
                .values()
                .next()
                .expect("new transfer")
                .path
                .clone()
        };
        assert_ne!(old_path, new_path);
        assert!(new_path.exists());
        assert_eq!(snapshot_part_files(&dir), vec![new_path]);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn handle_install_snapshot_rejects_out_of_order_chunk_and_clears_transfer() {
        let dir = temp_dir("raft-handle-install-snapshot-bad-offset");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let payload = idle_snapshot_payload();
        let bytes = serde_json::to_vec(&payload).expect("snapshot bytes");
        let checksum = sha256_hex(&bytes);
        let split = bytes.len() / 2;

        let first = raft.handle_install_snapshot(
            2,
            "n2".into(),
            7,
            2,
            Some(checksum.clone()),
            0,
            false,
            BASE64.encode(&bytes[..split]),
        );
        assert!(matches!(
            first,
            RaftRpcResponse::InstallSnapshot {
                term: 2,
                success: true,
                last_included_index: 0,
            }
        ));
        let staged_path = {
            let transfers = raft.snapshot_transfers.lock();
            assert_eq!(transfers.len(), 1);
            transfers
                .values()
                .next()
                .expect("pending transfer")
                .path
                .clone()
        };
        assert!(staged_path.exists());

        let bad = raft.handle_install_snapshot(
            2,
            "n2".into(),
            7,
            2,
            Some(checksum),
            split.saturating_add(1) as u64,
            true,
            BASE64.encode(&bytes[split..]),
        );
        assert!(
            matches!(bad, RaftRpcResponse::Error { ref error, .. } if error.contains("offset mismatch"))
        );
        assert!(raft.snapshot_transfers.lock().is_empty());
        assert!(!staged_path.exists());
        assert!(snapshot_part_files(&dir).is_empty());
        assert!(raft.log.latest_snapshot().is_none());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn handle_install_snapshot_duplicate_staged_chunk_is_idempotent() {
        let dir = temp_dir("raft-handle-install-snapshot-duplicate-chunk");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let mut payload = idle_snapshot_payload();
        payload["note"] = json!("x".repeat(512));
        let bytes = serde_json::to_vec(&payload).expect("snapshot bytes");
        let checksum = sha256_hex(&bytes);
        let first_end = bytes.len() / 3;
        let second_end = bytes.len() * 2 / 3;

        for (offset, done, range) in [
            (0, false, 0..first_end),
            (first_end as u64, false, first_end..second_end),
        ] {
            let response = raft.handle_install_snapshot(
                2,
                "n2".into(),
                7,
                2,
                Some(checksum.clone()),
                offset,
                done,
                BASE64.encode(&bytes[range]),
            );
            assert!(matches!(
                response,
                RaftRpcResponse::InstallSnapshot {
                    term: 2,
                    success: true,
                    last_included_index: 0,
                }
            ));
        }
        let staged_path = {
            let transfers = raft.snapshot_transfers.lock();
            assert_eq!(transfers.len(), 1);
            let pending = transfers.values().next().expect("pending transfer");
            assert_eq!(pending.bytes_written, second_end as u64);
            pending.path.clone()
        };
        assert_eq!(
            fs::metadata(&staged_path).expect("staged file").len(),
            second_end as u64
        );

        let duplicate = raft.handle_install_snapshot(
            2,
            "n2".into(),
            7,
            2,
            Some(checksum.clone()),
            first_end as u64,
            false,
            BASE64.encode(&bytes[first_end..second_end]),
        );
        assert!(matches!(
            duplicate,
            RaftRpcResponse::InstallSnapshot {
                term: 2,
                success: true,
                last_included_index: 0,
            }
        ));
        assert_eq!(
            fs::metadata(&staged_path)
                .expect("staged file after duplicate")
                .len(),
            second_end as u64,
            "duplicate chunk must not be appended twice"
        );

        let final_chunk = raft.handle_install_snapshot(
            2,
            "n2".into(),
            7,
            2,
            Some(checksum),
            second_end as u64,
            true,
            BASE64.encode(&bytes[second_end..]),
        );
        assert!(matches!(
            final_chunk,
            RaftRpcResponse::InstallSnapshot {
                term: 2,
                success: true,
                last_included_index: 7,
            }
        ));
        assert!(raft.snapshot_transfers.lock().is_empty());
        assert!(!staged_path.exists());
        assert_eq!(
            raft.log
                .latest_snapshot()
                .expect("installed snapshot")
                .last_included_index,
            7
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn handle_install_snapshot_rejects_missing_or_bad_checksum() {
        let dir = temp_dir("raft-handle-install-snapshot-checksum");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");

        let missing_payload = idle_snapshot_payload();
        let (_, missing_data) = snapshot_rpc_parts(&missing_payload);
        let missing =
            raft.handle_install_snapshot(2, "n2".into(), 7, 2, None, 0, true, missing_data);
        assert!(
            matches!(missing, RaftRpcResponse::Error { ref error, .. } if error.contains("missing payload checksum"))
        );

        let bad = raft.handle_install_snapshot(
            2,
            "n2".into(),
            7,
            2,
            Some("not-the-right-checksum".into()),
            0,
            true,
            snapshot_rpc_parts(&idle_snapshot_payload()).1,
        );
        assert!(
            matches!(bad, RaftRpcResponse::Error { ref error, .. } if error.contains("checksum mismatch"))
        );
        assert!(raft.snapshot_transfers.lock().is_empty());
        assert!(snapshot_part_files(&dir).is_empty());
        assert!(raft.log.latest_snapshot().is_none());
        assert_eq!(raft.log.last_index(), 0);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn handle_install_snapshot_ignores_stale_payload_membership() {
        let dir = temp_dir("raft-handle-install-snapshot-stale-membership");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        raft.apply_membership(RaftMembership::from_simple(five_test_peers()))
            .expect("apply current membership");
        let mut current_payload = idle_snapshot_payload();
        current_payload["membership"] =
            serde_json::to_value(RaftMembership::from_simple(five_test_peers())).unwrap();
        raft.log
            .write_snapshot(10, 3, current_payload)
            .expect("write newer local snapshot");

        let mut stale_payload = idle_snapshot_payload();
        stale_payload["membership"] = serde_json::to_value(duplicate_peer_membership()).unwrap();
        let (checksum, data) = snapshot_rpc_parts(&stale_payload);
        let response =
            raft.handle_install_snapshot(4, "n2".into(), 7, 2, Some(checksum), 0, true, data);

        assert!(matches!(
            response,
            RaftRpcResponse::InstallSnapshot {
                term: 4,
                success: true,
                last_included_index: 10,
            }
        ));
        assert_eq!(raft.active_cluster_size(), 5);
        assert_eq!(raft.active_quorum_size(), 3);
        assert!(raft.membership().contains_id("n5"));
        assert_eq!(
            raft.log
                .latest_snapshot()
                .expect("newer snapshot remains installed")
                .last_included_index,
            10
        );
        assert!(snapshot_part_files(&dir).is_empty());

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn install_snapshot_to_peer_sends_payload_in_chunks() {
        let dir = temp_dir("raft-send-install-snapshot-chunked");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake snapshot peer");
        let peer_addr = listener.local_addr().expect("fake snapshot peer addr");
        let mut cfg = test_raft_config(dir.clone());
        cfg.install_snapshot_chunk_bytes = 64;
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let mut payload = idle_snapshot_payload();
        payload["note"] = json!("x".repeat(512));
        let expected_bytes = serde_json::to_vec(&payload).expect("snapshot bytes");
        let expected_checksum = sha256_hex(&expected_bytes);
        raft.log
            .write_snapshot(7, 3, payload)
            .expect("write source snapshot");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 4;
            runtime.role = RaftRole::Leader;
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
        }

        let received = Arc::new(Mutex::new(Vec::new()));
        let received_for_server = Arc::clone(&received);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept snapshot peer");
            let mut reader = TokioBufReader::new(stream);
            let mut expected_offset = 0u64;
            loop {
                let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                    .await
                    .expect("read install snapshot chunk");
                let rpc: RaftRpc =
                    serde_json::from_str(&line).expect("parse install snapshot chunk");
                let (term, index, checksum, offset, done, data) = match rpc {
                    RaftRpc::InstallSnapshot {
                        term,
                        last_included_index,
                        payload_sha256,
                        offset,
                        done,
                        data,
                        ..
                    } => (
                        term,
                        last_included_index,
                        payload_sha256.expect("checksum"),
                        offset,
                        done,
                        data,
                    ),
                    other => panic!("unexpected rpc: {other:?}"),
                };
                assert_eq!(checksum, expected_checksum);
                assert_eq!(offset, expected_offset);
                let chunk = decode_snapshot_chunk(&data).expect("decode chunk");
                expected_offset += chunk.len() as u64;
                received_for_server.lock().extend_from_slice(&chunk);
                let response = RaftRpcResponse::InstallSnapshot {
                    term,
                    success: true,
                    last_included_index: if done { index } else { 0 },
                };
                let body = serde_json::to_vec(&response).expect("snapshot response");
                reader
                    .get_mut()
                    .write_all(&body)
                    .await
                    .expect("write snapshot response");
                reader
                    .get_mut()
                    .write_all(b"\n")
                    .await
                    .expect("write snapshot newline");
                reader
                    .get_mut()
                    .flush()
                    .await
                    .expect("flush snapshot response");
                if done {
                    break;
                }
            }
        });

        let peer = RaftPeerConfig {
            id: "n2".into(),
            addr: peer_addr.to_string(),
        };
        let outcome = raft
            .install_snapshot_to_peer(peer, 4, Some(7))
            .await
            .expect("install snapshot to fake peer");
        assert!(outcome.target_reached);
        server.await.expect("snapshot peer server");
        assert_eq!(received.lock().as_slice(), expected_bytes.as_slice());
        let expected_chunks = (expected_bytes.len() + 63) / 64;
        let telemetry = raft.telemetry_snapshot();
        assert_eq!(
            telemetry.install_snapshot_chunks_total,
            expected_chunks as u64
        );
        assert_eq!(
            telemetry.install_snapshot_bytes_total,
            expected_bytes.len() as u64
        );
        assert_eq!(telemetry.install_snapshot_progress_updates_total, 1);
        let metrics = raft.raft_metrics_text();
        assert!(metrics.contains(&format!(
            "dd_rust_network_mutex_raft_install_snapshot_chunks_total {expected_chunks}"
        )));
        assert!(metrics.contains(&format!(
            "dd_rust_network_mutex_raft_install_snapshot_bytes_total {}",
            expected_bytes.len()
        )));
        assert!(metrics
            .contains("dd_rust_network_mutex_raft_install_snapshot_progress_updates_total 1"));
        assert_eq!(
            raft.runtime
                .lock()
                .leader_progress
                .get("n2")
                .expect("progress")
                .match_index,
            7
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn install_snapshot_to_peer_stops_when_peer_already_has_snapshot() {
        let dir = temp_dir("raft-send-install-snapshot-peer-current");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake snapshot peer");
        let peer_addr = listener.local_addr().expect("fake snapshot peer addr");
        let mut cfg = test_raft_config(dir.clone());
        cfg.install_snapshot_chunk_bytes = 64;
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let mut payload = idle_snapshot_payload();
        payload["note"] = json!("x".repeat(512));
        raft.log
            .write_snapshot(7, 3, payload)
            .expect("write source snapshot");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 4;
            runtime.role = RaftRole::Leader;
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
        }

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept snapshot peer");
            let mut reader = TokioBufReader::new(stream);
            let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                .await
                .expect("read first install snapshot chunk");
            let rpc: RaftRpc = serde_json::from_str(&line).expect("parse install snapshot chunk");
            let term = match rpc {
                RaftRpc::InstallSnapshot {
                    term,
                    last_included_index,
                    offset,
                    done,
                    ..
                } => {
                    assert_eq!(last_included_index, 7);
                    assert_eq!(offset, 0);
                    assert!(!done, "test payload should require more than one chunk");
                    term
                }
                other => panic!("unexpected rpc: {other:?}"),
            };
            let response = RaftRpcResponse::InstallSnapshot {
                term,
                success: true,
                last_included_index: 7,
            };
            let body = serde_json::to_vec(&response).expect("snapshot response");
            reader
                .get_mut()
                .write_all(&body)
                .await
                .expect("write snapshot response");
            reader
                .get_mut()
                .write_all(b"\n")
                .await
                .expect("write snapshot newline");
            reader
                .get_mut()
                .flush()
                .await
                .expect("flush snapshot response");

            let second = tokio::time::timeout(
                Duration::from_millis(150),
                read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes()),
            )
            .await;
            assert!(
                second.is_err(),
                "leader should stop streaming chunks once peer reports the snapshot index"
            );
        });

        let peer = RaftPeerConfig {
            id: "n2".into(),
            addr: peer_addr.to_string(),
        };
        let outcome = raft
            .install_snapshot_to_peer(peer, 4, Some(7))
            .await
            .expect("install snapshot to fake peer");
        assert!(outcome.target_reached);
        server.await.expect("snapshot peer server");
        let progress = raft
            .runtime
            .lock()
            .leader_progress
            .get("n2")
            .copied()
            .expect("progress");
        assert_eq!(progress.match_index, 7);
        assert_eq!(progress.next_index, 8);

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn install_snapshot_response_after_stepdown_does_not_update_progress() {
        let dir = temp_dir("raft-send-install-snapshot-after-stepdown");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake snapshot peer");
        let peer_addr = listener.local_addr().expect("fake snapshot peer addr");
        let mut cfg = test_raft_config(dir.clone());
        cfg.install_snapshot_chunk_bytes = 64;
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let mut payload = idle_snapshot_payload();
        payload["note"] = json!("x".repeat(512));
        raft.log
            .write_snapshot(7, 3, payload)
            .expect("write source snapshot");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 4;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
        }

        let (request_seen_tx, request_seen_rx) = oneshot::channel();
        let (reply_tx, reply_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept snapshot peer");
            let mut reader = TokioBufReader::new(stream);
            let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                .await
                .expect("read first install snapshot chunk");
            let rpc: RaftRpc = serde_json::from_str(&line).expect("parse install snapshot chunk");
            let term = match rpc {
                RaftRpc::InstallSnapshot {
                    term,
                    last_included_index,
                    offset,
                    ..
                } => {
                    assert_eq!(last_included_index, 7);
                    assert_eq!(offset, 0);
                    term
                }
                other => panic!("unexpected rpc: {other:?}"),
            };
            request_seen_tx.send(()).expect("signal request seen");
            reply_rx.await.expect("wait for stepdown");
            let response = RaftRpcResponse::InstallSnapshot {
                term,
                success: true,
                last_included_index: 7,
            };
            let body = serde_json::to_vec(&response).expect("snapshot response");
            reader
                .get_mut()
                .write_all(&body)
                .await
                .expect("write snapshot response");
            reader
                .get_mut()
                .write_all(b"\n")
                .await
                .expect("write snapshot newline");
            reader
                .get_mut()
                .flush()
                .await
                .expect("flush snapshot response");
        });

        let peer = RaftPeerConfig {
            id: "n2".into(),
            addr: peer_addr.to_string(),
        };
        let node = raft.clone();
        let install =
            tokio::spawn(async move { node.install_snapshot_to_peer(peer, 4, Some(7)).await });
        request_seen_rx
            .await
            .expect("snapshot request should be sent");
        raft.step_down(5, None);
        reply_tx.send(()).expect("release fake peer response");
        let outcome = install
            .await
            .expect("snapshot task")
            .expect("snapshot result");

        assert!(!outcome.contacted);
        assert!(!outcome.target_reached);
        server.await.expect("fake snapshot peer server");
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 5);
            assert_eq!(runtime.role, RaftRole::Follower);
            let progress = runtime
                .leader_progress
                .get("n2")
                .copied()
                .expect("progress");
            assert_eq!(progress.match_index, 0);
            assert_eq!(progress.next_index, 1);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn install_snapshot_response_index_is_capped_to_sent_snapshot() {
        let dir = temp_dir("raft-send-install-snapshot-inflated-index");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake snapshot peer");
        let peer_addr = listener.local_addr().expect("fake snapshot peer addr");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        raft.log
            .write_snapshot(7, 3, idle_snapshot_payload())
            .expect("write source snapshot");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 4;
            runtime.role = RaftRole::Leader;
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
        }

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept snapshot peer");
            let mut reader = TokioBufReader::new(stream);
            let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                .await
                .expect("read install snapshot");
            let rpc: RaftRpc = serde_json::from_str(&line).expect("parse install snapshot");
            let term = match rpc {
                RaftRpc::InstallSnapshot {
                    term,
                    last_included_index,
                    done,
                    ..
                } => {
                    assert_eq!(last_included_index, 7);
                    assert!(done);
                    term
                }
                other => panic!("unexpected rpc: {other:?}"),
            };
            let response = RaftRpcResponse::InstallSnapshot {
                term,
                success: true,
                last_included_index: 99,
            };
            let body = serde_json::to_vec(&response).expect("snapshot response");
            reader
                .get_mut()
                .write_all(&body)
                .await
                .expect("write snapshot response");
            reader
                .get_mut()
                .write_all(b"\n")
                .await
                .expect("write snapshot newline");
            reader
                .get_mut()
                .flush()
                .await
                .expect("flush snapshot response");
        });

        let peer = RaftPeerConfig {
            id: "n2".into(),
            addr: peer_addr.to_string(),
        };
        let outcome = raft
            .install_snapshot_to_peer(peer, 4, Some(99))
            .await
            .expect("install snapshot to fake peer");
        assert!(
            !outcome.target_reached,
            "inflated snapshot index must not satisfy a target beyond the sent snapshot"
        );
        server.await.expect("snapshot peer server");
        let progress = raft
            .runtime
            .lock()
            .leader_progress
            .get("n2")
            .copied()
            .expect("progress");
        assert_eq!(progress.match_index, 7);
        assert_eq!(progress.next_index, 8);

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn install_snapshot_final_ack_below_snapshot_index_does_not_reach_target() {
        let dir = temp_dir("raft-send-install-snapshot-underreported-index");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake snapshot peer");
        let peer_addr = listener.local_addr().expect("fake snapshot peer addr");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        raft.log
            .write_snapshot(7, 3, idle_snapshot_payload())
            .expect("write source snapshot");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 4;
            runtime.role = RaftRole::Leader;
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
        }

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept snapshot peer");
            let mut reader = TokioBufReader::new(stream);
            let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                .await
                .expect("read install snapshot");
            let rpc: RaftRpc = serde_json::from_str(&line).expect("parse install snapshot");
            let term = match rpc {
                RaftRpc::InstallSnapshot {
                    term,
                    last_included_index,
                    done,
                    ..
                } => {
                    assert_eq!(last_included_index, 7);
                    assert!(done);
                    term
                }
                other => panic!("unexpected rpc: {other:?}"),
            };
            let response = RaftRpcResponse::InstallSnapshot {
                term,
                success: true,
                last_included_index: 0,
            };
            let body = serde_json::to_vec(&response).expect("snapshot response");
            reader
                .get_mut()
                .write_all(&body)
                .await
                .expect("write snapshot response");
            reader
                .get_mut()
                .write_all(b"\n")
                .await
                .expect("write snapshot newline");
            reader
                .get_mut()
                .flush()
                .await
                .expect("flush snapshot response");
        });

        let peer = RaftPeerConfig {
            id: "n2".into(),
            addr: peer_addr.to_string(),
        };
        let outcome = raft
            .install_snapshot_to_peer(peer, 4, None)
            .await
            .expect("install snapshot to fake peer");
        assert!(outcome.contacted);
        assert!(
            !outcome.target_reached,
            "an underreported final snapshot ack must not satisfy replication progress"
        );
        server.await.expect("fake snapshot peer server");
        let progress = raft
            .runtime
            .lock()
            .leader_progress
            .get("n2")
            .copied()
            .expect("progress");
        assert_eq!(progress.match_index, 0);
        assert_eq!(progress.next_index, 1);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn leader_conflict_hint_jumps_to_last_local_index_for_term() {
        let dir = temp_dir("raft-conflict-next-index");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        for term in [1, 2, 2, 3] {
            raft.log.append(term, RaftCommand::Noop).expect("append");
        }

        assert_eq!(
            raft.next_index_after_conflict(Some(2), Some(1), 5)
                .expect("known term jump"),
            4
        );
        assert_eq!(
            raft.next_index_after_conflict(Some(9), Some(2), 5)
                .expect("unknown term jump"),
            2
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn leader_conflict_fallback_clamps_to_retained_snapshot_boundary() {
        let dir = temp_dir("raft-conflict-clamps-snapshot-boundary");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        for term in [1, 2, 2, 3] {
            raft.log.append(term, RaftCommand::Noop).expect("append");
        }
        raft.log
            .write_snapshot(3, 2, idle_snapshot_payload())
            .expect("write snapshot");
        raft.log
            .compact_to_latest_snapshot()
            .expect("compact snapshot prefix");

        assert_eq!(raft.initial_replication_next_index(), 4);
        assert_eq!(
            raft.next_index_after_conflict(None, None, 2)
                .expect("no hint fallback"),
            4
        );
        assert_eq!(
            raft.next_index_after_conflict(None, Some(1), 5)
                .expect("low conflict index fallback"),
            4
        );
        assert_eq!(
            raft.next_index_after_conflict(Some(2), Some(1), 5)
                .expect("snapshot term fallback"),
            4
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn leader_unknown_conflict_term_keeps_conflict_index_above_snapshot_boundary() {
        let dir = temp_dir("raft-conflict-index-above-boundary");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        for term in [1, 2, 2, 3, 3] {
            raft.log.append(term, RaftCommand::Noop).expect("append");
        }
        raft.log
            .write_snapshot(3, 2, idle_snapshot_payload())
            .expect("write snapshot");
        raft.log
            .compact_to_latest_snapshot()
            .expect("compact snapshot prefix");

        assert_eq!(
            raft.next_index_after_conflict(Some(99), Some(5), 8)
                .expect("unknown term fallback"),
            5
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn last_index_for_term_uses_retained_term_index_without_full_log_scan() {
        let dir = temp_dir("raft-term-index-no-scan");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        for term in [1, 2, 2, 3] {
            raft.log.append(term, RaftCommand::Noop).expect("append");
        }
        append_raw_log_line(&dir, "{not valid json");

        assert_eq!(
            raft.log
                .last_index_for_term(2)
                .expect("term lookup should not scan the whole log"),
            Some(3)
        );
        assert!(
            raft.log.read_entries().is_err(),
            "full log reads should still detect the malformed appended line"
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn term_at_uses_retained_index_without_full_log_scan() {
        let dir = temp_dir("raft-term-at-index-no-scan");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        for term in [1, 2, 2, 3] {
            raft.log.append(term, RaftCommand::Noop).expect("append");
        }
        append_raw_log_line(&dir, "{not valid json");

        assert_eq!(
            raft.log
                .term_at(3)
                .expect("term lookup should not scan the whole log"),
            Some(2)
        );
        assert!(
            raft.log.read_entries().is_err(),
            "full log reads should still detect the malformed appended line"
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn append_entries_conflict_hint_uses_retained_first_term_index() {
        let dir = temp_dir("raft-conflict-first-term-index-no-scan");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        for term in [1, 2, 2, 3] {
            raft.log.append(term, RaftCommand::Noop).expect("append");
        }
        append_raw_log_line(&dir, "{not valid json");

        let report = raft
            .log
            .append_entries_from_leader(3, 99, 4, 0, Vec::new())
            .expect("conflict hint should not scan the whole log");
        assert!(!report.success);
        assert_eq!(report.conflict_term, Some(2));
        assert_eq!(report.conflict_index, Some(2));
        assert!(
            raft.log.read_entries().is_err(),
            "full log reads should still detect the malformed appended line"
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn retained_term_index_rebuilds_after_snapshot_compaction() {
        let dir = temp_dir("raft-term-index-compaction");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        for term in [1, 2, 2, 3] {
            raft.log.append(term, RaftCommand::Noop).expect("append");
        }
        raft.log
            .write_snapshot(3, 2, idle_snapshot_payload())
            .expect("write snapshot");
        raft.log
            .compact_to_latest_snapshot()
            .expect("compact snapshot prefix");

        assert_eq!(
            raft.log
                .last_index_for_term(2)
                .expect("snapshot boundary fallback"),
            Some(3)
        );
        assert_eq!(
            raft.log.last_index_for_term(3).expect("retained term"),
            Some(4)
        );
        assert_eq!(
            raft.log.last_index_for_term(1).expect("compacted old term"),
            None
        );
        assert_eq!(
            raft.log.term_at(3).expect("snapshot term fallback"),
            Some(2)
        );
        assert_eq!(raft.log.term_at(4).expect("retained term"), Some(3));
        assert_eq!(raft.log.term_at(2).expect("compacted term lookup"), None);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn leader_advances_commit_from_match_indexes_for_current_term_entry() {
        let dir = temp_dir("raft-leader-advance-commit-current-term");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        raft.log.append(1, RaftCommand::Noop).expect("append old");
        raft.log
            .append(2, RaftCommand::Noop)
            .expect("append current");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 2;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.commit_index = 0;
            runtime.last_applied = 0;
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 3,
                    match_index: 2,
                },
            );
            runtime.leader_progress.insert(
                "n3".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
        }

        let advanced = raft
            .advance_leader_commit_from_progress()
            .expect("advance leader commit");
        assert_eq!(advanced, Some(2));
        let hard_state = raft.log.read_hard_state().expect("read hard state");
        assert_eq!(
            hard_state.commit_index, 2,
            "leader commitIndex is persisted before applying committed entries so restart replay cannot lose them"
        );
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.commit_index, 2);
            assert_eq!(runtime.last_applied, 2);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn leader_progress_async_commit_finalization_persists_and_applies() {
        let dir = temp_dir("raft-leader-advance-commit-async");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        raft.log.append(1, RaftCommand::Noop).expect("append old");
        raft.log
            .append(2, RaftCommand::Noop)
            .expect("append current");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 2;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.commit_index = 0;
            runtime.last_applied = 0;
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 3,
                    match_index: 2,
                },
            );
            runtime.leader_progress.insert(
                "n3".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
        }

        let advanced = raft
            .advance_leader_commit_from_progress_blocking()
            .await
            .expect("advance leader commit on blocking pool");
        assert_eq!(advanced, Some(2));
        assert_eq!(
            raft.log.read_hard_state().expect("read hard state"),
            RaftHardState {
                current_term: 2,
                voted_for: None,
                commit_index: 2,
            }
        );
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.commit_index, 2);
            assert_eq!(runtime.last_applied, 2);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn leader_does_not_advance_commit_from_prior_term_match_indexes_only() {
        let dir = temp_dir("raft-leader-no-prior-term-commit");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        raft.log.append(1, RaftCommand::Noop).expect("append old 1");
        raft.log.append(1, RaftCommand::Noop).expect("append old 2");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 2;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.commit_index = 0;
            runtime.last_applied = 0;
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 3,
                    match_index: 2,
                },
            );
        }

        let advanced = raft
            .advance_leader_commit_from_progress()
            .expect("prior-term matches must not commit");
        assert_eq!(advanced, None);
        assert_eq!(
            raft.log.read_hard_state().expect("hard state").commit_index,
            0
        );
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.commit_index, 0);
            assert_eq!(runtime.last_applied, 0);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn leader_direct_async_commit_finalization_persists_and_applies() {
        let dir = temp_dir("raft-leader-direct-commit-async");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        raft.log
            .append(2, RaftCommand::Noop)
            .expect("append current");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 2;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.commit_index = 0;
            runtime.last_applied = 0;
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 2,
                    match_index: 1,
                },
            );
        }

        let committed = raft
            .commit_leader_index_in_term_blocking(1, 2, true)
            .await
            .expect("commit on blocking pool");
        assert!(committed);
        assert_eq!(
            raft.log.read_hard_state().expect("read hard state"),
            RaftHardState {
                current_term: 2,
                voted_for: None,
                commit_index: 1,
            }
        );
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.commit_index, 1);
            assert_eq!(runtime.last_applied, 1);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn leader_commit_fast_path_applies_previously_advanced_commit_index() {
        let dir = temp_dir("raft-leader-commit-already-advanced-applies");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let entry = raft
            .log
            .append(
                2,
                RaftCommand::SetMembership {
                    membership: RaftMembership::from_simple(five_test_peers()),
                },
            )
            .expect("append membership entry");
        raft.log
            .write_hard_state(&RaftHardState {
                current_term: 2,
                voted_for: None,
                commit_index: entry.index,
            })
            .expect("persist advanced commit index");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 2;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.commit_index = entry.index;
            runtime.last_applied = 0;
        }

        let committed = raft
            .commit_leader_index_in_term(entry.index, 2, true)
            .expect("commit fast path should apply");
        assert!(committed);
        assert_eq!(raft.runtime.lock().last_applied, entry.index);
        assert!(raft.membership().contains_id("n5"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn leader_commit_guard_rejects_stepdown_before_commit() {
        let dir = temp_dir("raft-leader-commit-stepdown-guard");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        raft.log
            .append(2, RaftCommand::Noop)
            .expect("append current");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.role = RaftRole::Follower;
            runtime.leader_id = Some("n2".into());
            runtime.commit_index = 0;
            runtime.last_applied = 0;
        }

        let committed = raft
            .commit_leader_index_in_term(1, 2, true)
            .expect("commit guard");
        assert!(!committed);
        assert_eq!(
            raft.log.read_hard_state().expect("hard state").commit_index,
            0
        );
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.commit_index, 0);
            assert_eq!(runtime.last_applied, 0);
            assert_eq!(runtime.role, RaftRole::Follower);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn leader_commit_guard_rejects_prior_term_entry() {
        let dir = temp_dir("raft-leader-commit-prior-term-guard");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        raft.log.append(1, RaftCommand::Noop).expect("append prior");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 2;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.commit_index = 0;
            runtime.last_applied = 0;
        }

        let committed = raft
            .commit_leader_index_in_term(1, 2, true)
            .expect("commit guard");
        assert!(!committed);
        assert_eq!(
            raft.log.read_hard_state().expect("hard state").commit_index,
            0
        );
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.commit_index, 0);
            assert_eq!(runtime.last_applied, 0);
            assert_eq!(runtime.role, RaftRole::Leader);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn hard_state_round_trips_term_vote_and_commit() {
        let dir = temp_dir("raft-hard-state");
        let store = RaftLogStore::open(&dir).expect("open store");
        let hard_state = RaftHardState {
            current_term: 7,
            voted_for: Some("n2".into()),
            commit_index: 42,
        };
        store
            .write_hard_state(&hard_state)
            .expect("write hard state");
        assert_eq!(
            store.read_hard_state().expect("read hard state"),
            hard_state
        );

        let reopened = RaftLogStore::open(&dir).expect("reopen store");
        assert_eq!(
            reopened.read_hard_state().expect("read hard state"),
            hard_state
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn hard_state_write_skips_identical_rewrite() {
        let dir = temp_dir("raft-hard-state-noop-write");
        let store = RaftLogStore::open(&dir).expect("open store");
        let hard_state = RaftHardState {
            current_term: 7,
            voted_for: Some("n2".into()),
            commit_index: 42,
        };
        store
            .write_hard_state(&hard_state)
            .expect("write hard state");
        let blocked_tmp = dir.join(HARD_STATE_FILE).with_extension("json.tmp");
        fs::create_dir_all(&blocked_tmp).expect("block hard-state temp path");

        store
            .write_hard_state(&hard_state)
            .expect("identical hard state write should be elided");

        assert_eq!(
            store.read_hard_state().expect("read cached hard state"),
            hard_state
        );
        assert_eq!(
            read_hard_state(&dir.join(HARD_STATE_FILE)).expect("read durable hard state"),
            hard_state
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn changed_hard_state_write_still_requires_durable_success() {
        let dir = temp_dir("raft-hard-state-changed-write-fails");
        let store = RaftLogStore::open(&dir).expect("open store");
        let initial_state = RaftHardState {
            current_term: 7,
            voted_for: Some("n2".into()),
            commit_index: 42,
        };
        let changed_state = RaftHardState {
            current_term: 8,
            voted_for: Some("n3".into()),
            commit_index: 43,
        };
        store
            .write_hard_state(&initial_state)
            .expect("write initial hard state");
        let blocked_tmp = dir.join(HARD_STATE_FILE).with_extension("json.tmp");
        fs::create_dir_all(&blocked_tmp).expect("block hard-state temp path");

        let err = store
            .write_hard_state(&changed_state)
            .expect_err("changed hard state must still hit durable storage");

        assert!(matches!(err, BrokerRaftError::Io(_)));
        assert_eq!(
            store.read_hard_state().expect("read cached hard state"),
            initial_state
        );
        assert_eq!(
            read_hard_state(&dir.join(HARD_STATE_FILE)).expect("read durable hard state"),
            initial_state
        );

        fs::remove_dir_all(&blocked_tmp).expect("unblock hard-state temp path");
        store
            .write_hard_state(&changed_state)
            .expect("write changed hard state after unblock");
        assert_eq!(
            store.read_hard_state().expect("read cached hard state"),
            changed_state
        );
        assert_eq!(
            read_hard_state(&dir.join(HARD_STATE_FILE)).expect("read durable hard state"),
            changed_state
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn elected_leader_appends_and_commits_current_term_noop() {
        let dir = temp_dir("raft-election-noop");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.membership = RaftMembership::from_simple(vec![test_peer("n1", 7980)]);
            runtime.current_term = 0;
            runtime.voted_for = None;
            runtime.role = RaftRole::Follower;
            runtime.leader_id = None;
        }

        raft.start_election().await.expect("single-node election");

        let entries = raft.log.read_entries().expect("read entries");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].term, 1);
        assert!(matches!(entries[0].command, RaftCommand::Noop));
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.role, RaftRole::Leader);
            assert_eq!(runtime.leader_id.as_deref(), Some("n1"));
            assert_eq!(runtime.current_term, 1);
            assert_eq!(runtime.voted_for.as_deref(), Some("n1"));
            assert_eq!(runtime.commit_index, entries[0].index);
            assert_eq!(runtime.last_applied, entries[0].index);
        }

        let hard_state = raft.log.read_hard_state().expect("read hard state");
        assert_eq!(hard_state.current_term, 1);
        assert_eq!(hard_state.voted_for.as_deref(), Some("n1"));
        assert_eq!(
            hard_state.commit_index, entries[0].index,
            "leader no-op commit persists commitIndex before applying"
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn start_election_does_not_mutate_runtime_when_hard_state_write_fails() {
        let dir = temp_dir("raft-election-hard-state-fails");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.membership = RaftMembership::from_simple(vec![test_peer("n1", 7980)]);
            runtime.current_term = 0;
            runtime.voted_for = None;
            runtime.role = RaftRole::Follower;
            runtime.leader_id = None;
        }
        fs::create_dir_all(dir.join(HARD_STATE_FILE).with_extension("json.tmp"))
            .expect("block hard-state temp path");

        let err = raft
            .start_election()
            .await
            .expect_err("self-vote persistence should fail");

        assert!(matches!(err, BrokerRaftError::Io(_)));
        assert!(raft.log.read_entries().expect("entries").is_empty());
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.role, RaftRole::Follower);
            assert_eq!(runtime.leader_id, None);
            assert_eq!(runtime.current_term, 0);
            assert_eq!(runtime.voted_for, None);
            assert_eq!(runtime.commit_index, 0);
            assert_eq!(runtime.last_applied, 0);
        }
        assert_eq!(
            raft.log.read_hard_state().expect("read hard state"),
            RaftHardState::default()
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn election_does_not_wait_for_slow_peer_after_quorum() {
        let dir = temp_dir("raft-election-fast-quorum");
        let listener_n2 = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fast voter");
        let addr_n2 = listener_n2.local_addr().expect("fast voter addr");
        let listener_n3 = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind slow voter");
        let addr_n3 = listener_n3.local_addr().expect("slow voter addr");

        let mut cfg = test_raft_config(dir.clone());
        cfg.heartbeat_interval = Duration::from_millis(10);
        cfg.election_timeout_min = Duration::from_millis(700);
        cfg.election_timeout_max = Duration::from_millis(1_400);
        cfg.peers = vec![
            test_peer("n1", 7980),
            RaftPeerConfig {
                id: "n2".into(),
                addr: addr_n2.to_string(),
            },
            RaftPeerConfig {
                id: "n3".into(),
                addr: addr_n3.to_string(),
            },
        ];
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let fast_server = tokio::spawn(async move {
            let (stream, _) = listener_n2.accept().await.expect("accept fast voter");
            let mut reader = TokioBufReader::new(stream);
            let pre_vote_line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                .await
                .expect("read pre-vote request");
            let pre_vote_term =
                match serde_json::from_str(&pre_vote_line).expect("parse pre-vote request") {
                    RaftRpc::PreVote { term, .. } => term,
                    other => panic!("unexpected pre-vote rpc: {other:?}"),
                };
            let pre_vote_response = serde_json::to_vec(&RaftRpcResponse::PreVote {
                term: pre_vote_term.saturating_sub(1),
                vote_granted: true,
            })
            .expect("serialize pre-vote response");
            reader
                .get_mut()
                .write_all(&pre_vote_response)
                .await
                .expect("write pre-vote response");
            reader
                .get_mut()
                .write_all(b"\n")
                .await
                .expect("write pre-vote newline");
            reader.get_mut().flush().await.expect("flush pre-vote");

            let vote_line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                .await
                .expect("read vote request");
            let vote_term = match serde_json::from_str(&vote_line).expect("parse vote request") {
                RaftRpc::RequestVote { term, .. } => term,
                other => panic!("unexpected vote rpc: {other:?}"),
            };
            let vote_response = serde_json::to_vec(&RaftRpcResponse::RequestVote {
                term: vote_term,
                vote_granted: true,
            })
            .expect("serialize vote response");
            reader
                .get_mut()
                .write_all(&vote_response)
                .await
                .expect("write vote response");
            reader
                .get_mut()
                .write_all(b"\n")
                .await
                .expect("write vote newline");
            reader.get_mut().flush().await.expect("flush vote");

            let append_line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                .await
                .expect("read leader noop append");
            let (append_term, match_index) =
                match serde_json::from_str(&append_line).expect("parse append request") {
                    RaftRpc::AppendEntries { term, entries, .. } => {
                        assert_eq!(
                            entries.iter().map(|entry| entry.index).collect::<Vec<_>>(),
                            vec![1]
                        );
                        (term, 1)
                    }
                    other => panic!("unexpected append rpc: {other:?}"),
                };
            let append_response = serde_json::to_vec(&RaftRpcResponse::AppendEntries {
                term: append_term,
                success: true,
                match_index,
                conflict_index: None,
                conflict_term: None,
            })
            .expect("serialize append response");
            reader
                .get_mut()
                .write_all(&append_response)
                .await
                .expect("write append response");
            reader
                .get_mut()
                .write_all(b"\n")
                .await
                .expect("write append newline");
            reader.get_mut().flush().await.expect("flush append");
        });
        let slow_server = tokio::spawn(async move {
            let (_stream, _) = listener_n3.accept().await.expect("accept slow voter");
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let start = Instant::now();
        raft.start_election().await.expect("start fast election");
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_millis(500),
            "election should not wait for slow peer timeout; elapsed={elapsed:?}"
        );
        fast_server.await.expect("fast voter server");
        slow_server.abort();
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.role, RaftRole::Leader);
            assert_eq!(runtime.leader_id.as_deref(), Some("n1"));
            assert_eq!(runtime.commit_index, 1);
            assert_eq!(runtime.last_applied, 1);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn leader_steps_down_when_heartbeat_cannot_observe_quorum_after_timeout() {
        let dir = temp_dir("raft-leader-check-quorum-stepdown");
        let listener_n2 = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind closed n2 addr");
        let addr_n2 = listener_n2.local_addr().expect("n2 addr");
        let listener_n3 = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind closed n3 addr");
        let addr_n3 = listener_n3.local_addr().expect("n3 addr");
        drop(listener_n2);
        drop(listener_n3);

        let mut cfg = test_raft_config(dir.clone());
        cfg.heartbeat_interval = Duration::from_millis(10);
        cfg.election_timeout_min = Duration::from_millis(50);
        cfg.election_timeout_max = Duration::from_millis(100);
        cfg.peers = vec![
            test_peer("n1", 7980),
            RaftPeerConfig {
                id: "n2".into(),
                addr: addr_n2.to_string(),
            },
            RaftPeerConfig {
                id: "n3".into(),
                addr: addr_n3.to_string(),
            },
        ];
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
            runtime.leader_progress.insert(
                "n3".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
        }
        raft.maintenance.lock().leader_quorum_observed_at =
            Instant::now() - Duration::from_millis(75);

        let acks = tokio::time::timeout(Duration::from_secs(2), raft.replicate_log_once(None))
            .await
            .expect("heartbeat should finish")
            .expect("heartbeat result");
        assert_eq!(acks, BTreeSet::from(["n1".to_string()]));
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 3);
            assert_eq!(runtime.role, RaftRole::Follower);
            assert_eq!(runtime.leader_id, None);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn step_down_does_not_mutate_runtime_when_hard_state_write_fails() {
        let dir = temp_dir("raft-stepdown-hard-state-fails");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let (initial_state, initial_deadline) = {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.voted_for = Some("n1".into());
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.election_deadline = Instant::now() + Duration::from_secs(60);
            (runtime.hard_state(), runtime.election_deadline)
        };
        raft.log
            .write_hard_state(&initial_state)
            .expect("persist initial hard state");
        fs::create_dir_all(dir.join(HARD_STATE_FILE).with_extension("json.tmp"))
            .expect("block hard-state temp path");

        raft.step_down(5, None);

        assert_eq!(
            raft.log.read_hard_state().expect("read hard state"),
            initial_state
        );
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 3);
            assert_eq!(runtime.voted_for.as_deref(), Some("n1"));
            assert_eq!(runtime.role, RaftRole::Leader);
            assert_eq!(runtime.leader_id.as_deref(), Some("n1"));
            assert_eq!(runtime.election_deadline, initial_deadline);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn is_leader_uses_cached_role_when_runtime_lock_is_contended() {
        let dir = temp_dir("raft-is-leader-cache-contended");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
        }
        assert!(raft.is_leader(), "uncontended read should refresh cache");

        let _runtime_guard = raft.runtime.lock();
        assert!(
            raft.is_leader(),
            "contended read should use the cached leader role instead of blocking on runtime"
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn step_down_clears_leader_cache_before_waiting_for_runtime_lock() {
        let dir = temp_dir("raft-stepdown-clears-leader-cache-before-lock");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
        }
        assert!(raft.is_leader(), "leader cache should start true");

        let runtime_guard = raft.runtime.lock();
        let node = raft.clone();
        let step_down = std::thread::spawn(move || {
            node.step_down(4, None);
        });

        let mut cache_cleared = false;
        for _ in 0..100 {
            if !raft.is_leader() {
                cache_cleared = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            cache_cleared,
            "step-down should clear cached leadership before it waits for the runtime lock"
        );

        drop(runtime_guard);
        step_down.join().expect("step-down task");
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 4);
            assert_eq!(runtime.role, RaftRole::Follower);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn stale_leader_is_not_ready_and_rejects_write_before_append() {
        let dir = temp_dir("raft-stale-leader-write-admission");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
        }
        raft.maintenance.lock().leader_quorum_observed_at = Instant::now() - Duration::from_secs(5);

        assert!(raft.is_leader());
        assert!(!raft.is_leader_ready());
        let progress = raft.progress_snapshot();
        assert!(progress.is_leader);
        assert!(!progress.is_leader_ready);
        assert!(progress
            .leader_quorum_age_ms
            .is_some_and(|age| age >= progress.leader_quorum_timeout_ms));
        let (client_id, _) = raft.register_client();
        let err = raft
            .handle_request(
                client_id,
                single_lock_request("stale-leader-lock", "stale-leader-key"),
            )
            .await
            .expect_err("stale leader must reject writes before appending");
        assert!(matches!(err, BrokerRaftError::NotLeader { .. }));
        assert_eq!(raft.log.read_entries().expect("read entries").len(), 0);
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.role, RaftRole::Follower);
            assert_eq!(runtime.leader_id, None);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn stale_leader_rejects_ephemeral_before_reserving_or_registering_client() {
        let dir = temp_dir("raft-stale-leader-ephemeral-admission");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let initial_client_sequence = raft.next_client_sequence.load(Ordering::Relaxed);
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
        }
        raft.maintenance.lock().leader_quorum_observed_at = Instant::now() - Duration::from_secs(5);

        let err = raft
            .run_ephemeral(
                single_lock_request("stale-ephemeral-lock", "stale-ephemeral-key"),
                "stale-ephemeral-request",
                Duration::ZERO,
                true,
            )
            .await
            .expect_err("stale leader must reject ephemeral writes at admission");

        assert!(matches!(err, BrokerRaftError::NotLeader { .. }));
        assert_eq!(raft.log.read_entries().expect("read entries").len(), 0);
        assert_eq!(
            raft.next_client_sequence.load(Ordering::Relaxed),
            initial_client_sequence,
            "stale leader should reject before allocating a local raft client id"
        );
        assert!(raft.client_response_cache.lock().entries.is_empty());
        assert!(raft.client_request_batch.lock().pending.is_empty());
        assert_eq!(raft.broker.metrics().holders, 0);
        assert_eq!(raft.broker.metrics().waiters, 0);
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.role, RaftRole::Follower);
            assert_eq!(runtime.leader_id, None);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn client_proposal_quorum_failure_steps_down_without_applying() {
        let dir = temp_dir("raft-client-proposal-quorum-failure-stepdown");
        let (rejecting_peer, rejecting_requests, rejecting_server) =
            spawn_rejecting_append_peer("n2").await;
        let closed_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind closed n3 addr");
        let closed_addr = closed_listener.local_addr().expect("closed n3 addr");
        drop(closed_listener);

        let mut cfg = test_raft_config(dir.clone());
        cfg.heartbeat_interval = Duration::from_millis(10);
        cfg.election_timeout_min = Duration::from_millis(50);
        cfg.election_timeout_max = Duration::from_millis(100);
        cfg.peers = vec![
            test_peer("n1", 7980),
            rejecting_peer.clone(),
            RaftPeerConfig {
                id: "n3".into(),
                addr: closed_addr.to_string(),
            },
        ];
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 7;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.leader_progress.insert(
                rejecting_peer.id.clone(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
            runtime.leader_progress.insert(
                "n3".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
        }
        raft.note_leader_quorum_observed();

        let (client_id, mut rx) = raft.register_client();
        let err = tokio::time::timeout(
            Duration::from_secs(2),
            raft.append_replicate_commit_apply(RaftCommand::ClientRequest {
                client_id,
                request: single_lock_request("failed-proposal-lock", "failed-proposal-key"),
                grant: None,
            }),
        )
        .await
        .expect("proposal quorum wait should finish")
        .expect_err("proposal must fail without target-index quorum");
        assert!(matches!(
            err,
            BrokerRaftError::QuorumUnavailable {
                index: 1,
                votes: 1,
                quorum: 2
            }
        ));
        assert!(
            rejecting_requests.load(Ordering::SeqCst) > 0,
            "reachable peer should have rejected at least one AppendEntries"
        );
        assert_eq!(raft.log.read_entries().expect("read entries").len(), 1);
        assert_eq!(raft.broker.metrics().holders, 0);
        assert_eq!(raft.broker.metrics().waiters, 0);
        assert!(rx.try_recv().is_err());
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.role, RaftRole::Follower);
            assert_eq!(runtime.leader_id, None);
            assert_eq!(runtime.commit_index, 0);
            assert_eq!(runtime.last_applied, 0);
        }
        assert!(!raft.is_leader_ready());

        rejecting_server.abort();
        let _ = rejecting_server.await;
        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn client_batch_quorum_failure_steps_down_without_applying() {
        let dir = temp_dir("raft-client-batch-quorum-failure-stepdown");
        let (rejecting_peer, rejecting_requests, rejecting_server) =
            spawn_rejecting_append_peer("n2").await;
        let closed_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind closed n3 addr");
        let closed_addr = closed_listener.local_addr().expect("closed n3 addr");
        drop(closed_listener);

        let mut cfg = test_raft_config(dir.clone());
        cfg.heartbeat_interval = Duration::from_millis(10);
        cfg.election_timeout_min = Duration::from_millis(50);
        cfg.election_timeout_max = Duration::from_millis(100);
        cfg.peers = vec![
            test_peer("n1", 7980),
            rejecting_peer.clone(),
            RaftPeerConfig {
                id: "n3".into(),
                addr: closed_addr.to_string(),
            },
        ];
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 9;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.leader_progress.insert(
                rejecting_peer.id.clone(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
            runtime.leader_progress.insert(
                "n3".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
        }
        raft.note_leader_quorum_observed();

        let (client_a, mut rx_a) = raft.register_client();
        let (client_b, mut rx_b) = raft.register_client();
        let err = tokio::time::timeout(
            Duration::from_secs(2),
            raft.append_replicate_commit_apply_client_batch(vec![
                RaftCommand::ClientRequest {
                    client_id: client_a,
                    request: single_lock_request("failed-batch-lock-a", "failed-batch-key-a"),
                    grant: None,
                },
                RaftCommand::ClientRequest {
                    client_id: client_b,
                    request: single_lock_request("failed-batch-lock-b", "failed-batch-key-b"),
                    grant: None,
                },
            ]),
        )
        .await
        .expect("batch quorum wait should finish")
        .expect_err("batch must fail without target-index quorum");
        assert!(matches!(
            err,
            BrokerRaftError::QuorumUnavailable {
                index: 2,
                votes: 1,
                quorum: 2
            }
        ));
        assert!(
            rejecting_requests.load(Ordering::SeqCst) > 0,
            "reachable peer should have rejected at least one AppendEntries"
        );
        assert_eq!(raft.log.read_entries().expect("read entries").len(), 2);
        assert_eq!(raft.broker.metrics().holders, 0);
        assert_eq!(raft.broker.metrics().waiters, 0);
        assert!(rx_a.try_recv().is_err());
        assert!(rx_b.try_recv().is_err());
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.role, RaftRole::Follower);
            assert_eq!(runtime.leader_id, None);
            assert_eq!(runtime.commit_index, 0);
            assert_eq!(runtime.last_applied, 0);
        }
        assert!(!raft.is_leader_ready());

        rejecting_server.abort();
        let _ = rejecting_server.await;
        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn leader_does_not_step_down_on_recent_quorum_miss() {
        let dir = temp_dir("raft-leader-check-quorum-recent-miss");
        let listener_n2 = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind closed n2 addr");
        let addr_n2 = listener_n2.local_addr().expect("n2 addr");
        let listener_n3 = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind closed n3 addr");
        let addr_n3 = listener_n3.local_addr().expect("n3 addr");
        drop(listener_n2);
        drop(listener_n3);

        let mut cfg = test_raft_config(dir.clone());
        cfg.heartbeat_interval = Duration::from_millis(10);
        cfg.election_timeout_min = Duration::from_millis(250);
        cfg.election_timeout_max = Duration::from_millis(500);
        cfg.peers = vec![
            test_peer("n1", 7980),
            RaftPeerConfig {
                id: "n2".into(),
                addr: addr_n2.to_string(),
            },
            RaftPeerConfig {
                id: "n3".into(),
                addr: addr_n3.to_string(),
            },
        ];
        let raft = BrokerRaft::open(cfg).expect("open raft");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 3;
            runtime.role = RaftRole::Leader;
            runtime.leader_id = Some("n1".into());
            runtime.leader_progress.insert(
                "n2".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
            runtime.leader_progress.insert(
                "n3".into(),
                RaftPeerProgress {
                    next_index: 1,
                    match_index: 0,
                },
            );
        }
        raft.note_leader_quorum_observed();

        let acks = tokio::time::timeout(Duration::from_secs(2), raft.replicate_log_once(None))
            .await
            .expect("heartbeat should finish")
            .expect("heartbeat result");
        assert_eq!(acks, BTreeSet::from(["n1".to_string()]));
        {
            let runtime = raft.runtime.lock();
            assert_eq!(runtime.current_term, 3);
            assert_eq!(runtime.role, RaftRole::Leader);
            assert_eq!(runtime.leader_id.as_deref(), Some("n1"));
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn open_replays_persisted_committed_log_entries() {
        let dir = temp_dir("raft-replay-committed");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg.clone()).expect("open raft");
        let entry = raft
            .log
            .append(
                3,
                RaftCommand::ClientRequest {
                    client_id: 99,
                    request: Request::Lock {
                        uuid: "replayed-lock-uuid".into(),
                        key: Some("restart-key".into()),
                        keys: None,
                        pid: None,
                        ttl: None,
                        max: None,
                        force: false,
                        retry_count: 0,
                        keep_locks_after_death: false,
                        wait: None,
                    },
                    grant: None,
                },
            )
            .expect("append committed entry");
        raft.log
            .write_hard_state(&RaftHardState {
                current_term: 3,
                voted_for: Some("n1".into()),
                commit_index: entry.index,
            })
            .expect("write hard state");
        drop(raft);

        let reopened = BrokerRaft::open(cfg).expect("reopen raft");
        let metrics = reopened.broker.metrics();
        assert_eq!(metrics.holders, 1);
        assert_eq!(metrics.waiters, 0);
        {
            let runtime = reopened.runtime.lock();
            assert_eq!(runtime.current_term, 3);
            assert_eq!(runtime.voted_for.as_deref(), Some("n1"));
            assert_eq!(runtime.commit_index, entry.index);
            assert_eq!(runtime.last_applied, entry.index);
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn apply_committed_stops_at_commit_index_before_unneeded_tail() {
        let dir = temp_dir("raft-apply-early-stop");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        for _ in 0..3 {
            raft.log.append(1, RaftCommand::Noop).expect("append");
        }
        append_raw_log_line(&dir, "{not valid json");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
            runtime.commit_index = 1;
            runtime.last_applied = 0;
        }

        raft.apply_committed()
            .expect("apply should stop at committed prefix");

        assert_eq!(raft.runtime.lock().last_applied, 1);
        assert!(
            raft.log.read_entries().is_err(),
            "full reads should still detect the malformed tail"
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn deterministic_grant_plan_replays_same_fencing_token_on_independent_nodes() {
        let request = single_lock_request("deterministic-raft-lock", "deterministic-key");
        let command = command_with_deterministic_grant(
            RaftCommand::ClientRequest {
                client_id: 42,
                request: request.clone(),
                grant: None,
            },
            11,
        );
        let expected_token = deterministic_fencing_seed(11);
        let expected_lock_uuid = deterministic_lock_uuid(11);

        for suffix in ["a", "b"] {
            let dir = temp_dir(&format!("raft-deterministic-fencing-{suffix}"));
            let cfg = test_raft_config(dir.clone());
            let raft = BrokerRaft::open(cfg).expect("open raft");
            let entry = raft.log.append(4, command.clone()).expect("append command");
            {
                let mut runtime = raft.runtime.lock();
                runtime.commit_index = entry.index;
            }
            raft.apply_committed().expect("apply deterministic command");
            let top = raft.broker.top_keys(1);
            assert_eq!(top.len(), 1);
            assert_eq!(top[0].key, "deterministic-key");
            assert_eq!(top[0].fencing_counter, expected_token);
            assert_eq!(
                raft.broker
                    .top_keys(1)
                    .first()
                    .map(|snapshot| snapshot.exclusive_holders),
                Some(1)
            );
            assert!(matches!(
                &command,
                RaftCommand::ClientRequest {
                    grant: Some(RaftGrantPlan {
                        lock_uuid: Some(lock_uuid),
                        fencing_seed: Some(seed),
                    }),
                    ..
                } if lock_uuid == &expected_lock_uuid && *seed == expected_token
            ));
            let _ = fs::remove_dir_all(dir);
        }
    }

    #[test]
    fn threshold_maintenance_snapshots_and_retains_trailing_entries() {
        let dir = temp_dir("raft-threshold-maintenance");
        let mut cfg = test_raft_config(dir.clone());
        cfg.snapshot_max_log_entries = 2;
        cfg.snapshot_max_log_bytes = u64::MAX;
        cfg.trailing_log_entries = 1;
        let raft = BrokerRaft::open(cfg).expect("open raft");
        for _ in 0..5 {
            raft.log.append(1, RaftCommand::Noop).expect("append");
        }
        {
            let mut runtime = raft.runtime.lock();
            runtime.commit_index = 5;
            runtime.last_applied = 5;
            runtime.current_term = 1;
        }

        raft.snapshot_and_compact_if_needed(false)
            .expect("maintenance compacts");

        assert_eq!(
            raft.log
                .latest_snapshot()
                .expect("snapshot")
                .last_included_index,
            5
        );
        let remaining = raft.log.read_entries().expect("remaining entries");
        assert_eq!(
            remaining
                .iter()
                .map(|entry| entry.index)
                .collect::<Vec<_>>(),
            vec![5]
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn overdue_snapshot_cadence_compacts_on_commit_path_without_threshold() {
        let dir = temp_dir("raft-overdue-cadence-maintenance");
        let mut cfg = test_raft_config(dir.clone());
        cfg.snapshot_interval = Duration::from_secs(60);
        cfg.snapshot_max_log_entries = 100_000;
        cfg.snapshot_max_log_bytes = u64::MAX;
        cfg.trailing_log_entries = 1;
        let raft = BrokerRaft::open(cfg).expect("open raft");
        for _ in 0..5 {
            raft.log.append(1, RaftCommand::Noop).expect("append");
        }
        {
            let mut runtime = raft.runtime.lock();
            runtime.commit_index = 5;
            runtime.last_applied = 5;
            runtime.current_term = 1;
        }
        raft.maintenance.lock().last_snapshot_at = Instant::now() - Duration::from_secs(61);

        raft.snapshot_and_compact_if_needed(false)
            .expect("overdue commit-path maintenance compacts");

        assert_eq!(
            raft.log
                .latest_snapshot()
                .expect("snapshot")
                .last_included_index,
            5
        );
        let remaining = raft.log.read_entries().expect("remaining entries");
        assert_eq!(
            remaining
                .iter()
                .map(|entry| entry.index)
                .collect::<Vec<_>>(),
            vec![5]
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn overdue_snapshot_cadence_does_not_reset_when_trailing_suffix_blocks_compaction() {
        let dir = temp_dir("raft-overdue-cadence-retained-suffix");
        let mut cfg = test_raft_config(dir.clone());
        cfg.snapshot_interval = Duration::from_secs(60);
        cfg.snapshot_max_log_entries = 100_000;
        cfg.snapshot_max_log_bytes = u64::MAX;
        cfg.trailing_log_entries = 10;
        let raft = BrokerRaft::open(cfg).expect("open raft");
        raft.log.append(1, RaftCommand::Noop).expect("append");
        {
            let mut runtime = raft.runtime.lock();
            runtime.commit_index = 1;
            runtime.last_applied = 1;
            runtime.current_term = 1;
        }
        let overdue_at = Instant::now() - Duration::from_secs(61);
        raft.maintenance.lock().last_snapshot_at = overdue_at;

        raft.snapshot_and_compact_if_needed(false)
            .expect("maintenance should skip safely");

        assert!(raft.log.latest_snapshot().is_none());
        assert_eq!(raft.maintenance.lock().last_snapshot_at, overdue_at);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn compaction_uses_retained_cache_for_snapshot_term() {
        let dir = temp_dir("raft-compaction-cache-term");
        let mut cfg = test_raft_config(dir.clone());
        cfg.snapshot_max_log_entries = 1;
        cfg.snapshot_max_log_bytes = u64::MAX;
        cfg.trailing_log_entries = 0;
        let raft = BrokerRaft::open(cfg).expect("open raft");
        raft.log.append(1, RaftCommand::Noop).expect("append old");
        let committed = raft
            .log
            .append(2, RaftCommand::Noop)
            .expect("append committed");
        append_raw_log_line(&dir, "{this is not valid raft json");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 2;
            runtime.commit_index = committed.index;
            runtime.last_applied = committed.index;
        }

        raft.snapshot_and_compact_if_needed(false)
            .expect("compaction should use retained cache, not reparse whole log");

        let snapshot = raft.log.latest_snapshot().expect("snapshot");
        assert_eq!(snapshot.last_included_index, committed.index);
        assert_eq!(snapshot.last_included_term, committed.term);
        assert!(raft
            .log
            .read_entries()
            .expect("compacted entries")
            .is_empty());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn compaction_snapshots_active_holder_without_waiters() {
        let dir = temp_dir("raft-active-state-maintenance");
        let mut cfg = test_raft_config(dir.clone());
        cfg.snapshot_max_log_entries = 1;
        cfg.snapshot_max_log_bytes = u64::MAX;
        cfg.trailing_log_entries = 0;
        let raft = BrokerRaft::open(cfg.clone()).expect("open raft");
        let entry = raft
            .log
            .append(
                1,
                RaftCommand::ClientRequest {
                    client_id: 7,
                    request: Request::Lock {
                        uuid: "active-lock-uuid".into(),
                        key: Some("active-key".into()),
                        keys: None,
                        pid: None,
                        ttl: None,
                        max: None,
                        force: false,
                        retry_count: 0,
                        keep_locks_after_death: false,
                        wait: None,
                    },
                    grant: None,
                },
            )
            .expect("append active lock");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
            runtime.commit_index = entry.index;
        }
        raft.apply_committed().expect("apply active lock");
        assert_eq!(raft.broker.metrics().holders, 1);

        raft.snapshot_and_compact_if_needed(false)
            .expect("active-state compaction check");

        assert_eq!(
            raft.log
                .latest_snapshot()
                .expect("active snapshot")
                .last_included_index,
            entry.index
        );
        assert_eq!(raft.log.read_entries().expect("entries").len(), 0);
        drop(raft);

        let reopened = BrokerRaft::open(cfg).expect("reopen active snapshot");
        assert_eq!(reopened.broker.metrics().holders, 1);
        assert_eq!(reopened.broker.metrics().waiters, 0);
        let (client, mut rx) = reopened.broker.register_client();
        reopened.broker.handle_request(
            client,
            Request::Unlock {
                uuid: "active-unlock-uuid".into(),
                key: Some("active-key".into()),
                keys: None,
                lock_uuid: Some("active-lock-uuid".into()),
                force: false,
            },
        );
        assert!(matches!(
            rx.try_recv(),
            Ok(Response::Unlock { unlocked: true, .. })
        ));
        assert_eq!(reopened.broker.metrics().holders, 0);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn compaction_snapshots_queued_waiters_and_preserves_grant_order() {
        let dir = temp_dir("raft-waiter-state-maintenance");
        let mut cfg = test_raft_config(dir.clone());
        cfg.snapshot_max_log_entries = 1;
        cfg.snapshot_max_log_bytes = u64::MAX;
        cfg.trailing_log_entries = 0;
        let raft = BrokerRaft::open(cfg.clone()).expect("open raft");
        let holder_entry = raft
            .log
            .append(
                1,
                RaftCommand::ClientRequest {
                    client_id: 7,
                    request: Request::Lock {
                        uuid: "waiter-holder-uuid".into(),
                        key: Some("waiter-key".into()),
                        keys: None,
                        pid: None,
                        ttl: None,
                        max: None,
                        force: false,
                        retry_count: 0,
                        keep_locks_after_death: false,
                        wait: None,
                    },
                    grant: None,
                },
            )
            .expect("append holder lock");
        let waiter_entry = raft
            .log
            .append(
                1,
                RaftCommand::ClientRequest {
                    client_id: 8,
                    request: Request::Lock {
                        uuid: "waiter-request-uuid".into(),
                        key: Some("waiter-key".into()),
                        keys: None,
                        pid: None,
                        ttl: None,
                        max: None,
                        force: false,
                        retry_count: 0,
                        keep_locks_after_death: false,
                        wait: None,
                    },
                    grant: None,
                },
            )
            .expect("append waiter lock");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
            runtime.commit_index = waiter_entry.index;
        }
        raft.apply_committed().expect("apply holder and waiter");
        assert!(holder_entry.index < waiter_entry.index);
        assert_eq!(raft.broker.metrics().holders, 1);
        assert_eq!(raft.broker.metrics().waiters, 1);

        raft.snapshot_and_compact_if_needed(false)
            .expect("waiter-state compaction check");

        assert_eq!(
            raft.log
                .latest_snapshot()
                .expect("snapshot")
                .last_included_index,
            waiter_entry.index
        );
        assert_eq!(raft.log.read_entries().expect("entries").len(), 0);

        let reopened = BrokerRaft::open(cfg).expect("reopen waiter snapshot");
        assert_eq!(reopened.broker.metrics().holders, 1);
        assert_eq!(reopened.broker.metrics().waiters, 1);

        let unlock_entry = reopened
            .log
            .append(
                1,
                RaftCommand::ClientRequest {
                    client_id: 7,
                    request: Request::Unlock {
                        uuid: "waiter-holder-unlock".into(),
                        key: Some("waiter-key".into()),
                        keys: None,
                        lock_uuid: Some("waiter-holder-uuid".into()),
                        force: false,
                    },
                    grant: None,
                },
            )
            .expect("append holder unlock");
        {
            let mut runtime = reopened.runtime.lock();
            runtime.current_term = 1;
            runtime.commit_index = unlock_entry.index;
        }
        reopened
            .apply_committed()
            .expect("apply unlock after restored waiter");
        assert_eq!(reopened.broker.metrics().holders, 1);
        assert_eq!(reopened.broker.metrics().waiters, 0);
        let top = reopened.broker.top_keys(1);
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].key, "waiter-key");
        assert_eq!(top[0].exclusive_holders, 1);
        assert_eq!(top[0].waiters, 0);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn compaction_clears_idle_committed_log_entries() {
        let dir = temp_dir("raft-idle-state-maintenance");
        let mut cfg = test_raft_config(dir.clone());
        cfg.snapshot_max_log_entries = 1;
        cfg.snapshot_max_log_bytes = u64::MAX;
        cfg.trailing_log_entries = 0;
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let lock_entry = raft
            .log
            .append(
                1,
                RaftCommand::ClientRequest {
                    client_id: 7,
                    request: Request::Lock {
                        uuid: "idle-lock-uuid".into(),
                        key: Some("idle-key".into()),
                        keys: None,
                        pid: None,
                        ttl: None,
                        max: None,
                        force: false,
                        retry_count: 0,
                        keep_locks_after_death: false,
                        wait: None,
                    },
                    grant: None,
                },
            )
            .expect("append lock");
        let unlock_entry = raft
            .log
            .append(
                1,
                RaftCommand::ClientRequest {
                    client_id: 7,
                    request: Request::Unlock {
                        uuid: "idle-unlock-uuid".into(),
                        key: Some("idle-key".into()),
                        keys: None,
                        lock_uuid: Some("idle-lock-uuid".into()),
                        force: false,
                    },
                    grant: None,
                },
            )
            .expect("append unlock");
        {
            let mut runtime = raft.runtime.lock();
            runtime.current_term = 1;
            runtime.commit_index = unlock_entry.index;
        }
        raft.apply_committed().expect("apply lock and unlock");
        assert_eq!(raft.broker.metrics().holders, 0);
        assert_eq!(raft.log.read_entries().expect("entries").len(), 2);

        raft.snapshot_and_compact_if_needed(false)
            .expect("idle-state compaction");

        assert_eq!(
            raft.log
                .latest_snapshot()
                .expect("snapshot")
                .last_included_index,
            unlock_entry.index
        );
        assert_eq!(raft.log.read_entries().expect("entries").len(), 0);
        assert_eq!(raft.log.last_index(), unlock_entry.index);
        assert!(lock_entry.index < unlock_entry.index);

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn raft_rpc_frame_reader_rejects_oversized_frames() {
        let mut bytes = vec![b'a'; 65];
        bytes.push(b'\n');
        let mut reader = TokioBufReader::new(&bytes[..]);
        let err = read_raft_frame_bounded(&mut reader, 64)
            .await
            .expect_err("oversized frame must fail");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn raft_rpc_frame_reader_accepts_unterminated_final_frame() {
        let mut reader = TokioBufReader::new(&b"{\"type\":\"requestVote\"}"[..]);
        let frame = read_raft_frame_bounded(&mut reader, 1024)
            .await
            .expect("unterminated EOF frame");
        assert_eq!(frame, "{\"type\":\"requestVote\"}");
    }

    #[tokio::test]
    async fn raft_rpc_stream_handles_multiple_frames_on_one_connection() {
        let dir = temp_dir("raft-rpc-stream-multi");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let addr = listener.local_addr().expect("listener addr");
        let server_raft = raft.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept raft RPC");
            server_raft
                .handle_rpc_stream(stream)
                .await
                .expect("serve raft RPC stream");
        });

        let stream = TcpStream::connect(addr).await.expect("connect test client");
        let mut reader = TokioBufReader::new(stream);
        for term in [1, 2] {
            let rpc = RaftRpc::RequestVote {
                auth_token: None,
                term,
                candidate_id: "n2".into(),
                last_log_index: 0,
                last_log_term: 0,
            };
            let body = serde_json::to_vec(&rpc).expect("serialize vote request");
            reader
                .get_mut()
                .write_all(&body)
                .await
                .expect("write rpc frame");
            reader
                .get_mut()
                .write_all(b"\n")
                .await
                .expect("write rpc newline");
            reader.get_mut().flush().await.expect("flush rpc frame");

            let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                .await
                .expect("read rpc response");
            match serde_json::from_str(&line).expect("parse rpc response") {
                RaftRpcResponse::RequestVote {
                    term: response_term,
                    ..
                } => assert_eq!(response_term, term),
                other => panic!("unexpected response: {other:?}"),
            }
        }

        drop(reader);
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("server exits after EOF")
            .expect("server task");
        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn pooled_peer_rpc_reuses_connection_for_multiple_calls() {
        let dir = temp_dir("raft-rpc-pool");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let addr = listener.local_addr().expect("listener addr");
        let accepted = Arc::new(AtomicUsize::new(0));
        let accepted_for_server = Arc::clone(&accepted);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept pooled RPC");
            accepted_for_server.fetch_add(1, Ordering::SeqCst);
            let mut reader = TokioBufReader::new(stream);
            for _ in 0..2 {
                let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                    .await
                    .expect("read pooled request");
                let _: RaftRpc = serde_json::from_str(&line).expect("parse pooled request");
                let body = serde_json::to_vec(&RaftRpcResponse::RequestVote {
                    term: 9,
                    vote_granted: true,
                })
                .expect("serialize pooled response");
                reader
                    .get_mut()
                    .write_all(&body)
                    .await
                    .expect("write pooled response");
                reader
                    .get_mut()
                    .write_all(b"\n")
                    .await
                    .expect("write pooled newline");
                reader
                    .get_mut()
                    .flush()
                    .await
                    .expect("flush pooled response");
            }
        });

        let peer = RaftPeerConfig {
            id: "n2".into(),
            addr: addr.to_string(),
        };
        for term in [1, 2] {
            let response = raft
                .send_rpc_to_peer(
                    &peer,
                    RaftRpc::RequestVote {
                        auth_token: None,
                        term,
                        candidate_id: "n1".into(),
                        last_log_index: 0,
                        last_log_term: 0,
                    },
                    Duration::from_secs(1),
                )
                .await
                .expect("pooled rpc response");
            assert!(matches!(
                response,
                RaftRpcResponse::RequestVote {
                    term: 9,
                    vote_granted: true
                }
            ));
        }
        server.await.expect("server task");
        assert_eq!(accepted.load(Ordering::SeqCst), 1);

        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn pooled_peer_rpc_resets_connection_when_call_is_cancelled() {
        let dir = temp_dir("raft-rpc-pool-cancel");
        let cfg = test_raft_config(dir.clone());
        let raft = BrokerRaft::open(cfg).expect("open raft");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let addr = listener.local_addr().expect("listener addr");
        let (first_seen_tx, first_seen_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept first RPC");
            let mut reader = TokioBufReader::new(stream);
            let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                .await
                .expect("read first pooled request");
            let _: RaftRpc = serde_json::from_str(&line).expect("parse first pooled request");
            let _ = first_seen_tx.send(());
            tokio::time::sleep(Duration::from_millis(100)).await;
            let stale = serde_json::to_vec(&RaftRpcResponse::RequestVote {
                term: 111,
                vote_granted: true,
            })
            .expect("serialize stale response");
            let _ = reader.get_mut().write_all(&stale).await;
            let _ = reader.get_mut().write_all(b"\n").await;
            let _ = reader.get_mut().flush().await;
            let _ = tokio::time::timeout(
                Duration::from_millis(250),
                read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes()),
            )
            .await;
            drop(reader);

            let (stream, _) = listener.accept().await.expect("accept replacement RPC");
            let mut reader = TokioBufReader::new(stream);
            let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                .await
                .expect("read replacement pooled request");
            let _: RaftRpc = serde_json::from_str(&line).expect("parse replacement pooled request");
            let fresh = serde_json::to_vec(&RaftRpcResponse::RequestVote {
                term: 222,
                vote_granted: true,
            })
            .expect("serialize fresh response");
            reader
                .get_mut()
                .write_all(&fresh)
                .await
                .expect("write fresh response");
            reader
                .get_mut()
                .write_all(b"\n")
                .await
                .expect("write fresh newline");
            reader
                .get_mut()
                .flush()
                .await
                .expect("flush fresh response");
        });

        let peer = RaftPeerConfig {
            id: "n2".into(),
            addr: addr.to_string(),
        };
        let first_node = raft.clone();
        let first_peer = peer.clone();
        let first_call = tokio::spawn(async move {
            first_node
                .send_rpc_to_peer(
                    &first_peer,
                    RaftRpc::RequestVote {
                        auth_token: None,
                        term: 1,
                        candidate_id: "n1".into(),
                        last_log_index: 0,
                        last_log_term: 0,
                    },
                    Duration::from_secs(5),
                )
                .await
        });
        first_seen_rx.await.expect("first request reached server");
        first_call.abort();
        assert!(first_call
            .await
            .expect_err("first RPC task should be cancelled")
            .is_cancelled());

        let response = raft
            .send_rpc_to_peer(
                &peer,
                RaftRpc::RequestVote {
                    auth_token: None,
                    term: 2,
                    candidate_id: "n1".into(),
                    last_log_index: 0,
                    last_log_term: 0,
                },
                Duration::from_secs(1),
            )
            .await
            .expect("replacement rpc response");
        assert!(matches!(
            response,
            RaftRpcResponse::RequestVote {
                term: 222,
                vote_granted: true
            }
        ));
        server.await.expect("server task");

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn log_store_serializes_concurrent_disk_operations() {
        let dir = temp_dir("raft-log-concurrent");
        let store = Arc::new(RaftLogStore::open(&dir).expect("open store"));
        let start = Arc::new(std::sync::Barrier::new(4));
        let mut handles = Vec::new();

        for _writer_id in 0..2 {
            let store = Arc::clone(&store);
            let start = Arc::clone(&start);
            handles.push(std::thread::spawn(move || {
                start.wait();
                for _ in 0..250 {
                    store
                        .append(1, RaftCommand::Noop)
                        .expect("append while concurrent readers run");
                    std::thread::yield_now();
                }
            }));
        }

        {
            let store = Arc::clone(&store);
            let start = Arc::clone(&start);
            handles.push(std::thread::spawn(move || {
                start.wait();
                for _ in 0..500 {
                    let entries = store.read_entries().expect("read concurrent log");
                    assert!(
                        entries.windows(2).all(|pair| pair[0].index < pair[1].index),
                        "log indexes must stay strictly ordered",
                    );
                    std::thread::yield_now();
                }
            }));
        }

        {
            let store = Arc::clone(&store);
            let start = Arc::clone(&start);
            handles.push(std::thread::spawn(move || {
                start.wait();
                for step in 0..120 {
                    let entries = store.read_entries().expect("read for compaction");
                    if let Some(entry) = entries.get(entries.len().saturating_sub(1) / 2) {
                        store
                            .write_snapshot(
                                entry.index,
                                entry.term,
                                json!({ "step": step, "kind": "concurrent-test" }),
                            )
                            .expect("snapshot while concurrent readers run");
                        store
                            .compact_through(entry.index)
                            .expect("compact while concurrent readers run");
                    }
                    std::thread::yield_now();
                }
            }));
        }

        for handle in handles {
            handle.join().expect("worker thread");
        }

        let entries = store.read_entries().expect("final read");
        assert!(
            entries.windows(2).all(|pair| pair[0].index < pair[1].index),
            "final log indexes must stay strictly ordered",
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn fuzz_log_store_append_replace_and_compact_invariants() {
        for seed in [
            0x51D0_5EED_u64,
            0xCAFE_BABE_u64,
            0xDEAD_BEEF_u64,
            0xA11C_EE55_u64,
        ] {
            let dir = temp_dir("raft-log-fuzz");
            let store = RaftLogStore::open(&dir).expect("open store");
            let mut rng = seed;
            let mut current_term = 1u64;
            let mut oracle: Vec<RaftLogEntry> = Vec::new();

            for step in 0..300 {
                match next_fuzz(&mut rng) % 6 {
                    0..=2 => {
                        if let Some(snapshot) = store.latest_snapshot() {
                            current_term = current_term.max(snapshot.last_included_term);
                        }
                        if let Some(last) = oracle.last() {
                            current_term = current_term.max(last.term);
                        }
                        if next_fuzz(&mut rng) % 5 == 0 {
                            current_term = current_term.saturating_add(1 + next_fuzz(&mut rng) % 3);
                        }
                        let entry = store
                            .append(current_term, RaftCommand::Noop)
                            .expect("append");
                        let expected_index = oracle
                            .last()
                            .map(|last| last.index + 1)
                            .or_else(|| {
                                store
                                    .latest_snapshot()
                                    .map(|snapshot| snapshot.last_included_index + 1)
                            })
                            .unwrap_or(1);
                        assert_eq!(entry.index, expected_index, "seed={seed} step={step}");
                        oracle.push(entry);
                    }
                    3 => {
                        let keep_len = if oracle.is_empty() {
                            0
                        } else {
                            (next_fuzz(&mut rng) as usize) % (oracle.len() + 1)
                        };
                        let replacement = oracle[..keep_len].to_vec();
                        store.replace_all(&replacement).expect("replace");
                        oracle = replacement;
                    }
                    _ => {
                        if oracle.is_empty() {
                            continue;
                        }
                        let pos = (next_fuzz(&mut rng) as usize) % oracle.len();
                        let snapshot_entry = oracle[pos].clone();
                        store
                            .write_snapshot(
                                snapshot_entry.index,
                                snapshot_entry.term,
                                json!({ "seed": seed, "step": step }),
                            )
                            .expect("snapshot");
                        store
                            .compact_through(snapshot_entry.index)
                            .expect("compact through snapshot");
                        oracle.retain(|entry| entry.index > snapshot_entry.index);
                    }
                }

                let actual = store.read_entries().expect("read entries");
                assert_eq!(
                    actual
                        .iter()
                        .map(|entry| (entry.index, entry.term))
                        .collect::<Vec<_>>(),
                    oracle
                        .iter()
                        .map(|entry| (entry.index, entry.term))
                        .collect::<Vec<_>>(),
                    "seed={seed} step={step}"
                );
                let expected_last = oracle
                    .last()
                    .map(|entry| (entry.index, entry.term))
                    .or_else(|| {
                        store.latest_snapshot().map(|snapshot| {
                            (snapshot.last_included_index, snapshot.last_included_term)
                        })
                    })
                    .unwrap_or((0, 0));
                assert_eq!(
                    store.last_index(),
                    expected_last.0,
                    "seed={seed} step={step}"
                );
                assert_eq!(
                    store.last_term(),
                    expected_last.1,
                    "seed={seed} step={step}"
                );
            }

            let _ = fs::remove_dir_all(dir);
        }
    }

    fn next_fuzz(state: &mut u64) -> u64 {
        *state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        *state
    }
}
