//! Raft-facing broker wrapper and durable local log plumbing.
//!
//! This module provides the `BrokerRaft` server backend: peer-list config,
//! leader election, quorum replication, durable append-only logs, snapshot
//! metadata, and compaction-by-snapshot-index.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Mutex as AsyncMutex};
use tokio::task::{JoinHandle, JoinSet};
use tracing::{debug, error, info, warn};

use crate::broker::{Broker, BrokerConfig, ClientId, GrantOverrides};
use crate::protocol::{Request, Response, MAX_COMPOSITE_KEYS};

const LOG_FILE: &str = "raft-log.ndjson";
const SNAPSHOT_FILE: &str = "raft-snapshot.json";
const HARD_STATE_FILE: &str = "raft-hard-state.json";
const DEFAULT_RAFT_RPC_MAX_FRAME_BYTES: usize = 128 * 1024 * 1024;
const DEFAULT_APPEND_ENTRIES_MAX_ENTRIES: usize = 256;
const DEFAULT_APPEND_ENTRIES_MAX_BYTES: usize = 1024 * 1024;
const DEFAULT_INSTALL_SNAPSHOT_CHUNK_BYTES: usize = 1024 * 1024;
const DEFAULT_CLIENT_BATCH_MAX_ENTRIES: usize = 32;
const DEFAULT_CLIENT_BATCH_MAX_DELAY: Duration = Duration::from_millis(1);
const RAFT_FENCING_TOKEN_BASE: u64 = 4_000_000_000_000_000;

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
    /// Small coalescing window for leader-local client request batches.
    pub client_batch_max_delay: Duration,
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
            client_batch_max_delay: DEFAULT_CLIENT_BATCH_MAX_DELAY,
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
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RaftPeerConfig {
    pub id: String,
    pub addr: String,
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

    fn normalized(self) -> Self {
        crate::routine_id!("ddl-routine-broker-raft-membership-normalized-1");
        match self {
            Self::Simple { peers } => Self::from_simple(peers),
            Self::Joint {
                old_peers,
                new_peers,
            } => Self::Joint {
                old_peers: normalize_peers(old_peers),
                new_peers: normalize_peers(new_peers),
            },
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
    Ok(normalize_peers(normalized))
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
    #[error("raft learner `{peer_id}` did not catch up to index {target_index} before promotion")]
    LearnerCatchUpFailed { peer_id: String, target_index: u64 },
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
    DropClient {
        client_id: ClientId,
    },
    SetMembership {
        membership: RaftMembership,
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
    RequestVote {
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
    },
    AppendEntries {
        term: u64,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<RaftLogEntry>,
        leader_commit: u64,
    },
    InstallSnapshot {
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
    membership: RaftMembership,
}

impl RaftRuntimeState {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftCompactionReport {
    pub compacted_through_index: u64,
    pub compacted_entries: usize,
    pub retained_entries: usize,
}

#[derive(Debug)]
struct RaftLogState {
    last_index: u64,
    last_term: u64,
    latest_snapshot: Option<RaftSnapshotMetadata>,
}

#[derive(Debug)]
struct RaftMaintenanceState {
    last_snapshot_at: Instant,
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
    result_tx: oneshot::Sender<Result<u64, ClientRequestBatchError>>,
}

#[derive(Default)]
struct ClientRequestBatchState {
    pending: VecDeque<PendingClientRequest>,
    driver_active: bool,
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
    state: Mutex<RaftLogState>,
}

impl RaftLogStore {
    pub fn open(data_dir: impl Into<PathBuf>) -> Result<Self, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-log-open-1");
        let data_dir = data_dir.into();
        fs::create_dir_all(&data_dir)?;
        let log_path = data_dir.join(LOG_FILE);
        let snapshot_path = data_dir.join(SNAPSHOT_FILE);
        let hard_state_path = data_dir.join(HARD_STATE_FILE);
        let latest_snapshot = read_snapshot_metadata(&snapshot_path)?;
        let (last_index, last_term) = read_last_log_position(&log_path)?
            .or_else(|| {
                latest_snapshot
                    .as_ref()
                    .map(|s| (s.last_included_index, s.last_included_term))
            })
            .unwrap_or((0, 0));

        Ok(Self {
            data_dir,
            log_path,
            snapshot_path,
            hard_state_path,
            state: Mutex::new(RaftLogState {
                last_index,
                last_term,
                latest_snapshot,
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
        let mut state = self.state.lock();
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)?;
        let created_at_ms = unix_ms();
        let mut entries = Vec::with_capacity(commands.len());
        for command in commands {
            let entry = RaftLogEntry {
                index: state.last_index.saturating_add(entries.len() as u64 + 1),
                term,
                created_at_ms,
                command,
            };
            serde_json::to_writer(&mut file, &entry)?;
            file.write_all(b"\n")?;
            entries.push(entry);
        }
        file.sync_data()?;

        if let Some(last) = entries.last() {
            state.last_index = last.index;
            state.last_term = last.term;
        }
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

    fn latest_snapshot_file(&self) -> Result<Option<RaftSnapshotFile>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-latest-snapshot-file-1");
        let _state = self.state.lock();
        read_snapshot_file(&self.snapshot_path)
    }

    pub fn read_entries(&self) -> Result<Vec<RaftLogEntry>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-read-entries-1");
        let _state = self.state.lock();
        read_log_entries(&self.log_path)
    }

    pub fn entries_from(&self, index: u64) -> Result<Vec<RaftLogEntry>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-entries-from-1");
        let _state = self.state.lock();
        Ok(read_log_entries(&self.log_path)?
            .into_iter()
            .filter(|entry| entry.index >= index)
            .collect())
    }

    pub fn entries_from_limited(
        &self,
        index: u64,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Vec<RaftLogEntry>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-entries-from-limited-1");
        let _state = self.state.lock();
        let max_entries = max_entries.max(1);
        let max_bytes = max_bytes.max(1);
        let mut selected = Vec::new();
        let mut bytes = 0usize;
        for entry in read_log_entries(&self.log_path)?
            .into_iter()
            .filter(|entry| entry.index >= index)
        {
            if selected.len() >= max_entries {
                break;
            }
            let entry_bytes = serde_json::to_vec(&entry)?.len();
            if !selected.is_empty() && bytes.saturating_add(entry_bytes) > max_bytes {
                break;
            }
            bytes = bytes.saturating_add(entry_bytes);
            selected.push(entry);
        }
        Ok(selected)
    }

    pub fn term_at(&self, index: u64) -> Result<Option<u64>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-term-at-1");
        let state = self.state.lock();
        let entries = read_log_entries(&self.log_path)?;
        Ok(term_at_index(&state, &entries, index))
    }

    pub fn last_index_for_term(&self, term: u64) -> Result<Option<u64>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-last-index-for-term-1");
        let state = self.state.lock();
        let entries = read_log_entries(&self.log_path)?;
        Ok(last_index_for_term(&state, &entries, term))
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
        let _state = self.state.lock();
        read_hard_state(&self.hard_state_path)
    }

    fn write_hard_state(&self, state: &RaftHardState) -> Result<(), BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-write-hard-state-1");
        let _log_state = self.state.lock();
        let tmp = self.hard_state_path.with_extension("json.tmp");
        {
            let mut file = File::create(&tmp)?;
            serde_json::to_writer_pretty(&mut file, state)?;
            file.write_all(b"\n")?;
            file.sync_all()?;
        }
        fs::rename(&tmp, &self.hard_state_path)?;
        sync_dir(&self.data_dir)?;
        Ok(())
    }

    pub fn replace_all(&self, entries: &[RaftLogEntry]) -> Result<(), BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-replace-all-1");
        let mut state = self.state.lock();
        rewrite_log(&self.log_path, entries)?;
        sync_dir(&self.data_dir)?;
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
        Ok(())
    }

    fn append_entries_from_leader(
        &self,
        prev_log_index: u64,
        prev_log_term: u64,
        leader_term: u64,
        entries: Vec<RaftLogEntry>,
    ) -> Result<RaftAppendReport, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-append-from-leader-1");
        validate_append_entries_shape(prev_log_index, leader_term, &entries)?;
        let mut state = self.state.lock();
        let mut local = read_log_entries(&self.log_path)?;
        let snapshot_index = state
            .latest_snapshot
            .as_ref()
            .map(|snapshot| snapshot.last_included_index)
            .unwrap_or(0);
        let local_last_index = local
            .last()
            .map(|entry| entry.index)
            .or_else(|| {
                state
                    .latest_snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.last_included_index)
            })
            .unwrap_or(0);

        if prev_log_index > local_last_index {
            return Ok(RaftAppendReport {
                success: false,
                match_index: local_last_index,
                conflict_index: Some(local_last_index.saturating_add(1)),
                conflict_term: None,
            });
        }

        match term_at_index(&state, &local, prev_log_index) {
            Some(term) if term == prev_log_term => {}
            Some(term) => {
                return Ok(RaftAppendReport {
                    success: false,
                    match_index: local_last_index.min(prev_log_index.saturating_sub(1)),
                    conflict_index: first_index_for_term(&state, &local, term),
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

        let mut rewrite_required = false;
        let mut append_only = Vec::new();
        let mut match_index = prev_log_index;
        for entry in entries
            .into_iter()
            .filter(|entry| entry.index > snapshot_index && entry.index > prev_log_index)
        {
            match_index = match_index.max(entry.index);
            if let Some(pos) = local.iter().position(|local| local.index == entry.index) {
                if local[pos].term != entry.term {
                    local.truncate(pos);
                    local.push(entry);
                    rewrite_required = true;
                }
            } else {
                if let Some(pos) = local.iter().position(|local| local.index > entry.index) {
                    local.truncate(pos);
                    rewrite_required = true;
                }
                if !rewrite_required {
                    append_only.push(entry.clone());
                }
                local.push(entry);
            }
        }

        if rewrite_required {
            rewrite_log(&self.log_path, &local)?;
            sync_dir(&self.data_dir)?;
        } else if !append_only.is_empty() {
            append_log_entries(&self.log_path, &append_only)?;
        }
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
        let tmp = self.snapshot_path.with_extension("json.tmp");
        {
            let mut file = File::create(&tmp)?;
            serde_json::to_writer_pretty(&mut file, &snapshot)?;
            file.write_all(b"\n")?;
            file.sync_all()?;
        }
        fs::rename(&tmp, &self.snapshot_path)?;
        sync_dir(&self.data_dir)?;

        state.latest_snapshot = Some(metadata.clone());
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

        let entries = read_log_entries(&self.log_path)?;
        let retain_suffix =
            term_at_index(&state, &entries, last_included_index) == Some(last_included_term);
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
        let tmp = self.snapshot_path.with_extension("json.tmp");
        {
            let mut file = File::create(&tmp)?;
            serde_json::to_writer_pretty(&mut file, &snapshot)?;
            file.write_all(b"\n")?;
            file.sync_all()?;
        }
        fs::rename(&tmp, &self.snapshot_path)?;
        rewrite_log(&self.log_path, &retained)?;
        sync_dir(&self.data_dir)?;

        state.latest_snapshot = Some(metadata.clone());
        if let Some(last) = retained.last() {
            state.last_index = last.index;
            state.last_term = last.term;
        } else {
            state.last_index = metadata.last_included_index;
            state.last_term = metadata.last_included_term;
        }
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

        let entries = read_log_entries(&self.log_path)?;
        let before = entries.len();
        let retained: Vec<RaftLogEntry> = entries
            .into_iter()
            .filter(|entry| entry.index > through_index)
            .collect();
        rewrite_log(&self.log_path, &retained)?;
        sync_dir(&self.data_dir)?;

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
    rpc_connections: Arc<Mutex<BTreeMap<String, Arc<AsyncMutex<RaftRpcConnection>>>>>,
    snapshot_transfers: Arc<Mutex<BTreeMap<String, PendingSnapshotTransfer>>>,
    client_request_batch: Arc<Mutex<ClientRequestBatchState>>,
}

impl BrokerRaft {
    pub fn open(config: BrokerRaftConfig) -> Result<Self, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-open-1");
        config.validate()?;
        let log_store = RaftLogStore::open(&config.data_dir)?;
        let mut hard_state = log_store.read_hard_state()?;
        let snapshot_index = log_store
            .latest_snapshot()
            .map(|snapshot| snapshot.last_included_index)
            .unwrap_or(0);
        let last_index = log_store.last_index();
        let current_term = hard_state.current_term.max(log_store.last_term());
        let commit_index = hard_state.commit_index.min(last_index);
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
        let raft = Self {
            broker: Broker::new(config.broker.clone()),
            runtime: Arc::new(Mutex::new(RaftRuntimeState {
                current_term: hard_state.current_term,
                voted_for: hard_state.voted_for,
                role: RaftRole::Follower,
                leader_id: None,
                commit_index: hard_state.commit_index,
                last_applied: snapshot_index.min(hard_state.commit_index),
                election_deadline,
                leader_progress: BTreeMap::new(),
                membership: RaftMembership::from_simple(config.peers.clone()),
            })),
            maintenance: Arc::new(Mutex::new(RaftMaintenanceState {
                last_snapshot_at: now,
            })),
            commit_lock: Arc::new(tokio::sync::Mutex::new(())),
            rpc_connections: Arc::new(Mutex::new(BTreeMap::new())),
            snapshot_transfers: Arc::new(Mutex::new(BTreeMap::new())),
            client_request_batch: Arc::new(Mutex::new(ClientRequestBatchState::default())),
            config,
            log,
        };
        if let Some(snapshot_file) = raft.log.latest_snapshot_file()? {
            raft.broker
                .install_raft_snapshot(&snapshot_file.payload)
                .map_err(BrokerRaftError::BrokerSnapshot)?;
            if let Some(membership) = membership_from_snapshot_payload(&snapshot_file.payload)? {
                raft.apply_membership(membership)?;
            }
        }
        raft.apply_committed()?;
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

    pub fn is_leader(&self) -> bool {
        crate::routine_id!("ddl-routine-broker-raft-is-leader-1");
        self.runtime.lock().role == RaftRole::Leader
    }

    pub fn leader_id(&self) -> Option<String> {
        crate::routine_id!("ddl-routine-broker-raft-leader-id-1");
        self.runtime.lock().leader_id.clone()
    }

    pub fn leader_addr(&self) -> Option<String> {
        crate::routine_id!("ddl-routine-broker-raft-leader-addr-1");
        let leader = self.leader_id()?;
        self.active_peers()
            .iter()
            .find(|p| p.id == leader)
            .map(|p| p.addr.clone())
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
        self.broker.register_client()
    }

    /// Append the request to the leader log, replicate it to a quorum, and
    /// only then apply it to the in-process broker.
    pub async fn handle_request(
        &self,
        client: ClientId,
        request: Request,
    ) -> Result<u64, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-handle-request-1");
        if !self.is_leader() {
            return Err(BrokerRaftError::NotLeader {
                leader_id: self.leader_id(),
                leader_addr: self.leader_addr(),
            });
        }
        self.enqueue_client_request(client, request).await
    }

    async fn enqueue_client_request(
        &self,
        client_id: ClientId,
        request: Request,
    ) -> Result<u64, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-enqueue-client-request-1");
        let (result_tx, result_rx) = oneshot::channel();
        let start_driver = {
            let mut state = self.client_request_batch.lock();
            state.pending.push_back(PendingClientRequest {
                client_id,
                request,
                result_tx,
            });
            if state.driver_active {
                false
            } else {
                state.driver_active = true;
                true
            }
        };
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
                tokio::time::sleep(self.config.client_batch_max_delay).await;
            }

            let batch = self.take_client_request_batch();
            if batch.is_empty() {
                let mut state = self.client_request_batch.lock();
                if state.pending.is_empty() {
                    state.driver_active = false;
                    return;
                }
                continue;
            }

            let result = {
                let commands = batch
                    .iter()
                    .map(|pending| RaftCommand::ClientRequest {
                        client_id: pending.client_id,
                        request: pending.request.clone(),
                        grant: None,
                    })
                    .collect::<Vec<_>>();
                let _commit_guard = self.commit_lock.lock().await;
                self.append_replicate_commit_apply_client_batch(commands)
                    .await
            };
            match result {
                Ok(indexes) => {
                    for (pending, index) in batch.into_iter().zip(indexes) {
                        let _ = pending.result_tx.send(Ok(index));
                    }
                }
                Err(err) => {
                    let error = ClientRequestBatchError::from_broker_error(err);
                    for pending in batch {
                        let _ = pending.result_tx.send(Err(error.clone()));
                    }
                }
            }
        }
    }

    fn take_client_request_batch(&self) -> Vec<PendingClientRequest> {
        crate::routine_id!("ddl-routine-broker-raft-take-client-batch-1");
        let mut state = self.client_request_batch.lock();
        let max_entries = self.config.client_batch_max_entries.max(1);
        let take = state.pending.len().min(max_entries);
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
        if !self.is_leader() {
            return Err(BrokerRaftError::NotLeader {
                leader_id: self.leader_id(),
                leader_addr: self.leader_addr(),
            });
        }
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
        if !self.is_leader() {
            return Err(BrokerRaftError::NotLeader {
                leader_id: self.leader_id(),
                leader_addr: self.leader_addr(),
            });
        }
        let _commit_guard = self.commit_lock.lock().await;
        if self.membership_is_joint() {
            return Err(BrokerRaftError::InvalidConfig(
                "cannot start a new raft membership change while joint consensus is active".into(),
            ));
        }
        let old_peers = self.active_peers();
        self.catch_up_new_membership_peers(&old_peers, &new_peers)
            .await?;
        let joint = RaftMembership::Joint {
            old_peers,
            new_peers: new_peers.clone(),
        };
        self.append_replicate_commit_apply(RaftCommand::SetMembership { membership: joint })
            .await?;
        self.append_replicate_commit_apply(RaftCommand::SetMembership {
            membership: RaftMembership::from_simple(new_peers),
        })
        .await
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
        for peer in learners {
            if let Err(err) = self.catch_up_learner_peer(peer.clone(), target_index).await {
                self.discard_staged_learners(&learner_ids, &old_ids);
                return Err(err);
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
            let (term, leader_commit) = {
                let runtime = self.runtime.lock();
                (runtime.current_term, runtime.commit_index)
            };
            if self
                .replicate_to_peer(peer.clone(), term, leader_commit, Some(target_index))
                .await?
            {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(BrokerRaftError::LearnerCatchUpFailed {
                    peer_id: peer.id,
                    target_index,
                });
            }
            tokio::time::sleep(
                self.config
                    .heartbeat_interval
                    .max(Duration::from_millis(25)),
            )
            .await;
        }
    }

    fn discard_staged_learners(&self, learner_ids: &[String], old_ids: &BTreeSet<String>) {
        crate::routine_id!("ddl-routine-broker-raft-discard-staged-learners-1");
        let learner_ids = learner_ids.iter().cloned().collect::<BTreeSet<_>>();
        self.runtime
            .lock()
            .leader_progress
            .retain(|peer_id, _| old_ids.contains(peer_id) || !learner_ids.contains(peer_id));
        self.rpc_connections
            .lock()
            .retain(|peer_id, _| old_ids.contains(peer_id) || !learner_ids.contains(peer_id));
    }

    async fn append_replicate_commit_apply(
        &self,
        command: RaftCommand,
    ) -> Result<u64, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-append-replicate-commit-1");
        if !self.is_leader() {
            return Err(BrokerRaftError::NotLeader {
                leader_id: self.leader_id(),
                leader_addr: self.leader_addr(),
            });
        }
        let term = self.runtime.lock().current_term;
        let next_index = self.log.last_index().saturating_add(1);
        let command = command_with_deterministic_grant(command, next_index);
        let entry = self.log.append(term, command)?;
        let acks = self.replicate_until_quorum(entry.index).await?;
        let quorum = self.active_quorum_size();
        if !self.quorum_met(&acks) {
            return Err(BrokerRaftError::QuorumUnavailable {
                index: entry.index,
                votes: acks.len(),
                quorum,
            });
        }
        let hard_state = {
            let mut runtime = self.runtime.lock();
            runtime.commit_index = runtime.commit_index.max(entry.index);
            runtime.hard_state()
        };
        self.log.write_hard_state(&hard_state)?;
        self.apply_committed()?;
        self.snapshot_and_compact_if_needed(false)?;
        let _ = self.replicate_log_once(None).await;
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
        if !self.is_leader() {
            return Err(BrokerRaftError::NotLeader {
                leader_id: self.leader_id(),
                leader_addr: self.leader_addr(),
            });
        }
        let term = self.runtime.lock().current_term;
        let first_index = self.log.last_index().saturating_add(1);
        let commands = commands
            .into_iter()
            .enumerate()
            .map(|(idx, command)| {
                command_with_deterministic_grant(command, first_index.saturating_add(idx as u64))
            })
            .collect::<Vec<_>>();
        let entries = self.log.append_batch(term, commands)?;
        let Some(last_entry) = entries.last() else {
            return Ok(Vec::new());
        };
        let target_index = last_entry.index;
        let acks = self.replicate_until_quorum(target_index).await?;
        let quorum = self.active_quorum_size();
        if !self.quorum_met(&acks) {
            return Err(BrokerRaftError::QuorumUnavailable {
                index: target_index,
                votes: acks.len(),
                quorum,
            });
        }
        let hard_state = {
            let mut runtime = self.runtime.lock();
            runtime.commit_index = runtime.commit_index.max(target_index);
            runtime.hard_state()
        };
        self.log.write_hard_state(&hard_state)?;
        self.apply_committed()?;
        self.snapshot_and_compact_if_needed(false)?;
        let _ = self.replicate_log_once(None).await;
        Ok(entries.into_iter().map(|entry| entry.index).collect())
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
        let fail_fast_acquire = request_is_fail_fast_acquire(&request);
        if !self.is_leader() {
            let leader_addr = self
                .leader_addr()
                .ok_or_else(|| BrokerRaftError::NotLeader {
                    leader_id: self.leader_id(),
                    leader_addr: None,
                })?;
            return match send_rpc(
                &leader_addr,
                RaftRpc::ProxyRequest {
                    request,
                    request_uuid: request_uuid.to_string(),
                    wait_ms: wait.as_millis() as u64,
                    is_acquire,
                },
                wait.max(Duration::from_secs(2)),
            )
            .await?
            {
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
        let (client_id, mut rx) = self.broker.register_client();
        let result = self.handle_request(client_id, request).await;
        if let Err(err) = result {
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
        match rpc {
            RaftRpc::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => self.handle_request_vote(term, candidate_id, last_log_index, last_log_term),
            RaftRpc::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => self.handle_append_entries(
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            ),
            RaftRpc::InstallSnapshot {
                term,
                leader_id,
                last_included_index,
                last_included_term,
                payload_sha256,
                offset,
                done,
                data,
            } => self.handle_install_snapshot(
                term,
                leader_id,
                last_included_index,
                last_included_term,
                payload_sha256,
                offset,
                done,
                data,
            ),
            RaftRpc::ProxyRequest {
                request,
                request_uuid,
                wait_ms,
                is_acquire,
            } => {
                let term = self.runtime.lock().current_term;
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
        let mut hard_state = None;
        let (response_term, granted) = {
            let mut runtime = self.runtime.lock();
            if term < runtime.current_term {
                return RaftRpcResponse::RequestVote {
                    term: runtime.current_term,
                    vote_granted: false,
                };
            }
            if term > runtime.current_term {
                runtime.current_term = term;
                runtime.voted_for = None;
                runtime.role = RaftRole::Follower;
                runtime.leader_id = None;
                hard_state = Some(runtime.hard_state());
            }

            let log_is_fresh = last_log_term > local_last_term
                || (last_log_term == local_last_term && last_log_index >= local_last_index);
            let can_vote = runtime
                .voted_for
                .as_ref()
                .is_none_or(|voted_for| voted_for == &candidate_id);
            let local_is_voter = runtime.membership.contains_id(&self.config.node_id);
            let candidate_is_voter = runtime.membership.contains_id(&candidate_id);
            let granted = local_is_voter && candidate_is_voter && can_vote && log_is_fresh;
            if granted {
                runtime.voted_for = Some(candidate_id);
                runtime.election_deadline = self.next_election_deadline();
                hard_state = Some(runtime.hard_state());
            }
            (runtime.current_term, granted)
        };
        if let Some(state) = hard_state {
            if let Err(err) = self.log.write_hard_state(&state) {
                return RaftRpcResponse::Error {
                    term: response_term,
                    error: err.to_string(),
                };
            }
        }
        RaftRpcResponse::RequestVote {
            term: response_term,
            vote_granted: granted,
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
        let mut hard_state = None;
        {
            let mut runtime = self.runtime.lock();
            if term < runtime.current_term {
                return RaftRpcResponse::AppendEntries {
                    term: runtime.current_term,
                    success: false,
                    match_index: self.log.last_index(),
                    conflict_index: None,
                    conflict_term: None,
                };
            }
            if term > runtime.current_term {
                runtime.current_term = term;
                runtime.voted_for = None;
                hard_state = Some(runtime.hard_state());
            }
            runtime.role = RaftRole::Follower;
            runtime.leader_id = Some(leader_id.clone());
            runtime.election_deadline = self.next_election_deadline();
        }
        if let Some(state) = hard_state.take() {
            if let Err(err) = self.log.write_hard_state(&state) {
                return RaftRpcResponse::Error {
                    term: state.current_term,
                    error: err.to_string(),
                };
            }
        }

        let append_report =
            match self
                .log
                .append_entries_from_leader(prev_log_index, prev_log_term, term, entries)
            {
                Ok(report) => report,
                Err(err) => {
                    return RaftRpcResponse::Error {
                        term: self.runtime.lock().current_term,
                        error: err.to_string(),
                    };
                }
            };
        if !append_report.success {
            return RaftRpcResponse::AppendEntries {
                term: self.runtime.lock().current_term,
                success: false,
                match_index: append_report.match_index,
                conflict_index: append_report.conflict_index,
                conflict_term: append_report.conflict_term,
            };
        }

        {
            let mut runtime = self.runtime.lock();
            let next_commit = runtime
                .commit_index
                .max(leader_commit.min(self.log.last_index()));
            if next_commit != runtime.commit_index {
                runtime.commit_index = next_commit;
                hard_state = Some(runtime.hard_state());
            }
        }
        if let Some(state) = hard_state {
            if let Err(err) = self.log.write_hard_state(&state) {
                return RaftRpcResponse::Error {
                    term: self.runtime.lock().current_term,
                    error: err.to_string(),
                };
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
        let mut hard_state = None;
        {
            let mut runtime = self.runtime.lock();
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
                runtime.current_term = term;
                runtime.voted_for = None;
                hard_state = Some(runtime.hard_state());
            }
            runtime.role = RaftRole::Follower;
            runtime.leader_id = Some(leader_id.clone());
            runtime.election_deadline = self.next_election_deadline();
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
        let current_snapshot_index = self
            .log
            .latest_snapshot()
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
        let snapshot_membership = match membership_from_snapshot_payload(&payload) {
            Ok(membership) => membership,
            Err(err) => {
                return RaftRpcResponse::Error {
                    term: self.runtime.lock().current_term,
                    error: err.to_string(),
                };
            }
        };
        let mut installed_index = current_snapshot_index;
        if last_included_index > current_snapshot_index {
            if let Err(err) = Broker::validate_raft_snapshot_payload(&payload) {
                return RaftRpcResponse::Error {
                    term: self.runtime.lock().current_term,
                    error: err,
                };
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
            if let Err(err) = self.broker.install_raft_snapshot(&payload) {
                return RaftRpcResponse::Error {
                    term: self.runtime.lock().current_term,
                    error: err,
                };
            }
        }
        if let Some(membership) = snapshot_membership {
            if let Err(err) = self.apply_membership(membership) {
                return RaftRpcResponse::Error {
                    term: self.runtime.lock().current_term,
                    error: err.to_string(),
                };
            }
        }

        {
            let mut runtime = self.runtime.lock();
            let next_commit = runtime.commit_index.max(installed_index);
            let next_applied = runtime.last_applied.max(installed_index);
            if next_commit != runtime.commit_index || next_applied != runtime.last_applied {
                runtime.commit_index = next_commit;
                runtime.last_applied = next_applied;
                hard_state = Some(runtime.hard_state());
            }
        }
        if let Some(state) = hard_state {
            if let Err(err) = self.log.write_hard_state(&state) {
                return RaftRpcResponse::Error {
                    term: self.runtime.lock().current_term,
                    error: err.to_string(),
                };
            }
        }

        RaftRpcResponse::InstallSnapshot {
            term: self.runtime.lock().current_term,
            success: true,
            last_included_index: installed_index,
        }
    }

    async fn election_loop(&self) {
        crate::routine_id!("ddl-routine-broker-raft-election-loop-1");
        loop {
            if self.is_leader() {
                if let Ok(_commit_guard) = self.commit_lock.try_lock() {
                    let _ = self.replicate_log_once(None).await;
                }
                tokio::time::sleep(self.config.heartbeat_interval).await;
                continue;
            }
            let should_start = {
                let runtime = self.runtime.lock();
                Instant::now() >= runtime.election_deadline
            };
            if should_start {
                if let Err(err) = self.start_election().await {
                    warn!(
                        target: "lmx::raft",
                        node_id = %self.config.node_id,
                        error = %err,
                        "raft election failed",
                    );
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn maintenance_loop(&self) {
        crate::routine_id!("ddl-routine-broker-raft-maintenance-loop-1");
        let interval = self.config.snapshot_interval.max(Duration::from_secs(1));
        loop {
            tokio::time::sleep(interval).await;
            if let Err(err) = self.snapshot_and_compact_if_needed(true) {
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
        let (term, hard_state) = {
            let mut runtime = self.runtime.lock();
            if !runtime.membership.contains_id(&self.config.node_id) {
                return Ok(());
            }
            runtime.role = RaftRole::Candidate;
            runtime.current_term = runtime.current_term.saturating_add(1);
            runtime.voted_for = Some(self.config.node_id.clone());
            runtime.leader_id = None;
            runtime.election_deadline = self.next_election_deadline();
            (runtime.current_term, runtime.hard_state())
        };
        self.log.write_hard_state(&hard_state)?;
        let mut votes = BTreeSet::new();
        votes.insert(self.config.node_id.clone());
        let last_log_index = self.log.last_index();
        let last_log_term = self.log.last_term();
        for peer in self.remote_peers() {
            let rpc = RaftRpc::RequestVote {
                term,
                candidate_id: self.config.node_id.clone(),
                last_log_index,
                last_log_term,
            };
            match self
                .send_rpc_to_peer(&peer, rpc, self.config.election_timeout_min)
                .await
            {
                Ok(RaftRpcResponse::RequestVote {
                    term: peer_term,
                    vote_granted,
                }) => {
                    if peer_term > term {
                        self.step_down(peer_term, None);
                        return Ok(());
                    }
                    if vote_granted {
                        votes.insert(peer.id.clone());
                    }
                }
                Ok(_) => {}
                Err(err) => {
                    debug!(
                        target: "lmx::raft",
                        node_id = %self.config.node_id,
                        peer = %peer.id,
                        error = %err,
                        "vote request failed",
                    );
                }
            }
        }

        let mut elected = false;
        if self.quorum_met(&votes) {
            let remote_peers = self.remote_peers();
            let next_index = self.log.last_index().saturating_add(1);
            let mut runtime = self.runtime.lock();
            if runtime.current_term == term && runtime.role == RaftRole::Candidate {
                runtime.role = RaftRole::Leader;
                runtime.leader_id = Some(self.config.node_id.clone());
                runtime.leader_progress = remote_peers
                    .into_iter()
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
                info!(
                    target: "lmx::raft",
                    node_id = %self.config.node_id,
                    term,
                    votes = votes.len(),
                    quorum = runtime.membership.quorum_size(),
                    "raft leader elected",
                );
                elected = true;
            }
        }
        if elected {
            self.append_leader_noop(term).await?;
        }
        Ok(())
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
        let (term, leader_commit) = {
            let runtime = self.runtime.lock();
            (runtime.current_term, runtime.commit_index)
        };
        let mut acks = BTreeSet::new();
        acks.insert(self.config.node_id.clone());
        if !self.is_leader() {
            return Ok(acks);
        }
        let mut tasks = JoinSet::new();
        for peer in self.remote_peers() {
            let node = self.clone();
            let peer_id = peer.id.clone();
            tasks.spawn(async move {
                node.replicate_to_peer(peer, term, leader_commit, target_index)
                    .await
                    .map(|acked| acked.then_some(peer_id))
            });
        }

        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(Some(peer_id))) => {
                    acks.insert(peer_id);
                }
                Ok(Ok(None)) => {}
                Ok(Err(err)) => return Err(err),
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
        }
        Ok(acks)
    }

    async fn replicate_to_peer(
        &self,
        peer: RaftPeerConfig,
        term: u64,
        leader_commit: u64,
        target_index: Option<u64>,
    ) -> Result<bool, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-replicate-peer-1");
        let next_index = {
            let mut runtime = self.runtime.lock();
            let fallback = self.log.last_index().saturating_add(1);
            runtime
                .leader_progress
                .entry(peer.id.clone())
                .or_insert(RaftPeerProgress {
                    next_index: fallback,
                    match_index: 0,
                })
                .next_index
                .max(1)
        };
        if self
            .log
            .latest_snapshot()
            .is_some_and(|snapshot| next_index <= snapshot.last_included_index)
        {
            debug!(
                target: "lmx::raft",
                node_id = %self.config.node_id,
                peer = %peer.id,
                next_index,
                "replicating snapshot before compacted log suffix",
            );
            return self
                .install_snapshot_to_peer(peer, term, target_index)
                .await;
        }
        let prev_log_index = next_index.saturating_sub(1);
        let Some(prev_log_term) = self.log.term_at(prev_log_index)? else {
            debug!(
                target: "lmx::raft",
                node_id = %self.config.node_id,
                peer = %peer.id,
                prev_log_index,
                "cannot replicate incremental entries before local snapshot boundary",
            );
            return self
                .install_snapshot_to_peer(peer, term, target_index)
                .await;
        };
        let entries = self.log.entries_from_limited(
            next_index,
            self.config.append_entries_max_entries,
            self.config.append_entries_max_bytes,
        )?;
        let rpc = RaftRpc::AppendEntries {
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
                    self.step_down(peer_term, None);
                    return Ok(false);
                }
                if success {
                    let mut runtime = self.runtime.lock();
                    let progress = runtime.leader_progress.entry(peer.id.clone()).or_insert(
                        RaftPeerProgress {
                            next_index: match_index.saturating_add(1),
                            match_index: 0,
                        },
                    );
                    progress.match_index = progress.match_index.max(match_index);
                    progress.next_index = progress
                        .next_index
                        .max(progress.match_index.saturating_add(1));
                    Ok(target_index.is_none_or(|target| progress.match_index >= target))
                } else {
                    let next_index =
                        self.next_index_after_conflict(conflict_term, conflict_index, next_index)?;
                    let mut runtime = self.runtime.lock();
                    let progress = runtime.leader_progress.entry(peer.id.clone()).or_insert(
                        RaftPeerProgress {
                            next_index,
                            match_index: 0,
                        },
                    );
                    progress.next_index = next_index.max(1);
                    Ok(false)
                }
            }
            Ok(RaftRpcResponse::Error {
                term: peer_term,
                error,
            }) => {
                if peer_term > term {
                    self.step_down(peer_term, None);
                }
                debug!(
                    target: "lmx::raft",
                    node_id = %self.config.node_id,
                    peer = %peer.id,
                    error,
                    "append entries rejected",
                );
                Ok(false)
            }
            Ok(_) => Ok(false),
            Err(err) => {
                debug!(
                    target: "lmx::raft",
                    node_id = %self.config.node_id,
                    peer = %peer.id,
                    error = %err,
                    "append entries failed",
                );
                Ok(false)
            }
        }
    }

    async fn install_snapshot_to_peer(
        &self,
        peer: RaftPeerConfig,
        term: u64,
        target_index: Option<u64>,
    ) -> Result<bool, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-install-snapshot-peer-1");
        let Some(snapshot) = self.log.latest_snapshot_file()? else {
            debug!(
                target: "lmx::raft",
                node_id = %self.config.node_id,
                peer = %peer.id,
                "cannot install snapshot because no local snapshot exists",
            );
            return Ok(false);
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
            let end = if payload_bytes.is_empty() {
                0
            } else {
                offset.saturating_add(chunk_size).min(payload_bytes.len())
            };
            let done = end >= payload_bytes.len();
            let data = BASE64.encode(&payload_bytes[offset..end]);
            let rpc = RaftRpc::InstallSnapshot {
                term,
                leader_id: self.config.node_id.clone(),
                last_included_index,
                last_included_term: snapshot.metadata.last_included_term,
                payload_sha256: Some(payload_sha256.clone()),
                offset: offset as u64,
                done,
                data,
            };
            match self.send_rpc_to_peer(&peer, rpc, timeout).await {
                Ok(RaftRpcResponse::InstallSnapshot {
                    term: peer_term,
                    success,
                    last_included_index: installed_index,
                }) => {
                    if peer_term > term {
                        self.step_down(peer_term, None);
                        return Ok(false);
                    }
                    if !success {
                        return Ok(false);
                    }
                    if done {
                        let mut runtime = self.runtime.lock();
                        let progress = runtime.leader_progress.entry(peer.id.clone()).or_insert(
                            RaftPeerProgress {
                                next_index: installed_index.saturating_add(1),
                                match_index: 0,
                            },
                        );
                        progress.match_index = progress.match_index.max(installed_index);
                        progress.next_index = progress
                            .next_index
                            .max(progress.match_index.saturating_add(1));
                        return Ok(target_index.is_none_or(|target| progress.match_index >= target));
                    }
                }
                Ok(RaftRpcResponse::Error {
                    term: peer_term,
                    error,
                }) => {
                    if peer_term > term {
                        self.step_down(peer_term, None);
                    }
                    debug!(
                        target: "lmx::raft",
                        node_id = %self.config.node_id,
                        peer = %peer.id,
                        error,
                        "install snapshot rejected",
                    );
                    return Ok(false);
                }
                Ok(_) => return Ok(false),
                Err(err) => {
                    debug!(
                        target: "lmx::raft",
                        node_id = %self.config.node_id,
                        peer = %peer.id,
                        error = %err,
                        "install snapshot failed",
                    );
                    return Ok(false);
                }
            }
            if done {
                return Ok(false);
            }
            offset = end;
            if payload_bytes.is_empty() {
                return Ok(false);
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
            let acks = self.replicate_log_once(Some(target_index)).await?;
            if acks.len() > best_acks.len() {
                best_acks = acks.clone();
            }
            if self.quorum_met(&acks) || !self.is_leader() {
                return Ok(acks);
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(best_acks);
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
        let entries = self.log.read_entries()?;
        for entry in entries {
            let should_apply = {
                let runtime = self.runtime.lock();
                entry.index > runtime.last_applied && entry.index <= runtime.commit_index
            };
            if !should_apply {
                continue;
            }
            match entry.command.clone() {
                RaftCommand::Noop => {}
                RaftCommand::ClientRequest {
                    client_id,
                    request,
                    grant,
                } => {
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
                RaftCommand::DropClient { client_id } => {
                    self.broker.drop_client(client_id);
                }
                RaftCommand::SetMembership { membership } => {
                    self.apply_membership(membership)?;
                }
            }
            self.runtime.lock().last_applied = entry.index;
        }
        Ok(())
    }

    fn snapshot_and_compact_if_needed(&self, periodic: bool) -> Result<(), BrokerRaftError> {
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

        let entries = self.log.read_entries()?;
        if entries.is_empty() {
            return Ok(());
        }
        let bytes = self.log.log_len_bytes()?;
        let elapsed = self.maintenance.lock().last_snapshot_at.elapsed();
        let latest_snapshot_index = self
            .log
            .latest_snapshot()
            .map(|snapshot| snapshot.last_included_index)
            .unwrap_or(0);

        let threshold_reached = (self.config.snapshot_max_log_entries > 0
            && entries.len() as u64 >= self.config.snapshot_max_log_entries)
            || (self.config.snapshot_max_log_bytes > 0
                && bytes >= self.config.snapshot_max_log_bytes);
        let periodic_due = periodic && elapsed >= self.config.snapshot_interval;
        if !threshold_reached && !periodic_due {
            return Ok(());
        }

        let metrics = self.broker.metrics();
        if metrics.waiters != 0 {
            debug!(
                target: "lmx::raft",
                node_id = %self.config.node_id,
                holders = metrics.holders,
                waiters = metrics.waiters,
                pending_deadlines = metrics.pending_deadlines,
                "raft log compaction skipped because queued broker waiters need log replay",
            );
            return Ok(());
        }

        let compact_through = commit_index.saturating_sub(self.config.trailing_log_entries);
        if compact_through == 0 || compact_through <= latest_snapshot_index {
            self.maintenance.lock().last_snapshot_at = Instant::now();
            return Ok(());
        }

        let snapshot_term = entries
            .iter()
            .find(|entry| entry.index == commit_index)
            .map(|entry| entry.term)
            .unwrap_or(current_term);
        let broker_snapshot = self
            .broker
            .snapshot_for_raft()
            .map_err(BrokerRaftError::BrokerSnapshot)?;
        let payload = serde_json::json!({
            "nodeId": self.config.node_id,
            "note": "Broker state snapshot. Queued waiters are intentionally not snapshotted yet, so compaction only runs when waiter count is zero.",
            "membership": self.membership(),
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
                    updated_at_ms: unix_ms(),
                },
            );
        }
        let Some(pending) = transfers.get_mut(&key) else {
            return Err(BrokerRaftError::Rpc(format!(
                "snapshot chunk for index {last_included_index} started at offset {offset} without offset 0"
            )));
        };
        let expected_offset = pending.bytes_written;
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
        pending.bytes_written = pending.bytes_written.saturating_add(chunk.len() as u64);
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
            "raft-install-snapshot-{}.json.part",
            sha256_hex(key.as_bytes())
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

    async fn send_rpc_to_peer(
        &self,
        peer: &RaftPeerConfig,
        rpc: RaftRpc,
        timeout: Duration,
    ) -> Result<RaftRpcResponse, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-send-rpc-peer-1");
        let connection = {
            let mut connections = self.rpc_connections.lock();
            connections
                .entry(peer.id.clone())
                .or_insert_with(|| Arc::new(AsyncMutex::new(RaftRpcConnection::default())))
                .clone()
        };
        let mut connection = connection.lock().await;
        connection.call(&peer.addr, rpc, timeout).await
    }

    fn apply_membership(&self, membership: RaftMembership) -> Result<(), BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-apply-membership-1");
        let membership = membership.normalized();
        let active_peers = membership.active_peers();
        let active_ids: BTreeSet<String> =
            active_peers.iter().map(|peer| peer.id.clone()).collect();
        let next_index = self.log.last_index().saturating_add(1);
        let hard_state = {
            let mut runtime = self.runtime.lock();
            runtime.membership = membership;
            runtime.leader_progress.retain(|peer_id, _| {
                active_ids.contains(peer_id) && peer_id != &self.config.node_id
            });
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
            if !active_ids.contains(&self.config.node_id) {
                runtime.role = RaftRole::Follower;
                runtime.leader_id = None;
                runtime.voted_for = None;
                runtime.leader_progress.clear();
                Some(runtime.hard_state())
            } else {
                None
            }
        };
        if let Some(state) = hard_state {
            self.log.write_hard_state(&state)?;
        }
        self.rpc_connections
            .lock()
            .retain(|peer_id, _| active_ids.contains(peer_id) && peer_id != &self.config.node_id);
        Ok(())
    }

    fn step_down(&self, term: u64, leader_id: Option<String>) {
        crate::routine_id!("ddl-routine-broker-raft-step-down-1");
        let hard_state = {
            let mut runtime = self.runtime.lock();
            if term >= runtime.current_term {
                runtime.current_term = term;
                runtime.role = RaftRole::Follower;
                runtime.voted_for = None;
                runtime.leader_id = leader_id;
                runtime.election_deadline = self.next_election_deadline();
                Some(runtime.hard_state())
            } else {
                None
            }
        };
        if let Some(state) = hard_state {
            if let Err(err) = self.log.write_hard_state(&state) {
                warn!(
                    target: "lmx::raft",
                    node_id = %self.config.node_id,
                    error = %err,
                    "failed to persist raft hard state after step down",
                );
            }
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
        if let Some(term) = conflict_term {
            if let Some(last_index) = self.log.last_index_for_term(term)? {
                return Ok(last_index.saturating_add(1).max(1));
            }
        }
        Ok(conflict_index
            .unwrap_or_else(|| current_next_index.saturating_sub(1))
            .max(1))
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

fn unix_ms() -> u64 {
    crate::routine_id!("ddl-routine-broker-raft-unix-ms-1");
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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

async fn send_rpc(
    addr: &str,
    rpc: RaftRpc,
    timeout: Duration,
) -> Result<RaftRpcResponse, BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-send-rpc-1");
    let fut = async {
        let mut stream = TcpStream::connect(addr).await?;
        let body = serde_json::to_vec(&rpc)?;
        stream.write_all(&body).await?;
        stream.write_all(b"\n").await?;
        stream.flush().await?;

        let mut reader = TokioBufReader::new(stream);
        let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes()).await?;
        let response = serde_json::from_str(line.trim())?;
        Ok::<_, BrokerRaftError>(response)
    };
    tokio::time::timeout(timeout.max(Duration::from_millis(50)), fut)
        .await
        .map_err(|err| BrokerRaftError::Rpc(err.to_string()))?
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
    Ok(Some(membership.normalized()))
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

fn read_last_log_position(path: &Path) -> Result<Option<(u64, u64)>, BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-read-last-pos-1");
    let entries = read_log_entries(path)?;
    Ok(entries.last().map(|entry| (entry.index, entry.term)))
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
    Ok(entries)
}

fn term_at_index(state: &RaftLogState, entries: &[RaftLogEntry], index: u64) -> Option<u64> {
    crate::routine_id!("ddl-routine-broker-raft-term-at-index-1");
    if index == 0 {
        return Some(0);
    }
    if let Some(snapshot) = &state.latest_snapshot {
        if snapshot.last_included_index == index {
            return Some(snapshot.last_included_term);
        }
        if index < snapshot.last_included_index {
            return None;
        }
    }
    entries
        .iter()
        .find(|entry| entry.index == index)
        .map(|entry| entry.term)
}

fn validate_append_entries_shape(
    prev_log_index: u64,
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
        if entry.term > leader_term {
            return Err(BrokerRaftError::InvalidAppendEntries(format!(
                "entry index {} has term {} greater than leader term {}",
                entry.index, entry.term, leader_term
            )));
        }
        expected_index = expected_index.saturating_add(1);
    }
    Ok(())
}

fn first_index_for_term(state: &RaftLogState, entries: &[RaftLogEntry], term: u64) -> Option<u64> {
    crate::routine_id!("ddl-routine-broker-raft-first-index-for-term-1");
    let snapshot_match = state
        .latest_snapshot
        .as_ref()
        .filter(|snapshot| snapshot.last_included_term == term)
        .map(|snapshot| snapshot.last_included_index);
    entries
        .iter()
        .find(|entry| entry.term == term)
        .map(|entry| entry.index)
        .or(snapshot_match)
}

fn last_index_for_term(state: &RaftLogState, entries: &[RaftLogEntry], term: u64) -> Option<u64> {
    crate::routine_id!("ddl-routine-broker-raft-last-index-for-term-1");
    entries
        .iter()
        .rev()
        .find(|entry| entry.term == term)
        .map(|entry| entry.index)
        .or_else(|| {
            state
                .latest_snapshot
                .as_ref()
                .filter(|snapshot| snapshot.last_included_term == term)
                .map(|snapshot| snapshot.last_included_index)
        })
}

fn append_log_entries(path: &Path, entries: &[RaftLogEntry]) -> Result<(), BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-append-log-entries-1");
    if entries.is_empty() {
        return Ok(());
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    for entry in entries {
        serde_json::to_writer(&mut file, entry)?;
        file.write_all(b"\n")?;
    }
    file.sync_data()?;
    Ok(())
}

fn rewrite_log(path: &Path, entries: &[RaftLogEntry]) -> Result<(), BrokerRaftError> {
    crate::routine_id!("ddl-routine-broker-raft-rewrite-log-1");
    let tmp = path.with_extension("ndjson.tmp");
    {
        let mut file = File::create(&tmp)?;
        for entry in entries {
            serde_json::to_writer(&mut file, entry)?;
            file.write_all(b"\n")?;
        }
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
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
                            .is_some_and(|name| {
                                name.starts_with("raft-install-snapshot-")
                                    && name.ends_with(".json.part")
                            })
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

    async fn serve_append_entries_until_simple_membership(listener: TcpListener) -> Vec<Vec<u64>> {
        crate::routine_id!("ddl-routine-broker-raft-test-serve-append-until-simple-1");
        let (stream, _) = listener.accept().await.expect("accept append peer");
        let mut reader = TokioBufReader::new(stream);
        let mut observed = Vec::new();
        loop {
            let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes())
                .await
                .expect("read append frame");
            let rpc: RaftRpc = serde_json::from_str(&line).expect("parse append frame");
            let (term, match_index, indexes, saw_simple_membership) = match rpc {
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
                    let saw_simple_membership = entries.iter().any(|entry| {
                        matches!(
                            entry.command,
                            RaftCommand::SetMembership {
                                membership: RaftMembership::Simple { .. }
                            }
                        )
                    });
                    (term, match_index, indexes, saw_simple_membership)
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
            if saw_simple_membership {
                break;
            }
        }
        observed
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
            .append_entries_from_leader(4, 99, 3, Vec::new())
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
    fn append_entries_rejects_malformed_non_contiguous_batches() {
        let dir = temp_dir("raft-append-malformed-gap");
        let store = RaftLogStore::open(&dir).expect("open store");
        store.append(1, RaftCommand::Noop).expect("append seed");

        let err = store
            .append_entries_from_leader(
                1,
                1,
                2,
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
            .append_entries_from_leader(2, 1, 2, leader_entries)
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
        cfg.election_timeout_min = Duration::from_millis(50);
        cfg.election_timeout_max = Duration::from_millis(100);
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
            RaftCommand::ClientRequest {
                request: Request::Lock {
                    wait: Some(false),
                    ..
                },
                ..
            }
        ));

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
        let caught_up = raft
            .install_snapshot_to_peer(peer, 4, Some(7))
            .await
            .expect("install snapshot to fake peer");
        assert!(caught_up);
        server.await.expect("snapshot peer server");
        assert_eq!(received.lock().as_slice(), expected_bytes.as_slice());
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
        assert_eq!(hard_state.commit_index, entries[0].index);

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
    fn compaction_skips_queued_waiters_to_preserve_replay() {
        let dir = temp_dir("raft-waiter-state-maintenance");
        let mut cfg = test_raft_config(dir.clone());
        cfg.snapshot_max_log_entries = 1;
        cfg.snapshot_max_log_bytes = u64::MAX;
        cfg.trailing_log_entries = 0;
        let raft = BrokerRaft::open(cfg).expect("open raft");
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

        assert!(raft.log.latest_snapshot().is_none());
        assert_eq!(raft.log.read_entries().expect("entries").len(), 2);

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

    #[test]
    fn log_store_serializes_concurrent_disk_operations() {
        let dir = temp_dir("raft-log-concurrent");
        let store = Arc::new(RaftLogStore::open(&dir).expect("open store"));
        let start = Arc::new(std::sync::Barrier::new(4));
        let mut handles = Vec::new();

        for writer_id in 0..2 {
            let store = Arc::clone(&store);
            let start = Arc::clone(&start);
            handles.push(std::thread::spawn(move || {
                start.wait();
                for _ in 0..250 {
                    store
                        .append(1 + writer_id, RaftCommand::Noop)
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
            let mut oracle: Vec<RaftLogEntry> = Vec::new();

            for step in 0..300 {
                match next_fuzz(&mut rng) % 6 {
                    0..=2 => {
                        let term = 1 + (next_fuzz(&mut rng) % 17);
                        let entry = store.append(term, RaftCommand::Noop).expect("append");
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
                        let keep_from = if oracle.is_empty() {
                            0
                        } else {
                            (next_fuzz(&mut rng) as usize) % (oracle.len() + 1)
                        };
                        let replacement = oracle[keep_from..].to_vec();
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
