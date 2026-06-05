//! Raft-facing broker wrapper and durable local log plumbing.
//!
//! This module provides the `BrokerRaft` server backend: peer-list config,
//! leader election, quorum replication, durable append-only logs, snapshot
//! metadata, and compaction-by-snapshot-index.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinHandle, JoinSet};
use tracing::{debug, error, info, warn};

use crate::broker::{Broker, BrokerConfig, ClientId};
use crate::protocol::{Request, Response};

const LOG_FILE: &str = "raft-log.ndjson";
const SNAPSHOT_FILE: &str = "raft-snapshot.json";
const HARD_STATE_FILE: &str = "raft-hard-state.json";
const DEFAULT_RAFT_RPC_MAX_FRAME_BYTES: usize = 128 * 1024 * 1024;

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
        let n = self.cluster_size();
        if n == 0 {
            0
        } else {
            (n / 2) + 1
        }
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
        if self.peers.len() < 3 {
            return Err(BrokerRaftError::InvalidConfig(
                "raft.enabled=true requires at least 3 peers for failover".into(),
            ));
        }
        if self.peers.len() % 2 == 0 {
            return Err(BrokerRaftError::InvalidConfig(
                "raft peers should be an odd-sized cluster, e.g. 3 or 5".into(),
            ));
        }
        if !self.peers.iter().any(|p| p.id == self.node_id) {
            return Err(BrokerRaftError::InvalidConfig(format!(
                "raft.node_id `{}` must appear in raft.peers",
                self.node_id
            )));
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
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RaftPeerConfig {
    pub id: String,
    pub addr: String,
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
    },
    DropClient {
        client_id: ClientId,
    },
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
        payload: serde_json::Value,
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
        let mut state = self.state.lock();
        let entry = RaftLogEntry {
            index: state.last_index.saturating_add(1),
            term,
            created_at_ms: unix_ms(),
            command,
        };

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)?;
        serde_json::to_writer(&mut file, &entry)?;
        file.write_all(b"\n")?;
        file.sync_data()?;

        state.last_index = entry.index;
        state.last_term = entry.term;
        Ok(entry)
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
        entries: Vec<RaftLogEntry>,
    ) -> Result<RaftAppendReport, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-append-from-leader-1");
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
        let metadata = RaftSnapshotMetadata {
            last_included_index,
            last_included_term,
            created_at_ms: unix_ms(),
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
        payload: serde_json::Value,
    ) -> Result<RaftSnapshotMetadata, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-install-snapshot-log-1");
        let mut state = self.state.lock();
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
            })),
            maintenance: Arc::new(Mutex::new(RaftMaintenanceState {
                last_snapshot_at: now,
            })),
            commit_lock: Arc::new(tokio::sync::Mutex::new(())),
            config,
            log,
        };
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
        self.config
            .peers
            .iter()
            .find(|p| p.id == leader)
            .map(|p| p.addr.clone())
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
            quorum = self.config.quorum_size(),
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
            quorum = self.config.quorum_size(),
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
        let _commit_guard = self.commit_lock.lock().await;
        if !self.is_leader() {
            return Err(BrokerRaftError::NotLeader {
                leader_id: self.leader_id(),
                leader_addr: self.leader_addr(),
            });
        }
        let term = self.runtime.lock().current_term;
        let entry = self.log.append(
            term,
            RaftCommand::ClientRequest {
                client_id: client,
                request: request.clone(),
            },
        )?;
        let votes = self.replicate_until_quorum(entry.index).await?;
        let quorum = self.config.quorum_size();
        if votes < quorum {
            return Err(BrokerRaftError::QuorumUnavailable {
                index: entry.index,
                votes,
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

    pub async fn drop_client(&self, client: ClientId) -> Result<u64, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-drop-client-1");
        if !self.is_leader() {
            return Err(BrokerRaftError::NotLeader {
                leader_id: self.leader_id(),
                leader_addr: self.leader_addr(),
            });
        }
        let _commit_guard = self.commit_lock.lock().await;
        if !self.is_leader() {
            return Err(BrokerRaftError::NotLeader {
                leader_id: self.leader_id(),
                leader_addr: self.leader_addr(),
            });
        }
        let term = self.runtime.lock().current_term;
        let entry = self
            .log
            .append(term, RaftCommand::DropClient { client_id: client })?;
        let votes = self.replicate_until_quorum(entry.index).await?;
        let quorum = self.config.quorum_size();
        if votes < quorum {
            return Err(BrokerRaftError::QuorumUnavailable {
                index: entry.index,
                votes,
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

    pub async fn run_ephemeral(
        &self,
        request: Request,
        request_uuid: &str,
        wait: Duration,
        is_acquire: bool,
    ) -> Result<Option<Response>, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-run-ephemeral-1");
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
            if let Err(err) = self.drop_client(client_id).await {
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
        let line = read_raft_frame_bounded(&mut reader, raft_rpc_max_frame_bytes()).await?;
        let rpc: RaftRpc = serde_json::from_str(line.trim())?;
        let response = self.handle_rpc(rpc).await;
        let mut stream = reader.into_inner();
        serde_json::to_writer(&mut Vec::new(), &response)?;
        let body = serde_json::to_vec(&response)?;
        stream.write_all(&body).await?;
        stream.write_all(b"\n").await?;
        stream.flush().await?;
        Ok(())
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
            let granted = can_vote && log_is_fresh;
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
            runtime.leader_id = Some(leader_id);
            runtime.election_deadline = self.next_election_deadline();
        }

        let append_report =
            match self
                .log
                .append_entries_from_leader(prev_log_index, prev_log_term, entries)
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
            runtime.role = RaftRole::Candidate;
            runtime.current_term = runtime.current_term.saturating_add(1);
            runtime.voted_for = Some(self.config.node_id.clone());
            runtime.leader_id = None;
            runtime.election_deadline = self.next_election_deadline();
            (runtime.current_term, runtime.hard_state())
        };
        self.log.write_hard_state(&hard_state)?;
        let mut votes = 1usize;
        let last_log_index = self.log.last_index();
        let last_log_term = self.log.last_term();
        for peer in self.remote_peers() {
            let rpc = RaftRpc::RequestVote {
                term,
                candidate_id: self.config.node_id.clone(),
                last_log_index,
                last_log_term,
            };
            match send_rpc(&peer.addr, rpc, self.config.election_timeout_min).await {
                Ok(RaftRpcResponse::RequestVote {
                    term: peer_term,
                    vote_granted,
                }) => {
                    if peer_term > term {
                        self.step_down(peer_term, None);
                        return Ok(());
                    }
                    if vote_granted {
                        votes += 1;
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

        if votes >= self.config.quorum_size() {
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
                    votes,
                    quorum = self.config.quorum_size(),
                    "raft leader elected",
                );
            }
        }
        Ok(())
    }

    async fn replicate_log_once(
        &self,
        target_index: Option<u64>,
    ) -> Result<usize, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-replicate-once-1");
        let (term, leader_commit) = {
            let runtime = self.runtime.lock();
            (runtime.current_term, runtime.commit_index)
        };
        let mut votes = 1usize;
        let mut tasks = JoinSet::new();
        for peer in self.remote_peers() {
            let node = self.clone();
            tasks.spawn(async move {
                node.replicate_to_peer(peer, term, leader_commit, target_index)
                    .await
            });
        }

        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok(true)) => votes += 1,
                Ok(Ok(false)) => {}
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
                return Ok(votes);
            }
        }
        Ok(votes)
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
        let prev_log_index = next_index.saturating_sub(1);
        let Some(prev_log_term) = self.log.term_at(prev_log_index)? else {
            debug!(
                target: "lmx::raft",
                node_id = %self.config.node_id,
                peer = %peer.id,
                prev_log_index,
                "cannot replicate incremental entries before local snapshot boundary",
            );
            return Ok(false);
        };
        let entries = self.log.entries_from(next_index)?;
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
        match send_rpc(&peer.addr, rpc, timeout).await {
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

    async fn replicate_until_quorum(&self, target_index: u64) -> Result<usize, BrokerRaftError> {
        crate::routine_id!("ddl-routine-broker-raft-replicate-until-quorum-1");
        let quorum = self.config.quorum_size();
        let timeout = self
            .config
            .election_timeout_max
            .saturating_mul(2)
            .max(Duration::from_millis(500));
        let deadline = deadline_after(timeout);
        let mut best_votes = 0usize;
        loop {
            let votes = self.replicate_log_once(Some(target_index)).await?;
            best_votes = best_votes.max(votes);
            if votes >= quorum || !self.is_leader() {
                return Ok(votes);
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(best_votes);
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
                RaftCommand::ClientRequest { client_id, request } => {
                    let grant_lock_uuid = match &request {
                        Request::Lock { uuid, .. } => Some(uuid.clone()),
                        _ => None,
                    };
                    self.broker
                        .handle_request_with_grant_uuid(client_id, request, grant_lock_uuid);
                }
                RaftCommand::DropClient { client_id } => {
                    self.broker.drop_client(client_id);
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
        let broker_is_idle =
            metrics.holders == 0 && metrics.waiters == 0 && metrics.pending_deadlines == 0;
        if !broker_is_idle {
            debug!(
                target: "lmx::raft",
                node_id = %self.config.node_id,
                holders = metrics.holders,
                waiters = metrics.waiters,
                pending_deadlines = metrics.pending_deadlines,
                "raft log compaction skipped because broker state is not idle",
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
        let payload = serde_json::json!({
            "nodeId": self.config.node_id,
            "note": "Idle broker-state snapshot. Log compaction only runs while the applied broker state is idle, so restart restore can safely resume from an empty lock table.",
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
        self.config
            .peers
            .iter()
            .filter(|peer| peer.id != self.config.node_id)
            .cloned()
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
    Ok(Some(snapshot))
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
            .append_entries_from_leader(4, 99, Vec::new())
            .expect("append entries check");

        assert!(!report.success);
        assert_eq!(report.conflict_term, Some(2));
        assert_eq!(report.conflict_index, Some(3));

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
            .append_entries_from_leader(2, 1, leader_entries)
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
    fn compaction_skips_active_broker_state_to_preserve_replay() {
        let dir = temp_dir("raft-active-state-maintenance");
        let mut cfg = test_raft_config(dir.clone());
        cfg.snapshot_max_log_entries = 1;
        cfg.snapshot_max_log_bytes = u64::MAX;
        cfg.trailing_log_entries = 0;
        let raft = BrokerRaft::open(cfg).expect("open raft");
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

        assert!(raft.log.latest_snapshot().is_none());
        assert_eq!(raft.log.read_entries().expect("entries").len(), 1);

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
