//! In-memory Raft cluster simulator.
//!
//! `RaftSim` runs real [`BrokerRaft`](crate::BrokerRaft) nodes in one Tokio
//! process and routes peer RPCs through memory instead of TCP sockets. It is
//! intended for deterministic-ish integration tests, fuzz harnesses, and local
//! experiments that need the Raft backend without binding ports.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::broker::BrokerConfig;
use crate::broker_raft::{BrokerRaft, RaftInMemoryNetwork, RaftProgressSnapshot};
use crate::protocol::{Request, Response};
use crate::{BrokerRaftConfig, BrokerRaftError, RaftPeerConfig};

const DEFAULT_SIM_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(25);
const DEFAULT_SIM_ELECTION_TIMEOUT_MIN: Duration = Duration::from_millis(150);
const DEFAULT_SIM_ELECTION_TIMEOUT_MAX: Duration = Duration::from_millis(300);
const DEFAULT_SIM_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct RaftSimConfig {
    pub node_count: usize,
    pub data_dir: PathBuf,
    pub cleanup_data_dir: bool,
    pub broker: BrokerConfig,
    pub heartbeat_interval: Duration,
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub snapshot_interval: Duration,
    pub snapshot_max_log_entries: u64,
    pub snapshot_max_log_bytes: u64,
    pub trailing_log_entries: u64,
    pub append_entries_max_entries: usize,
    pub append_entries_max_bytes: usize,
    pub append_entries_max_inline_batches: usize,
    pub sync_log: bool,
    pub sync_commit: bool,
    pub peer_token: Option<String>,
}

impl Default for RaftSimConfig {
    fn default() -> Self {
        crate::routine_id!("ddl-routine-raft-sim-config-default-1");
        let defaults = BrokerRaftConfig::default();
        Self {
            node_count: 3,
            data_dir: std::env::temp_dir().join(format!("lmx-raft-sim-{}", Uuid::new_v4())),
            cleanup_data_dir: true,
            broker: BrokerConfig::default(),
            heartbeat_interval: DEFAULT_SIM_HEARTBEAT_INTERVAL,
            election_timeout_min: DEFAULT_SIM_ELECTION_TIMEOUT_MIN,
            election_timeout_max: DEFAULT_SIM_ELECTION_TIMEOUT_MAX,
            snapshot_interval: defaults.snapshot_interval,
            snapshot_max_log_entries: defaults.snapshot_max_log_entries,
            snapshot_max_log_bytes: defaults.snapshot_max_log_bytes,
            trailing_log_entries: defaults.trailing_log_entries,
            append_entries_max_entries: defaults.append_entries_max_entries,
            append_entries_max_bytes: defaults.append_entries_max_bytes,
            append_entries_max_inline_batches: defaults.append_entries_max_inline_batches,
            sync_log: defaults.sync_log,
            sync_commit: defaults.sync_commit,
            peer_token: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftSimLock {
    pub key: String,
    pub lock_uuid: String,
    pub fencing_token: Option<u64>,
}

#[derive(Debug, thiserror::Error)]
pub enum RaftSimError {
    #[error(transparent)]
    Raft(#[from] BrokerRaftError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("raft simulation timed out after {timeout_ms}ms waiting for {operation}")]
    Timeout {
        operation: &'static str,
        timeout_ms: u64,
    },
    #[error("raft simulation node `{0}` was not found")]
    NodeNotFound(String),
    #[error("raft simulation request `{request_id}` completed without a broker response")]
    NoResponse { request_id: String },
    #[error("raft simulation request `{request_id}` returned unexpected response: {response:?}")]
    UnexpectedResponse {
        request_id: String,
        response: Response,
    },
}

pub struct RaftSim {
    nodes: BTreeMap<String, BrokerRaft>,
    tasks: BTreeMap<String, Vec<JoinHandle<()>>>,
    network: Arc<RaftInMemoryNetwork>,
    data_dir: PathBuf,
    cleanup_data_dir: bool,
}

impl RaftSim {
    pub async fn new(node_count: usize) -> Result<Self, RaftSimError> {
        crate::routine_id!("ddl-routine-raft-sim-new-1");
        Self::with_config(RaftSimConfig {
            node_count,
            ..RaftSimConfig::default()
        })
        .await
    }

    pub async fn with_config(config: RaftSimConfig) -> Result<Self, RaftSimError> {
        crate::routine_id!("ddl-routine-raft-sim-with-config-1");
        if config.node_count < 3 {
            return Err(BrokerRaftError::InvalidConfig(
                "raft simulation requires at least 3 nodes".into(),
            )
            .into());
        }
        fs::create_dir_all(&config.data_dir)?;
        let network = Arc::new(RaftInMemoryNetwork::new());
        let peers = sim_peers(config.node_count);
        let mut nodes = BTreeMap::new();

        for peer in &peers {
            let mut raft_config = BrokerRaftConfig::default();
            raft_config.enabled = true;
            raft_config.node_id = peer.id.clone();
            raft_config.bind_addr = Some("127.0.0.1:0".parse().expect("valid sim bind addr"));
            raft_config.advertise_addr = Some(peer.addr.clone());
            raft_config.data_dir = config.data_dir.join(&peer.id);
            raft_config.data_dir_lock = false;
            raft_config.broker = config.broker.clone();
            raft_config.heartbeat_interval = config.heartbeat_interval;
            raft_config.election_timeout_min = config.election_timeout_min;
            raft_config.election_timeout_max = config.election_timeout_max;
            raft_config.snapshot_interval = config.snapshot_interval;
            raft_config.snapshot_max_log_entries = config.snapshot_max_log_entries;
            raft_config.snapshot_max_log_bytes = config.snapshot_max_log_bytes;
            raft_config.trailing_log_entries = config.trailing_log_entries;
            raft_config.append_entries_max_entries = config.append_entries_max_entries;
            raft_config.append_entries_max_bytes = config.append_entries_max_bytes;
            raft_config.append_entries_max_inline_batches =
                config.append_entries_max_inline_batches;
            raft_config.sync_log = config.sync_log;
            raft_config.sync_commit = config.sync_commit;
            raft_config.peer_token = config.peer_token.clone();
            raft_config.peers = peers.clone();

            let node = BrokerRaft::open_in_memory(raft_config, Arc::clone(&network))?;
            network.register(node.clone());
            nodes.insert(peer.id.clone(), node);
        }

        let mut tasks = BTreeMap::new();
        for (node_id, node) in &nodes {
            tasks.insert(node_id.clone(), node.spawn_in_memory_raft_tasks());
        }

        Ok(Self {
            nodes,
            tasks,
            network,
            data_dir: config.data_dir,
            cleanup_data_dir: config.cleanup_data_dir,
        })
    }

    pub fn data_dir(&self) -> &Path {
        crate::routine_id!("ddl-routine-raft-sim-data-dir-1");
        &self.data_dir
    }

    pub fn node_ids(&self) -> Vec<String> {
        crate::routine_id!("ddl-routine-raft-sim-node-ids-1");
        self.nodes.keys().cloned().collect()
    }

    pub fn node(&self, node_id: &str) -> Option<&BrokerRaft> {
        crate::routine_id!("ddl-routine-raft-sim-node-1");
        self.nodes.get(node_id)
    }

    pub fn nodes(&self) -> &BTreeMap<String, BrokerRaft> {
        crate::routine_id!("ddl-routine-raft-sim-nodes-1");
        &self.nodes
    }

    pub fn restart_node(&mut self, node_id: &str) -> Result<(), RaftSimError> {
        crate::routine_id!("ddl-routine-raft-sim-restart-node-1");
        let config = self
            .nodes
            .get(node_id)
            .ok_or_else(|| RaftSimError::NodeNotFound(node_id.to_string()))?
            .config()
            .clone();
        if let Some(handles) = self.tasks.remove(node_id) {
            for handle in handles {
                handle.abort();
            }
        }
        self.network.unregister(node_id);
        self.nodes.remove(node_id);

        let node = BrokerRaft::open_in_memory(config, Arc::clone(&self.network))?;
        self.network.register(node.clone());
        self.tasks
            .insert(node_id.to_string(), node.spawn_in_memory_raft_tasks());
        self.nodes.insert(node_id.to_string(), node);
        Ok(())
    }

    pub fn add_node(&mut self, node_id: impl Into<String>) -> Result<RaftPeerConfig, RaftSimError> {
        crate::routine_id!("ddl-routine-raft-sim-add-node-1");
        let node_id = node_id.into();
        if self.nodes.contains_key(&node_id) {
            return Err(BrokerRaftError::InvalidConfig(format!(
                "raft simulation node `{node_id}` already exists"
            ))
            .into());
        }
        let template = self.nodes.values().next().ok_or_else(|| {
            BrokerRaftError::InvalidConfig("raft simulation has no existing config template".into())
        })?;
        let peer = RaftPeerConfig {
            id: node_id.clone(),
            addr: format!("memory://{node_id}"),
        };
        let mut config = template.config().clone();
        config.node_id = node_id.clone();
        config.bind_addr = Some("127.0.0.1:0".parse().expect("valid sim bind addr"));
        config.advertise_addr = Some(peer.addr.clone());
        config.data_dir = self.data_dir.join(&node_id);
        config.data_dir_lock = false;
        config.peers = template.active_peers();

        let node = BrokerRaft::open_in_memory(config, Arc::clone(&self.network))?;
        self.network.register(node.clone());
        self.tasks
            .insert(node_id.clone(), node.spawn_in_memory_raft_tasks());
        self.nodes.insert(node_id, node);
        Ok(peer)
    }

    pub fn progress(&self) -> Vec<RaftProgressSnapshot> {
        crate::routine_id!("ddl-routine-raft-sim-progress-1");
        self.nodes
            .values()
            .map(BrokerRaft::progress_snapshot)
            .collect()
    }

    pub fn leader(&self) -> Option<BrokerRaft> {
        crate::routine_id!("ddl-routine-raft-sim-leader-1");
        self.nodes.values().find(|node| node.is_leader()).cloned()
    }

    pub fn ready_leader(&self) -> Option<BrokerRaft> {
        crate::routine_id!("ddl-routine-raft-sim-ready-leader-1");
        self.nodes
            .values()
            .find(|node| node.is_leader_ready())
            .cloned()
    }

    pub async fn wait_for_leader(&self, timeout: Duration) -> Result<BrokerRaft, RaftSimError> {
        crate::routine_id!("ddl-routine-raft-sim-wait-leader-1");
        let deadline = deadline_after(timeout);
        loop {
            if let Some(leader) = self.ready_leader() {
                return Ok(leader);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RaftSimError::Timeout {
                    operation: "ready raft leader",
                    timeout_ms: duration_ms_u64(timeout),
                });
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    pub async fn wait_for_quorum_commit(
        &self,
        index: u64,
        timeout: Duration,
    ) -> Result<Vec<RaftProgressSnapshot>, RaftSimError> {
        crate::routine_id!("ddl-routine-raft-sim-wait-quorum-commit-1");
        let deadline = deadline_after(timeout);
        loop {
            let progress = self.progress();
            let membership = progress
                .iter()
                .find(|snapshot| snapshot.is_leader || snapshot.leader_id.is_some())
                .or_else(|| progress.first())
                .map(|snapshot| snapshot.membership.clone());
            let committed = progress
                .iter()
                .filter(|snapshot| snapshot.commit_index >= index)
                .map(|snapshot| snapshot.node_id.clone())
                .collect::<BTreeSet<_>>();
            if membership
                .as_ref()
                .is_some_and(|membership| membership.quorum_met(&committed))
            {
                return Ok(progress);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RaftSimError::Timeout {
                    operation: "raft quorum commit",
                    timeout_ms: duration_ms_u64(timeout),
                });
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    pub async fn run_on_node(
        &self,
        node_id: &str,
        request: Request,
        request_uuid: &str,
        wait: Duration,
        is_acquire: bool,
    ) -> Result<Option<Response>, RaftSimError> {
        crate::routine_id!("ddl-routine-raft-sim-run-node-1");
        let node = self
            .nodes
            .get(node_id)
            .ok_or_else(|| RaftSimError::NodeNotFound(node_id.to_string()))?;
        node.run_ephemeral(request, request_uuid, wait, is_acquire)
            .await
            .map_err(Into::into)
    }

    pub async fn run_on_leader(
        &self,
        request: Request,
        request_uuid: &str,
        wait: Duration,
        is_acquire: bool,
    ) -> Result<Option<Response>, RaftSimError> {
        crate::routine_id!("ddl-routine-raft-sim-run-leader-1");
        let deadline = deadline_after(DEFAULT_SIM_REQUEST_TIMEOUT);
        let mut last_not_leader: Option<BrokerRaftError> = None;
        loop {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                if let Some(err) = last_not_leader {
                    return Err(err.into());
                }
                return Err(RaftSimError::Timeout {
                    operation: "ready raft leader",
                    timeout_ms: duration_ms_u64(DEFAULT_SIM_REQUEST_TIMEOUT),
                });
            }
            let leader = self
                .wait_for_leader(deadline.saturating_duration_since(now))
                .await?;
            match leader
                .run_ephemeral(request.clone(), request_uuid, wait, is_acquire)
                .await
            {
                Ok(response) => return Ok(response),
                Err(err @ BrokerRaftError::NotLeader { .. }) => {
                    last_not_leader = Some(err);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(err) => return Err(err.into()),
            }
        }
    }

    pub async fn change_membership(&self, peers: Vec<RaftPeerConfig>) -> Result<u64, RaftSimError> {
        crate::routine_id!("ddl-routine-raft-sim-change-membership-1");
        let deadline = deadline_after(DEFAULT_SIM_REQUEST_TIMEOUT);
        let mut last_not_leader: Option<BrokerRaftError> = None;
        loop {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                if let Some(err) = last_not_leader {
                    return Err(err.into());
                }
                return Err(RaftSimError::Timeout {
                    operation: "ready raft leader",
                    timeout_ms: duration_ms_u64(DEFAULT_SIM_REQUEST_TIMEOUT),
                });
            }
            let leader = self
                .wait_for_leader(deadline.saturating_duration_since(now))
                .await?;
            match leader.change_membership(peers.clone()).await {
                Ok(index) => return Ok(index),
                Err(err @ BrokerRaftError::NotLeader { .. }) => {
                    last_not_leader = Some(err);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(err) => return Err(err.into()),
            }
        }
    }

    pub async fn acquire(&self, key: impl Into<String>) -> Result<RaftSimLock, RaftSimError> {
        crate::routine_id!("ddl-routine-raft-sim-acquire-1");
        self.acquire_with_wait(key, Duration::ZERO).await
    }

    pub async fn acquire_with_wait(
        &self,
        key: impl Into<String>,
        wait: Duration,
    ) -> Result<RaftSimLock, RaftSimError> {
        crate::routine_id!("ddl-routine-raft-sim-acquire-wait-1");
        let key = key.into();
        let request_uuid = format!("sim-acquire-{}", Uuid::new_v4());
        let response = self
            .run_on_leader(
                Request::Lock {
                    uuid: request_uuid.clone(),
                    key: Some(key.clone()),
                    keys: None,
                    pid: None,
                    ttl: None,
                    max: None,
                    force: false,
                    retry_count: 0,
                    keep_locks_after_death: false,
                    wait: None,
                },
                &request_uuid,
                wait,
                true,
            )
            .await?
            .ok_or_else(|| RaftSimError::NoResponse {
                request_id: request_uuid.clone(),
            })?;
        match response {
            Response::Lock {
                acquired: true,
                lock_uuid: Some(lock_uuid),
                fencing_token,
                ..
            } => Ok(RaftSimLock {
                key,
                lock_uuid,
                fencing_token,
            }),
            response => Err(RaftSimError::UnexpectedResponse {
                request_id: request_uuid,
                response,
            }),
        }
    }

    pub async fn release(&self, lock: &RaftSimLock) -> Result<Response, RaftSimError> {
        crate::routine_id!("ddl-routine-raft-sim-release-1");
        self.release_key(&lock.key, Some(lock.lock_uuid.clone()), false)
            .await
    }

    pub async fn release_key(
        &self,
        key: impl Into<String>,
        lock_uuid: Option<String>,
        force: bool,
    ) -> Result<Response, RaftSimError> {
        crate::routine_id!("ddl-routine-raft-sim-release-key-1");
        let key = key.into();
        let request_uuid = format!("sim-release-{}", Uuid::new_v4());
        self.run_on_leader(
            Request::Unlock {
                uuid: request_uuid.clone(),
                key: Some(key),
                keys: None,
                lock_uuid,
                force,
            },
            &request_uuid,
            Duration::from_secs(2),
            false,
        )
        .await?
        .ok_or_else(|| RaftSimError::NoResponse {
            request_id: request_uuid,
        })
    }

    pub fn disconnect_node(&self, node_id: &str) {
        crate::routine_id!("ddl-routine-raft-sim-disconnect-node-1");
        self.network.disconnect_node(node_id);
    }

    pub fn reconnect_node(&self, node_id: &str) {
        crate::routine_id!("ddl-routine-raft-sim-reconnect-node-1");
        self.network.reconnect_node(node_id);
    }

    pub fn partition(&self, left: &[impl AsRef<str>], right: &[impl AsRef<str>]) {
        crate::routine_id!("ddl-routine-raft-sim-partition-1");
        let left = left
            .iter()
            .map(|node_id| node_id.as_ref().to_string())
            .collect::<Vec<_>>();
        let right = right
            .iter()
            .map(|node_id| node_id.as_ref().to_string())
            .collect::<Vec<_>>();
        self.network.partition(&left, &right);
    }

    pub fn heal(&self) {
        crate::routine_id!("ddl-routine-raft-sim-heal-1");
        self.network.heal();
    }
}

impl Drop for RaftSim {
    fn drop(&mut self) {
        crate::routine_id!("ddl-routine-raft-sim-drop-1");
        for handles in self.tasks.values() {
            for handle in handles {
                handle.abort();
            }
        }
        for node_id in self.nodes.keys() {
            self.network.unregister(node_id);
        }
        if self.cleanup_data_dir {
            let _ = fs::remove_dir_all(&self.data_dir);
        }
    }
}

fn sim_peers(node_count: usize) -> Vec<RaftPeerConfig> {
    crate::routine_id!("ddl-routine-raft-sim-peers-1");
    (1..=node_count)
        .map(|idx| RaftPeerConfig {
            id: format!("node-{idx}"),
            addr: format!("memory://node-{idx}"),
        })
        .collect()
}

fn deadline_after(timeout: Duration) -> tokio::time::Instant {
    crate::routine_id!("ddl-routine-raft-sim-deadline-after-1");
    let now = tokio::time::Instant::now();
    now.checked_add(timeout)
        .unwrap_or_else(|| now + Duration::from_secs(365 * 24 * 60 * 60))
}

fn duration_ms_u64(duration: Duration) -> u64 {
    crate::routine_id!("ddl-routine-raft-sim-duration-ms-1");
    duration.as_millis().min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RaftMembership;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_memory_cluster_elects_leader_and_replicates_lock() {
        let sim = RaftSim::new(3).await.expect("start in-memory raft sim");
        let leader = sim
            .wait_for_leader(Duration::from_secs(5))
            .await
            .expect("leader elected");
        assert!(leader.is_leader_ready());

        let lock = sim
            .acquire(format!("sim-key-{}", Uuid::new_v4()))
            .await
            .expect("acquire through raft sim");
        let release = sim.release(&lock).await.expect("release through raft sim");
        assert!(matches!(release, Response::Unlock { unlocked: true, .. }));

        let target_index = sim
            .ready_leader()
            .expect("ready leader after release")
            .commit_index();
        let committed = sim
            .wait_for_quorum_commit(target_index, Duration::from_secs(2))
            .await
            .expect("quorum commit after release")
            .into_iter()
            .filter(|progress| progress.commit_index >= target_index)
            .count();
        assert!(committed >= 2, "expected quorum to commit lock and unlock");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_memory_cluster_proxies_requests_from_followers() {
        let sim = RaftSim::new(3).await.expect("start in-memory raft sim");
        let leader = sim
            .wait_for_leader(Duration::from_secs(5))
            .await
            .expect("leader elected");
        let follower_id = sim
            .node_ids()
            .into_iter()
            .find(|node_id| node_id != leader.config().node_id.as_str())
            .expect("follower id");
        let request_uuid = format!("sim-follower-acquire-{}", Uuid::new_v4());
        let key = format!("sim-follower-key-{}", Uuid::new_v4());
        let response = sim
            .run_on_node(
                &follower_id,
                Request::Lock {
                    uuid: request_uuid.clone(),
                    key: Some(key),
                    keys: None,
                    pid: None,
                    ttl: None,
                    max: None,
                    force: false,
                    retry_count: 0,
                    keep_locks_after_death: false,
                    wait: None,
                },
                &request_uuid,
                Duration::ZERO,
                true,
            )
            .await
            .expect("proxied follower request")
            .expect("broker response");
        assert!(matches!(response, Response::Lock { acquired: true, .. }));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_memory_seeded_chaos_preserves_lock_model_after_failover_and_restarts() {
        let mut sim = RaftSim::new(3).await.expect("start in-memory raft sim");
        let mut rng = SimRng::new(0xC0DE_5EED_D15C_A11C);
        let node_ids = sim.node_ids();
        let keys = (0..5)
            .map(|idx| format!("sim-chaos-key-{idx}-{}", Uuid::new_v4()))
            .collect::<Vec<_>>();
        let mut held = BTreeMap::<String, RaftSimLock>::new();

        let pre_partition_index =
            run_seeded_lock_model_steps(&sim, &node_ids, &keys, &mut held, &mut rng, 12, "healthy")
                .await
                .expect("healthy seeded lock-model operations should complete");
        wait_for_all_applied(&sim, pre_partition_index, Duration::from_secs(5))
            .await
            .expect("healthy operations should apply everywhere before partition");

        let old_leader = sim
            .wait_for_leader(Duration::from_secs(5))
            .await
            .expect("leader before chaos partition");
        let old_leader_id = old_leader.config().node_id.clone();
        let majority = node_ids
            .iter()
            .filter(|node_id| *node_id != &old_leader_id)
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(majority.len(), 2);

        sim.partition(
            &[old_leader_id.as_str()],
            &[majority[0].as_str(), majority[1].as_str()],
        );

        let stale_commit_before = old_leader.commit_index();
        let stale_apply_before = old_leader.progress_snapshot().last_applied;
        let stale_result = acquire_key_on_node(
            &sim,
            &old_leader_id,
            format!("sim-chaos-stale-{}", Uuid::new_v4()),
            "stale-leader",
        )
        .await;
        assert!(
            stale_result.is_err() || stale_result.as_ref().is_ok_and(Option::is_none),
            "isolated stale leader must not grant a write: {stale_result:?}"
        );
        let stale_progress = old_leader.progress_snapshot();
        assert_eq!(
            stale_progress.commit_index, stale_commit_before,
            "isolated stale leader must not commit during minority partition"
        );
        assert_eq!(
            stale_progress.last_applied, stale_apply_before,
            "isolated stale leader must not apply during minority partition"
        );

        wait_for_ready_leader_in(&sim, &majority, Duration::from_secs(5))
            .await
            .expect("majority partition should elect a ready leader");
        let partition_index = run_seeded_lock_model_steps(
            &sim,
            &majority,
            &keys,
            &mut held,
            &mut rng,
            8,
            "majority-partition",
        )
        .await
        .expect("majority-side seeded lock-model operations should complete");
        let majority_ids = majority.iter().cloned().collect::<BTreeSet<_>>();
        wait_for_applied_on(&sim, &majority_ids, partition_index, Duration::from_secs(5))
            .await
            .expect("majority-side operations should apply on the majority partition");

        sim.restart_node(&old_leader_id)
            .expect("restart isolated old leader with durable local state");
        sim.heal();
        wait_for_all_applied(&sim, partition_index, Duration::from_secs(5))
            .await
            .expect("healed cluster should repair and apply majority partition writes");

        for node_id in &node_ids {
            sim.restart_node(node_id)
                .expect("restart healed raft node during chaos test");
            sim.wait_for_leader(Duration::from_secs(5))
                .await
                .expect("cluster should keep electing after rolling restart");
        }
        wait_for_all_applied(&sim, partition_index, Duration::from_secs(5))
            .await
            .expect("rolling restarts should preserve applied state");

        release_all_held_on_seeded_nodes(&sim, &node_ids, &mut held, &mut rng, "cleanup")
            .await
            .expect("cleanup releases should complete after restarts");
        let cleanup_index = sim
            .wait_for_leader(Duration::from_secs(5))
            .await
            .expect("ready leader after cleanup")
            .commit_index();
        wait_for_all_applied(&sim, cleanup_index, Duration::from_secs(5))
            .await
            .expect("cleanup releases should apply on every node");

        for (idx, key) in keys.iter().enumerate() {
            let node_id = &node_ids[idx % node_ids.len()];
            let lock = acquire_key_on_node(&sim, node_id, key.clone(), "post-cleanup")
                .await
                .expect("post-cleanup acquire should reach leader")
                .expect("post-cleanup key should grant");
            release_lock_on_node(&sim, node_id, &lock, "post-cleanup")
                .await
                .expect("post-cleanup release should succeed");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_memory_composite_lock_survives_failover_restart_and_log_repair() {
        let mut sim = RaftSim::new(3).await.expect("start in-memory raft sim");
        let leader = sim
            .wait_for_leader(Duration::from_secs(5))
            .await
            .expect("leader before composite acquire");
        let old_leader_id = leader.config().node_id.clone();
        let node_ids = sim.node_ids();
        let follower_id = node_ids
            .iter()
            .find(|node_id| *node_id != &old_leader_id)
            .expect("follower for composite acquire")
            .clone();
        let composite_keys = vec![
            format!("sim-composite-a-{}", Uuid::new_v4()),
            format!("sim-composite-b-{}", Uuid::new_v4()),
            format!("sim-composite-c-{}", Uuid::new_v4()),
        ];

        let composite =
            acquire_composite_on_node(&sim, &follower_id, composite_keys.clone(), "composite")
                .await
                .expect("composite acquire should reach leader")
                .expect("composite acquire should grant");
        assert_eq!(
            composite.keys, composite_keys,
            "composite response should preserve sorted member keys"
        );

        let overlapping_single = acquire_key_on_node(
            &sim,
            &follower_id,
            composite.keys[0].clone(),
            "composite-overlap",
        )
        .await
        .expect("overlapping single should reach leader");
        assert!(
            overlapping_single.is_none(),
            "single-key acquire must not grant while composite holds that key"
        );

        let overlapping_composite = acquire_composite_on_node(
            &sim,
            &follower_id,
            vec![
                composite.keys[1].clone(),
                format!("sim-composite-extra-{}", Uuid::new_v4()),
            ],
            "composite-overlap",
        )
        .await
        .expect("overlapping composite should reach leader");
        assert!(
            overlapping_composite.is_none(),
            "composite acquire must use union overlap semantics"
        );

        let before_partition_index = sim
            .wait_for_leader(Duration::from_secs(5))
            .await
            .expect("leader after composite overlap checks")
            .commit_index();
        wait_for_all_applied(&sim, before_partition_index, Duration::from_secs(5))
            .await
            .expect("composite acquire and overlap checks should apply before partition");

        let majority = node_ids
            .iter()
            .filter(|node_id| *node_id != &old_leader_id)
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(majority.len(), 2);
        sim.partition(
            &[old_leader_id.as_str()],
            &[majority[0].as_str(), majority[1].as_str()],
        );
        sim.restart_node(&old_leader_id)
            .expect("restart old leader while it is isolated");

        wait_for_ready_leader_in(&sim, &majority, Duration::from_secs(5))
            .await
            .expect("majority should elect leader while old leader is isolated");
        release_composite_on_node(&sim, &majority[0], &composite, "composite-failover")
            .await
            .expect("majority-side composite release should commit");
        let release_index = wait_for_ready_leader_in(&sim, &majority, Duration::from_secs(5))
            .await
            .expect("majority leader after composite release")
            .commit_index();
        let majority_ids = majority.iter().cloned().collect::<BTreeSet<_>>();
        wait_for_applied_on(&sim, &majority_ids, release_index, Duration::from_secs(5))
            .await
            .expect("majority should apply composite release");

        sim.heal();
        wait_for_all_applied(&sim, release_index, Duration::from_secs(5))
            .await
            .expect("restarted old leader should repair through composite release");

        for key in &composite.keys {
            let lock = acquire_key_on_node(&sim, &old_leader_id, key.clone(), "composite-healed")
                .await
                .expect("old leader should proxy or handle post-heal single acquire")
                .expect("released composite member key should grant after heal");
            release_lock_on_node(&sim, &old_leader_id, &lock, "composite-healed")
                .await
                .expect("post-heal single release should succeed");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_memory_majority_partition_elects_new_leader_and_heals_old_leader() {
        let sim = RaftSim::new(3).await.expect("start in-memory raft sim");
        let old_leader = sim
            .wait_for_leader(Duration::from_secs(5))
            .await
            .expect("initial leader elected");
        let old_leader_id = old_leader.config().node_id.clone();
        let majority = sim
            .node_ids()
            .into_iter()
            .filter(|node_id| node_id != &old_leader_id)
            .collect::<Vec<_>>();
        assert_eq!(
            majority.len(),
            2,
            "3-node sim should leave a 2-node majority"
        );

        let baseline = sim
            .acquire(format!("sim-pre-partition-{}", Uuid::new_v4()))
            .await
            .expect("baseline acquire before partition");
        let baseline_index = sim
            .ready_leader()
            .expect("ready leader after baseline acquire")
            .commit_index();
        sim.wait_for_quorum_commit(baseline_index, Duration::from_secs(2))
            .await
            .expect("baseline should be quorum-committed before partition");
        sim.release(&baseline)
            .await
            .expect("release baseline before partition");

        sim.partition(
            &[old_leader_id.as_str()],
            &[majority[0].as_str(), majority[1].as_str()],
        );

        let isolated_commit_before = old_leader.commit_index();
        let isolated_applied_before = old_leader.progress_snapshot().last_applied;
        let isolated_request_uuid = format!("sim-isolated-acquire-{}", Uuid::new_v4());
        let isolated_result = sim
            .run_on_node(
                &old_leader_id,
                Request::Lock {
                    uuid: isolated_request_uuid.clone(),
                    key: Some(format!("sim-isolated-key-{}", Uuid::new_v4())),
                    keys: None,
                    pid: None,
                    ttl: None,
                    max: None,
                    force: false,
                    retry_count: 0,
                    keep_locks_after_death: false,
                    wait: None,
                },
                &isolated_request_uuid,
                Duration::ZERO,
                true,
            )
            .await;
        assert!(
            matches!(
                isolated_result,
                Err(RaftSimError::Raft(
                    BrokerRaftError::QuorumUnavailable { .. }
                )) | Err(RaftSimError::Raft(BrokerRaftError::NotLeader { .. }))
            ),
            "isolated old leader must not grant a write without quorum: {isolated_result:?}"
        );
        let isolated_progress = old_leader.progress_snapshot();
        assert_eq!(
            isolated_progress.commit_index, isolated_commit_before,
            "isolated old leader must not commit the failed proposal"
        );
        assert_eq!(
            isolated_progress.last_applied, isolated_applied_before,
            "isolated old leader must not apply the failed proposal"
        );

        let new_leader = wait_for_ready_leader_in(&sim, &majority, Duration::from_secs(5))
            .await
            .expect("majority partition should elect a new ready leader");
        let new_leader_id = new_leader.config().node_id.clone();
        assert_ne!(new_leader_id, old_leader_id);

        let partition_key = format!("sim-post-partition-{}", Uuid::new_v4());
        let request_uuid = format!("sim-partition-acquire-{}", Uuid::new_v4());
        let acquire_response = sim
            .run_on_node(
                &majority[0],
                Request::Lock {
                    uuid: request_uuid.clone(),
                    key: Some(partition_key.clone()),
                    keys: None,
                    pid: None,
                    ttl: None,
                    max: None,
                    force: false,
                    retry_count: 0,
                    keep_locks_after_death: false,
                    wait: None,
                },
                &request_uuid,
                Duration::ZERO,
                true,
            )
            .await
            .expect("majority-side request should reach the new leader")
            .expect("majority-side acquire response");
        let partition_lock_uuid = match acquire_response {
            Response::Lock {
                acquired: true,
                lock_uuid: Some(lock_uuid),
                ..
            } => lock_uuid,
            response => panic!("partition acquire should grant; got {response:?}"),
        };
        let partition_commit_index = new_leader.commit_index();
        sim.wait_for_quorum_commit(partition_commit_index, Duration::from_secs(2))
            .await
            .expect("majority-side acquire should commit on quorum");

        sim.heal();
        wait_for_all_applied(&sim, partition_commit_index, Duration::from_secs(5))
            .await
            .expect("old leader should catch up after heal");

        let release_uuid = format!("sim-healed-release-{}", Uuid::new_v4());
        let release_response = sim
            .run_on_node(
                &old_leader_id,
                Request::Unlock {
                    uuid: release_uuid.clone(),
                    key: Some(partition_key),
                    keys: None,
                    lock_uuid: Some(partition_lock_uuid),
                    force: false,
                },
                &release_uuid,
                Duration::from_secs(2),
                false,
            )
            .await
            .expect("old leader should proxy or handle after healing")
            .expect("release response through healed old leader");
        assert!(matches!(
            release_response,
            Response::Unlock { unlocked: true, .. }
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_memory_restarted_old_leader_repairs_uncommitted_suffix_after_heal() {
        let mut sim = RaftSim::new(3).await.expect("start in-memory raft sim");
        let old_leader = sim
            .wait_for_leader(Duration::from_secs(5))
            .await
            .expect("initial leader elected");
        let old_leader_id = old_leader.config().node_id.clone();
        let majority = sim
            .node_ids()
            .into_iter()
            .filter(|node_id| node_id != &old_leader_id)
            .collect::<Vec<_>>();
        assert_eq!(
            majority.len(),
            2,
            "3-node sim should leave a 2-node majority"
        );

        sim.partition(
            &[old_leader_id.as_str()],
            &[majority[0].as_str(), majority[1].as_str()],
        );

        let isolated_request_uuid = format!("sim-isolated-restart-{}", Uuid::new_v4());
        let isolated_result = sim
            .run_on_node(
                &old_leader_id,
                Request::Lock {
                    uuid: isolated_request_uuid.clone(),
                    key: Some(format!("sim-isolated-restart-key-{}", Uuid::new_v4())),
                    keys: None,
                    pid: None,
                    ttl: None,
                    max: None,
                    force: false,
                    retry_count: 0,
                    keep_locks_after_death: false,
                    wait: None,
                },
                &isolated_request_uuid,
                Duration::ZERO,
                true,
            )
            .await;
        assert!(
            matches!(
                isolated_result,
                Err(RaftSimError::Raft(
                    BrokerRaftError::QuorumUnavailable { .. }
                )) | Err(RaftSimError::Raft(BrokerRaftError::NotLeader { .. }))
            ),
            "isolated old leader must not grant a write without quorum: {isolated_result:?}"
        );

        sim.restart_node(&old_leader_id)
            .expect("restart isolated old leader with its local durable suffix");

        let new_leader = wait_for_ready_leader_in(&sim, &majority, Duration::from_secs(5))
            .await
            .expect("majority partition should elect a new ready leader");
        let partition_key = format!("sim-restart-repair-{}", Uuid::new_v4());
        let request_uuid = format!("sim-restart-repair-acquire-{}", Uuid::new_v4());
        let acquire_response = sim
            .run_on_node(
                &majority[0],
                Request::Lock {
                    uuid: request_uuid.clone(),
                    key: Some(partition_key.clone()),
                    keys: None,
                    pid: None,
                    ttl: None,
                    max: None,
                    force: false,
                    retry_count: 0,
                    keep_locks_after_death: false,
                    wait: None,
                },
                &request_uuid,
                Duration::ZERO,
                true,
            )
            .await
            .expect("majority-side request should reach the new leader")
            .expect("majority-side acquire response");
        let lock_uuid = match acquire_response {
            Response::Lock {
                acquired: true,
                lock_uuid: Some(lock_uuid),
                ..
            } => lock_uuid,
            response => panic!("majority acquire should grant; got {response:?}"),
        };
        let target_index = new_leader.commit_index();
        sim.wait_for_quorum_commit(target_index, Duration::from_secs(2))
            .await
            .expect("majority-side acquire should commit on quorum");

        sim.heal();
        wait_for_all_applied(&sim, target_index, Duration::from_secs(5))
            .await
            .expect("restarted old leader should repair its suffix and catch up after heal");

        let release_uuid = format!("sim-restart-repair-release-{}", Uuid::new_v4());
        let release_response = sim
            .run_on_node(
                &old_leader_id,
                Request::Unlock {
                    uuid: release_uuid.clone(),
                    key: Some(partition_key),
                    keys: None,
                    lock_uuid: Some(lock_uuid),
                    force: false,
                },
                &release_uuid,
                Duration::from_secs(2),
                false,
            )
            .await
            .expect("restarted old leader should proxy or handle after heal")
            .expect("release response through restarted old leader");
        assert!(matches!(
            release_response,
            Response::Unlock { unlocked: true, .. }
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_memory_lagging_follower_catches_up_over_bounded_append_entries_without_snapshot() {
        let sim = RaftSim::with_config(RaftSimConfig {
            snapshot_interval: Duration::from_secs(60 * 60),
            snapshot_max_log_entries: u64::MAX,
            snapshot_max_log_bytes: u64::MAX,
            trailing_log_entries: 10_000,
            append_entries_max_entries: 2,
            append_entries_max_bytes: usize::MAX,
            append_entries_max_inline_batches: 2,
            election_timeout_min: Duration::from_secs(10),
            election_timeout_max: Duration::from_secs(12),
            ..RaftSimConfig::default()
        })
        .await
        .expect("start append-only catch-up in-memory raft sim");
        let leader = sim
            .wait_for_leader(Duration::from_secs(15))
            .await
            .expect("initial leader elected");
        let leader_id = leader.config().node_id.clone();
        let lagging_id = sim
            .node_ids()
            .into_iter()
            .find(|node_id| node_id != &leader_id)
            .expect("lagging follower id");

        sim.disconnect_node(&lagging_id);
        for idx in 0..6 {
            let lock = sim
                .acquire(format!("sim-append-lag-{idx}-{}", Uuid::new_v4()))
                .await
                .expect("majority acquire while follower is disconnected");
            sim.release(&lock)
                .await
                .expect("majority release while follower is disconnected");
        }

        let target_before_reconnect = sim
            .node(&leader_id)
            .expect("leader node still present")
            .commit_index();
        let lagging_before = sim
            .node(&lagging_id)
            .expect("lagging node still present")
            .progress_snapshot();
        assert!(
            lagging_before.commit_index < target_before_reconnect,
            "disconnected follower should lag before append-only catch-up"
        );

        let batches_before = append_entries_batches_total(&sim);
        let sent_before = append_entries_sent_total(&sim);
        let snapshot_fallbacks_before = append_snapshot_fallbacks_total(&sim);
        let snapshot_installs_before = install_snapshot_successes_total(&sim);

        sim.reconnect_node(&lagging_id);
        let trigger = sim
            .acquire(format!("sim-append-trigger-{}", Uuid::new_v4()))
            .await
            .expect("trigger acquire after reconnect");
        sim.release(&trigger)
            .await
            .expect("trigger release after reconnect");
        let target_index = sim
            .ready_leader()
            .expect("ready leader after append-only trigger")
            .commit_index();

        wait_for_all_applied(&sim, target_index, Duration::from_secs(5))
            .await
            .expect("lagging follower should catch up through bounded AppendEntries");

        let batches_delta = append_entries_batches_total(&sim).saturating_sub(batches_before);
        let sent_delta = append_entries_sent_total(&sim).saturating_sub(sent_before);
        assert!(
            batches_delta >= 2,
            "bounded catch-up should require multiple AppendEntries batches, got {batches_delta}"
        );
        assert!(
            sent_delta >= 4,
            "bounded catch-up should send retained suffix entries, got {sent_delta}"
        );
        assert_eq!(
            append_snapshot_fallbacks_total(&sim),
            snapshot_fallbacks_before,
            "ordinary retained-suffix catch-up must not fall back to InstallSnapshot"
        );
        assert_eq!(
            install_snapshot_successes_total(&sim),
            snapshot_installs_before,
            "ordinary retained-suffix catch-up must not install a snapshot"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_memory_lagging_follower_recovers_via_install_snapshot_after_compaction() {
        let sim = RaftSim::with_config(RaftSimConfig {
            snapshot_interval: Duration::from_secs(60 * 60),
            snapshot_max_log_entries: 4,
            snapshot_max_log_bytes: u64::MAX,
            trailing_log_entries: 0,
            election_timeout_min: Duration::from_secs(3),
            election_timeout_max: Duration::from_secs(4),
            ..RaftSimConfig::default()
        })
        .await
        .expect("start compacting in-memory raft sim");
        let leader = sim
            .wait_for_leader(Duration::from_secs(5))
            .await
            .expect("initial leader elected");
        let leader_id = leader.config().node_id.clone();
        let lagging_id = sim
            .node_ids()
            .into_iter()
            .find(|node_id| node_id != &leader_id)
            .expect("lagging follower id");
        let leader_compactions_before = leader.telemetry_snapshot().log_compactions_total;
        let snapshot_fallbacks_before = append_snapshot_fallbacks_total(&sim);
        let snapshot_installs_before = install_snapshot_successes_total(&sim);

        sim.disconnect_node(&lagging_id);
        for idx in 0..8 {
            let lock = sim
                .acquire(format!("sim-snapshot-lag-{idx}-{}", Uuid::new_v4()))
                .await
                .expect("majority acquire while follower is disconnected");
            sim.release(&lock)
                .await
                .expect("majority release while follower is disconnected");
        }

        wait_for_node_compaction(
            &sim,
            &leader_id,
            leader_compactions_before,
            Duration::from_secs(2),
        )
        .await
        .expect("leader should compact retained entries while follower lags");
        let target_before_reconnect = sim
            .node(&leader_id)
            .expect("leader node still present")
            .commit_index();
        let lagging_before = sim
            .node(&lagging_id)
            .expect("lagging node still present")
            .progress_snapshot();
        assert!(
            lagging_before.commit_index < target_before_reconnect,
            "disconnected follower should lag before snapshot catch-up"
        );

        sim.reconnect_node(&lagging_id);
        let trigger = sim
            .acquire(format!("sim-snapshot-trigger-{}", Uuid::new_v4()))
            .await
            .expect("trigger acquire after reconnect");
        sim.release(&trigger)
            .await
            .expect("trigger release after reconnect");
        let target_index = sim
            .ready_leader()
            .expect("ready leader after snapshot trigger")
            .commit_index();

        wait_for_install_snapshot_success(&sim, snapshot_installs_before, Duration::from_secs(5))
            .await
            .expect("lagging follower should receive InstallSnapshot");
        assert!(
            append_snapshot_fallbacks_total(&sim) > snapshot_fallbacks_before,
            "leader should fall back from AppendEntries to InstallSnapshot"
        );
        wait_for_all_applied(&sim, target_index, Duration::from_secs(5))
            .await
            .expect("all nodes should apply through the post-snapshot write");
        let lagging_after = sim
            .node(&lagging_id)
            .expect("lagging node after reconnect")
            .progress_snapshot();
        assert!(
            lagging_after.last_applied >= target_index,
            "lagging follower should catch up through target index {target_index}, got {}",
            lagging_after.last_applied
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_memory_cluster_expands_membership_to_five_and_commits_with_new_quorum() {
        let mut sim = RaftSim::new(3).await.expect("start in-memory raft sim");
        let leader = sim
            .wait_for_leader(Duration::from_secs(5))
            .await
            .expect("initial leader elected");
        let mut new_peers = leader.active_peers();

        let seed = sim
            .acquire(format!("sim-membership-seed-{}", Uuid::new_v4()))
            .await
            .expect("seed acquire before membership expansion");
        sim.release(&seed)
            .await
            .expect("seed release before membership expansion");

        let peer4 = sim.add_node("node-4").expect("add node-4 as learner");
        let peer5 = sim.add_node("node-5").expect("add node-5 as learner");
        new_peers.push(peer4);
        new_peers.push(peer5);

        let final_membership_index = sim
            .change_membership(new_peers.clone())
            .await
            .expect("expand membership through joint consensus");
        wait_for_all_applied(&sim, final_membership_index, Duration::from_secs(5))
            .await
            .expect("all five nodes should apply final membership");

        for progress in sim.progress() {
            assert!(
                !progress.membership_joint,
                "node {} should leave joint consensus",
                progress.node_id
            );
            assert_eq!(
                progress.membership.active_peers(),
                new_peers,
                "node {} should converge on the five-node membership",
                progress.node_id
            );
        }

        let leader = sim
            .wait_for_leader(Duration::from_secs(5))
            .await
            .expect("leader after membership expansion");
        assert_eq!(leader.active_cluster_size(), 5);
        assert_eq!(leader.active_quorum_size(), 3);

        let lock = sim
            .acquire(format!("sim-membership-five-quorum-{}", Uuid::new_v4()))
            .await
            .expect("acquire under five-node membership");
        let commit_index = sim
            .ready_leader()
            .expect("ready leader after five-node acquire")
            .commit_index();
        let progress = sim
            .wait_for_quorum_commit(commit_index, Duration::from_secs(2))
            .await
            .expect("five-node membership should commit with quorum");
        let committed = progress
            .iter()
            .filter(|node| node.commit_index >= commit_index)
            .count();
        assert!(
            committed >= 3,
            "five-node membership requires a three-node quorum, got {committed}"
        );
        sim.release(&lock)
            .await
            .expect("release under five-node membership");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_memory_cluster_expands_with_snapshot_catchup_for_new_learners() {
        let mut sim = RaftSim::with_config(RaftSimConfig {
            snapshot_interval: Duration::from_secs(60 * 60),
            snapshot_max_log_entries: 4,
            snapshot_max_log_bytes: u64::MAX,
            trailing_log_entries: 0,
            ..RaftSimConfig::default()
        })
        .await
        .expect("start compacting in-memory raft sim");
        let leader = sim
            .wait_for_leader(Duration::from_secs(5))
            .await
            .expect("initial leader elected");
        let leader_id = leader.config().node_id.clone();
        let leader_compactions_before = leader.telemetry_snapshot().log_compactions_total;

        for idx in 0..4 {
            let lock = sim
                .acquire(format!(
                    "sim-learner-snapshot-seed-{idx}-{}",
                    Uuid::new_v4()
                ))
                .await
                .expect("seed acquire before adding snapshot learners");
            sim.release(&lock)
                .await
                .expect("seed release before adding snapshot learners");
        }
        wait_for_node_compaction(
            &sim,
            &leader_id,
            leader_compactions_before,
            Duration::from_secs(2),
        )
        .await
        .expect("leader should compact before learner expansion");

        let snapshot_installs_before = install_snapshot_successes_total(&sim);
        let snapshot_fallbacks_before = append_snapshot_fallbacks_total(&sim);
        let mut new_peers = sim
            .node(&leader_id)
            .expect("leader node after compaction")
            .active_peers();
        let peer4 = sim.add_node("node-4").expect("add node-4 after compaction");
        let peer5 = sim.add_node("node-5").expect("add node-5 after compaction");
        let new_learner_ids = [peer4.id.clone(), peer5.id.clone()]
            .into_iter()
            .collect::<BTreeSet<_>>();
        new_peers.push(peer4);
        new_peers.push(peer5);

        let final_membership_index = sim
            .change_membership(new_peers.clone())
            .await
            .expect("expand compacted cluster through snapshot learner catch-up");
        wait_for_install_snapshot_success(&sim, snapshot_installs_before, Duration::from_secs(5))
            .await
            .expect("new learners should be caught up with InstallSnapshot");
        assert!(
            append_snapshot_fallbacks_total(&sim) > snapshot_fallbacks_before,
            "learner catch-up should fall back from AppendEntries to InstallSnapshot"
        );
        wait_for_all_applied(&sim, final_membership_index, Duration::from_secs(5))
            .await
            .expect("all nodes should apply final membership after snapshot catch-up");

        let final_membership = RaftMembership::from_simple(new_peers.clone());
        for learner_id in &new_learner_ids {
            let progress = sim
                .node(learner_id)
                .expect("promoted learner present")
                .progress_snapshot();
            assert_eq!(
                progress.membership, final_membership,
                "promoted learner {learner_id} should converge on final membership"
            );
            assert!(
                progress.last_applied >= final_membership_index,
                "promoted learner {learner_id} should apply through membership index"
            );
        }

        let lock = sim
            .acquire(format!("sim-learner-snapshot-quorum-{}", Uuid::new_v4()))
            .await
            .expect("acquire after snapshot-backed learner promotion");
        let commit_index = sim
            .ready_leader()
            .expect("ready leader after snapshot-backed expansion")
            .commit_index();
        let progress = sim
            .wait_for_quorum_commit(commit_index, Duration::from_secs(2))
            .await
            .expect("five-node cluster should commit after snapshot-backed expansion");
        let committed = progress
            .iter()
            .filter(|node| node.commit_index >= commit_index)
            .count();
        assert!(
            committed >= 3,
            "snapshot-backed expansion should use a three-node quorum, got {committed}"
        );
        sim.release(&lock)
            .await
            .expect("release after snapshot-backed expansion");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_memory_expanded_membership_survives_restarts_and_commits() {
        let mut sim = RaftSim::new(3).await.expect("start in-memory raft sim");
        let leader = sim
            .wait_for_leader(Duration::from_secs(5))
            .await
            .expect("initial leader elected");
        let mut new_peers = leader.active_peers();
        new_peers.push(sim.add_node("node-4").expect("add node-4"));
        new_peers.push(sim.add_node("node-5").expect("add node-5"));

        let final_membership_index = sim
            .change_membership(new_peers.clone())
            .await
            .expect("expand membership before restart");
        wait_for_all_applied(&sim, final_membership_index, Duration::from_secs(5))
            .await
            .expect("all nodes should apply expanded membership before restart");

        let restart_ids = sim.node_ids();
        for node_id in &restart_ids {
            sim.restart_node(node_id)
                .expect("restart node after membership expansion");
        }
        wait_for_all_applied(&sim, final_membership_index, Duration::from_secs(5))
            .await
            .expect("all restarted nodes should replay expanded membership");

        let final_membership = RaftMembership::from_simple(new_peers.clone());
        for progress in sim.progress() {
            assert_eq!(
                progress.membership, final_membership,
                "restarted node {} should recover expanded membership",
                progress.node_id
            );
            assert!(!progress.membership_joint);
            assert!(progress.last_applied >= final_membership_index);
        }

        let leader = sim
            .wait_for_leader(Duration::from_secs(5))
            .await
            .expect("leader after restarting expanded membership");
        assert_eq!(leader.active_cluster_size(), 5);
        assert_eq!(leader.active_quorum_size(), 3);

        let lock = sim
            .acquire(format!("sim-expanded-restart-quorum-{}", Uuid::new_v4()))
            .await
            .expect("acquire after expanded membership restart");
        let commit_index = sim
            .ready_leader()
            .expect("ready leader after restarted acquire")
            .commit_index();
        let progress = sim
            .wait_for_quorum_commit(commit_index, Duration::from_secs(2))
            .await
            .expect("restarted expanded cluster should commit with quorum");
        let committed = progress
            .iter()
            .filter(|node| node.commit_index >= commit_index)
            .count();
        assert!(
            committed >= 3,
            "restarted expanded membership requires a three-node quorum, got {committed}"
        );
        sim.release(&lock)
            .await
            .expect("release after expanded membership restart");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_memory_cluster_shrinks_membership_and_removed_nodes_reject_clients() {
        let sim = RaftSim::new(5).await.expect("start five-node raft sim");
        let leader = sim
            .wait_for_leader(Duration::from_secs(5))
            .await
            .expect("initial five-node leader elected");
        let leader_id = leader.config().node_id.clone();
        let active_peers = leader.active_peers();
        assert_eq!(active_peers.len(), 5);

        let mut final_peers = Vec::new();
        final_peers.push(
            active_peers
                .iter()
                .find(|peer| peer.id == leader_id)
                .expect("leader peer in active membership")
                .clone(),
        );
        final_peers.extend(
            active_peers
                .iter()
                .filter(|peer| peer.id != leader_id)
                .take(2)
                .cloned(),
        );
        final_peers.sort_by(|left, right| left.id.cmp(&right.id));
        let final_ids = final_peers
            .iter()
            .map(|peer| peer.id.clone())
            .collect::<BTreeSet<_>>();
        let removed_ids = active_peers
            .iter()
            .filter(|peer| !final_ids.contains(&peer.id))
            .map(|peer| peer.id.clone())
            .collect::<Vec<_>>();
        assert_eq!(final_ids.len(), 3);
        assert_eq!(removed_ids.len(), 2);

        let seed = sim
            .acquire(format!("sim-shrink-seed-{}", Uuid::new_v4()))
            .await
            .expect("seed acquire before membership shrink");
        sim.release(&seed)
            .await
            .expect("seed release before membership shrink");

        let final_index = sim
            .change_membership(final_peers.clone())
            .await
            .expect("shrink membership through joint consensus");
        wait_for_applied_on(&sim, &final_ids, final_index, Duration::from_secs(5))
            .await
            .expect("remaining voters should apply final simple membership");
        wait_for_removed_member_guards(&sim, &removed_ids, Duration::from_secs(5))
            .await
            .expect("removed voters should observe removed-member client guard");

        let final_membership = RaftMembership::from_simple(final_peers.clone());
        for node_id in &final_ids {
            let progress = sim
                .node(node_id)
                .expect("remaining voter present")
                .progress_snapshot();
            assert_eq!(
                progress.membership, final_membership,
                "remaining voter {node_id} should converge on final membership"
            );
            assert!(!progress.membership_joint);
        }

        let leader = sim
            .wait_for_leader(Duration::from_secs(5))
            .await
            .expect("leader after membership shrink");
        assert!(final_ids.contains(leader.config().node_id.as_str()));
        assert_eq!(leader.active_cluster_size(), 3);
        assert_eq!(leader.active_quorum_size(), 2);

        let lock = sim
            .acquire(format!("sim-shrink-quorum-{}", Uuid::new_v4()))
            .await
            .expect("acquire under shrunk membership");
        let commit_index = sim
            .ready_leader()
            .expect("ready leader after shrunk acquire")
            .commit_index();
        wait_for_applied_on(&sim, &final_ids, commit_index, Duration::from_secs(5))
            .await
            .expect("remaining voters should apply post-shrink write");
        sim.release(&lock)
            .await
            .expect("release under shrunk membership");

        for removed_id in &removed_ids {
            assert_removed_node_rejects_client(&sim, removed_id).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_memory_removed_voters_stay_guarded_after_restart() {
        let mut sim = RaftSim::new(5).await.expect("start five-node raft sim");
        let leader = sim
            .wait_for_leader(Duration::from_secs(5))
            .await
            .expect("initial five-node leader elected");
        let leader_id = leader.config().node_id.clone();
        let active_peers = leader.active_peers();
        assert_eq!(active_peers.len(), 5);

        let mut final_peers = Vec::new();
        final_peers.push(
            active_peers
                .iter()
                .find(|peer| peer.id == leader_id)
                .expect("leader peer in active membership")
                .clone(),
        );
        final_peers.extend(
            active_peers
                .iter()
                .filter(|peer| peer.id != leader_id)
                .take(2)
                .cloned(),
        );
        final_peers.sort_by(|left, right| left.id.cmp(&right.id));
        let final_ids = final_peers
            .iter()
            .map(|peer| peer.id.clone())
            .collect::<BTreeSet<_>>();
        let removed_ids = active_peers
            .iter()
            .filter(|peer| !final_ids.contains(&peer.id))
            .map(|peer| peer.id.clone())
            .collect::<Vec<_>>();
        assert_eq!(final_ids.len(), 3);
        assert_eq!(removed_ids.len(), 2);

        let final_index = sim
            .change_membership(final_peers.clone())
            .await
            .expect("shrink membership before removed-node restarts");
        wait_for_applied_on(&sim, &final_ids, final_index, Duration::from_secs(5))
            .await
            .expect("remaining voters should apply final simple membership");
        wait_for_removed_member_guards(&sim, &removed_ids, Duration::from_secs(5))
            .await
            .expect("removed voters should activate guards before restart");

        for removed_id in &removed_ids {
            sim.restart_node(removed_id)
                .expect("restart removed voter after final membership");
        }
        wait_for_removed_member_guards(&sim, &removed_ids, Duration::from_secs(5))
            .await
            .expect("removed voters should reload guards from disk after restart");

        for removed_id in &removed_ids {
            assert_removed_node_rejects_client(&sim, removed_id).await;
        }

        let final_membership = RaftMembership::from_simple(final_peers);
        for node_id in &final_ids {
            let progress = sim
                .node(node_id)
                .expect("remaining voter present after removed restarts")
                .progress_snapshot();
            assert_eq!(
                progress.membership, final_membership,
                "remaining voter {node_id} should keep final membership"
            );
        }

        let final_node_ids = final_ids.iter().cloned().collect::<Vec<_>>();
        let leader = wait_for_ready_leader_in(&sim, &final_node_ids, Duration::from_secs(5))
            .await
            .expect("remaining voters should still elect/keep a ready leader");
        assert_eq!(leader.active_cluster_size(), 3);
        assert_eq!(leader.active_quorum_size(), 2);

        let lock = sim
            .acquire(format!("sim-removed-restart-quorum-{}", Uuid::new_v4()))
            .await
            .expect("remaining cluster should commit after removed voters restart");
        let commit_index = sim
            .ready_leader()
            .expect("ready leader after removed-restart acquire")
            .commit_index();
        wait_for_applied_on(&sim, &final_ids, commit_index, Duration::from_secs(5))
            .await
            .expect("remaining voters should apply post-removed-restart write");
        sim.release(&lock)
            .await
            .expect("release after removed voters restart");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn in_memory_cluster_removes_current_leader_and_remaining_voters_continue() {
        let sim = RaftSim::new(5).await.expect("start five-node raft sim");
        let seed = sim
            .acquire(format!("sim-remove-leader-seed-{}", Uuid::new_v4()))
            .await
            .expect("seed acquire before removing leader");
        sim.release(&seed)
            .await
            .expect("seed release before removing leader");

        let old_leader = sim
            .wait_for_leader(Duration::from_secs(5))
            .await
            .expect("leader before membership change");
        let old_leader_id = old_leader.config().node_id.clone();
        let active_peers = old_leader.active_peers();
        assert_eq!(active_peers.len(), 5);

        let mut final_peers = active_peers
            .iter()
            .filter(|peer| peer.id != old_leader_id)
            .take(3)
            .cloned()
            .collect::<Vec<_>>();
        final_peers.sort_by(|left, right| left.id.cmp(&right.id));
        let final_ids = final_peers
            .iter()
            .map(|peer| peer.id.clone())
            .collect::<BTreeSet<_>>();
        let removed_ids = active_peers
            .iter()
            .filter(|peer| !final_ids.contains(&peer.id))
            .map(|peer| peer.id.clone())
            .collect::<Vec<_>>();
        assert_eq!(final_ids.len(), 3);
        assert_eq!(removed_ids.len(), 2);
        assert!(
            removed_ids.contains(&old_leader_id),
            "the current leader must be removed by this scenario"
        );

        let final_index = old_leader
            .change_membership(final_peers.clone())
            .await
            .expect("current leader should commit its own removal");
        wait_for_applied_on(&sim, &final_ids, final_index, Duration::from_secs(5))
            .await
            .expect("remaining voters should apply final membership after leader removal");
        wait_for_removed_member_guards(&sim, &removed_ids, Duration::from_secs(5))
            .await
            .expect("removed voters should activate local client guards");

        let final_membership = RaftMembership::from_simple(final_peers);
        for node_id in &final_ids {
            let progress = sim
                .node(node_id)
                .expect("remaining voter present")
                .progress_snapshot();
            assert_eq!(
                progress.membership, final_membership,
                "remaining voter {node_id} should converge on final membership"
            );
            assert!(!progress.membership_joint);
        }

        let final_node_ids = final_ids.iter().cloned().collect::<Vec<_>>();
        let new_leader = wait_for_ready_leader_in(&sim, &final_node_ids, Duration::from_secs(5))
            .await
            .expect("remaining voters should elect a ready leader");
        assert_ne!(new_leader.config().node_id, old_leader_id);
        assert_eq!(new_leader.active_cluster_size(), 3);
        assert_eq!(new_leader.active_quorum_size(), 2);

        assert_removed_node_rejects_client(&sim, &old_leader_id).await;
        for removed_id in removed_ids
            .iter()
            .filter(|node_id| *node_id != &old_leader_id)
        {
            assert_removed_node_rejects_client(&sim, removed_id).await;
        }

        let lock = sim
            .acquire(format!("sim-remove-leader-quorum-{}", Uuid::new_v4()))
            .await
            .expect("remaining voters should commit after leader removal");
        let commit_index = sim
            .ready_leader()
            .expect("ready leader after post-removal acquire")
            .commit_index();
        wait_for_applied_on(&sim, &final_ids, commit_index, Duration::from_secs(5))
            .await
            .expect("remaining voters should apply post-removal write");
        sim.release(&lock)
            .await
            .expect("release after leader removal");
    }

    #[derive(Debug, Clone)]
    struct SimRng {
        state: u64,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct RaftSimCompositeLock {
        keys: Vec<String>,
        lock_uuid: String,
    }

    impl SimRng {
        fn new(seed: u64) -> Self {
            crate::routine_id!("ddl-routine-raft-sim-test-rng-new-1");
            Self { state: seed }
        }

        fn next_u64(&mut self) -> u64 {
            crate::routine_id!("ddl-routine-raft-sim-test-rng-next-1");
            self.state = self
                .state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            self.state
        }

        fn index(&mut self, len: usize) -> usize {
            crate::routine_id!("ddl-routine-raft-sim-test-rng-index-1");
            debug_assert!(len > 0);
            (self.next_u64() as usize) % len
        }
    }

    async fn run_seeded_lock_model_steps(
        sim: &RaftSim,
        node_ids: &[String],
        keys: &[String],
        held: &mut BTreeMap<String, RaftSimLock>,
        rng: &mut SimRng,
        steps: usize,
        label: &str,
    ) -> Result<u64, RaftSimError> {
        crate::routine_id!("ddl-routine-raft-sim-test-run-seeded-lock-model-1");
        let mut last_index = sim
            .wait_for_leader(Duration::from_secs(5))
            .await?
            .commit_index();
        for step in 0..steps {
            let node_id = &node_ids[rng.index(node_ids.len())];
            let key = keys[rng.index(keys.len())].clone();
            let should_release = !held.is_empty() && (step % 3 == 2 || held.contains_key(&key));

            if should_release {
                let release_key = if held.contains_key(&key) {
                    key
                } else {
                    let held_keys = held.keys().cloned().collect::<Vec<_>>();
                    held_keys[rng.index(held_keys.len())].clone()
                };
                let lock = held
                    .remove(&release_key)
                    .expect("selected held lock should be present");
                release_lock_on_node(sim, node_id, &lock, label).await?;
            } else if let Some(existing) = held.get(&key) {
                let contended =
                    acquire_key_on_node(sim, node_id, existing.key.clone(), label).await?;
                assert!(
                    contended.is_none(),
                    "{label} step {step} granted a second holder for {}",
                    existing.key
                );
            } else if let Some(lock) = acquire_key_on_node(sim, node_id, key.clone(), label).await?
            {
                assert!(
                    held.insert(key.clone(), lock).is_none(),
                    "{label} step {step} replaced an existing held lock for {key}"
                );
            }

            let leader = sim.wait_for_leader(Duration::from_secs(5)).await?;
            last_index = leader.commit_index();
            sim.wait_for_quorum_commit(last_index, Duration::from_secs(5))
                .await?;
        }
        Ok(last_index)
    }

    async fn acquire_composite_on_node(
        sim: &RaftSim,
        node_id: &str,
        keys: Vec<String>,
        label: &str,
    ) -> Result<Option<RaftSimCompositeLock>, RaftSimError> {
        crate::routine_id!("ddl-routine-raft-sim-test-acquire-composite-node-1");
        let request_uuid = format!("sim-{label}-composite-acquire-{}", Uuid::new_v4());
        let response = sim
            .run_on_node(
                node_id,
                Request::Lock {
                    uuid: request_uuid.clone(),
                    key: None,
                    keys: Some(keys),
                    pid: None,
                    ttl: None,
                    max: None,
                    force: false,
                    retry_count: 0,
                    keep_locks_after_death: false,
                    wait: None,
                },
                &request_uuid,
                Duration::ZERO,
                true,
            )
            .await?
            .ok_or_else(|| RaftSimError::NoResponse {
                request_id: request_uuid.clone(),
            })?;
        match response {
            Response::CompositeLock {
                acquired: true,
                keys,
                lock_uuid: Some(lock_uuid),
                fencing_tokens,
                ..
            } => {
                if let Some(tokens) = &fencing_tokens {
                    assert_eq!(
                        tokens.len(),
                        keys.len(),
                        "composite grant should include one fencing token per member key"
                    );
                }
                Ok(Some(RaftSimCompositeLock { keys, lock_uuid }))
            }
            Response::CompositeLock {
                acquired: false, ..
            } => Ok(None),
            response => Err(RaftSimError::UnexpectedResponse {
                request_id: request_uuid,
                response,
            }),
        }
    }

    async fn acquire_key_on_node(
        sim: &RaftSim,
        node_id: &str,
        key: String,
        label: &str,
    ) -> Result<Option<RaftSimLock>, RaftSimError> {
        crate::routine_id!("ddl-routine-raft-sim-test-acquire-key-node-1");
        let request_uuid = format!("sim-{label}-acquire-{}", Uuid::new_v4());
        let response = sim
            .run_on_node(
                node_id,
                Request::Lock {
                    uuid: request_uuid.clone(),
                    key: Some(key.clone()),
                    keys: None,
                    pid: None,
                    ttl: None,
                    max: None,
                    force: false,
                    retry_count: 0,
                    keep_locks_after_death: false,
                    wait: None,
                },
                &request_uuid,
                Duration::ZERO,
                true,
            )
            .await?
            .ok_or_else(|| RaftSimError::NoResponse {
                request_id: request_uuid.clone(),
            })?;
        match response {
            Response::Lock {
                acquired: true,
                lock_uuid: Some(lock_uuid),
                fencing_token,
                ..
            } => Ok(Some(RaftSimLock {
                key,
                lock_uuid,
                fencing_token,
            })),
            Response::Lock {
                acquired: false, ..
            } => Ok(None),
            response => Err(RaftSimError::UnexpectedResponse {
                request_id: request_uuid,
                response,
            }),
        }
    }

    async fn release_composite_on_node(
        sim: &RaftSim,
        node_id: &str,
        lock: &RaftSimCompositeLock,
        label: &str,
    ) -> Result<(), RaftSimError> {
        crate::routine_id!("ddl-routine-raft-sim-test-release-composite-node-1");
        let request_uuid = format!("sim-{label}-composite-release-{}", Uuid::new_v4());
        let response = sim
            .run_on_node(
                node_id,
                Request::Unlock {
                    uuid: request_uuid.clone(),
                    key: None,
                    keys: Some(lock.keys.clone()),
                    lock_uuid: Some(lock.lock_uuid.clone()),
                    force: false,
                },
                &request_uuid,
                Duration::from_secs(2),
                false,
            )
            .await?
            .ok_or_else(|| RaftSimError::NoResponse {
                request_id: request_uuid.clone(),
            })?;
        match response {
            Response::Unlock { unlocked: true, .. } => Ok(()),
            response => Err(RaftSimError::UnexpectedResponse {
                request_id: request_uuid,
                response,
            }),
        }
    }

    async fn release_lock_on_node(
        sim: &RaftSim,
        node_id: &str,
        lock: &RaftSimLock,
        label: &str,
    ) -> Result<(), RaftSimError> {
        crate::routine_id!("ddl-routine-raft-sim-test-release-lock-node-1");
        let request_uuid = format!("sim-{label}-release-{}", Uuid::new_v4());
        let response = sim
            .run_on_node(
                node_id,
                Request::Unlock {
                    uuid: request_uuid.clone(),
                    key: Some(lock.key.clone()),
                    keys: None,
                    lock_uuid: Some(lock.lock_uuid.clone()),
                    force: false,
                },
                &request_uuid,
                Duration::from_secs(2),
                false,
            )
            .await?
            .ok_or_else(|| RaftSimError::NoResponse {
                request_id: request_uuid.clone(),
            })?;
        match response {
            Response::Unlock { unlocked: true, .. } => Ok(()),
            response => Err(RaftSimError::UnexpectedResponse {
                request_id: request_uuid,
                response,
            }),
        }
    }

    async fn release_all_held_on_seeded_nodes(
        sim: &RaftSim,
        node_ids: &[String],
        held: &mut BTreeMap<String, RaftSimLock>,
        rng: &mut SimRng,
        label: &str,
    ) -> Result<(), RaftSimError> {
        crate::routine_id!("ddl-routine-raft-sim-test-release-all-held-1");
        let keys = held.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            let node_id = &node_ids[rng.index(node_ids.len())];
            let lock = held
                .remove(&key)
                .expect("held lock key collected from map should still exist");
            release_lock_on_node(sim, node_id, &lock, label).await?;
            let leader = sim.wait_for_leader(Duration::from_secs(5)).await?;
            sim.wait_for_quorum_commit(leader.commit_index(), Duration::from_secs(5))
                .await?;
        }
        Ok(())
    }

    async fn wait_for_ready_leader_in(
        sim: &RaftSim,
        node_ids: &[String],
        timeout: Duration,
    ) -> Result<BrokerRaft, RaftSimError> {
        crate::routine_id!("ddl-routine-raft-sim-test-wait-leader-in-1");
        let allowed = node_ids.iter().cloned().collect::<BTreeSet<_>>();
        let deadline = deadline_after(timeout);
        loop {
            if let Some(leader) = allowed
                .iter()
                .filter_map(|node_id| sim.node(node_id))
                .find(|node| node.is_leader_ready())
                .cloned()
            {
                return Ok(leader);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RaftSimError::Timeout {
                    operation: "ready raft leader in node subset",
                    timeout_ms: duration_ms_u64(timeout),
                });
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn wait_for_all_applied(
        sim: &RaftSim,
        index: u64,
        timeout: Duration,
    ) -> Result<(), RaftSimError> {
        crate::routine_id!("ddl-routine-raft-sim-test-wait-all-applied-1");
        let deadline = deadline_after(timeout);
        loop {
            let progress = sim.progress();
            if progress
                .iter()
                .all(|node| node.commit_index >= index && node.last_applied >= index)
            {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RaftSimError::Timeout {
                    operation: "all raft nodes applied index",
                    timeout_ms: duration_ms_u64(timeout),
                });
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn wait_for_applied_on(
        sim: &RaftSim,
        node_ids: &BTreeSet<String>,
        index: u64,
        timeout: Duration,
    ) -> Result<(), RaftSimError> {
        crate::routine_id!("ddl-routine-raft-sim-test-wait-applied-on-1");
        let deadline = deadline_after(timeout);
        loop {
            let all_applied = node_ids.iter().all(|node_id| {
                sim.node(node_id)
                    .map(|node| {
                        let progress = node.progress_snapshot();
                        progress.commit_index >= index && progress.last_applied >= index
                    })
                    .unwrap_or(false)
            });
            if all_applied {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RaftSimError::Timeout {
                    operation: "selected raft nodes applied index",
                    timeout_ms: duration_ms_u64(timeout),
                });
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn wait_for_removed_member_guards(
        sim: &RaftSim,
        node_ids: &[String],
        timeout: Duration,
    ) -> Result<(), RaftSimError> {
        crate::routine_id!("ddl-routine-raft-sim-test-wait-removed-member-guards-1");
        let deadline = deadline_after(timeout);
        loop {
            let all_guarded = node_ids.iter().all(|node_id| {
                sim.node(node_id)
                    .map(|node| removed_member_guard_active(&node.progress_snapshot()))
                    .unwrap_or(false)
            });
            if all_guarded {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RaftSimError::Timeout {
                    operation: "removed raft member client guard",
                    timeout_ms: duration_ms_u64(timeout),
                });
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn removed_member_guard_active(progress: &RaftProgressSnapshot) -> bool {
        crate::routine_id!("ddl-routine-raft-sim-test-removed-member-guard-1");
        match &progress.membership {
            RaftMembership::Simple { peers } => {
                !peers.iter().any(|peer| peer.id == progress.node_id)
            }
            RaftMembership::Joint {
                old_peers,
                new_peers,
            } => {
                old_peers.iter().any(|peer| peer.id == progress.node_id)
                    && !new_peers.iter().any(|peer| peer.id == progress.node_id)
            }
        }
    }

    async fn assert_removed_node_rejects_client(sim: &RaftSim, removed_id: &str) {
        crate::routine_id!("ddl-routine-raft-sim-test-removed-node-rejects-client-1");
        let telemetry_before = sim
            .node(removed_id)
            .expect("removed node present")
            .telemetry_snapshot();
        let request_uuid = format!("sim-removed-client-{}", Uuid::new_v4());
        let err = sim
            .run_on_node(
                removed_id,
                Request::Lock {
                    uuid: request_uuid.clone(),
                    key: Some(format!("sim-removed-key-{}", Uuid::new_v4())),
                    keys: None,
                    pid: None,
                    ttl: None,
                    max: None,
                    force: false,
                    retry_count: 0,
                    keep_locks_after_death: false,
                    wait: None,
                },
                &request_uuid,
                Duration::ZERO,
                true,
            )
            .await
            .expect_err("removed node should reject public client request");
        assert!(
            matches!(err, RaftSimError::Raft(BrokerRaftError::NotLeader { .. })),
            "removed node {removed_id} should return NotLeader, got {err:?}"
        );
        let telemetry_after = sim
            .node(removed_id)
            .expect("removed node present after reject")
            .telemetry_snapshot();
        assert_eq!(
            telemetry_after.client_removed_member_rejections_total,
            telemetry_before
                .client_removed_member_rejections_total
                .saturating_add(1),
            "removed node {removed_id} should count the local client rejection"
        );
        assert_eq!(
            telemetry_after.proxy_requests_forwarded_total,
            telemetry_before.proxy_requests_forwarded_total,
            "removed node {removed_id} must reject locally instead of proxying"
        );
    }

    fn append_entries_batches_total(sim: &RaftSim) -> u64 {
        crate::routine_id!("ddl-routine-raft-sim-test-append-batches-total-1");
        sim.nodes()
            .values()
            .map(|node| node.telemetry_snapshot().append_entries_batches_total)
            .sum()
    }

    fn append_entries_sent_total(sim: &RaftSim) -> u64 {
        crate::routine_id!("ddl-routine-raft-sim-test-append-sent-total-1");
        sim.nodes()
            .values()
            .map(|node| node.telemetry_snapshot().append_entries_sent_total)
            .sum()
    }

    fn append_snapshot_fallbacks_total(sim: &RaftSim) -> u64 {
        crate::routine_id!("ddl-routine-raft-sim-test-append-snapshot-fallbacks-1");
        sim.nodes()
            .values()
            .map(|node| node.telemetry_snapshot().append_snapshot_fallbacks_total)
            .sum()
    }

    fn install_snapshot_successes_total(sim: &RaftSim) -> u64 {
        crate::routine_id!("ddl-routine-raft-sim-test-install-snapshot-successes-1");
        sim.nodes()
            .values()
            .map(|node| node.telemetry_snapshot().install_snapshot_successes_total)
            .sum()
    }

    async fn wait_for_node_compaction(
        sim: &RaftSim,
        node_id: &str,
        previous_compactions: u64,
        timeout: Duration,
    ) -> Result<(), RaftSimError> {
        crate::routine_id!("ddl-routine-raft-sim-test-wait-node-compaction-1");
        let deadline = deadline_after(timeout);
        loop {
            let node = sim
                .node(node_id)
                .ok_or_else(|| RaftSimError::NodeNotFound(node_id.to_string()))?;
            if node.telemetry_snapshot().log_compactions_total > previous_compactions {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RaftSimError::Timeout {
                    operation: "raft node log compaction",
                    timeout_ms: duration_ms_u64(timeout),
                });
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn wait_for_install_snapshot_success(
        sim: &RaftSim,
        previous_successes: u64,
        timeout: Duration,
    ) -> Result<(), RaftSimError> {
        crate::routine_id!("ddl-routine-raft-sim-test-wait-install-snapshot-1");
        let deadline = deadline_after(timeout);
        loop {
            if install_snapshot_successes_total(sim) > previous_successes {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RaftSimError::Timeout {
                    operation: "raft InstallSnapshot success",
                    timeout_ms: duration_ms_u64(timeout),
                });
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
