//! Fencing-authority admission wrapper around the lock-state broker.
//!
//! `broker.rs` remains the lock/fairness/TTL engine. This module owns the
//! authority namespace presented by the public/server-facing `Broker` type:
//! every authority-bearing request reserves its complete fencing-token range
//! before the raw broker is allowed to mutate lock state. That gives us one
//! fail-closed boundary for single-key, RW, semaphore, and composite grants.

use std::ops::Deref;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use crate::broker_raw::{Broker as RawBroker, GrantOverrides};
use crate::protocol::{Request, Response, MAX_COMPOSITE_KEYS};

pub use crate::broker_raw::{
    BrokerConfig, BrokerMetrics, ClientId, KeyContentionSnapshot, Sender,
};

/// Largest fencing value that every first-class JSON client can represent
/// exactly. Keep this identical to `fenced_client::MAX_FENCING_TOKEN`.
pub const MAX_SERVER_FENCING_TOKEN: u64 = crate::fenced_client::MAX_FENCING_TOKEN;

#[derive(Debug)]
struct AuthorityAllocator {
    next: u64,
}

impl AuthorityAllocator {
    fn new() -> Self {
        crate::routine_id!("ddl-routine-hardened-broker-authority-allocator-new-1");
        // Microseconds give restarts a much larger natural epoch jump than the
        // raw broker's historical millisecond seed while remaining comfortably
        // below 2^53 for centuries. Raft-provided deterministic seeds still win
        // when present; this is the standalone/server-side allocator.
        let wall_clock_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_micros())
            .unwrap_or(1)
            .min(u128::from(MAX_SERVER_FENCING_TOKEN)) as u64;
        return Self {
            next: wall_clock_micros.max(1),
        };
    }

    fn reserve(&mut self, width: u64) -> Result<u64, String> {
        crate::routine_id!("ddl-routine-hardened-broker-authority-allocator-reserve-1");
        if width == 0 {
            return Err("fencing reservation width must be at least one".into());
        }
        if self.next == 0 || self.next > MAX_SERVER_FENCING_TOKEN {
            return Err(format!(
                "fencing authority exhausted at fleet-safe ceiling {MAX_SERVER_FENCING_TOKEN}"
            ));
        }
        let last = self
            .next
            .checked_add(width.saturating_sub(1))
            .ok_or_else(|| "fencing authority range overflow".to_string())?;
        if last > MAX_SERVER_FENCING_TOKEN {
            return Err(format!(
                "fencing authority range would exceed fleet-safe ceiling {MAX_SERVER_FENCING_TOKEN}"
            ));
        }
        let first = self.next;
        self.next = last.saturating_add(1);
        return Ok(first);
    }

    fn observe_reserved_range(&mut self, first: u64, width: u64) -> Result<(), String> {
        crate::routine_id!("ddl-routine-hardened-broker-authority-observe-range-1");
        if first == 0 || width == 0 {
            return Err("fencing override must use a positive token and width".into());
        }
        let last = first
            .checked_add(width.saturating_sub(1))
            .ok_or_else(|| "fencing override range overflow".to_string())?;
        if last > MAX_SERVER_FENCING_TOKEN {
            return Err(format!(
                "fencing override range exceeds fleet-safe ceiling {MAX_SERVER_FENCING_TOKEN}"
            ));
        }
        if last >= self.next {
            self.next = last.saturating_add(1);
        }
        return Ok(());
    }
}

/// Public/server-facing broker. Non-authority methods delegate to the raw
/// broker via `Deref`; grant admission is shadowed here so unsafe authority can
/// never reach the mutation engine.
#[derive(Clone)]
pub struct Broker {
    inner: RawBroker,
    authority: Arc<Mutex<AuthorityAllocator>>,
}

impl Broker {
    pub fn new(config: BrokerConfig) -> Self {
        crate::routine_id!("ddl-routine-hardened-broker-new-1");
        return Self {
            inner: RawBroker::new(config),
            authority: Arc::new(Mutex::new(AuthorityAllocator::new())),
        };
    }

    pub(crate) fn with_response_observer(
        config: BrokerConfig,
        response_observer: Arc<dyn Fn(&Response) + Send + Sync + 'static>,
    ) -> Self {
        crate::routine_id!("ddl-routine-hardened-broker-with-observer-1");
        return Self {
            inner: RawBroker::with_response_observer(config, response_observer),
            authority: Arc::new(Mutex::new(AuthorityAllocator::new())),
        };
    }

    pub fn handle_request(&self, client: ClientId, request: Request) {
        crate::routine_id!("ddl-routine-hardened-broker-handle-request-1");
        self.handle_request_with_grant_overrides(client, request, GrantOverrides::default());
    }

    pub(crate) fn handle_request_with_grant_uuid(
        &self,
        client: ClientId,
        request: Request,
        grant_lock_uuid: Option<String>,
    ) {
        crate::routine_id!("ddl-routine-hardened-broker-handle-request-with-uuid-1");
        self.handle_request_with_grant_overrides(
            client,
            request,
            GrantOverrides {
                lock_uuid: grant_lock_uuid,
                fencing_seed: None,
            },
        );
    }

    pub(crate) fn handle_request_with_grant_overrides(
        &self,
        client: ClientId,
        request: Request,
        mut grant_overrides: GrantOverrides,
    ) {
        crate::routine_id!("ddl-routine-hardened-broker-handle-request-with-overrides-1");
        let Some(width) = authority_width(&request) else {
            self.inner
                .handle_request_with_grant_overrides(client, request, grant_overrides);
            return;
        };

        let authority_result = {
            let mut allocator = self.authority.lock();
            match grant_overrides.fencing_seed {
                Some(seed) => allocator
                    .observe_reserved_range(seed, width)
                    .map(|_| seed),
                None => allocator.reserve(width),
            }
        };

        let seed = match authority_result {
            Ok(seed) => seed,
            Err(reason) => {
                let response = authority_error_response(&request, reason);
                let _ = self.inner.try_send(client, response);
                return;
            }
        };
        grant_overrides.fencing_seed = Some(seed);
        self.inner
            .handle_request_with_grant_overrides(client, request, grant_overrides);
    }

    pub(crate) fn validate_raft_snapshot_payload(
        payload: &serde_json::Value,
    ) -> Result<(), String> {
        crate::routine_id!("ddl-routine-hardened-broker-validate-raft-snapshot-1");
        return RawBroker::validate_raft_snapshot_payload(payload);
    }

    pub(crate) fn validate_idle_snapshot_payload(
        payload: &serde_json::Value,
    ) -> Result<(), String> {
        crate::routine_id!("ddl-routine-hardened-broker-validate-idle-snapshot-1");
        return RawBroker::validate_idle_snapshot_payload(payload);
    }
}

impl Deref for Broker {
    type Target = RawBroker;

    fn deref(&self) -> &Self::Target {
        crate::routine_id!("ddl-routine-hardened-broker-deref-1");
        return &self.inner;
    }
}

fn authority_width(request: &Request) -> Option<u64> {
    crate::routine_id!("ddl-routine-hardened-broker-authority-width-1");
    match request {
        Request::Lock {
            key: Some(_),
            keys: None,
            ..
        } => return Some(1),
        Request::Lock {
            key: None,
            keys: Some(keys),
            ..
        } if !keys.is_empty() && keys.len() <= MAX_COMPOSITE_KEYS => {
            let distinct = keys.iter().collect::<std::collections::BTreeSet<_>>();
            return Some(distinct.len() as u64);
        }
        Request::RegisterRead { .. } | Request::RegisterWrite { .. } => return Some(1),
        _ => return None,
    }
}

fn authority_error_response(request: &Request, reason: String) -> Response {
    crate::routine_id!("ddl-routine-hardened-broker-authority-error-response-1");
    match request {
        Request::Lock {
            uuid,
            key: Some(key),
            keys: None,
            ..
        } => {
            return Response::Lock {
                uuid: uuid.clone(),
                key: key.clone(),
                acquired: false,
                lock_request_count: 0,
                lock_uuid: None,
                fencing_token: None,
                readers_count: None,
                error: Some(reason),
            };
        }
        Request::Lock {
            uuid,
            key: None,
            keys: Some(keys),
            ..
        } => {
            return Response::CompositeLock {
                uuid: uuid.clone(),
                keys: keys.clone(),
                acquired: false,
                lock_uuid: None,
                fencing_tokens: None,
                error: Some(reason),
            };
        }
        Request::RegisterRead { uuid, .. } | Request::RegisterWrite { uuid, .. } => {
            return Response::Error {
                uuid: uuid.clone(),
                error: reason,
            };
        }
        _ => {
            return Response::Error {
                uuid: request.correlation_uuid().to_string(),
                error: reason,
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocator_reserves_composite_range_atomically_and_fails_closed() {
        let mut allocator = AuthorityAllocator {
            next: MAX_SERVER_FENCING_TOKEN - 1,
        };
        assert_eq!(
            allocator.reserve(2).expect("last two safe values"),
            MAX_SERVER_FENCING_TOKEN - 1
        );
        assert!(allocator.reserve(1).is_err());
    }

    #[test]
    fn external_override_cannot_cross_exact_integer_ceiling() {
        let mut allocator = AuthorityAllocator { next: 1 };
        assert!(allocator
            .observe_reserved_range(MAX_SERVER_FENCING_TOKEN, 1)
            .is_ok());
        assert!(allocator
            .observe_reserved_range(MAX_SERVER_FENCING_TOKEN, 2)
            .is_err());
    }

    #[test]
    fn composite_width_is_distinct_key_count() {
        let request = Request::Lock {
            uuid: "r".into(),
            key: None,
            keys: Some(vec!["b".into(), "a".into(), "a".into()]),
            pid: None,
            ttl: None,
            max: None,
            force: false,
            retry_count: 0,
            keep_locks_after_death: false,
            wait: None,
        };
        assert_eq!(authority_width(&request), Some(2));
    }
}
