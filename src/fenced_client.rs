//! Fail-closed client facades for authority-bearing lock grants.
//!
//! The transport implementation in `client.rs` intentionally retains the
//! historical optional fencing fields for wire compatibility. Public crate
//! exports route through this module so a successful acquire cannot escape to
//! application code without complete, exact fencing authority.

use std::path::Path;
use std::time::Duration;

use crate::client::{
    Client as RawClient, ClientConfig, ClientError, LockGuard, LockInfo,
    RwClient as RawRwClient, RwReadGuard, RwWriteGuard,
};

/// Largest authority value that every first-class JSON client can represent
/// exactly. Servers must fail closed before exceeding this boundary.
pub const MAX_FENCING_TOKEN: u64 = 9_007_199_254_740_991;

fn valid_token(token: u64) -> bool {
    (1..=MAX_FENCING_TOKEN).contains(&token)
}

fn invalid(msg: impl Into<String>) -> ClientError {
    ClientError::Invalid(format!("invalid fenced authority from broker: {}", msg.into()))
}

fn validate_guard(guard: &LockGuard) -> Result<(), ClientError> {
    if guard.keys.is_empty() {
        return Err(invalid("grant contained no protected keys"));
    }
    if guard.lock_uuid.is_empty() {
        return Err(invalid("grant omitted lock_uuid"));
    }

    if guard.keys.len() == 1 {
        let token = guard
            .fencing_token
            .ok_or_else(|| invalid("single-key grant omitted fencing_token"))?;
        if !valid_token(token) {
            return Err(invalid(format!("single-key fencing token {token} outside 1..={MAX_FENCING_TOKEN}")));
        }
        if guard.fencing_tokens.len() != 1 || guard.fencing_tokens.get(&guard.keys[0]) != Some(&token) {
            return Err(invalid("single-key grant token map does not exactly match fencing_token"));
        }
        return Ok(());
    }

    if guard.fencing_token.is_some() {
        return Err(invalid("composite grant exposed an ambiguous scalar fencing_token"));
    }
    if guard.fencing_tokens.len() != guard.keys.len() {
        return Err(invalid("composite grant token/key cardinality mismatch"));
    }
    let mut unique = std::collections::BTreeSet::new();
    for key in &guard.keys {
        if !unique.insert(key) {
            return Err(invalid(format!("composite grant repeated key {key:?}")));
        }
        let token = guard
            .fencing_tokens
            .get(key)
            .copied()
            .ok_or_else(|| invalid(format!("composite grant omitted token for key {key:?}")))?;
        if !valid_token(token) {
            return Err(invalid(format!("composite token for {key:?} outside 1..={MAX_FENCING_TOKEN}")));
        }
    }
    Ok(())
}

fn validate_rw_token(lock_uuid: &str, token: Option<u64>, kind: &str) -> Result<(), ClientError> {
    if lock_uuid.is_empty() {
        return Err(invalid(format!("{kind} grant omitted lock_uuid")));
    }
    let token = token.ok_or_else(|| invalid(format!("{kind} grant omitted fencing_token")))?;
    if !valid_token(token) {
        return Err(invalid(format!("{kind} fencing token {token} outside 1..={MAX_FENCING_TOKEN}")));
    }
    Ok(())
}

/// Public exclusive/composite client. Transport and correlation behavior is
/// inherited from the raw client; only successful grant admission is stricter.
#[derive(Clone)]
pub struct Client {
    inner: RawClient,
}

impl Client {
    pub async fn connect_tcp(
        addr: impl tokio::net::ToSocketAddrs,
        config: ClientConfig,
    ) -> Result<Self, ClientError> {
        Ok(Self { inner: RawClient::connect_tcp(addr, config).await? })
    }

    #[cfg(unix)]
    pub async fn connect_uds(
        path: impl AsRef<Path>,
        config: ClientConfig,
    ) -> Result<Self, ClientError> {
        Ok(Self { inner: RawClient::connect_uds(path, config).await? })
    }

    #[cfg(not(unix))]
    pub async fn connect_uds(
        path: impl AsRef<Path>,
        config: ClientConfig,
    ) -> Result<Self, ClientError> {
        Ok(Self { inner: RawClient::connect_uds(path, config).await? })
    }

    pub async fn acquire(&self, key: &str, ttl: Duration) -> Result<LockGuard, ClientError> {
        let guard = self.inner.acquire(key, ttl).await?;
        validate_guard(&guard)?;
        Ok(guard)
    }

    pub async fn acquire_with_max(
        &self,
        key: &str,
        max: u32,
        ttl: Duration,
    ) -> Result<LockGuard, ClientError> {
        let guard = self.inner.acquire_with_max(key, max, ttl).await?;
        validate_guard(&guard)?;
        Ok(guard)
    }

    pub async fn acquire_composite(
        &self,
        keys: &[&str],
        ttl: Duration,
    ) -> Result<LockGuard, ClientError> {
        let guard = self.inner.acquire_composite(keys, ttl).await?;
        validate_guard(&guard)?;
        Ok(guard)
    }

    pub async fn try_acquire(
        &self,
        key: &str,
        ttl: Duration,
    ) -> Result<Option<LockGuard>, ClientError> {
        let guard = self.inner.try_acquire(key, ttl).await?;
        if let Some(ref g) = guard {
            validate_guard(g)?;
        }
        Ok(guard)
    }

    pub async fn try_acquire_composite(
        &self,
        keys: &[&str],
        ttl: Duration,
    ) -> Result<Option<LockGuard>, ClientError> {
        let guard = self.inner.try_acquire_composite(keys, ttl).await?;
        if let Some(ref g) = guard {
            validate_guard(g)?;
        }
        Ok(guard)
    }

    pub async fn release(&self, guard: &LockGuard) -> Result<(), ClientError> {
        self.inner.release(guard).await
    }

    pub async fn lock_info(&self, key: &str) -> Result<LockInfo, ClientError> {
        self.inner.lock_info(key).await
    }

    pub async fn ls(&self) -> Result<Vec<String>, ClientError> {
        self.inner.ls().await
    }

    pub fn config(&self) -> &ClientConfig {
        self.inner.config()
    }

    /// Explicit escape hatch for migration/testing code. Application code that
    /// performs external effects should not bypass fenced grant admission.
    pub fn into_raw(self) -> RawClient {
        self.inner
    }
}

/// Public RW facade. Both read and write grants require exact fencing authority.
#[derive(Clone)]
pub struct RwClient {
    inner: RawRwClient,
}

impl RwClient {
    pub async fn connect_tcp(
        addr: impl tokio::net::ToSocketAddrs,
        config: ClientConfig,
    ) -> Result<Self, ClientError> {
        Ok(Self { inner: RawRwClient::connect_tcp(addr, config).await? })
    }

    pub async fn connect_uds(
        path: impl AsRef<Path>,
        config: ClientConfig,
    ) -> Result<Self, ClientError> {
        Ok(Self { inner: RawRwClient::connect_uds(path, config).await? })
    }

    pub async fn acquire_read(&self, key: &str) -> Result<RwReadGuard, ClientError> {
        let guard = self.inner.acquire_read(key).await?;
        validate_rw_token(&guard.lock_uuid, guard.fencing_token, "read")?;
        Ok(guard)
    }

    pub async fn acquire_write(&self, key: &str) -> Result<RwWriteGuard, ClientError> {
        let guard = self.inner.acquire_write(key).await?;
        validate_rw_token(&guard.lock_uuid, guard.fencing_token, "write")?;
        Ok(guard)
    }

    pub fn into_raw(self) -> RawRwClient {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn rejects_unfenced_and_incomplete_guards() {
        let missing = LockGuard {
            keys: vec!["k".into()],
            lock_uuid: "l".into(),
            fencing_token: None,
            fencing_tokens: BTreeMap::new(),
        };
        assert!(validate_guard(&missing).is_err());

        let mut partial = BTreeMap::new();
        partial.insert("a".into(), 1);
        let composite = LockGuard {
            keys: vec!["a".into(), "b".into()],
            lock_uuid: "l".into(),
            fencing_token: None,
            fencing_tokens: partial,
        };
        assert!(validate_guard(&composite).is_err());
    }

    #[test]
    fn accepts_exact_single_and_composite_authority() {
        let mut one = BTreeMap::new();
        one.insert("k".into(), MAX_FENCING_TOKEN);
        let single = LockGuard {
            keys: vec!["k".into()],
            lock_uuid: "l".into(),
            fencing_token: Some(MAX_FENCING_TOKEN),
            fencing_tokens: one,
        };
        validate_guard(&single).unwrap();

        let mut many = BTreeMap::new();
        many.insert("a".into(), 1);
        many.insert("b".into(), 2);
        let composite = LockGuard {
            keys: vec!["a".into(), "b".into()],
            lock_uuid: "l".into(),
            fencing_token: None,
            fencing_tokens: many,
        };
        validate_guard(&composite).unwrap();
    }
}
