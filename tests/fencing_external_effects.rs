//! End-to-end fencing boundary for external side effects.
//!
//! The broker orders lock ownership; the external datastore must independently
//! reject a delayed writer whose lease was superseded. This test obtains real
//! broker grants and applies them to a tiny durable-watermark state machine.

use dd_rust_network_mutex::{
    broker::{Broker, BrokerConfig},
    protocol::{Request, Response},
};
use tokio::sync::mpsc::UnboundedReceiver;

fn drain(rx: &mut UnboundedReceiver<Response>) -> Vec<Response> {
    let mut out = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        out.push(msg);
    }
    out
}

fn lock_req(uuid: &str, key: &str) -> Request {
    Request::Lock {
        uuid: uuid.into(),
        key: Some(key.into()),
        keys: None,
        pid: None,
        ttl: Some(120_000),
        max: None,
        force: false,
        retry_count: 0,
        keep_locks_after_death: false,
        wait: Some(false),
    }
}

fn unlock_req(uuid: &str, key: &str, lock_uuid: &str) -> Request {
    Request::Unlock {
        uuid: uuid.into(),
        key: Some(key.into()),
        keys: None,
        lock_uuid: Some(lock_uuid.into()),
        force: false,
    }
}

fn single_grant(msgs: &[Response]) -> (u64, String) {
    msgs.iter()
        .find_map(|m| match m {
            Response::Lock {
                acquired: true,
                lock_uuid: Some(lock_uuid),
                fencing_token: Some(token),
                ..
            } => Some((*token, lock_uuid.clone())),
            _ => None,
        })
        .expect("expected successful lock grant with fencing token")
}

#[derive(Debug, Clone)]
struct ExternalWrite {
    fencing_token: u64,
    operation_id: &'static str,
    payload_sha256: &'static str,
}

#[derive(Debug, Default)]
struct ExternalWatermark {
    fencing_token: u64,
    operation_id: &'static str,
    payload_sha256: &'static str,
}

#[derive(Debug, PartialEq, Eq)]
enum ExternalDecision {
    Advanced,
    Replay,
    Stale,
    TokenReuse,
}

fn apply_external_write(
    watermark: &mut ExternalWatermark,
    write: &ExternalWrite,
) -> ExternalDecision {
    if write.fencing_token < watermark.fencing_token {
        return ExternalDecision::Stale;
    }

    if write.fencing_token == watermark.fencing_token {
        return if write.operation_id == watermark.operation_id
            && write.payload_sha256 == watermark.payload_sha256
        {
            ExternalDecision::Replay
        } else {
            ExternalDecision::TokenReuse
        };
    }

    watermark.fencing_token = write.fencing_token;
    watermark.operation_id = write.operation_id;
    watermark.payload_sha256 = write.payload_sha256;
    ExternalDecision::Advanced
}

#[test]
fn successor_token_fences_delayed_external_writer() {
    let broker = Broker::new(BrokerConfig::default());
    let (client_id, mut rx) = broker.register_client();
    let key = "external-fence/orders/42";

    broker.handle_request(client_id, lock_req("owner-a", key));
    let (token_a, lock_a) = single_grant(&drain(&mut rx));
    assert!(token_a > 0, "owner A must receive a positive fencing token");

    broker.handle_request(client_id, unlock_req("release-a", key, &lock_a));
    let _ = drain(&mut rx);

    broker.handle_request(client_id, lock_req("owner-b", key));
    let (token_b, lock_b) = single_grant(&drain(&mut rx));
    assert!(
        token_b > token_a,
        "successor owner must receive a strictly newer fence: {token_b} !> {token_a}"
    );

    let mut external = ExternalWatermark::default();
    let write_b = ExternalWrite {
        fencing_token: token_b,
        operation_id: "charge-order-42-v2",
        payload_sha256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    };
    assert_eq!(
        apply_external_write(&mut external, &write_b),
        ExternalDecision::Advanced
    );
    assert_eq!(
        apply_external_write(&mut external, &write_b),
        ExternalDecision::Replay,
        "exact retry must be a successful no-op"
    );

    let delayed_a = ExternalWrite {
        fencing_token: token_a,
        operation_id: "charge-order-42-v1-zombie",
        payload_sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    };
    assert_eq!(
        apply_external_write(&mut external, &delayed_a),
        ExternalDecision::Stale,
        "a delayed old owner must be rejected after the successor commits"
    );

    let token_reuse = ExternalWrite {
        fencing_token: token_b,
        operation_id: "different-operation",
        payload_sha256: write_b.payload_sha256,
    };
    assert_eq!(
        apply_external_write(&mut external, &token_reuse),
        ExternalDecision::TokenReuse,
        "the same fence cannot authorize unrelated work"
    );

    broker.handle_request(client_id, unlock_req("release-b", key, &lock_b));
}
