//! Hardening tests beyond unlock-force.
//!
//!   h1  `force: true` on `Lock` jumps to the head of the FIFO, even
//!       when there are existing waiters.
//!
//!   h2  `drop_client` only iterates the touched key set, not every
//!       lock in the broker. We cannot assert wall-clock cost cheaply,
//!       so we instead pin a behavioral invariant: keys the dropped
//!       client never touched must still have their state intact (no
//!       spurious `try_grant_next` side effects).
//!
//!   h3  Wrap-around safety on `next_client_id`. We can't realistically
//!       allocate u64::MAX clients, but we can verify the broker still
//!       hands out *some* client id on registration after a tight loop.

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

#[test]
fn h1_force_lock_jumps_to_head_of_queue() {
    let broker = Broker::new(BrokerConfig::default());
    let (a, mut a_rx) = broker.register_client();
    let (b, mut b_rx) = broker.register_client();
    let (c, mut c_rx) = broker.register_client();
    let (urgent, mut urgent_rx) = broker.register_client();

    // A holds the lock.
    broker.handle_request(
        a,
        Request::Lock {
            uuid: "ra".into(),
            key: Some("hot".into()),
            keys: None,
            pid: None,
            ttl: Some(60_000),
            max: None,
            force: false,
            retry_count: 0,
            keep_locks_after_death: false,
            wait: None,
        },
    );
    let lock_uuid_a = drain(&mut a_rx)
        .into_iter()
        .find_map(|m| match m {
            Response::Lock { lock_uuid, .. } => lock_uuid,
            _ => None,
        })
        .unwrap();

    // B and C queue normally.
    for (cid, rx, uuid) in [(b, &mut b_rx, "rb"), (c, &mut c_rx, "rc")] {
        broker.handle_request(
            cid,
            Request::Lock {
                uuid: uuid.into(),
                key: Some("hot".into()),
                keys: None,
                pid: None,
                ttl: Some(60_000),
                max: None,
                force: false,
                retry_count: 0,
                keep_locks_after_death: false,
                wait: None,
            },
        );
        let _ = drain(rx);
    }

    // Urgent client comes in with `force: true` — should land at the
    // head of the queue, in front of B and C.
    broker.handle_request(
        urgent,
        Request::Lock {
            uuid: "ru".into(),
            key: Some("hot".into()),
            keys: None,
            pid: None,
            ttl: Some(60_000),
            max: None,
            force: true,
            retry_count: 0,
            keep_locks_after_death: false,
            wait: None,
        },
    );
    let _ = drain(&mut urgent_rx);

    // A releases. The next grant should go to the urgent (force)
    // client, NOT B (which queued before urgent).
    broker.handle_request(
        a,
        Request::Unlock {
            uuid: "ua".into(),
            key: Some("hot".into()),
            keys: None,
            lock_uuid: Some(lock_uuid_a),
            force: false,
        },
    );
    let _ = drain(&mut a_rx);

    let urgent_msgs = drain(&mut urgent_rx);
    let urgent_granted = urgent_msgs.iter().any(|m| {
        matches!(
            m,
            Response::Lock {
                acquired: true,
                ..
            }
        )
    });
    let b_granted = drain(&mut b_rx).iter().any(|m| {
        matches!(
            m,
            Response::Lock {
                acquired: true,
                ..
            }
        )
    });
    let c_granted = drain(&mut c_rx).iter().any(|m| {
        matches!(
            m,
            Response::Lock {
                acquired: true,
                ..
            }
        )
    });
    assert!(
        urgent_granted,
        "urgent (force) client should have jumped the queue; got {urgent_msgs:?}"
    );
    assert!(!b_granted, "B should still be queued behind urgent");
    assert!(!c_granted, "C should still be queued behind urgent");
}

#[test]
fn h2_drop_client_does_not_disturb_unrelated_keys() {
    let broker = Broker::new(BrokerConfig::default());
    // U holds an unrelated key the disconnecting client D never
    // touched. D briefly held its own key K_d, then disconnects.
    let (u, mut u_rx) = broker.register_client();
    let (d, mut d_rx) = broker.register_client();
    let (waiter, mut waiter_rx) = broker.register_client();

    broker.handle_request(
        u,
        Request::Lock {
            uuid: "ru".into(),
            key: Some("k_u".into()),
            keys: None,
            pid: None,
            ttl: Some(60_000),
            max: None,
            force: false,
            retry_count: 0,
            keep_locks_after_death: false,
            wait: None,
        },
    );
    let _ = drain(&mut u_rx);

    broker.handle_request(
        d,
        Request::Lock {
            uuid: "rd".into(),
            key: Some("k_d".into()),
            keys: None,
            pid: None,
            ttl: Some(60_000),
            max: None,
            force: false,
            retry_count: 0,
            keep_locks_after_death: false,
            wait: None,
        },
    );
    let _ = drain(&mut d_rx);

    // Waiter queues on k_d.
    broker.handle_request(
        waiter,
        Request::Lock {
            uuid: "rw".into(),
            key: Some("k_d".into()),
            keys: None,
            pid: None,
            ttl: Some(60_000),
            max: None,
            force: false,
            retry_count: 0,
            keep_locks_after_death: false,
            wait: None,
        },
    );
    let _ = drain(&mut waiter_rx);

    broker.drop_client(d);

    // Waiter on k_d should now be promoted to holder.
    let waiter_msgs = drain(&mut waiter_rx);
    assert!(
        waiter_msgs.iter().any(|m| matches!(
            m,
            Response::Lock {
                acquired: true,
                ..
            }
        )),
        "waiter on k_d should be promoted after drop_client(d); got {waiter_msgs:?}"
    );

    // U on k_u is untouched.
    let snapshot = broker.metrics();
    assert_eq!(
        snapshot.holders, 2,
        "exactly two holders should remain (U on k_u, waiter on k_d); got {}",
        snapshot.holders
    );
}

#[test]
fn h3_register_client_keeps_handing_out_ids_after_burst() {
    // We can't trivially test `wrapping_add` at u64::MAX, but the
    // common case must keep working — registering many clients in
    // a row hands out distinct, non-zero ids.
    let broker = Broker::new(BrokerConfig::default());
    let mut ids = std::collections::HashSet::new();
    for _ in 0..10_000 {
        let (id, _rx) = broker.register_client();
        ids.insert(id);
    }
    assert_eq!(ids.len(), 10_000, "client ids must be unique");
}
