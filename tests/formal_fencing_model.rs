use std::collections::{HashSet, VecDeque};

const MAX_GRANTS: u8 = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
enum Client {
    A,
    B,
}

impl Client {
    fn index(self) -> usize {
        match self {
            Self::A => 0,
            Self::B => 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
enum Work {
    One,
    Two,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Decision {
    Advanced,
    Replay,
    Stale,
    TokenReuse,
    NoToken,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
struct State {
    holder: Option<Client>,
    highest_grant: u8,
    last_token: [Option<u8>; 2],
    downstream_watermark: u8,
    downstream_identity: Option<(Client, Work)>,
}

impl State {
    fn initial() -> Self {
        Self {
            holder: None,
            highest_grant: 0,
            last_token: [None, None],
            downstream_watermark: 0,
            downstream_identity: None,
        }
    }
}

fn assert_invariants(state: &State) {
    assert!(
        state.downstream_watermark <= state.highest_grant,
        "external state cannot observe authority that was never granted: {state:?}"
    );

    if state.highest_grant == 0 {
        assert_eq!(state.last_token, [None, None]);
        assert_eq!(state.downstream_watermark, 0);
        assert_eq!(state.downstream_identity, None);
    }

    if let Some(holder) = state.holder {
        assert_eq!(
            state.last_token[holder.index()],
            Some(state.highest_grant),
            "current exclusive owner must hold the newest grant: {state:?}"
        );
    }

    if state.downstream_watermark == 0 {
        assert_eq!(state.downstream_identity, None);
    } else {
        assert!(state.downstream_identity.is_some());
    }
}

fn acquire(state: State, client: Client) -> Option<State> {
    if state.holder.is_some() || state.highest_grant >= MAX_GRANTS {
        return None;
    }

    let mut next = state;
    let token = state.highest_grant + 1;
    assert!(token > state.highest_grant, "successor token must strictly increase");
    next.highest_grant = token;
    next.holder = Some(client);
    next.last_token[client.index()] = Some(token);
    Some(next)
}

fn release(state: State, client: Client) -> Option<State> {
    if state.holder != Some(client) {
        return None;
    }

    let mut next = state;
    next.holder = None;
    Some(next)
}

fn write(state: State, client: Client, work: Work) -> (State, Decision) {
    let Some(token) = state.last_token[client.index()] else {
        return (state, Decision::NoToken);
    };

    if token < state.downstream_watermark {
        return (state, Decision::Stale);
    }

    if token == state.downstream_watermark {
        return if state.downstream_identity == Some((client, work)) {
            (state, Decision::Replay)
        } else {
            (state, Decision::TokenReuse)
        };
    }

    let mut next = state;
    next.downstream_watermark = token;
    next.downstream_identity = Some((client, work));
    (next, Decision::Advanced)
}

fn verify_write_transition(before: &State, after: &State, client: Client, work: Work, decision: Decision) {
    let token = before.last_token[client.index()];
    match decision {
        Decision::NoToken => assert!(token.is_none()),
        Decision::Advanced => {
            let token = token.expect("advanced write must have a grant");
            assert!(token > before.downstream_watermark);
            assert_eq!(after.downstream_watermark, token);
            assert_eq!(after.downstream_identity, Some((client, work)));
        }
        Decision::Replay => {
            let token = token.expect("replay must have a grant");
            assert_eq!(token, before.downstream_watermark);
            assert_eq!(before.downstream_identity, Some((client, work)));
            assert_eq!(after, before, "exact replay must not mutate state");
        }
        Decision::Stale => {
            let token = token.expect("stale write must have an old grant");
            assert!(token < before.downstream_watermark);
            assert_eq!(after, before, "stale writer must not mutate state");
        }
        Decision::TokenReuse => {
            let token = token.expect("token reuse must have a grant");
            assert_eq!(token, before.downstream_watermark);
            assert_ne!(before.downstream_identity, Some((client, work)));
            assert_eq!(after, before, "same-token different work must be rejected");
        }
    }
}

#[test]
fn bounded_exhaustive_fencing_model_preserves_safety() {
    let initial = State::initial();
    let mut seen = HashSet::from([initial]);
    let mut queue = VecDeque::from([initial]);

    while let Some(state) = queue.pop_front() {
        assert_invariants(&state);

        for client in [Client::A, Client::B] {
            if let Some(next) = acquire(state, client) {
                assert_invariants(&next);
                if seen.insert(next) {
                    queue.push_back(next);
                }
            }

            if let Some(next) = release(state, client) {
                assert_eq!(
                    next.highest_grant, state.highest_grant,
                    "release must never roll back fencing authority"
                );
                assert_invariants(&next);
                if seen.insert(next) {
                    queue.push_back(next);
                }
            }

            for work in [Work::One, Work::Two] {
                let (next, decision) = write(state, client, work);
                verify_write_transition(&state, &next, client, work, decision);
                assert_invariants(&next);
                if seen.insert(next) {
                    queue.push_back(next);
                }
            }
        }
    }

    assert!(
        seen.len() > 100,
        "model should explore a non-trivial state space; visited {} states",
        seen.len()
    );

    // A concrete zombie-writer witness inside the same abstract semantics.
    let a = acquire(initial, Client::A).unwrap();
    let a_token = a.last_token[Client::A.index()].unwrap();
    let released = release(a, Client::A).unwrap();
    let b = acquire(released, Client::B).unwrap();
    let b_token = b.last_token[Client::B.index()].unwrap();
    assert!(b_token > a_token);
    let (committed, decision) = write(b, Client::B, Work::One);
    assert_eq!(decision, Decision::Advanced);
    let (_, stale) = write(committed, Client::A, Work::One);
    assert_eq!(stale, Decision::Stale);
}
