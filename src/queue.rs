//! Doubly-linked queue with O(1) push/pop at both ends and O(1) removal of an
//! arbitrary element by its `K` key.
//!
//! This mirrors the role `@oresoftware/linked-queue` plays in the upstream
//! Node.js `live-mutex` broker: each pending lock request needs to live in a
//! FIFO/priority queue per key, but we also need to remove a request from the
//! middle of that queue in O(1) when the requesting client times out, errors,
//! or disconnects (cleanupConnection in upstream broker.ts).
//!
//! Internally the queue is an arena-backed doubly-linked list. Nodes live in a
//! `Vec<Slot<K, V>>`; `head`/`tail`/`prev`/`next` are slot indexes (`usize`).
//! Freed slots form a free-list so memory is reused. A `HashMap<K, usize>`
//! gives us O(1) lookup-by-key for `remove`/`get`/`contains`.
//!
//! The arena approach keeps the data structure inside a single `Mutex` cleanly
//! (no `Rc`/`RefCell` games) and gives stable indices we can hand out as
//! "queue tokens" if we ever want to.

use std::collections::HashMap;
use std::hash::Hash;

const NIL: usize = usize::MAX;

#[derive(Debug)]
struct Slot<K, V> {
    key: Option<K>,
    value: Option<V>,
    prev: usize,
    next: usize,
}

/// FIFO queue keyed by `K`. Each `K` is unique within the queue; pushing the
/// same `K` again is a no-op (matching upstream `notify.contains` semantics).
#[derive(Debug)]
pub struct LinkedQueue<K: Eq + Hash + Clone, V> {
    slots: Vec<Slot<K, V>>,
    free: Vec<usize>,
    index: HashMap<K, usize>,
    head: usize,
    tail: usize,
    len: usize,
}

impl<K: Eq + Hash + Clone, V> Default for LinkedQueue<K, V> {
    fn default() -> Self {
        crate::routine_id!("ddl-routine-jXkFFGyoB-wXxOy7FC");
        Self::new()
    }
}

impl<K: Eq + Hash + Clone, V> LinkedQueue<K, V> {
    pub fn new() -> Self {
        crate::routine_id!("ddl-routine-u7zVp19R7LDL_wL2Au");
        Self {
            slots: Vec::new(),
            free: Vec::new(),
            index: HashMap::new(),
            head: NIL,
            tail: NIL,
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        crate::routine_id!("ddl-routine-yOpS-zAjNsvBXBLSOt");
        self.len
    }

    pub fn is_empty(&self) -> bool {
        crate::routine_id!("ddl-routine-2R0CreVvd2mDaVd-G-");
        self.len == 0
    }

    pub fn contains(&self, key: &K) -> bool {
        crate::routine_id!("ddl-routine-AER48-JGALNyb5R_xP");
        self.index.contains_key(key)
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        crate::routine_id!("ddl-routine-1TZ61XitZ7fwwXz0Mm");
        let idx = *self.index.get(key)?;
        self.slots[idx].value.as_ref()
    }

    /// Append to the tail (FIFO). No-op if `key` is already present.
    pub fn push_back(&mut self, key: K, value: V) -> bool {
        crate::routine_id!("ddl-routine-_luiuBgLux_GbtPGIo");
        if self.index.contains_key(&key) {
            return false;
        }
        let idx = self.alloc(key.clone(), value);
        if self.tail == NIL {
            self.head = idx;
            self.tail = idx;
        } else {
            self.slots[self.tail].next = idx;
            self.slots[idx].prev = self.tail;
            self.tail = idx;
        }
        self.index.insert(key, idx);
        self.len = self.len.saturating_add(1);
        true
    }

    /// Push to the head. Used for force/retry insertions that should jump the
    /// queue. No-op if `key` is already present.
    pub fn push_front(&mut self, key: K, value: V) -> bool {
        crate::routine_id!("ddl-routine-JWMK_pZuMQVZLjDpAI");
        if self.index.contains_key(&key) {
            return false;
        }
        let idx = self.alloc(key.clone(), value);
        if self.head == NIL {
            self.head = idx;
            self.tail = idx;
        } else {
            self.slots[self.head].prev = idx;
            self.slots[idx].next = self.head;
            self.head = idx;
        }
        self.index.insert(key, idx);
        self.len = self.len.saturating_add(1);
        true
    }

    /// Pop the head element (FIFO dequeue). Returns `(key, value)`.
    pub fn pop_front(&mut self) -> Option<(K, V)> {
        crate::routine_id!("ddl-routine-z5ECAdo119VbDKreQJ");
        if self.head == NIL {
            return None;
        }
        let idx = self.head;
        let next = self.slots[idx].next;
        self.head = next;
        if next == NIL {
            self.tail = NIL;
        } else {
            self.slots[next].prev = NIL;
        }
        self.detach(idx)
    }

    /// Peek at the head without removing it.
    pub fn front(&self) -> Option<(&K, &V)> {
        crate::routine_id!("ddl-routine-HdN0Nd-ERLc5HFSzKW");
        if self.head == NIL {
            return None;
        }
        let slot = &self.slots[self.head];
        Some((slot.key.as_ref()?, slot.value.as_ref()?))
    }

    /// Remove an element by `key` in O(1). Returns the value if present.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        crate::routine_id!("ddl-routine-2kna_P2-hQO1bUmmZD");
        let idx = self.index.remove(key)?;
        let prev = self.slots[idx].prev;
        let next = self.slots[idx].next;
        if prev == NIL {
            self.head = next;
        } else {
            self.slots[prev].next = next;
        }
        if next == NIL {
            self.tail = prev;
        } else {
            self.slots[next].prev = prev;
        }
        let (_, v) = self.detach_no_index(idx)?;
        Some(v)
    }

    /// Iterate from head to tail without consuming.
    pub fn iter(&self) -> Iter<'_, K, V> {
        crate::routine_id!("ddl-routine-YL8_XRwkTGmdu0Eo2b");
        Iter {
            queue: self,
            cursor: self.head,
        }
    }

    fn alloc(&mut self, key: K, value: V) -> usize {
        crate::routine_id!("ddl-routine-bkjbWoCU2ZCCSUDKWF");
        if let Some(idx) = self.free.pop() {
            let slot = &mut self.slots[idx];
            slot.key = Some(key);
            slot.value = Some(value);
            slot.prev = NIL;
            slot.next = NIL;
            idx
        } else {
            let idx = self.slots.len();
            self.slots.push(Slot {
                key: Some(key),
                value: Some(value),
                prev: NIL,
                next: NIL,
            });
            idx
        }
    }

    fn detach(&mut self, idx: usize) -> Option<(K, V)> {
        crate::routine_id!("ddl-routine-ejn9IHUzFwBP2k1RTD");
        let slot = &mut self.slots[idx];
        let key = slot.key.take()?;
        let value = slot.value.take()?;
        slot.prev = NIL;
        slot.next = NIL;
        self.free.push(idx);
        self.index.remove(&key);
        // `saturating_sub` defends against an internal logic bug
        // double-detaching a slot. We'd rather under-count by 1 than
        // panic in release.
        self.len = self.len.saturating_sub(1);
        Some((key, value))
    }

    fn detach_no_index(&mut self, idx: usize) -> Option<(K, V)> {
        crate::routine_id!("ddl-routine-PqD3OTR8Tp2LAsy-dl");
        let slot = &mut self.slots[idx];
        let key = slot.key.take()?;
        let value = slot.value.take()?;
        slot.prev = NIL;
        slot.next = NIL;
        self.free.push(idx);
        self.len = self.len.saturating_sub(1);
        Some((key, value))
    }
}

pub struct Iter<'a, K: Eq + Hash + Clone, V> {
    queue: &'a LinkedQueue<K, V>,
    cursor: usize,
}

impl<'a, K: Eq + Hash + Clone, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        crate::routine_id!("ddl-routine-_GapTQepfQqvSD-41z");
        if self.cursor == NIL {
            return None;
        }
        let slot = &self.queue.slots[self.cursor];
        self.cursor = slot.next;
        Some((slot.key.as_ref()?, slot.value.as_ref()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fifo_order() {
        crate::routine_id!("ddl-routine--er5KgxaOJcyZbEpMe");
        let mut q: LinkedQueue<&'static str, u32> = LinkedQueue::new();
        assert!(q.push_back("a", 1));
        assert!(q.push_back("b", 2));
        assert!(q.push_back("c", 3));
        assert_eq!(q.len(), 3);
        assert_eq!(q.pop_front(), Some(("a", 1)));
        assert_eq!(q.pop_front(), Some(("b", 2)));
        assert_eq!(q.pop_front(), Some(("c", 3)));
        assert!(q.pop_front().is_none());
    }

    #[test]
    fn duplicate_key_is_noop() {
        crate::routine_id!("ddl-routine-WR0Bc-O2-i_NWp3JYr");
        let mut q: LinkedQueue<&'static str, u32> = LinkedQueue::new();
        assert!(q.push_back("a", 1));
        assert!(!q.push_back("a", 99));
        assert_eq!(q.len(), 1);
        assert_eq!(q.get(&"a"), Some(&1));
    }

    #[test]
    fn remove_from_middle_o1() {
        crate::routine_id!("ddl-routine-tIny8nlxpTWQXqG772");
        let mut q: LinkedQueue<&'static str, u32> = LinkedQueue::new();
        q.push_back("a", 1);
        q.push_back("b", 2);
        q.push_back("c", 3);
        q.push_back("d", 4);
        assert_eq!(q.remove(&"b"), Some(2));
        assert_eq!(q.remove(&"d"), Some(4));
        assert_eq!(q.len(), 2);
        assert_eq!(q.pop_front(), Some(("a", 1)));
        assert_eq!(q.pop_front(), Some(("c", 3)));
        assert!(q.pop_front().is_none());
    }

    #[test]
    fn remove_head_and_tail() {
        crate::routine_id!("ddl-routine-32PjlKmz6LNtCvFQ6T");
        let mut q: LinkedQueue<&'static str, u32> = LinkedQueue::new();
        q.push_back("a", 1);
        q.push_back("b", 2);
        q.push_back("c", 3);
        assert_eq!(q.remove(&"a"), Some(1));
        assert_eq!(q.remove(&"c"), Some(3));
        assert_eq!(q.len(), 1);
        assert_eq!(q.pop_front(), Some(("b", 2)));
    }

    #[test]
    fn push_front_jumps_queue() {
        crate::routine_id!("ddl-routine-HsNPxbgCEqaBQ7LUFq");
        let mut q: LinkedQueue<&'static str, u32> = LinkedQueue::new();
        q.push_back("a", 1);
        q.push_back("b", 2);
        q.push_front("c", 3);
        assert_eq!(q.pop_front(), Some(("c", 3)));
        assert_eq!(q.pop_front(), Some(("a", 1)));
        assert_eq!(q.pop_front(), Some(("b", 2)));
    }

    #[test]
    fn slots_get_reused() {
        crate::routine_id!("ddl-routine-hXGfUqonWsQZcIALxa");
        let mut q: LinkedQueue<u32, u32> = LinkedQueue::new();
        for i in 0..1000 {
            q.push_back(i, i);
        }
        for i in 0..1000 {
            assert_eq!(q.remove(&i), Some(i));
        }
        for i in 0..1000 {
            q.push_back(i, i * 2);
        }
        // Free list should keep total slots bounded near peak occupancy.
        assert!(q.slots.len() <= 1000);
        assert_eq!(q.len(), 1000);
    }

    /// Randomised sequence of ops driven by an LCG. The invariant
    /// we verify: a parallel `VecDeque` model agrees with our queue
    /// on `front`, `len`, `contains`, and full iteration order
    /// after every step. This catches any drift between
    /// `head`/`tail`/`prev`/`next` and the index without needing a
    /// fuzzer.
    #[test]
    fn fuzz_against_vecdeque_oracle() {
        crate::routine_id!("ddl-routine-fuzz-vecdeque-_4M");
        use std::collections::VecDeque;

        let mut q: LinkedQueue<u32, u32> = LinkedQueue::new();
        let mut model: VecDeque<(u32, u32)> = VecDeque::new();
        // Tiny LCG so the test is deterministic across stdlib versions.
        let mut state: u64 = 0xdeadbeefcafef00d;
        let mut rng = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            state
        };

        for step in 0..10_000u32 {
            let op = rng() % 6;
            let k = (rng() % 64) as u32;
            let v = (rng() % 1024) as u32;
            match op {
                0 => {
                    let inserted = q.push_back(k, v);
                    if !model.iter().any(|(mk, _)| *mk == k) {
                        assert!(inserted, "step {step}: push_back({k}) should have inserted");
                        model.push_back((k, v));
                    } else {
                        assert!(!inserted, "step {step}: push_back({k}) should be idempotent");
                    }
                }
                1 => {
                    let inserted = q.push_front(k, v);
                    if !model.iter().any(|(mk, _)| *mk == k) {
                        assert!(inserted, "step {step}: push_front({k}) should have inserted");
                        model.push_front((k, v));
                    } else {
                        assert!(!inserted, "step {step}: push_front({k}) should be idempotent");
                    }
                }
                2 => {
                    let popped = q.pop_front();
                    let model_pop = model.pop_front();
                    assert_eq!(popped, model_pop, "step {step}: pop_front mismatch");
                }
                3 => {
                    let removed = q.remove(&k);
                    let pos = model.iter().position(|(mk, _)| *mk == k);
                    let model_removed = pos.map(|p| model.remove(p).unwrap().1);
                    assert_eq!(removed, model_removed, "step {step}: remove({k}) mismatch");
                }
                4 => {
                    assert_eq!(
                        q.contains(&k),
                        model.iter().any(|(mk, _)| *mk == k),
                        "step {step}: contains({k}) mismatch"
                    );
                }
                _ => {
                    assert_eq!(q.len(), model.len(), "step {step}: len mismatch");
                    let q_order: Vec<(u32, u32)> = q.iter().map(|(k, v)| (*k, *v)).collect();
                    let m_order: Vec<(u32, u32)> = model.iter().copied().collect();
                    assert_eq!(q_order, m_order, "step {step}: iter order drift");
                    assert_eq!(
                        q.front().map(|(k, v)| (*k, *v)),
                        model.front().copied(),
                        "step {step}: front mismatch"
                    );
                }
            }
        }
    }
}
