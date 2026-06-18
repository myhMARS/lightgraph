//! Transaction Manager — lock-free ticket-lock commit protocol.
//!
//! ## Design
//!
//! Two atomics, zero locks:
//!
//! ```
//! next_tx_id:     AtomicU64  — ticket dispenser (fetch_add)
//! committed_tx_id: AtomicU64  — now-serving counter (CAS)
//! ```
//!
//! - **begin_write**: `fetch_add` on next_tx_id → instant ticket.
//! - **begin_read**: `load` committed_tx_id → instant snapshot.
//! - **commit(tx_id)**: spin until committed_tx_id == tx_id - 1,
//!   then store tx_id. In-order, lock-free.
//! - **rollback(tx_id)**: same spin, same store — but no writes were applied,
//!   so readers see nothing from this TxId. Correct for MVCC.
//!
//! ## Why in-order commit is correct
//!
//! MVCC visibility: a reader at snapshot S sees records where
//! `created_tx ≤ S < deleted_tx`. If a rolled-back tx never created
//! any records, there's nothing to see — advancing committed_tx_id
//! past it is harmless.
//!
//! ## Convoy effect mitigation
//!
//! If a transaction holds a ticket but takes too long to commit,
//! later tickets wait (spin). To prevent indefinite waiting:
//! - `commit()` spins with exponential backoff
//! - After `SPIN_LIMIT` iterations, returns `Err("timeout")`
//! - Caller should retry with a new ticket

use std::sync::atomic::{AtomicU64, Ordering};
use std::hint;

use crate::types::TxId;

/// Max spin iterations before commit times out (≈ 1ms on modern CPU).
const SPIN_LIMIT: u32 = 100_000;

/// Manages the lifecycle of all transactions — completely lock-free.
pub struct TxManager {
    /// Next TxId to hand out (ticket dispenser).
    next_tx_id: AtomicU64,

    /// Highest TxId that has been committed or rolled back.
    /// Read transactions use this as their snapshot.
    committed_tx_id: AtomicU64,
}

impl TxManager {
    pub fn new() -> Self {
        Self {
            next_tx_id: AtomicU64::new(1),
            committed_tx_id: AtomicU64::new(0),
        }
    }

    /// Begin a read-only transaction.
    /// Returns the latest committed snapshot. O(1), lock-free.
    pub fn begin_read(&self) -> TxId {
        self.committed_tx_id.load(Ordering::Acquire)
    }

    /// Begin a write transaction.
    /// Returns a new, unique TxId. O(1), lock-free.
    pub fn begin_write(&self) -> TxId {
        self.next_tx_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Commit a write transaction.
    ///
    /// Spins until our turn (all earlier tickets have committed or rolled back),
    /// then advances committed_tx_id.
    ///
    /// Returns `Ok(())` on success, `Err("timeout")` if spin limit exceeded.
    ///
    /// After commit, all subsequent `begin_read()` calls will see the new snapshot.
    pub fn commit(&self, tx_id: TxId) -> Result<(), &'static str> {
        let expected = tx_id - 1;
        let mut spins: u32 = 0;

        loop {
            let current = self.committed_tx_id.load(Ordering::Acquire);
            if current == expected {
                // Our turn — advance the counter
                self.committed_tx_id.store(tx_id, Ordering::Release);
                return Ok(());
            }

            spins += 1;
            if spins > SPIN_LIMIT {
                return Err("timeout");
            }

            // Exponential backoff
            if spins < 16 {
                hint::spin_loop();
            } else if spins < 256 {
                for _ in 0..16 { hint::spin_loop(); }
            } else {
                std::thread::yield_now();
            }
        }
    }

    /// Rollback a write transaction.
    ///
    /// Advances committed_tx_id past this ticket without applying any writes.
    /// Other transactions waiting to commit can then proceed.
    pub fn rollback(&self, tx_id: TxId) {
        // Spin until our turn, then skip
        let expected = tx_id - 1;
        loop {
            let current = self.committed_tx_id.load(Ordering::Acquire);
            if current == expected {
                self.committed_tx_id.store(tx_id, Ordering::Release);
                return;
            }
            hint::spin_loop();
        }
    }

    /// Latest committed TxId (snapshot for new reads).
    pub fn latest_committed(&self) -> TxId {
        self.committed_tx_id.load(Ordering::Acquire)
    }
}

impl Default for TxManager {
    fn default() -> Self { Self::new() }
}

// ── Tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_snapshot() {
        let tm = TxManager::new();
        assert_eq!(tm.begin_read(), 0);
        let tx = tm.begin_write();
        tm.commit(tx).unwrap();
        assert_eq!(tm.begin_read(), tx);
    }

    #[test]
    fn test_monotonic_tx_ids() {
        let tm = TxManager::new();
        let t1 = tm.begin_write();
        let t2 = tm.begin_write();
        let t3 = tm.begin_write();
        assert!(t1 < t2 && t2 < t3);
    }

    #[test]
    fn test_commit_updates_snapshot() {
        let tm = TxManager::new();
        let snap = tm.begin_read();
        assert_eq!(snap, 0);

        let tx = tm.begin_write();
        tm.commit(tx).unwrap();
        assert!(tm.begin_read() > snap);
    }

    #[test]
    fn test_rollback_advances_committed() {
        let tm = TxManager::new();
        let _snap = tm.latest_committed();

        let tx = tm.begin_write();
        tm.rollback(tx);

        // Rollback should advance committed_tx_id so later tx can proceed
        assert_eq!(tm.latest_committed(), tx);
    }

    #[test]
    fn test_ordered_commits() {
        let tm = TxManager::new();
        let t1 = tm.begin_write();
        let t2 = tm.begin_write();
        let t3 = tm.begin_write();

        // Commit t1 first (unblocks t2)
        tm.commit(t1).unwrap();
        // Commit t2 (unblocks t3)
        tm.commit(t2).unwrap();
        // Now t3 can commit
        tm.commit(t3).unwrap();

        assert_eq!(tm.latest_committed(), 3);
    }

    #[test]
    fn test_concurrent_writers_lock_free() {
        use std::sync::Arc;
        use std::thread;

        let tm = Arc::new(TxManager::new());
        let mut handles = Vec::new();

        for _ in 0..4 {
            let tm = Arc::clone(&tm);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let tx = tm.begin_write();
                    // Simulate work
                    for _ in 0..10 { std::hint::spin_loop(); }
                    tm.commit(tx).unwrap();
                }
            }));
        }

        for h in handles { h.join().unwrap(); }
        // All 400 transactions committed in order
        assert_eq!(tm.latest_committed(), 400);
    }

    #[test]
    fn test_rollback_unblocks_others() {
        let tm = TxManager::new();
        let t1 = tm.begin_write();
        let t2 = tm.begin_write();
        let t3 = tm.begin_write();

        // Rollback t1 → t2 and t3 should be unblocked
        tm.rollback(t1);
        tm.commit(t2).unwrap();
        tm.commit(t3).unwrap();
        assert_eq!(tm.latest_committed(), 3);
    }

    #[test]
    fn test_commit_timeout_on_gap() {
        // This tests that commit doesn't hang when there's a permanent gap.
        // In the ticket-lock design, a gap can only occur if begin_write()
        // is called but neither commit nor rollback is called.
        // We handle this with a spin timeout.
        let tm = TxManager::new();
        let _t1 = tm.begin_write(); // t1 = 1, never committed or rolled back
        let t2 = tm.begin_write();  // t2 = 2

        // t2 should timeout because t1 is blocking it
        assert!(tm.commit(t2).is_err());
    }
}
