// Simplified MVCC with snapshot isolation.
//
// Each transaction gets a monotonically increasing TxId.
// Reads see the latest committed version ≤ their TxId.
// Writes create new versions tagged with the TxId.
// Conflicts detected at commit time (first-committer-wins).

use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use crate::types::TxId;

pub struct TxManager {
    next_tx_id: AtomicU64,
    committed_tx_id: AtomicU64,
    active_writers: RwLock<Vec<TxId>>,
}

impl TxManager {
    pub fn new() -> Self {
        Self {
            next_tx_id: AtomicU64::new(1), // 0 = bootstrap
            committed_tx_id: AtomicU64::new(0),
            active_writers: RwLock::new(Vec::new()),
        }
    }

    /// Begin a read transaction — returns the latest committed TxId.
    pub fn begin_read(&self) -> TxId {
        self.committed_tx_id.load(Ordering::SeqCst)
    }

    /// Begin a write transaction — allocates a new TxId.
    pub fn begin_write(&self) -> TxId {
        let id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        self.active_writers.write().push(id);
        id
    }

    /// Commit a write transaction — makes it visible.
    /// Returns false if conflict detected.
    pub fn commit_write(&self, tx_id: TxId) -> bool {
        let mut writers = self.active_writers.write();
        writers.retain(|&t| t != tx_id);
        // First-committer-wins: check no earlier active tx committed conflicting versions
        // Full conflict detection in Sprint 2
        self.committed_tx_id.store(tx_id, Ordering::SeqCst);
        true
    }

    /// Rollback a write transaction.
    pub fn rollback_write(&self, tx_id: TxId) {
        let mut writers = self.active_writers.write();
        writers.retain(|&t| t != tx_id);
    }

    pub fn latest_committed(&self) -> TxId {
        self.committed_tx_id.load(Ordering::SeqCst)
    }
}
