//! Persistent NodeStore — DashMap cache + dedicated WAL thread.
//!
//! ## Architecture
//!
//! ```
//! Business threads (N)          WAL Thread (1)
//! ┌──────────────────┐         ┌──────────────────────┐
//! │ DashMap::insert   │         │ recv(channel)         │
//! │ wal.send_insert ──┼────────►│ batch serialize       │
//! │ return immediately│         │ append to wal.log     │
//! └──────────────────┘         │ group fsync (64KB/5ms)│
//!                               └──────────────────────┘
//! ```
//!
//! - Business threads never touch disk.
//! - Write latency = DashMap insert + channel send ≈ nanoseconds.
//! - Durability: `flush()` drains channel → fsync. Call on tx commit.
//! - Recovery: `replay_wal()` → rebuild DashMap.
//! - In-memory mode: if no path given, no WAL thread.

use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam::queue::SegQueue;
use dashmap::DashMap;

use super::consistency::Consistency;
use super::wal_thread::WalThread;
use super::Node;
use crate::types::{NodeId, LabelId, TxId};

pub struct NodeStore {
    nodes: DashMap<NodeId, Node>,
    next_id: AtomicU64,
    free_list: SegQueue<NodeId>,
    free_count: AtomicU64,
    total_ever: AtomicU64,
    wal: Option<WalThread>,
}

impl NodeStore {
    // ── Constructors ──────────────────────────────────────────────

    pub fn new() -> Self {
        Self {
            nodes: DashMap::with_capacity(1_000_000),
            next_id: AtomicU64::new(0),
            free_list: SegQueue::new(),
            free_count: AtomicU64::new(0),
            total_ever: AtomicU64::new(0),
            wal: None,
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            nodes: DashMap::with_capacity(cap),
            next_id: AtomicU64::new(0),
            free_list: SegQueue::new(),
            free_count: AtomicU64::new(0),
            total_ever: AtomicU64::new(0),
            wal: None,
        }
    }

    /// Open with an explicit consistency contract.
    ///
    /// ```ignore
    /// use lightgraph::storage::consistency::Consistency;
    /// let store = NodeStore::open(path, Consistency::balanced())?;
    /// let store = NodeStore::open(path, Consistency::immediate())?;
    /// ```
    pub fn open(data_dir: &Path, consistency: Consistency) -> io::Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let wal_path = data_dir.join("nodes.wal");

        let mut store = Self {
            nodes: DashMap::with_capacity(1_000_000),
            next_id: AtomicU64::new(0),
            free_list: SegQueue::new(),
            free_count: AtomicU64::new(0),
            total_ever: AtomicU64::new(0),
            wal: None,
        };

        // Replay WAL to rebuild state
        if wal_path.exists() && std::fs::metadata(&wal_path)?.len() > 4 {
            let mut max_id: u64 = 0;
            super::wal_thread::replay_wal(&wal_path, |opcode, id, payload| {
                match opcode {
                    1 => {
                        // INSERT
                        if let Ok(node) = bincode::deserialize::<Node>(payload) {
                            max_id = max_id.max(id);
                            store.total_ever.fetch_add(1, Ordering::Relaxed);
                            store.nodes.insert(id, node);
                        }
                    }
                    2 => {
                        // DELETE
                        if store.nodes.remove(&id).is_some() {
                            store.free_list.push(id);
                            store.free_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    _ => {}
                }
            })?;
            store.next_id.store(max_id + 1, Ordering::SeqCst);
        }

        // Spawn WAL thread with the chosen durability contract
        store.wal = Some(WalThread::spawn(
            &data_dir.join("nodes.wal"),
            consistency.durability,
            consistency.wal_channel_capacity,
        )?);

        Ok(store)
    }

    // ── WAL helpers ──────────────────────────────────────────────

    fn wal_insert(&self, id: NodeId, node: &Node) {
        if let Some(ref wal) = self.wal {
            if let Ok(data) = bincode::serialize(node) {
                wal.send_insert(id, data);
            }
        }
    }

    fn wal_delete(&self, id: NodeId) {
        if let Some(ref wal) = self.wal {
            wal.send_delete(id);
        }
    }

    // ── ID allocation ────────────────────────────────────────────

    #[inline]
    fn alloc_id(&self) -> NodeId {
        if let Some(id) = self.free_list.pop() {
            self.free_count.fetch_sub(1, Ordering::Relaxed);
            return id;
        }
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Allocate an ID without inserting (for transaction buffering).
    pub fn alloc_id_direct(&self) -> NodeId {
        self.alloc_id()
    }

    /// Insert with a pre-allocated ID (called by Transaction::commit).
    pub fn insert_with_id(&self, id: NodeId, labels: Vec<LabelId>, props_row: u32, tx_id: TxId) {
        let node = Node::new(id, labels, props_row, tx_id);
        self.nodes.insert(id, node.clone());
        self.total_ever.fetch_add(1, Ordering::Relaxed);
        self.wal_insert(id, &node);
    }

    // ── CRUD ──────────────────────────────────────────────────────

    pub fn insert_node(&self, labels: Vec<LabelId>, props_row: u32, tx_id: TxId) -> NodeId {
        let id = self.alloc_id();
        let node = Node::new(id, labels, props_row, tx_id);

        // 1. Insert to memory
        self.nodes.insert(id, node.clone());
        self.total_ever.fetch_add(1, Ordering::Relaxed);

        // 2. Send to WAL (non-blocking)
        self.wal_insert(id, &node);
        id
    }

    #[inline]
    pub fn get(&self, id: NodeId) -> Option<dashmap::mapref::one::Ref<'_, NodeId, Node>> {
        self.nodes.get(&id)
    }

    #[inline]
    pub fn get_mut(&self, id: NodeId) -> Option<dashmap::mapref::one::RefMut<'_, NodeId, Node>> {
        self.nodes.get_mut(&id)
    }

    pub fn update_labels(&self, id: NodeId, labels: Vec<LabelId>) -> bool {
        match self.nodes.get_mut(&id) {
            Some(mut node) => {
                node.labels = labels;
                // Re-serialize full node to WAL
                if let Some(ref wal) = self.wal {
                    if let Ok(data) = bincode::serialize(&*node) {
                        wal.send_insert(id, data);
                    }
                }
                true
            }
            None => false,
        }
    }

    pub fn update_props_row(&self, id: NodeId, props_row: u32) -> bool {
        match self.nodes.get_mut(&id) {
            Some(mut node) => {
                node.props_row = props_row;
                self.wal_insert(id, &node);
                true
            }
            None => false,
        }
    }

    pub fn set_first_out(&self, id: NodeId, edge_id: crate::types::EdgeId) -> bool {
        match self.nodes.get_mut(&id) {
            Some(mut node) => {
                node.first_out = edge_id;
                self.wal_insert(id, &node);
                true
            }
            None => false,
        }
    }

    pub fn set_first_in(&self, id: NodeId, edge_id: crate::types::EdgeId) -> bool {
        match self.nodes.get_mut(&id) {
            Some(mut node) => {
                node.first_in = edge_id;
                self.wal_insert(id, &node);
                true
            }
            None => false,
        }
    }

    pub fn soft_delete(&self, id: NodeId, tx_id: TxId) -> bool {
        match self.nodes.get_mut(&id) {
            Some(mut node) => {
                node.deleted_tx = tx_id;
                self.wal_insert(id, &node);
                true
            }
            None => false,
        }
    }

    /// Hard-delete: remove from memory → send delete to WAL.
    pub fn hard_delete(&self, id: NodeId) -> bool {
        self.wal_delete(id);
        if self.nodes.remove(&id).is_some() {
            self.free_list.push(id);
            self.free_count.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    // ── MVCC ──────────────────────────────────────────────────────

    pub fn visible_nodes(&self, tx: TxId) -> Vec<NodeId> {
        self.nodes.iter()
            .filter(|e| e.value().is_alive(tx))
            .map(|e| *e.key())
            .collect()
    }

    pub fn visible_count(&self, tx: TxId) -> usize {
        self.nodes.iter().filter(|e| e.value().is_alive(tx)).count()
    }

    // ── Statistics ────────────────────────────────────────────────

    pub fn total_ever(&self) -> u64 { self.total_ever.load(Ordering::Relaxed) }
    pub fn len(&self) -> usize { self.nodes.len() }
    pub fn free_count(&self) -> u64 { self.free_count.load(Ordering::Relaxed) }
    pub fn next_fresh_id(&self) -> u64 { self.next_id.load(Ordering::SeqCst) }
    pub fn pending_wal_bytes(&self) -> u64 {
        self.wal.as_ref().map(|w| w.pending_bytes()).unwrap_or(0)
    }

    // ── Compaction ────────────────────────────────────────────────

    pub fn compact(&self, oldest_active_tx: TxId) -> usize {
        let to_remove: Vec<NodeId> = self.nodes.iter()
            .filter(|e| e.value().deleted_tx < oldest_active_tx)
            .map(|e| *e.key())
            .collect();
        let n = to_remove.len();
        for id in to_remove { self.hard_delete(id); }
        n
    }

    #[inline]
    pub fn contains(&self, id: NodeId) -> bool { self.nodes.contains_key(&id) }

    /// Flush: block until all pending WAL commands are synced to disk.
    pub fn flush(&self) {
        if let Some(ref wal) = self.wal {
            wal.flush();
        }
    }
}

impl Default for NodeStore {
    fn default() -> Self { Self::new() }
}

// ── Tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::MAX_TX_ID;
    use tempfile::TempDir;

    fn mk_store() -> NodeStore { NodeStore::with_capacity(64) }

    // ── Persistence with WAL thread ─────────────────────────────

    #[test]
    fn test_persist_and_recover() {
        let dir = TempDir::new().unwrap();
        let path = dir.path();

        {
            let store = NodeStore::open(path, Consistency::immediate()).unwrap();
            store.insert_node(vec![1, 2], 10, 1);
            store.insert_node(vec![3], 20, 2);
            store.insert_node(vec![4], 30, 3);
            store.soft_delete(0, 100);
            store.flush(); // sync to disk
            assert_eq!(store.len(), 3);
        }

        {
            let store = NodeStore::open(path, Consistency::immediate()).unwrap();
            assert_eq!(store.len(), 3);
            let n0 = store.get(0).unwrap();
            assert_eq!(n0.labels, vec![1, 2]);
            assert_eq!(n0.props_row, 10);
            assert_eq!(n0.deleted_tx, 100);
        }
    }

    #[test]
    fn test_persist_hard_delete_and_recover() {
        let dir = TempDir::new().unwrap();
        let path = dir.path();

        {
            let store = NodeStore::open(path, Consistency::immediate()).unwrap();
            store.insert_node(vec![1], 0, 1);
            store.insert_node(vec![2], 0, 1);
            store.insert_node(vec![3], 0, 1);
            store.hard_delete(1);
            store.flush();
        }

        {
            let store = NodeStore::open(path, Consistency::immediate()).unwrap();
            assert_eq!(store.len(), 2);
            assert!(store.get(1).is_none());
            assert!(store.get(0).is_some());
            assert!(store.get(2).is_some());
        }
    }

    #[test]
    fn test_persist_update_and_recover() {
        let dir = TempDir::new().unwrap();
        let path = dir.path();

        {
            let store = NodeStore::open(path, Consistency::immediate()).unwrap();
            let id = store.insert_node(vec![0], 0, 1);
            store.update_labels(id, vec![99]);
            store.update_props_row(id, 55);
            store.set_first_out(id, 12345);
            store.flush();
        }

        {
            let store = NodeStore::open(path, Consistency::immediate()).unwrap();
            let n = store.get(0).unwrap();
            assert_eq!(n.labels, vec![99]);
            assert_eq!(n.props_row, 55);
            assert_eq!(n.first_out, 12345);
        }
    }

    // ── In-memory tests (unchanged) ─────────────────────────────

    #[test]
    fn test_alloc_monotonic() {
        let store = mk_store();
        assert_eq!(store.alloc_id(), 0);
        assert_eq!(store.alloc_id(), 1);
        assert_eq!(store.alloc_id(), 2);
    }

    #[test]
    fn test_alloc_reuse_after_hard_delete() {
        let store = mk_store();
        let _id0 = store.insert_node(vec![0], 0, 1);
        let id1 = store.insert_node(vec![1], 1, 1);
        let _id2 = store.insert_node(vec![2], 2, 1);
        store.hard_delete(id1);
        assert_eq!(store.insert_node(vec![3], 3, 2), id1);
        assert_eq!(store.insert_node(vec![4], 4, 2), 3);
    }

    #[test]
    fn test_insert_and_get() {
        let store = mk_store();
        let id = store.insert_node(vec![10, 20], 5, 1);
        let n = store.get(id).unwrap();
        assert_eq!(n.labels, vec![10, 20]);
        assert_eq!(n.deleted_tx, MAX_TX_ID);
    }

    #[test]
    fn test_soft_delete_makes_node_invisible() {
        let store = mk_store();
        let id = store.insert_node(vec![0], 0, 1);
        store.soft_delete(id, 5);
        assert!(store.get(id).unwrap().is_alive(4));
        assert!(!store.get(id).unwrap().is_alive(5));
    }

    #[test]
    fn test_hard_delete_removes_node() {
        let store = mk_store();
        let id = store.insert_node(vec![0], 0, 1);
        assert!(store.hard_delete(id));
        assert!(!store.contains(id));
    }

    #[test]
    fn test_visible_nodes_respects_mvcc() {
        let store = mk_store();
        let n0 = store.insert_node(vec![0], 0, 1);
        let n1 = store.insert_node(vec![1], 1, 3);
        let n2 = store.insert_node(vec![2], 2, 5);
        store.soft_delete(n1, 10);
        let vis = store.visible_nodes(4);
        assert!(vis.contains(&n0));
        assert!(vis.contains(&n1));
        assert!(!vis.contains(&n2));
    }

    #[test]
    fn test_concurrent_inserts() {
        use std::sync::Arc;
        use std::thread;
        let store = Arc::new(NodeStore::with_capacity(1024));
        let mut hs = Vec::new();
        for t in 0..8 {
            let s = Arc::clone(&store);
            hs.push(thread::spawn(move || {
                for i in 0..1000 {
                    s.insert_node(vec![(t*1000+i) as u32], 0, 1);
                }
            }));
        }
        for h in hs { h.join().unwrap(); }
        assert_eq!(store.len(), 8000);
    }

    // ── Two-step insert (for transaction layer) ──────────────────────

    #[test]
    fn test_alloc_id_direct_and_insert_with_id() {
        let store = mk_store();
        // Step 1: allocate ID (e.g. during transaction buffering)
        let id = store.alloc_id_direct();
        assert_eq!(id, 0);
        // Step 2: insert with pre-allocated ID (on commit)
        store.insert_with_id(id, vec![10], 99, 5);
        let n = store.get(id).unwrap();
        assert_eq!(n.labels, vec![10]);
        assert_eq!(n.props_row, 99);
        assert_eq!(n.created_tx, 5);
        assert!(store.contains(id));
    }

    #[test]
    fn test_alloc_id_direct_reuses_freed_ids() {
        let store = mk_store();
        let a = store.insert_node(vec![0], 0, 1);
        let b = store.insert_node(vec![0], 0, 1);
        store.hard_delete(a);
        store.hard_delete(b);
        // alloc_id_direct should recycle a's ID
        let recycled = store.alloc_id_direct();
        assert_eq!(recycled, a);
        store.insert_with_id(recycled, vec![1], 0, 2);
        assert!(store.contains(recycled));
    }

    // ── visible_count ────────────────────────────────────────────────

    #[test]
    fn test_visible_count_respects_mvcc() {
        let store = mk_store();
        let n0 = store.insert_node(vec![0], 0, 1);
        let _n1 = store.insert_node(vec![1], 1, 3);
        let _n2 = store.insert_node(vec![2], 2, 5);
        store.soft_delete(n0, 10);
        // tx=4: n0 visible (created 1, deleted 10), n1 visible (created 3, no delete)
        // n2 not visible (created 5 > 4)
        assert_eq!(store.visible_count(4), 2);
        // tx=6: n0 visible, n1 visible, n2 visible (created 5)
        assert_eq!(store.visible_count(6), 3);
        // tx=10: n0 dead (deleted at 10), n1 + n2 visible
        assert_eq!(store.visible_count(10), 2);
        // tx=u64::MAX-1: same as tx=10 since deleted_tx for alive nodes is MAX_TX_ID (=u64::MAX)
        assert_eq!(store.visible_count(MAX_TX_ID - 1), 2);
    }

    // ── Stats ────────────────────────────────────────────────────────

    #[test]
    fn test_stats_accuracy() {
        let store = mk_store();
        assert_eq!(store.total_ever(), 0);
        assert_eq!(store.len(), 0);
        assert_eq!(store.next_fresh_id(), 0);
        assert_eq!(store.free_count(), 0);

        let id0 = store.insert_node(vec![0], 0, 1);
        assert_eq!(store.total_ever(), 1);
        assert_eq!(store.len(), 1);
        assert_eq!(store.next_fresh_id(), 1);

        store.insert_node(vec![0], 0, 1);
        assert_eq!(store.total_ever(), 2);
        assert_eq!(store.len(), 2);
        assert_eq!(store.next_fresh_id(), 2);

        store.hard_delete(id0);
        assert_eq!(store.total_ever(), 2); // total_ever never decreases
        assert_eq!(store.len(), 1);
        assert_eq!(store.free_count(), 1);
        // next_fresh_id still 2 — freed IDs don't advance it
        assert_eq!(store.next_fresh_id(), 2);
    }

    // ── In-memory mode (no WAL) ────────────────────────────────────

    #[test]
    fn test_in_memory_mode_no_wal() {
        let store = mk_store();
        // with_capacity creates a store without WAL — operations should not panic
        let id = store.insert_node(vec![0], 0, 1);
        assert!(store.contains(id));
        store.update_labels(id, vec![1, 2]);
        store.update_props_row(id, 42);
        store.set_first_out(id, 100);
        store.set_first_in(id, 200);
        store.soft_delete(id, 10);
        assert!(!store.get(id).unwrap().is_alive(10));
        store.hard_delete(id);
        assert!(!store.contains(id));
        // flush is a no-op without WAL — must not panic
        store.flush();
        // stats still work
        assert_eq!(store.pending_wal_bytes(), 0);
    }
}
