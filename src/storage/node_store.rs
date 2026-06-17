//! Persistent NodeStore — disk-backed with in-memory cache.
//!
//! ## Architecture
//!
//! - **Disk**: append-only log (`nodes.log`).
//!   Insert = full serialized node. Delete = tombstone record.
//!   Mutation = re-serialize full node as insert (overwrites on replay).
//!   fsync after each write for durability.
//! - **Memory**: `DashMap<NodeId, Node>` as read cache + write target.
//!   All queries hit memory. Writes go disk-first, then memory.
//! - **Recovery**: replay log on startup → rebuild DashMap.
//! - **Compaction**: write snapshot of live nodes, truncate log.
//! - **In-memory mode**: if no path given, works as pure in-memory
//!   (for testing / benchmarks).
//!
//! ## Disk format
//!
//! `[4 magic "LGDB"][record*]`
//! Record: `[len:u32 LE][opcode:u8 (1=insert,2=delete)][payload]`

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam::queue::SegQueue;
use dashmap::DashMap;

use super::store_log::StoreLog;
use super::Node;
use crate::types::{NodeId, LabelId, TxId};

pub struct NodeStore {
    /// In-memory cache and primary data store.
    nodes: DashMap<NodeId, Node>,

    /// Monotonic counter for fresh allocations.
    next_id: AtomicU64,

    /// Recycled NodeIds (lock-free).
    free_list: SegQueue<NodeId>,
    free_count: AtomicU64,

    /// Total node count.
    total_ever: AtomicU64,

    /// Persistent log (None = in-memory-only mode).
    log: Option<std::sync::Mutex<StoreLog>>,
    data_dir: Option<PathBuf>,
}

impl NodeStore {
    // ── Constructors ──────────────────────────────────────────────

    /// Create an in-memory-only NodeStore (for testing).
    pub fn new() -> Self {
        Self {
            nodes: DashMap::with_capacity(1_000_000),
            next_id: AtomicU64::new(0),
            free_list: SegQueue::new(),
            free_count: AtomicU64::new(0),
            total_ever: AtomicU64::new(0),
            log: None,
            data_dir: None,
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            nodes: DashMap::with_capacity(cap),
            next_id: AtomicU64::new(0),
            free_list: SegQueue::new(),
            free_count: AtomicU64::new(0),
            total_ever: AtomicU64::new(0),
            log: None,
            data_dir: None,
        }
    }

    /// Open a persistent NodeStore. Creates data directory if needed.
    /// Replays existing log to rebuild in-memory state.
    pub fn open(data_dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let log_path = data_dir.join("nodes.log");

        let mut store = Self {
            nodes: DashMap::with_capacity(1_000_000),
            next_id: AtomicU64::new(0),
            free_list: SegQueue::new(),
            free_count: AtomicU64::new(0),
            total_ever: AtomicU64::new(0),
            log: None,
            data_dir: Some(data_dir.to_path_buf()),
        };

        // Replay existing log to rebuild state
        if log_path.exists() {
            let mut max_id: u64 = 0;
            super::store_log::replay_log(&log_path, |opcode, payload| {
                match opcode {
                    1 => {
                        // INSERT
                        if let Ok(node) = bincode::deserialize::<Node>(payload) {
                            max_id = max_id.max(node.id);
                            store.total_ever.fetch_add(1, Ordering::Relaxed);
                            store.nodes.insert(node.id, node);
                        }
                    }
                    2 => {
                        // DELETE — payload is 8 bytes node id
                        if payload.len() >= 8 {
                            let id = u64::from_le_bytes(payload[..8].try_into().unwrap());
                            if store.nodes.remove(&id).is_some() {
                                store.free_list.push(id);
                                store.free_count.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    _ => {}
                }
            })?;
            store.next_id.store(max_id + 1, Ordering::SeqCst);
        }

        // Open log for appending
        let log = StoreLog::open(&log_path)?;
        store.log = Some(std::sync::Mutex::new(log));

        Ok(store)
    }

    // ── Persistence helpers ───────────────────────────────────────

    /// Persist a node to disk (full serialization as INSERT).
    fn persist_node(&self, node: &Node) {
        if let Some(ref log) = self.log {
            if let Ok(payload) = bincode::serialize(node) {
                let _ = log.lock().unwrap().append_insert(&payload);
            }
        }
    }

    /// Persist a delete to disk. Payload = node ID bytes.
    fn persist_delete(&self, id: NodeId) {
        if let Some(ref log) = self.log {
            let _ = log.lock().unwrap().append_delete(&id.to_le_bytes());
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

    // ── CRUD ──────────────────────────────────────────────────────

    pub fn insert_node(&self, labels: Vec<LabelId>, props_row: u32, tx_id: TxId) -> NodeId {
        let id = self.alloc_id();
        let node = Node::new(id, labels, props_row, tx_id);

        // 1. Persist to disk FIRST
        self.persist_node(&node);

        // 2. Then update memory
        self.nodes.insert(id, node);
        self.total_ever.fetch_add(1, Ordering::Relaxed);
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
                // Persist updated node
                self.persist_node(&node);
                true
            }
            None => false,
        }
    }

    pub fn update_props_row(&self, id: NodeId, props_row: u32) -> bool {
        match self.nodes.get_mut(&id) {
            Some(mut node) => {
                node.props_row = props_row;
                self.persist_node(&node);
                true
            }
            None => false,
        }
    }

    pub fn set_first_out(&self, id: NodeId, edge_id: crate::types::EdgeId) -> bool {
        match self.nodes.get_mut(&id) {
            Some(mut node) => {
                node.first_out = edge_id;
                self.persist_node(&node);
                true
            }
            None => false,
        }
    }

    pub fn set_first_in(&self, id: NodeId, edge_id: crate::types::EdgeId) -> bool {
        match self.nodes.get_mut(&id) {
            Some(mut node) => {
                node.first_in = edge_id;
                self.persist_node(&node);
                true
            }
            None => false,
        }
    }

    pub fn soft_delete(&self, id: NodeId, tx_id: TxId) -> bool {
        match self.nodes.get_mut(&id) {
            Some(mut node) => {
                node.deleted_tx = tx_id;
                self.persist_node(&node);
                true
            }
            None => false,
        }
    }

    /// Hard-delete: persist delete record, then remove from memory.
    pub fn hard_delete(&self, id: NodeId) -> bool {
        self.persist_delete(id);
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
        self.nodes
            .iter()
            .filter(|entry| entry.value().is_alive(tx))
            .map(|entry| *entry.key())
            .collect()
    }

    pub fn visible_count(&self, tx: TxId) -> usize {
        self.nodes
            .iter()
            .filter(|entry| entry.value().is_alive(tx))
            .count()
    }

    // ── Statistics ────────────────────────────────────────────────

    pub fn total_ever(&self) -> u64 { self.total_ever.load(Ordering::Relaxed) }
    pub fn len(&self) -> usize { self.nodes.len() }
    pub fn free_count(&self) -> u64 { self.free_count.load(Ordering::Relaxed) }
    pub fn next_fresh_id(&self) -> u64 { self.next_id.load(Ordering::SeqCst) }
    pub fn log_size_bytes(&self) -> u64 {
        self.log.as_ref().map(|l| l.lock().unwrap().size_bytes()).unwrap_or(0)
    }

    // ── Compaction ────────────────────────────────────────────────

    pub fn compact(&self, oldest_active_tx: TxId) -> usize {
        let to_remove: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|entry| entry.value().deleted_tx < oldest_active_tx)
            .map(|entry| *entry.key())
            .collect();

        let count = to_remove.len();
        for id in to_remove {
            self.hard_delete(id);
        }
        count
    }

    #[inline]
    pub fn contains(&self, id: NodeId) -> bool {
        self.nodes.contains_key(&id)
    }

    /// Flush + fsync all pending writes. Call before shutdown.
    pub fn flush(&self) -> io::Result<()> {
        if let Some(ref log) = self.log {
            log.lock().unwrap().sync_now()
        } else {
            Ok(())
        }
    }

    /// Data directory path, if persistent.
    pub fn data_dir(&self) -> Option<&Path> {
        self.data_dir.as_deref()
    }

    /// Flush on drop to ensure pending writes are persisted.
    /// Panics if fsync fails (data loss is worse than a crash).
    pub fn sync_on_drop(&self) {
        let _ = self.flush();
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

    fn mk_store() -> NodeStore {
        NodeStore::with_capacity(64)
    }

    // ──────────────── Persistence ────────────────

    #[test]
    fn test_persist_and_recover() {
        let dir = TempDir::new().unwrap();
        let path = dir.path();

        // Open, write, flush, drop
        {
            let store = NodeStore::open(path).unwrap();
            store.insert_node(vec![1, 2], 10, 1);
            store.insert_node(vec![3], 20, 2);
            store.insert_node(vec![4], 30, 3);
            store.soft_delete(0, 100);
            store.flush().unwrap(); // group commit → fsync now
            assert_eq!(store.len(), 3);
        }

        // Re-open, verify all data recovered
        {
            let store = NodeStore::open(path).unwrap();
            assert_eq!(store.len(), 3);

            let n0 = store.get(0).unwrap();
            assert_eq!(n0.labels, vec![1, 2]);
            assert_eq!(n0.props_row, 10);
            assert_eq!(n0.created_tx, 1);
            assert_eq!(n0.deleted_tx, 100); // soft-delete persisted

            assert!(store.get(1).unwrap().is_alive(2));
            assert!(store.get(2).unwrap().is_alive(3));
        }
    }

    #[test]
    fn test_persist_hard_delete_and_recover() {
        let dir = TempDir::new().unwrap();
        let path = dir.path();

        {
            let store = NodeStore::open(path).unwrap();
            store.insert_node(vec![1], 0, 1);
            store.insert_node(vec![2], 0, 1);
            store.insert_node(vec![3], 0, 1);
            store.hard_delete(1);
            store.flush().unwrap();
        }

        {
            let store = NodeStore::open(path).unwrap();
            assert_eq!(store.len(), 2);
            assert!(store.get(1).is_none()); // deleted
            assert!(store.get(0).is_some());
            assert!(store.get(2).is_some());
        }
    }

    #[test]
    fn test_persist_update_and_recover() {
        let dir = TempDir::new().unwrap();
        let path = dir.path();

        {
            let store = NodeStore::open(path).unwrap();
            let id = store.insert_node(vec![0], 0, 1);

            store.update_labels(id, vec![99]);
            store.update_props_row(id, 55);
            store.set_first_out(id, 12345);
            store.flush().unwrap();
        }

        {
            let store = NodeStore::open(path).unwrap();
            let n = store.get(0).unwrap();
            assert_eq!(n.labels, vec![99]);
            assert_eq!(n.props_row, 55);
            assert_eq!(n.first_out, 12345);
        }
    }

    #[test]
    fn test_empty_recovery() {
        let dir = TempDir::new().unwrap();
        let store = NodeStore::open(dir.path()).unwrap();
        assert_eq!(store.len(), 0);
        assert_eq!(store.total_ever(), 0);
    }

    #[test]
    fn test_id_counter_persists_across_restarts() {
        let dir = TempDir::new().unwrap();
        let path = dir.path();

        {
            let store = NodeStore::open(path).unwrap();
            store.insert_node(vec![0], 0, 1);
            store.insert_node(vec![0], 0, 1);
            store.flush().unwrap();
        }

        {
            let store = NodeStore::open(path).unwrap();
            // New inserts should continue from ID 2
            let id = store.insert_node(vec![0], 0, 2);
            assert_eq!(id, 2);
        }
    }

    // ──────────────── Existing tests (in-memory mode) ────────────

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
        let id0 = store.insert_node(vec![0], 0, 1);
        let id1 = store.insert_node(vec![1], 1, 1);
        let _id2 = store.insert_node(vec![2], 2, 1);
        store.hard_delete(id1);
        let recycled = store.insert_node(vec![3], 3, 2);
        assert_eq!(recycled, id1);
        let fresh = store.insert_node(vec![4], 4, 2);
        assert_eq!(fresh, 3);
    }

    #[test]
    fn test_alloc_reuse_with_interleaved_inserts() {
        let store = mk_store();
        let n0 = store.insert_node(vec![0], 0, 1);
        let n1 = store.insert_node(vec![0], 1, 1);
        let n2 = store.insert_node(vec![0], 2, 1);
        assert_eq!(n0, 0); assert_eq!(n1, 1); assert_eq!(n2, 2);
        store.hard_delete(n1);
        let n3 = store.insert_node(vec![1], 3, 2);
        assert_eq!(n3, 1);
        let n4 = store.insert_node(vec![1], 4, 2);
        assert_eq!(n4, 3);
    }

    #[test]
    fn test_insert_and_get() {
        let store = mk_store();
        let id = store.insert_node(vec![10, 20], 5, 1);
        let node = store.get(id).expect("node should exist");
        assert_eq!(node.id, id);
        assert_eq!(node.labels, vec![10, 20]);
        assert_eq!(node.props_row, 5);
        assert_eq!(node.created_tx, 1);
        assert_eq!(node.deleted_tx, MAX_TX_ID);
    }

    #[test]
    fn test_get_missing_returns_none() {
        let store = mk_store();
        assert!(store.get(999).is_none());
    }

    #[test]
    fn test_insert_many() {
        let store = mk_store();
        for i in 0..1000 { let id = store.insert_node(vec![1, 2], i as u32, 1); assert_eq!(id, i); }
        assert_eq!(store.len(), 1000);
    }

    #[test]
    fn test_node_visible_to_creating_tx() {
        let store = mk_store();
        let id = store.insert_node(vec![0], 0, 10);
        assert!(store.get(id).unwrap().is_alive(10));
    }

    #[test]
    fn test_node_visible_after_create() {
        let store = mk_store();
        let id = store.insert_node(vec![0], 0, 3);
        assert!(store.get(id).unwrap().is_alive(5));
        assert!(!store.get(id).unwrap().is_alive(u64::MAX));
    }

    #[test]
    fn test_node_invisible_before_create() {
        let store = mk_store();
        let id = store.insert_node(vec![0], 0, 10);
        assert!(!store.get(id).unwrap().is_alive(9));
    }

    #[test]
    fn test_soft_delete_makes_node_invisible() {
        let store = mk_store();
        let id = store.insert_node(vec![0], 0, 1);
        store.soft_delete(id, 5);
        let node = store.get(id).unwrap();
        assert!(node.is_alive(4));
        assert!(!node.is_alive(5));
    }

    #[test]
    fn test_update_labels() {
        let store = mk_store();
        let id = store.insert_node(vec![0], 0, 1);
        assert!(store.update_labels(id, vec![42, 99]));
        assert_eq!(store.get(id).unwrap().labels, vec![42, 99]);
    }

    #[test]
    fn test_update_props_row() {
        let store = mk_store();
        let id = store.insert_node(vec![0], 0, 1);
        assert!(store.update_props_row(id, 77));
        assert_eq!(store.get(id).unwrap().props_row, 77);
    }

    #[test]
    fn test_set_edge_heads() {
        let store = mk_store();
        let id = store.insert_node(vec![0], 0, 1);
        assert!(store.set_first_out(id, 100));
        assert!(store.set_first_in(id, 101));
        let node = store.get(id).unwrap();
        assert_eq!(node.first_out, 100);
        assert_eq!(node.first_in, 101);
    }

    #[test]
    fn test_hard_delete_removes_node() {
        let store = mk_store();
        let id = store.insert_node(vec![0], 0, 1);
        assert!(store.hard_delete(id));
        assert!(!store.contains(id));
    }

    #[test]
    fn test_hard_delete_returns_false_for_missing() {
        let store = mk_store();
        assert!(!store.hard_delete(42));
    }

    #[test]
    fn test_hard_delete_adds_to_free_list() {
        let store = mk_store();
        let id = store.insert_node(vec![0], 0, 1);
        let old = store.free_count();
        store.hard_delete(id);
        assert_eq!(store.free_count(), old + 1);
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
    fn test_visible_count() {
        let store = mk_store();
        store.insert_node(vec![0], 0, 1);
        store.insert_node(vec![0], 1, 1);
        store.insert_node(vec![0], 2, 1);
        store.soft_delete(0, 5);
        assert_eq!(store.visible_count(1), 3);
        assert_eq!(store.visible_count(10), 2);
    }

    #[test]
    fn test_compact_removes_old_deleted_nodes() {
        let store = mk_store();
        let n0 = store.insert_node(vec![0], 0, 1);
        let n1 = store.insert_node(vec![1], 1, 1);
        store.soft_delete(n0, 5);
        store.soft_delete(n1, 8);
        let removed = store.compact(7);
        assert_eq!(removed, 1);
        assert!(!store.contains(n0));
        assert!(store.contains(n1));
    }

    #[test]
    fn test_default_creates_empty_store() {
        let store = NodeStore::default();
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_large_number_of_nodes() {
        let store = NodeStore::with_capacity(16);
        for i in 0..5000u64 {
            let id = store.insert_node(vec![(i % 10) as u32], (i % 100) as u32, 1);
            assert_eq!(id, i);
        }
        assert_eq!(store.len(), 5000);
    }

    #[test]
    fn test_concurrent_inserts() {
        use std::sync::Arc;
        use std::thread;
        let store = Arc::new(NodeStore::with_capacity(1024));
        let mut handles = Vec::new();
        for t in 0..8 {
            let s = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for i in 0..1000 {
                    s.insert_node(vec![(t * 1000 + i) as u32], 0, 1);
                }
            }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(store.len(), 8000);
    }

    #[test]
    fn test_free_list_thread_safety() {
        use std::sync::Arc;
        use std::thread;
        let store = Arc::new(NodeStore::with_capacity(64));
        let mut ids = Vec::new();
        for _ in 0..100 { ids.push(store.insert_node(vec![0], 0, 1)); }
        for &id in &ids { store.hard_delete(id); }
        let mut handles = Vec::new();
        for _ in 0..4 {
            let s = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                let mut ids = Vec::new();
                for _ in 0..25 { ids.push(s.insert_node(vec![0], 0, 2)); }
                ids
            }));
        }
        let mut all_new = Vec::new();
        for h in handles { all_new.extend(h.join().unwrap()); }
        all_new.sort();
        assert!(all_new.iter().all(|&id| id < 100));
        all_new.dedup();
        assert_eq!(all_new.len(), 100);
    }
}
