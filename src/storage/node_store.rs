//! DashMap-backed NodeStore with slab allocation and MVCC soft-delete.
//!
//! ## Design
//!
//! - **Lock-free slab allocation**: freed IDs are recycled via a
//!   `crossbeam::queue::SegQueue` — no mutex contention on the write path.
//!   `alloc_id` pops from the queue; when empty, bumps an atomic counter.
//! - **MVCC soft-delete**: nodes are never physically removed on write transactions.
//!   Instead, `deleted_tx` is set to the current TxId. Only transactions with
//!   TxId < deleted_tx can see the node.
//! - **Read path**: `DashMap::get()` is lock-free (sharded). No GC pause.
//! - **Write path**: fully lock-free ID allocation (SegQueue + atomic counter).
//!   DashMap internal shard locks still serialise writes to the same shard,
//!   but writes to different shards proceed in parallel.
//! - **Hard delete**: `compact()` physically removes nodes deleted before the
//!   oldest active transaction. This is called by the snapshot/garbage-collection
//!   process, not on the hot write path.

use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam::queue::SegQueue;
use dashmap::DashMap;

use super::Node;
use crate::types::{NodeId, LabelId, TxId};

/// The NodeStore holds all graph nodes in a sharded concurrent hash map.
pub struct NodeStore {
    /// Active and soft-deleted nodes, keyed by NodeId.
    nodes: DashMap<NodeId, Node>,

    /// Monotonic counter for fresh allocations (when free-list is empty).
    next_id: AtomicU64,

    /// Recycled NodeIds from hard-deleted nodes. Lock-free MPMC queue.
    free_list: SegQueue<NodeId>,

    /// Approximate count of recycled IDs in the free-list.
    free_count: AtomicU64,

    /// Total node count including soft-deleted (for statistics).
    total_ever: AtomicU64,
}

impl NodeStore {
    /// Create a new empty NodeStore.
    ///
    /// Pre-allocates capacity for 1M nodes to reduce rehashing under load.
    pub fn new() -> Self {
        Self {
            nodes: DashMap::with_capacity(1_000_000),
            next_id: AtomicU64::new(0),
            free_list: SegQueue::new(),
            free_count: AtomicU64::new(0),
            total_ever: AtomicU64::new(0),
        }
    }

    /// Create a NodeStore with a custom initial capacity.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            nodes: DashMap::with_capacity(cap),
            next_id: AtomicU64::new(0),
            free_list: SegQueue::new(),
            free_count: AtomicU64::new(0),
            total_ever: AtomicU64::new(0),
        }
    }

    // ── ID allocation (lock-free) ──────────────────────────────────

    /// Allocate a fresh NodeId. Recycles from the lock-free queue if available,
    /// otherwise bumps the atomic counter.
    #[inline]
    fn alloc_id(&self) -> NodeId {
        // Fast path: try the lock-free free-list
        if let Some(id) = self.free_list.pop() {
            self.free_count.fetch_sub(1, Ordering::Relaxed);
            return id;
        }
        // Slow path: bump counter (free-list was empty)
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    // ── CRUD ──────────────────────────────────────────────────────

    /// Insert a new node. Returns the allocated NodeId.
    pub fn insert_node(
        &self,
        labels: Vec<LabelId>,
        props_row: u32,
        tx_id: TxId,
    ) -> NodeId {
        let id = self.alloc_id();
        let node = Node::new(id, labels, props_row, tx_id);
        self.nodes.insert(id, node);
        self.total_ever.fetch_add(1, Ordering::Relaxed);
        id
    }

    /// Get a reference to a node by ID.
    ///
    /// Returns `None` if the node does not exist or has been hard-deleted.
    #[inline]
    pub fn get(&self, id: NodeId) -> Option<dashmap::mapref::one::Ref<'_, NodeId, Node>> {
        self.nodes.get(&id)
    }

    /// Get a mutable reference to a node (for updates).
    #[inline]
    pub fn get_mut(&self, id: NodeId) -> Option<dashmap::mapref::one::RefMut<'_, NodeId, Node>> {
        self.nodes.get_mut(&id)
    }

    /// Update a node's labels. Returns false if node doesn't exist.
    pub fn update_labels(&self, id: NodeId, labels: Vec<LabelId>) -> bool {
        match self.nodes.get_mut(&id) {
            Some(mut node) => {
                node.labels = labels;
                true
            }
            None => false,
        }
    }

    /// Update the node's property row pointer.
    pub fn update_props_row(&self, id: NodeId, props_row: u32) -> bool {
        match self.nodes.get_mut(&id) {
            Some(mut node) => {
                node.props_row = props_row;
                true
            }
            None => false,
        }
    }

    /// Set the outgoing edge chain head for a node.
    pub fn set_first_out(&self, id: NodeId, edge_id: crate::types::EdgeId) -> bool {
        match self.nodes.get_mut(&id) {
            Some(mut node) => {
                node.first_out = edge_id;
                true
            }
            None => false,
        }
    }

    /// Set the incoming edge chain head for a node.
    pub fn set_first_in(&self, id: NodeId, edge_id: crate::types::EdgeId) -> bool {
        match self.nodes.get_mut(&id) {
            Some(mut node) => {
                node.first_in = edge_id;
                true
            }
            None => false,
        }
    }

    /// Soft-delete a node: sets `deleted_tx` to `tx_id`.
    ///
    /// The node is not physically removed. It becomes invisible to transactions
    /// with TxId ≥ deleted_tx.
    /// Returns false if the node doesn't exist.
    pub fn soft_delete(&self, id: NodeId, tx_id: TxId) -> bool {
        match self.nodes.get_mut(&id) {
            Some(mut node) => {
                node.deleted_tx = tx_id;
                true
            }
            None => false,
        }
    }

    /// Hard-delete a node: physically removes it and pushes its ID
    /// to the lock-free free-list.
    ///
    /// Only call this during compaction when no active transaction can
    /// see this node.
    pub fn hard_delete(&self, id: NodeId) -> bool {
        if self.nodes.remove(&id).is_some() {
            self.free_list.push(id);
            self.free_count.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    // ── MVCC iteration ────────────────────────────────────────────

    /// Return all NodeIds visible to the given transaction snapshot.
    ///
    /// This scans the entire map — use sparingly (e.g., for full scans
    /// or index rebuilds).
    pub fn visible_nodes(&self, tx: TxId) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|entry| entry.value().is_alive(tx))
            .map(|entry| *entry.key())
            .collect()
    }

    /// Count of nodes visible to the given transaction.
    pub fn visible_count(&self, tx: TxId) -> usize {
        self.nodes
            .iter()
            .filter(|entry| entry.value().is_alive(tx))
            .count()
    }

    // ── Statistics ────────────────────────────────────────────────

    /// Total nodes ever created (including soft-deleted and hard-deleted).
    pub fn total_ever(&self) -> u64 {
        self.total_ever.load(Ordering::Relaxed)
    }

    /// Nodes currently in the DashMap (soft-deleted + alive, not hard-deleted).
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Approximate free-list size (recyclable IDs).
    ///
    /// This is approximate because concurrent pushes/pops make the exact count
    /// momentarily inconsistent.
    pub fn free_count(&self) -> u64 {
        self.free_count.load(Ordering::Relaxed)
    }

    /// Next fresh ID value.
    pub fn next_fresh_id(&self) -> u64 {
        self.next_id.load(Ordering::SeqCst)
    }

    // ── Compaction ────────────────────────────────────────────────

    /// Physically remove all nodes deleted before `oldest_active_tx`.
    ///
    /// Returns the number of nodes removed.
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

    /// Returns true if the node exists (even if soft-deleted).
    #[inline]
    pub fn contains(&self, id: NodeId) -> bool {
        self.nodes.contains_key(&id)
    }
}

impl Default for NodeStore {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::MAX_TX_ID;

    fn mk_store() -> NodeStore {
        NodeStore::with_capacity(64)
    }

    // ──────────────── ID Allocation ────────────────

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

        assert_eq!(n0, 0);
        assert_eq!(n1, 1);
        assert_eq!(n2, 2);

        store.hard_delete(n1);
        let n3 = store.insert_node(vec![1], 3, 2);
        assert_eq!(n3, 1); // reused

        let n4 = store.insert_node(vec![1], 4, 2);
        assert_eq!(n4, 3);
    }

    // ──────────────── Insert / Get ────────────────

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
        assert!(node.is_alive(1));
        assert!(node.is_alive(100));
    }

    #[test]
    fn test_get_missing_returns_none() {
        let store = mk_store();
        assert!(store.get(999).is_none());
    }

    #[test]
    fn test_insert_many() {
        let store = mk_store();
        for i in 0..1000 {
            let id = store.insert_node(vec![1, 2], i as u32, 1);
            assert_eq!(id, i);
        }
        assert_eq!(store.len(), 1000);
        assert_eq!(store.total_ever(), 1000);
    }

    // ──────────────── MVCC Visibility ────────────────

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
        assert!(store.get(id).unwrap().is_alive(1_000_000));
        assert!(!store.get(id).unwrap().is_alive(u64::MAX));
    }

    #[test]
    fn test_node_invisible_before_create() {
        let store = mk_store();
        let id = store.insert_node(vec![0], 0, 10);
        assert!(!store.get(id).unwrap().is_alive(9));
        assert!(!store.get(id).unwrap().is_alive(0));
    }

    #[test]
    fn test_soft_delete_makes_node_invisible() {
        let store = mk_store();
        let id = store.insert_node(vec![0], 0, 1);
        store.soft_delete(id, 5);

        let node = store.get(id).unwrap();
        assert!(node.is_alive(4));
        assert!(!node.is_alive(5));
        assert!(!node.is_alive(6));
    }

    #[test]
    fn test_soft_delete_nonexistent_returns_false() {
        let store = mk_store();
        assert!(!store.soft_delete(42, 5));
    }

    // ──────────────── Update ────────────────

    #[test]
    fn test_update_labels() {
        let store = mk_store();
        let id = store.insert_node(vec![0], 0, 1);
        assert!(store.update_labels(id, vec![42, 99]));
        assert_eq!(store.get(id).unwrap().labels, vec![42, 99]);
    }

    #[test]
    fn test_update_labels_nonexistent() {
        let store = mk_store();
        assert!(!store.update_labels(999, vec![1]));
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
        let eid = 100u64;
        assert!(store.set_first_out(id, eid));
        assert!(store.set_first_in(id, eid + 1));
        let node = store.get(id).unwrap();
        assert_eq!(node.first_out, 100);
        assert_eq!(node.first_in, 101);
    }

    // ──────────────── Delete ────────────────

    #[test]
    fn test_hard_delete_removes_node() {
        let store = mk_store();
        let id = store.insert_node(vec![0], 0, 1);
        assert!(store.contains(id));
        assert!(store.hard_delete(id));
        assert!(!store.contains(id));
        assert!(store.get(id).is_none());
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
        let old_free = store.free_count();
        store.hard_delete(id);
        assert_eq!(store.free_count(), old_free + 1);
    }

    // ──────────────── Visible Nodes ────────────────

    #[test]
    fn test_visible_nodes_respects_mvcc() {
        let store = mk_store();
        let n0 = store.insert_node(vec![0], 0, 1);
        let n1 = store.insert_node(vec![1], 1, 3);
        let n2 = store.insert_node(vec![2], 2, 5);
        store.soft_delete(n1, 10);

        let vis_tx4 = store.visible_nodes(4);
        assert!(vis_tx4.contains(&n0));
        assert!(vis_tx4.contains(&n1));
        assert!(!vis_tx4.contains(&n2));

        let vis_tx100 = store.visible_nodes(100);
        assert!(vis_tx100.contains(&n0));
        assert!(!vis_tx100.contains(&n1));
        assert!(vis_tx100.contains(&n2));
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

    // ──────────────── Compaction ────────────────

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
    fn test_compact_on_alive_nodes_does_nothing() {
        let store = mk_store();
        store.insert_node(vec![0], 0, 1);
        store.insert_node(vec![0], 1, 1);
        let removed = store.compact(100);
        assert_eq!(removed, 0);
        assert_eq!(store.len(), 2);
    }

    // ──────────────── Default ────────────────

    #[test]
    fn test_default_creates_empty_store() {
        let store = NodeStore::default();
        assert_eq!(store.len(), 0);
        assert_eq!(store.total_ever(), 0);
    }

    // ──────────────── Edge Cases ────────────────

    #[test]
    fn test_large_number_of_nodes() {
        let store = NodeStore::with_capacity(16);
        for i in 0..5000u64 {
            let id = store.insert_node(vec![(i % 10) as u32], (i % 100) as u32, 1);
            assert_eq!(id, i);
        }
        assert_eq!(store.len(), 5000);
        assert!(store.get(0).is_some());
        assert!(store.get(2500).is_some());
        assert!(store.get(4999).is_some());
        assert!(store.get(5000).is_none());
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
                    let label = (t * 1000 + i) as u32;
                    s.insert_node(vec![label], 0, 1);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(store.len(), 8000);
    }

    #[test]
    fn test_free_list_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let store = Arc::new(NodeStore::with_capacity(64));

        let mut ids = Vec::new();
        for _ in 0..100 {
            ids.push(store.insert_node(vec![0], 0, 1));
        }
        for &id in &ids {
            store.hard_delete(id);
        }

        let mut handles = Vec::new();
        for _ in 0..4 {
            let s = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                let mut ids = Vec::new();
                for _ in 0..25 {
                    ids.push(s.insert_node(vec![0], 0, 2));
                }
                ids
            }));
        }

        let mut all_new = Vec::new();
        for h in handles {
            all_new.extend(h.join().unwrap());
        }

        all_new.sort();
        assert!(all_new.iter().all(|&id| id < 100));
        all_new.dedup();
        assert_eq!(all_new.len(), 100);
    }
}
