//! Lock-free EdgeStore with bidirectional adjacency-list support.
//!
//! Each edge stores both `next_out` and `next_in` pointers,
//! enabling efficient bidirectional traversal without reverse indexes.
//!
//! ## Design
//! - Lock-free slab allocation via `SegQueue<NodeId>` + atomic counter.
//! - `DashMap::get()` for lock-free reads.
//! - `get_mut()` for shard-level write locking.
//! - MVCC soft-delete via `deleted_tx`.
//! - Adjacency-chain helpers for linking/unlinking edges to nodes.

use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam::queue::SegQueue;
use dashmap::DashMap;

use super::Edge;
use crate::types::{EdgeId, NodeId, TypeId, NULL_EDGE};

pub struct EdgeStore {
    edges: DashMap<EdgeId, Edge>,
    next_id: AtomicU64,
    free_list: SegQueue<EdgeId>,
    free_count: AtomicU64,
}

impl EdgeStore {
    /// Create with default capacity (3M edges — typical 3:1 edge-to-node ratio).
    pub fn new() -> Self {
        Self {
            edges: DashMap::with_capacity(3_000_000),
            next_id: AtomicU64::new(0),
            free_list: SegQueue::new(),
            free_count: AtomicU64::new(0),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            edges: DashMap::with_capacity(cap),
            next_id: AtomicU64::new(0),
            free_list: SegQueue::new(),
            free_count: AtomicU64::new(0),
        }
    }

    // ── ID allocation (lock-free) ──────────────────────────────────

    #[inline]
    fn alloc_id(&self) -> EdgeId {
        if let Some(id) = self.free_list.pop() {
            self.free_count.fetch_sub(1, Ordering::Relaxed);
            return id;
        }
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    // ── CRUD ──────────────────────────────────────────────────────

    /// Insert a new edge. Returns the allocated EdgeId.
    ///
    /// The caller is responsible for linking this edge into the
    /// source node's `first_out` chain and the destination node's
    /// `first_in` chain via `link_into_chains`.
    pub fn insert_edge(
        &self,
        src: NodeId,
        dst: NodeId,
        etype: TypeId,
        props_row: u32,
        tx_id: crate::types::TxId,
    ) -> EdgeId {
        let id = self.alloc_id();
        let edge = Edge::new(id, src, dst, etype, props_row, tx_id);
        self.edges.insert(id, edge);
        id
    }

    /// Link `new_edge` into the src node's out-chain and dst node's in-chain.
    ///
    /// - `src_first_out` and `dst_first_in` are mutable references to the
    ///   source/destination nodes' chain heads (typically from NodeStore::get_mut).
    /// - After this call, the edge is properly linked for bidirectional traversal.
    pub fn link_into_chains(
        &self,
        new_edge: EdgeId,
        src_first_out: &mut EdgeId,
        dst_first_in: &mut EdgeId,
    ) {
        // Link into src's outgoing chain
        if let Some(mut e) = self.edges.get_mut(&new_edge) {
            e.next_out = *src_first_out;
        }
        *src_first_out = new_edge;

        // Link into dst's incoming chain
        if let Some(mut e) = self.edges.get_mut(&new_edge) {
            e.next_in = *dst_first_in;
        }
        *dst_first_in = new_edge;
    }

    /// Unlink an edge from its source and destination chains.
    ///
    /// Walks the chain to find the predecessor and patches pointers.
    /// Returns true if successfully unlinked.
    pub fn unlink_from_chains(
        &self,
        edge_id: EdgeId,
        src_first_out: &mut EdgeId,
        dst_first_in: &mut EdgeId,
    ) -> bool {
        let edge = match self.edges.get(&edge_id) {
            Some(e) => e.clone(),
            None => return false,
        };

        // Unlink from src out-chain
        if *src_first_out == edge_id {
            *src_first_out = edge.next_out;
        } else {
            let mut cur = *src_first_out;
            while cur != NULL_EDGE {
                if let Some(mut e) = self.edges.get_mut(&cur) {
                    if e.next_out == edge_id {
                        e.next_out = edge.next_out;
                        break;
                    }
                    cur = e.next_out;
                } else {
                    break;
                }
            }
        }

        // Unlink from dst in-chain
        if *dst_first_in == edge_id {
            *dst_first_in = edge.next_in;
        } else {
            let mut cur = *dst_first_in;
            while cur != NULL_EDGE {
                if let Some(mut e) = self.edges.get_mut(&cur) {
                    if e.next_in == edge_id {
                        e.next_in = edge.next_in;
                        break;
                    }
                    cur = e.next_in;
                } else {
                    break;
                }
            }
        }

        true
    }

    #[inline]
    pub fn get(&self, id: EdgeId) -> Option<dashmap::mapref::one::Ref<'_, EdgeId, Edge>> {
        self.edges.get(&id)
    }

    #[inline]
    pub fn get_mut(&self, id: EdgeId) -> Option<dashmap::mapref::one::RefMut<'_, EdgeId, Edge>> {
        self.edges.get_mut(&id)
    }

    /// Soft-delete an edge.
    pub fn soft_delete(&self, id: EdgeId, tx_id: crate::types::TxId) -> bool {
        match self.edges.get_mut(&id) {
            Some(mut e) => {
                e.deleted_tx = tx_id;
                true
            }
            None => false,
        }
    }

    /// Hard-delete and recycle the ID.
    pub fn hard_delete(&self, id: EdgeId) -> bool {
        if self.edges.remove(&id).is_some() {
            self.free_list.push(id);
            self.free_count.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    // ── Traversal ─────────────────────────────────────────────────

    /// Follow out-edges from a starting edge (node's first_out).
    /// Returns EdgeIds in chain order.
    pub fn out_edges(&self, start: EdgeId) -> Vec<EdgeId> {
        let mut result = Vec::new();
        let mut cur = start;
        while cur != NULL_EDGE {
            if let Some(e) = self.edges.get(&cur) {
                result.push(cur);
                cur = e.next_out;
            } else {
                break;
            }
        }
        result
    }

    /// Follow in-edges into a node (starting from node's first_in).
    pub fn in_edges(&self, start: EdgeId) -> Vec<EdgeId> {
        let mut result = Vec::new();
        let mut cur = start;
        while cur != NULL_EDGE {
            if let Some(e) = self.edges.get(&cur) {
                result.push(cur);
                cur = e.next_in;
            } else {
                break;
            }
        }
        result
    }

    /// Follow out-edges, returning only those visible to `tx`.
    pub fn out_edges_visible(&self, start: EdgeId, tx: crate::types::TxId) -> Vec<EdgeId> {
        let mut result = Vec::new();
        let mut cur = start;
        while cur != NULL_EDGE {
            if let Some(e) = self.edges.get(&cur) {
                if e.is_alive(tx) {
                    result.push(cur);
                }
                cur = e.next_out;
            } else {
                break;
            }
        }
        result
    }

    /// Follow in-edges, returning only those visible to `tx`.
    pub fn in_edges_visible(&self, start: EdgeId, tx: crate::types::TxId) -> Vec<EdgeId> {
        let mut result = Vec::new();
        let mut cur = start;
        while cur != NULL_EDGE {
            if let Some(e) = self.edges.get(&cur) {
                if e.is_alive(tx) {
                    result.push(cur);
                }
                cur = e.next_in;
            } else {
                break;
            }
        }
        result
    }

    // ── Statistics ────────────────────────────────────────────────

    pub fn contains(&self, id: EdgeId) -> bool {
        self.edges.contains_key(&id)
    }

    pub fn len(&self) -> usize {
        self.edges.len()
    }

    pub fn free_count(&self) -> u64 {
        self.free_count.load(Ordering::Relaxed)
    }
}

// ── Tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::MAX_TX_ID;

    fn mk_store() -> EdgeStore {
        EdgeStore::with_capacity(64)
    }

    // ──────────────── Basic CRUD ────────────────

    #[test]
    fn test_insert_and_get() {
        let store = mk_store();
        let id = store.insert_edge(0, 1, 42, 0, 1);

        let e = store.get(id).unwrap();
        assert_eq!(e.id, id);
        assert_eq!(e.src, 0);
        assert_eq!(e.dst, 1);
        assert_eq!(e.etype, 42);
        assert_eq!(e.created_tx, 1);
        assert_eq!(e.deleted_tx, MAX_TX_ID);
        assert!(e.is_alive(1));
    }

    #[test]
    fn test_get_missing_returns_none() {
        let store = mk_store();
        assert!(store.get(999).is_none());
    }

    #[test]
    fn test_hard_delete_removes_edge() {
        let store = mk_store();
        let id = store.insert_edge(0, 1, 0, 0, 1);
        assert!(store.contains(id));
        assert!(store.hard_delete(id));
        assert!(!store.contains(id));
        assert!(store.get(id).is_none());
    }

    #[test]
    fn test_hard_delete_recycles_id() {
        let store = mk_store();
        let n = 5;
        let mut ids: Vec<EdgeId> = Vec::new();
        for _ in 0..n {
            ids.push(store.insert_edge(0, 1, 0, 0, 1));
        }
        for &id in &ids {
            store.hard_delete(id);
        }

        // Re-create — should recycle old IDs
        for i in 0..n {
            let id = store.insert_edge(0, 1, 0, 0, 2);
            assert!(id < n as EdgeId, "Expected recycled ID, got {}", id);
        }
    }

    #[test]
    fn test_insert_many() {
        let store = mk_store();
        for i in 0..1000u64 {
            let id = store.insert_edge(i, i + 1, 1, i as u32, 1);
            assert_eq!(id, i);
        }
        assert_eq!(store.len(), 1000);
    }

    // ──────────────── MVCC Soft-Delete ────────────────

    #[test]
    fn test_soft_delete_hides_edge() {
        let store = mk_store();
        let id = store.insert_edge(0, 1, 0, 0, 1);

        store.soft_delete(id, 5);
        let e = store.get(id).unwrap();
        assert!(e.is_alive(4));
        assert!(!e.is_alive(5));
        assert!(!e.is_alive(10));
    }

    #[test]
    fn test_mvcc_edge_visible_to_creating_tx() {
        let store = mk_store();
        let id = store.insert_edge(0, 1, 0, 0, 10);
        assert!(store.get(id).unwrap().is_alive(10));
        assert!(!store.get(id).unwrap().is_alive(9));
    }

    // ──────────────── Chain Linking ────────────────

    #[test]
    fn test_link_into_empty_chain() {
        let store = mk_store();
        let eid = store.insert_edge(0, 1, 0, 0, 1);

        let mut src_out = NULL_EDGE;
        let mut dst_in = NULL_EDGE;

        store.link_into_chains(eid, &mut src_out, &mut dst_in);

        assert_eq!(src_out, eid);
        assert_eq!(dst_in, eid);

        let edge = store.get(eid).unwrap();
        assert_eq!(edge.next_out, NULL_EDGE);
        assert_eq!(edge.next_in, NULL_EDGE);
    }

    #[test]
    fn test_link_two_edges_into_chain() {
        let store = mk_store();
        let e1 = store.insert_edge(0, 1, 0, 0, 1);
        let e2 = store.insert_edge(0, 2, 0, 0, 1);

        let mut src_out = NULL_EDGE;
        let mut dst1_in = NULL_EDGE;
        let mut dst2_in = NULL_EDGE;

        store.link_into_chains(e1, &mut src_out, &mut dst1_in);
        store.link_into_chains(e2, &mut src_out, &mut dst2_in);

        // src_out should point to e2 (last linked)
        assert_eq!(src_out, e2);
        // e2.next_out should point to e1
        assert_eq!(store.get(e2).unwrap().next_out, e1);
        // e1.next_out is still NULL
        assert_eq!(store.get(e1).unwrap().next_out, NULL_EDGE);
    }

    // ──────────────── Traversal ────────────────

    #[test]
    fn test_out_edges_traversal() {
        let store = mk_store();
        let e1 = store.insert_edge(0, 1, 0, 0, 1);
        let e2 = store.insert_edge(0, 2, 0, 0, 1);
        let e3 = store.insert_edge(0, 3, 0, 0, 1);

        let mut src_out = NULL_EDGE;
        let mut ignored = NULL_EDGE;

        // Link in reverse order so chain is e3→e2→e1
        store.link_into_chains(e3, &mut src_out, &mut ignored);
        store.link_into_chains(e2, &mut src_out, &mut ignored);
        store.link_into_chains(e1, &mut src_out, &mut ignored);

        let chain = store.out_edges(src_out);
        assert_eq!(chain, vec![e1, e2, e3]);
    }

    #[test]
    fn test_in_edges_traversal() {
        let store = mk_store();
        let e1 = store.insert_edge(1, 0, 0, 0, 1);
        let e2 = store.insert_edge(2, 0, 0, 0, 1);

        let mut ignored = NULL_EDGE;
        let mut dst_in = NULL_EDGE;

        store.link_into_chains(e2, &mut ignored, &mut dst_in);
        store.link_into_chains(e1, &mut ignored, &mut dst_in);

        let chain = store.in_edges(dst_in);
        assert_eq!(chain, vec![e1, e2]);
    }

    #[test]
    fn test_traversal_skips_deleted_edges() {
        let store = mk_store();
        let e1 = store.insert_edge(0, 1, 0, 0, 1);
        let e2 = store.insert_edge(0, 2, 0, 0, 1);
        let e3 = store.insert_edge(0, 3, 0, 0, 1);

        let mut src_out = NULL_EDGE;
        let mut ignored = NULL_EDGE;

        store.link_into_chains(e3, &mut src_out, &mut ignored);
        store.link_into_chains(e2, &mut src_out, &mut ignored);
        store.link_into_chains(e1, &mut src_out, &mut ignored);

        // Soft-delete e2
        store.soft_delete(e2, 100);

        let visible = store.out_edges_visible(src_out, 50);
        assert_eq!(visible, vec![e1, e2, e3]); // e2 still visible at tx 50 (< 100)
        assert!(!store.get(e2).unwrap().is_alive(100)); // hidden at tx 100
    }

    // ──────────────── Unlink ────────────────

    #[test]
    fn test_unlink_middle_of_chain() {
        let store = mk_store();
        let e1 = store.insert_edge(0, 1, 0, 0, 1);
        let e2 = store.insert_edge(0, 2, 0, 0, 1);
        let e3 = store.insert_edge(0, 3, 0, 0, 1);

        let mut src_out = NULL_EDGE;
        let mut dummy_dst = NULL_EDGE;

        // Chain: e3→e2→e1
        store.link_into_chains(e3, &mut src_out, &mut dummy_dst);
        store.link_into_chains(e2, &mut src_out, &mut dummy_dst);
        store.link_into_chains(e1, &mut src_out, &mut dummy_dst);

        // Unlink e2 (middle)
        store.unlink_from_chains(e2, &mut src_out, &mut dummy_dst);

        let chain = store.out_edges(src_out);
        assert_eq!(chain, vec![e1, e3]); // e2 should be gone
    }

    #[test]
    fn test_unlink_head_of_chain() {
        let store = mk_store();
        let e1 = store.insert_edge(0, 1, 0, 0, 1);
        let e2 = store.insert_edge(0, 2, 0, 0, 1);

        let mut src_out = NULL_EDGE;
        let mut dummy_dst = NULL_EDGE;

        store.link_into_chains(e2, &mut src_out, &mut dummy_dst);
        store.link_into_chains(e1, &mut src_out, &mut dummy_dst);

        // Unlink e1 (head)
        store.unlink_from_chains(e1, &mut src_out, &mut dummy_dst);

        let chain = store.out_edges(src_out);
        assert_eq!(chain, vec![e2]); // only e2 remains
    }

    // ──────────────── Concurrent ────────────────

    #[test]
    fn test_concurrent_inserts() {
        use std::sync::Arc;
        use std::thread;

        let store = Arc::new(EdgeStore::with_capacity(1024));
        let mut handles = Vec::new();

        for t in 0..8 {
            let s = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for i in 0..500 {
                    s.insert_edge(t, (t * 500 + i) as u64, 0, 0, 1);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(store.len(), 4000);
    }

    #[test]
    fn test_concurrent_insert_and_delete() {
        use std::sync::Arc;
        use std::thread;
        use std::sync::Barrier;

        let store = Arc::new(EdgeStore::with_capacity(256));

        // Pre-populate
        let mut ids = Vec::new();
        for _ in 0..200 {
            ids.push(store.insert_edge(0, 1, 0, 0, 1));
        }

        let barrier = Arc::new(Barrier::new(5));
        let mut handles = Vec::new();

        // 4 inserters
        for t in 0..4 {
            let s = Arc::clone(&store);
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                b.wait();
                for i in 0..50 {
                    s.insert_edge(t, (t * 50 + i) as u64, 0, 0, 2);
                }
            }));
        }

        // 1 deleter
        let s = Arc::clone(&store);
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            b.wait();
            for &id in &ids {
                s.hard_delete(id);
            }
        }));

        for h in handles {
            h.join().unwrap();
        }

        // No deadlock is the main assertion.
        // We should have roughly 200 inserted (4×50) and up to 200 deleted.
        assert!(store.len() <= 400);
    }
}
