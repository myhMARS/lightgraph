//! Persistent EdgeStore — DashMap cache + dedicated WAL thread.
//!
//! Same architecture as NodeStore: business threads update memory
//! and send to channel; WAL thread serializes and fsyncs in batches.

use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crossbeam::queue::SegQueue;
use dashmap::DashMap;

use super::wal_thread::WalThread;
use super::Edge;
use crate::types::{EdgeId, NodeId, TypeId, NULL_EDGE};

pub struct EdgeStore {
    edges: DashMap<EdgeId, Edge>,
    next_id: AtomicU64,
    free_list: SegQueue<EdgeId>,
    free_count: AtomicU64,
    wal: Option<WalThread>,
}

impl EdgeStore {
    pub fn new() -> Self {
        Self {
            edges: DashMap::with_capacity(3_000_000),
            next_id: AtomicU64::new(0),
            free_list: SegQueue::new(),
            free_count: AtomicU64::new(0),
            wal: None,
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            edges: DashMap::with_capacity(cap),
            next_id: AtomicU64::new(0),
            free_list: SegQueue::new(),
            free_count: AtomicU64::new(0),
            wal: None,
        }
    }

    /// Open with WAL thread.
    pub fn open(data_dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let wal_path = data_dir.join("edges.wal");

        let mut store = Self {
            edges: DashMap::with_capacity(3_000_000),
            next_id: AtomicU64::new(0),
            free_list: SegQueue::new(),
            free_count: AtomicU64::new(0),
            wal: None,
        };

        if wal_path.exists() && std::fs::metadata(&wal_path)?.len() > 4 {
            let mut max_id: u64 = 0;
            super::wal_thread::replay_wal(&wal_path, |opcode, id, payload| {
                match opcode {
                    1 => {
                        if let Ok(edge) = bincode::deserialize::<Edge>(payload) {
                            max_id = max_id.max(id);
                            store.edges.insert(id, edge);
                        }
                    }
                    2 => {
                        if store.edges.remove(&id).is_some() {
                            store.free_list.push(id);
                            store.free_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    _ => {}
                }
            })?;
            store.next_id.store(max_id + 1, Ordering::SeqCst);
        }

        store.wal = Some(WalThread::spawn(
            &data_dir.join("edges.wal"), 65536, Duration::from_millis(5), 4096)?);
        Ok(store)
    }

    // ── WAL helpers ──────────────────────────────────────────────

    fn wal_insert(&self, id: EdgeId, edge: &Edge) {
        if let Some(ref wal) = self.wal {
            if let Ok(data) = bincode::serialize(edge) {
                wal.send_insert(id, data);
            }
        }
    }

    fn wal_delete(&self, id: EdgeId) {
        if let Some(ref wal) = self.wal {
            wal.send_delete(id);
        }
    }

    // ── ID allocation ────────────────────────────────────────────

    #[inline]
    fn alloc_id(&self) -> EdgeId {
        if let Some(id) = self.free_list.pop() {
            self.free_count.fetch_sub(1, Ordering::Relaxed);
            return id;
        }
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    // ── CRUD ──────────────────────────────────────────────────────

    pub fn insert_edge(&self, src: NodeId, dst: NodeId, etype: TypeId,
                       props_row: u32, tx_id: crate::types::TxId) -> EdgeId {
        let id = self.alloc_id();
        let edge = Edge::new(id, src, dst, etype, props_row, tx_id);
        self.edges.insert(id, edge.clone());
        self.wal_insert(id, &edge);
        id
    }

    #[inline]
    pub fn get(&self, id: EdgeId) -> Option<dashmap::mapref::one::Ref<'_, EdgeId, Edge>> {
        self.edges.get(&id)
    }

    #[inline]
    pub fn get_mut(&self, id: EdgeId) -> Option<dashmap::mapref::one::RefMut<'_, EdgeId, Edge>> {
        self.edges.get_mut(&id)
    }

    /// Link edge into adjacency chains. Persists the updated edge.
    pub fn link_into_chains(&self, new_edge: EdgeId,
                            src_first_out: &mut EdgeId, dst_first_in: &mut EdgeId) {
        if let Some(mut e) = self.edges.get_mut(&new_edge) {
            e.next_out = *src_first_out;
        }
        *src_first_out = new_edge;

        if let Some(mut e) = self.edges.get_mut(&new_edge) {
            e.next_in = *dst_first_in;
        }
        *dst_first_in = new_edge;

        // Persist updated edge (after RefMut dropped)
        if let Some(e) = self.edges.get(&new_edge) {
            self.wal_insert(new_edge, &e);
        }
    }

    /// Unlink edge from chains. Persists affected predecessors.
    pub fn unlink_from_chains(&self, edge_id: EdgeId,
                              src_first_out: &mut EdgeId, dst_first_in: &mut EdgeId) -> bool {
        let edge = match self.edges.get(&edge_id) {
            Some(e) => e.clone(),
            None => return false,
        };

        let mut src_pred = None;
        if *src_first_out == edge_id {
            *src_first_out = edge.next_out;
        } else {
            let mut cur = *src_first_out;
            while cur != NULL_EDGE {
                if let Some(mut e) = self.edges.get_mut(&cur) {
                    if e.next_out == edge_id {
                        e.next_out = edge.next_out;
                        src_pred = Some(cur);
                        break;
                    }
                    cur = e.next_out;
                } else { break; }
            }
        }

        let mut dst_pred = None;
        if *dst_first_in == edge_id {
            *dst_first_in = edge.next_in;
        } else {
            let mut cur = *dst_first_in;
            while cur != NULL_EDGE {
                if let Some(mut e) = self.edges.get_mut(&cur) {
                    if e.next_in == edge_id {
                        e.next_in = edge.next_in;
                        dst_pred = Some(cur);
                        break;
                    }
                    cur = e.next_in;
                } else { break; }
            }
        }

        // Persist predecessors
        if let Some(id) = src_pred {
            if let Some(e) = self.edges.get(&id) { self.wal_insert(id, &e); }
        }
        if let Some(id) = dst_pred {
            if let Some(e) = self.edges.get(&id) { self.wal_insert(id, &e); }
        }
        true
    }

    pub fn soft_delete(&self, id: EdgeId, tx_id: crate::types::TxId) -> bool {
        match self.edges.get_mut(&id) {
            Some(mut e) => {
                e.deleted_tx = tx_id;
                if let Some(edge) = self.edges.get(&id) { self.wal_insert(id, &edge); }
                true
            }
            None => false,
        }
    }

    pub fn hard_delete(&self, id: EdgeId) -> bool {
        self.wal_delete(id);
        if self.edges.remove(&id).is_some() {
            self.free_list.push(id);
            self.free_count.fetch_add(1, Ordering::Relaxed);
            true
        } else { false }
    }

    // ── Traversal ─────────────────────────────────────────────────

    pub fn out_edges(&self, start: EdgeId) -> Vec<EdgeId> {
        let mut r = Vec::new(); let mut cur = start;
        while cur != NULL_EDGE {
            if let Some(e) = self.edges.get(&cur) { r.push(cur); cur = e.next_out; }
            else { break; }
        }
        r
    }

    pub fn in_edges(&self, start: EdgeId) -> Vec<EdgeId> {
        let mut r = Vec::new(); let mut cur = start;
        while cur != NULL_EDGE {
            if let Some(e) = self.edges.get(&cur) { r.push(cur); cur = e.next_in; }
            else { break; }
        }
        r
    }

    pub fn out_edges_visible(&self, start: EdgeId, tx: crate::types::TxId) -> Vec<EdgeId> {
        let mut r = Vec::new(); let mut cur = start;
        while cur != NULL_EDGE {
            if let Some(e) = self.edges.get(&cur) {
                if e.is_alive(tx) { r.push(cur); }
                cur = e.next_out;
            } else { break; }
        }
        r
    }

    // ── Stats ─────────────────────────────────────────────────────

    pub fn contains(&self, id: EdgeId) -> bool { self.edges.contains_key(&id) }
    pub fn len(&self) -> usize { self.edges.len() }
    pub fn free_count(&self) -> u64 { self.free_count.load(Ordering::Relaxed) }

    pub fn flush(&self) {
        if let Some(ref wal) = self.wal { wal.flush(); }
    }
}

// ── Tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::MAX_TX_ID;
    use tempfile::TempDir;

    fn mk() -> EdgeStore { EdgeStore::with_capacity(64) }

    #[test]
    fn test_persist_and_recover() {
        let dir = TempDir::new().unwrap();
        let p = dir.path();
        {
            let s = EdgeStore::open(p).unwrap();
            s.insert_edge(0, 1, 10, 100, 1);
            s.insert_edge(1, 2, 20, 101, 1);
            s.soft_delete(0, 50);
            s.flush();
        }
        {
            let s = EdgeStore::open(p).unwrap();
            assert_eq!(s.len(), 2);
            assert!(!s.get(0).unwrap().is_alive(50));
            assert_eq!(s.get(1).unwrap().etype, 20);
        }
    }

    #[test]
    fn test_insert_and_get() {
        let s = mk();
        let id = s.insert_edge(0, 1, 42, 0, 1);
        let e = s.get(id).unwrap();
        assert_eq!(e.src, 0); assert_eq!(e.dst, 1);
        assert_eq!(e.deleted_tx, MAX_TX_ID);
    }

    #[test]
    fn test_chain_traversal() {
        let s = mk();
        let e1 = s.insert_edge(0, 1, 0, 0, 1);
        let e2 = s.insert_edge(0, 2, 0, 0, 1);
        let mut out = NULL_EDGE; let mut dummy = NULL_EDGE;
        s.link_into_chains(e2, &mut out, &mut dummy);
        s.link_into_chains(e1, &mut out, &mut dummy);
        assert_eq!(s.out_edges(out), vec![e1, e2]);
    }

    #[test]
    fn test_soft_delete_hides() {
        let s = mk();
        let id = s.insert_edge(0, 1, 0, 0, 1);
        s.soft_delete(id, 10);
        assert!(s.get(id).unwrap().is_alive(5));
        assert!(!s.get(id).unwrap().is_alive(10));
    }
}
