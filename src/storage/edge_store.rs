//! SlotMap-based bidirectional adjacency list.
//!
//! Each edge stores both next_out and next_in pointers,
//! enabling efficient bidirectional traversal without reverse indexes.

use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use parking_lot::Mutex;

use super::Edge;
use crate::types::{EdgeId, NodeId, TypeId, NULL_EDGE};

pub struct EdgeStore {
    edges: DashMap<EdgeId, Edge>,
    next_id: AtomicU64,
    free_list: Mutex<Vec<EdgeId>>,
}

impl EdgeStore {
    pub fn new() -> Self {
        Self {
            edges: DashMap::with_capacity(3_000_000),
            next_id: AtomicU64::new(0),
            free_list: Mutex::new(Vec::new()),
        }
    }

    fn alloc_id(&self) -> EdgeId {
        if let Some(id) = self.free_list.lock().pop() {
            return id;
        }
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    #[inline]
    pub fn get(&self, id: EdgeId) -> Option<dashmap::mapref::one::Ref<'_, EdgeId, Edge>> {
        self.edges.get(&id)
    }

    #[inline]
    pub fn get_mut(&self, id: EdgeId) -> Option<dashmap::mapref::one::RefMut<'_, EdgeId, Edge>> {
        self.edges.get_mut(&id)
    }

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

    /// Follow out-edges from a node (starting from `first_out` edge).
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

    /// Follow in-edges into a node (starting from `first_in` edge).
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
            self.free_list.lock().push(id);
            true
        } else {
            false
        }
    }

    pub fn contains(&self, id: EdgeId) -> bool {
        self.edges.contains_key(&id)
    }

    pub fn len(&self) -> usize {
        self.edges.len()
    }
}
