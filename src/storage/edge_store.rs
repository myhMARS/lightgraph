//! SlotMap-based bidirectional adjacency list.
//!
//! Each edge stores both next_out and next_in pointers,
//! enabling efficient bidirectional traversal without reverse indexes.

use crate::types::{EdgeId, NodeId, TypeId, NULL_EDGE};
use dashmap::DashMap;
use atomic::Atomic;

use super::Edge;

pub struct EdgeStore {
    edges: DashMap<EdgeId, Edge>,
    next_id: atomic::AtomicU64,
}

impl EdgeStore {
    pub fn new() -> Self {
        Self {
            edges: DashMap::with_capacity(3_000_000),
            next_id: atomic::AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn get(&self, id: EdgeId) -> Option<dashmap::mapref::one::Ref<EdgeId, Edge>> {
        self.edges.get(&id)
    }

    pub fn insert(&self, edge: Edge) -> EdgeId {
        let id = self.next_id.fetch_add(1, atomic::Ordering::SeqCst);
        self.edges.insert(id, edge);
        id
    }

    /// Follow out-edges from a node
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

    /// Follow in-edges into a node
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

    pub fn len(&self) -> usize {
        self.edges.len()
    }
}
