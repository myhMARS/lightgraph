//! DashMap-backed Node storage with slab allocation.
//!
//! Reads are lock-free (DashMap sharded reads).
//! Writes lock only the target slot.

use crate::types::{NodeId, NULL_NODE};
use dashmap::DashMap;
use parking_lot::RwLock;

use super::Node;

pub struct NodeStore {
    nodes: DashMap<NodeId, Node>,
    next_id: atomic::AtomicU64,
    deleted: DashMap<NodeId, ()>,  // tombstone map for hard deletes
}

impl NodeStore {
    pub fn new() -> Self {
        Self {
            nodes: DashMap::with_capacity(1_000_000),
            next_id: atomic::AtomicU64::new(0),
            deleted: DashMap::new(),
        }
    }

    #[inline]
    pub fn get(&self, id: NodeId) -> Option<dashmap::mapref::one::Ref<NodeId, Node>> {
        self.nodes.get(&id)
    }

    pub fn insert(&self, node: Node) -> NodeId {
        let id = self.next_id.fetch_add(1, atomic::Ordering::SeqCst);
        self.nodes.insert(id, node);
        id
    }

    pub fn remove(&self, id: NodeId) {
        self.nodes.remove(&id);
        self.deleted.insert(id, ());
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }
}
