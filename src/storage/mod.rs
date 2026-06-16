// Storage layer — in-memory, lock-free reads, fine-grained write locks.
//
// node_store: DashMap<NodeId, Node> with slab allocation
// edge_store: SlotMap-based bidirectional adjacency list
// prop_store: Columnar property storage per label

pub mod node_store;
pub mod edge_store;
pub mod prop_store;

use crate::types::{NodeId, EdgeId, LabelId, TypeId};
use dashmap::DashMap;
use parking_lot::RwLock;

/// A Node in the graph
pub struct Node {
    pub labels: Vec<LabelId>,
    pub first_out: EdgeId,   // head of outgoing edge chain
    pub first_in: EdgeId,    // head of incoming edge chain
    pub props_row: u32,      // row index in columnar prop store
    pub created_tx: TxId,
    pub deleted_tx: TxId,    // TxId::MAX if alive
}

/// A directed Edge between two nodes
pub struct Edge {
    pub src: NodeId,
    pub dst: NodeId,
    pub etype: TypeId,
    pub next_out: EdgeId,    // next edge with same src
    pub next_in: EdgeId,     // next edge with same dst
    pub props_row: u32,
    pub created_tx: TxId,
    pub deleted_tx: TxId,
}

impl Node {
    pub fn is_alive(&self, tx: TxId) -> bool {
        self.created_tx <= tx && self.deleted_tx > tx
    }
}

impl Edge {
    pub fn is_alive(&self, tx: TxId) -> bool {
        self.created_tx <= tx && self.deleted_tx > tx
    }
}
