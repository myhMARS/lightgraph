// Storage layer — in-memory, lock-free reads, fine-grained write locks.
//
// node_store: DashMap<NodeId, Node> with slab allocation + MVCC soft-delete
// edge_store: SlotMap-based bidirectional adjacency list
// prop_store: Columnar property storage per label

pub mod node_store;
pub mod edge_store;
pub mod prop_store;

use crate::types::{NodeId, EdgeId, LabelId, TypeId, TxId, NULL_EDGE, MAX_TX_ID};

/// A Node in the graph.
#[derive(Debug, Clone)]
pub struct Node {
    pub id: NodeId,
    pub labels: Vec<LabelId>,
    pub first_out: EdgeId,   // head of outgoing edge chain
    pub first_in: EdgeId,    // head of incoming edge chain
    pub props_row: u32,      // row index in columnar prop store
    pub created_tx: TxId,
    pub deleted_tx: TxId,    // MAX_TX_ID if alive
}

/// A directed Edge between two nodes.
#[derive(Debug, Clone)]
pub struct Edge {
    pub id: EdgeId,
    pub src: NodeId,
    pub dst: NodeId,
    pub etype: TypeId,
    pub next_out: EdgeId,    // next edge with same src
    pub next_in: EdgeId,     // next edge with same dst
    pub props_row: u32,
    pub created_tx: TxId,
    pub deleted_tx: TxId,    // MAX_TX_ID if alive
}

impl Node {
    /// Create a new node with the given labels and first edge pointers.
    pub fn new(id: NodeId, labels: Vec<LabelId>, props_row: u32, created_tx: TxId) -> Self {
        Self {
            id,
            labels,
            first_out: NULL_EDGE,
            first_in: NULL_EDGE,
            props_row,
            created_tx,
            deleted_tx: MAX_TX_ID,
        }
    }

    /// Check if this node is visible to a transaction at snapshot `tx`.
    pub fn is_alive(&self, tx: TxId) -> bool {
        self.created_tx <= tx && self.deleted_tx > tx
    }

    /// Mark as deleted at the given transaction.
    pub fn mark_deleted(&mut self, tx: TxId) {
        self.deleted_tx = tx;
    }
}

impl Edge {
    /// Create a new edge.
    pub fn new(id: EdgeId, src: NodeId, dst: NodeId, etype: TypeId,
               props_row: u32, created_tx: TxId) -> Self {
        Self {
            id,
            src,
            dst,
            etype,
            next_out: NULL_EDGE,
            next_in: NULL_EDGE,
            props_row,
            created_tx,
            deleted_tx: MAX_TX_ID,
        }
    }

    /// Check if this edge is visible to a transaction at snapshot `tx`.
    pub fn is_alive(&self, tx: TxId) -> bool {
        self.created_tx <= tx && self.deleted_tx > tx
    }

    /// Mark as deleted at the given transaction.
    pub fn mark_deleted(&mut self, tx: TxId) {
        self.deleted_tx = tx;
    }
}
