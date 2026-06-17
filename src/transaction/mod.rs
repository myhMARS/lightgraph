// Transaction layer — MVCC snapshot isolation + atomic multi-store writes.
//
// tx_manager: TxId allocation, commit/rollback, first-committer-wins
// transaction: buffered writes, atomic commit across NodeStore+EdgeStore+PropStore

pub mod tx_manager;

use parking_lot::Mutex;

use crate::types::{NodeId, EdgeId, LabelId, TxId};
use crate::storage::prop_store::Value;
use crate::storage::node_store::NodeStore;
use crate::storage::edge_store::EdgeStore;
use crate::storage::prop_store::PropStore;
use crate::storage::consistency::Consistency;
use tx_manager::TxManager;

/// A buffered write transaction spanning all stores.
///
/// ## Usage
///
/// ```ignore
/// let tx = db.begin_write();
/// tx.create_node("Person", props);
/// tx.create_edge(src, dst, "KNOWS", props);
/// tx.commit().unwrap(); // atomic across all stores
/// ```
pub struct Transaction<'a> {
    /// Our write TxId (not yet committed).
    tx_id: TxId,
    /// Snapshot for reads within this transaction.
    snapshot: TxId,
    /// Reference to the global TxManager.
    tx_manager: &'a TxManager,
    /// Stores (shared references).
    nodes: &'a NodeStore,
    edges: &'a EdgeStore,
    props: &'a PropStore,

    // ── Write buffer ───────────────────────────────────────────

    /// Buffered node inserts: (NodeId, labels, props_row)
    node_inserts: Mutex<Vec<(NodeId, Vec<LabelId>, u32)>>,
    /// Buffered node deletes
    node_deletes: Mutex<Vec<NodeId>>,
    /// Buffered edge inserts: (EdgeId, src, dst, etype, props_row)
    edge_inserts: Mutex<Vec<(EdgeId, NodeId, NodeId, LabelId, u32)>>,
    /// Buffered edge deletes
    edge_deletes: Mutex<Vec<EdgeId>>,
    /// Buffered property writes: (label, prop_name, row, value)
    prop_writes: Mutex<Vec<(LabelId, String, u32, Option<Value>)>>,
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(
        tx_id: TxId,
        snapshot: TxId,
        tx_manager: &'a TxManager,
        nodes: &'a NodeStore,
        edges: &'a EdgeStore,
        props: &'a PropStore,
    ) -> Self {
        Self {
            tx_id,
            snapshot,
            tx_manager,
            nodes,
            edges,
            props,
            node_inserts: Mutex::new(Vec::new()),
            node_deletes: Mutex::new(Vec::new()),
            edge_inserts: Mutex::new(Vec::new()),
            edge_deletes: Mutex::new(Vec::new()),
            prop_writes: Mutex::new(Vec::new()),
        }
    }

    /// Our TxId (not committed yet — visible only to us).
    pub fn tx_id(&self) -> TxId { self.tx_id }

    /// Snapshot TxId for reads within this transaction.
    pub fn snapshot(&self) -> TxId { self.snapshot }

    // ── Buffered writes ─────────────────────────────────────────

    /// Create a node. Applied on commit.
    pub fn create_node(&self, labels: Vec<LabelId>, props_row: u32) -> NodeId {
        let id = self.nodes.alloc_id_direct();
        self.node_inserts.lock().push((id, labels, props_row));
        id
    }

    /// Delete a node. Applied on commit.
    pub fn delete_node(&self, id: NodeId) {
        self.node_deletes.lock().push(id);
    }

    /// Create an edge. Applied on commit.
    pub fn create_edge(&self, src: NodeId, dst: NodeId, etype: LabelId, props_row: u32) -> EdgeId {
        let id = self.edges.alloc_id_direct();
        self.edge_inserts.lock().push((id, src, dst, etype, props_row));
        id
    }

    /// Delete an edge. Applied on commit.
    pub fn delete_edge(&self, id: EdgeId) {
        self.edge_deletes.lock().push(id);
    }

    /// Set a property. Applied on commit.
    pub fn set_prop(&self, label: LabelId, prop: &str, row: u32, value: Option<Value>) {
        self.prop_writes.lock().push((label, prop.to_string(), row, value));
    }

    // ── Reads (see snapshot) ────────────────────────────────────

    /// Read a node as of our snapshot.
    pub fn get_node(&self, id: NodeId) -> Option<crate::storage::Node> {
        self.nodes.get(id).map(|n| n.clone()).filter(|n| n.is_alive(self.snapshot))
    }

    /// Read an edge as of our snapshot.
    pub fn get_edge(&self, id: EdgeId) -> Option<crate::storage::Edge> {
        self.edges.get(id).map(|e| e.clone()).filter(|e| e.is_alive(self.snapshot))
    }

    // ── Commit / Rollback ───────────────────────────────────────

    /// Atomically apply all buffered writes.
    ///
    /// Order: nodes → edges → props (edges reference nodes).
    /// On crash between any step, WAL replay recovers consistently.
    pub fn commit(&self) -> Result<(), &'static str> {
        // Phase 1: Try to commit at TxManager level
        self.tx_manager.commit(self.tx_id)?;

        // Phase 2: Apply buffered writes to stores
        for (id, labels, props_row) in self.node_inserts.lock().iter() {
            self.nodes.insert_with_id(*id, labels.clone(), *props_row, self.tx_id);
        }
        for id in self.node_deletes.lock().iter() {
            self.nodes.soft_delete(*id, self.tx_id);
        }
        for (id, src, dst, etype, props_row) in self.edge_inserts.lock().iter() {
            self.edges.insert_with_id(*id, *src, *dst, *etype, *props_row, self.tx_id);
        }
        for id in self.edge_deletes.lock().iter() {
            self.edges.soft_delete(*id, self.tx_id);
        }
        for (label, prop, row, value) in self.prop_writes.lock().iter() {
            self.props.insert_prop(*label, prop, *row, value.clone());
        }

        // Phase 3: Flush all stores to disk
        self.nodes.flush();
        self.edges.flush();
        self.props.flush();

        Ok(())
    }

    /// Discard all buffered writes.
    pub fn rollback(&self) {
        self.tx_manager.rollback(self.tx_id);
        // Buffers are simply dropped — no store changes were made.
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        // Auto-rollback if not committed
        self.tx_manager.rollback(self.tx_id);
    }
}

/// A database handle that owns all stores and the TxManager.
///
/// This is the top-level entry point for users.
pub struct Database {
    pub tx_manager: TxManager,
    pub nodes: NodeStore,
    pub edges: EdgeStore,
    pub props: PropStore,
}

impl Database {
    /// Open a database at the given path with a consistency contract.
    pub fn open(path: &std::path::Path, consistency: Consistency) -> std::io::Result<Self> {
        Ok(Self {
            tx_manager: TxManager::new(),
            nodes: NodeStore::open(path, consistency.clone())?,
            edges: EdgeStore::open(path, consistency.clone())?,
            props: PropStore::open(path, consistency)?,
        })
    }

    /// Create an in-memory database (for testing).
    pub fn memory() -> Self {
        Self {
            tx_manager: TxManager::new(),
            nodes: NodeStore::new(),
            edges: EdgeStore::new(),
            props: PropStore::new(),
        }
    }

    /// Begin a read-only transaction.
    /// The returned Transaction cannot commit (commit is a no-op, rollback is automatic).
    pub fn begin_read(&self) -> Transaction {
        let snap = self.tx_manager.begin_read();
        // Use snapshot as tx_id so rollback in Drop is harmless
        Transaction::new(snap, snap, &self.tx_manager, &self.nodes, &self.edges, &self.props)
    }

    /// Begin a write transaction.
    pub fn begin_write(&self) -> Transaction {
        let tx_id = self.tx_manager.begin_write();
        let snap = self.tx_manager.latest_committed();
        Transaction::new(tx_id, snap, &self.tx_manager, &self.nodes, &self.edges, &self.props)
    }
}
