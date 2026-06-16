// LightGraph — High-performance Distributed Graph Database
//
// Architecture layers:
//   storage/     — Node/Edge/Property stores (DashMap + SlotMap + Columnar)
//   index/       — Full-text (FST+CJK), Vector (HNSW), Property (BTree+Bitmap)
//   query/       — Hybrid query engine with predicate pre-filter
//   transaction/ — Simplified MVCC with snapshot isolation
//   wal/         — Write-Ahead Log with group commit
//   snapshot/    — FlatBuffers-based full snapshots
//   distributed/ — Raft consensus + Consistent hashing
//   pyo3/        — Python bindings via PyO3

pub mod storage;
pub mod index;
pub mod query;
pub mod transaction;
pub mod wal;
pub mod snapshot;
pub mod distributed;

#[cfg(feature = "python")]
pub mod pyo3;

/// Core types used across all modules
mod types {
    pub type NodeId = u64;
    pub type EdgeId = u64;
    pub type LabelId = u32;
    pub type TypeId = u32;
    pub type TxId = u64;
    pub type Score = f32;

    pub const NULL_NODE: NodeId = u64::MAX;
    pub const NULL_EDGE: EdgeId = u64::MAX;
}
