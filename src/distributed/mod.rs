// Distributed layer — Raft consensus + Consistent hashing sharding.
//
// Raft: leader election, log replication, state machine integration.
// Sharding: consistent hashing ring over NodeId/EdgeId space.
// Indexes are per-shard (local FST/HNSW), queries fan out and merge.

mod raft;
mod shard;
mod router;

pub use raft::RaftNode;
pub use shard::HashRing;
pub use router::ShardRouter;

/// Configuration for a cluster node.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub node_id: u64,
    pub raft_addr: String,
    pub grpc_addr: String,
    pub data_dir: String,
    pub peers: Vec<u64>,
}

/// A shard owns a contiguous range of the hash ring.
#[derive(Debug, Clone)]
pub struct ShardInfo {
    pub shard_id: u64,
    pub owner: u64,     // raft node id
    pub range_start: u64,
    pub range_end: u64,
}

/// Result from a distributed query — merged from multiple shards.
#[derive(Debug)]
pub struct DistributedResult {
    pub results: Vec<crate::query::QueryResult>,
    pub shards_contacted: usize,
    pub shards_responded: usize,
    pub merge_time_ms: f64,
}
