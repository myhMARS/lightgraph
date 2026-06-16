// Consistent hashing ring for data sharding.
// Sprint 12.

use crate::types::NodeId;

pub struct HashRing {
    virtual_nodes: usize,
    // Ring: sorted hash → shard_id mapping
}

impl HashRing {
    pub fn new(virtual_nodes: usize) -> Self {
        Self { virtual_nodes }
    }

    pub fn locate(&self, _node_id: NodeId) -> u64 {
        // TODO: Find responsible shard for a key
        unimplemented!("HashRing::locate — Sprint 12")
    }
}
