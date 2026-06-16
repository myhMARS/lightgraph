// Query routing across shards.
// Sprint 12-13.

use crate::distributed::DistributedResult;
use crate::query::QueryPlan;

pub struct ShardRouter {
    // TODO: grpc clients to each shard
}

impl ShardRouter {
    pub fn new() -> Self {
        Self {}
    }

    pub fn execute(&self, _plan: &QueryPlan) -> DistributedResult {
        // Fan out query to relevant shards, merge results
        unimplemented!("ShardRouter::execute — Sprint 12")
    }
}
