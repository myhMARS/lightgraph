// HNSW implementation — placeholder structure.
//
// In production this would use a crate like hnsw_rs or usearch,
// or a hand-rolled implementation. For the roadmap, the interface
// is the contract.

use crate::types::{NodeId, Score};

pub struct HnswIndex {
    dim: usize,
    m: usize,
    ef_construction: usize,
}

impl HnswIndex {
    pub fn new(dim: usize, m: usize, ef_construction: usize) -> Self {
        Self { dim, m, ef_construction }
    }

    pub fn insert(&mut self, _id: NodeId, _vector: Vec<f32>) {
        // TODO: HNSW insert with layer assignment (random exponential decay)
        // layer = floor(-ln(random) * ml)
        // insert into each layer ≤ layer, building neighbor lists
        unimplemented!("HNSW insert — Sprint 7")
    }

    pub fn search(&self, _query: &[f32], _k: usize, _ef: usize) -> Vec<(NodeId, Score)> {
        // TODO: HNSW search — descend from top layer, search bottom layer
        // Maintain candidate heap, visited set, result set
        unimplemented!("HNSW search — Sprint 7")
    }
}
