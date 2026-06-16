// HNSW Vector Index
//
// Hierarchical Navigable Small World — state-of-the-art ANN search.
// Build M=16, ef_construction=200. Search ef=100.
// Supported metrics: Cosine, Euclidean, DotProduct.

mod distance;
mod hnsw;

pub use distance::DistanceMetric;
pub use hnsw::HnswIndex;

use crate::types::{NodeId, Score};

/// A vector index implementation
pub struct VectorIndex {
    name: String,
    dim: usize,
    metric: DistanceMetric,
    hnsw: HnswIndex,
    /// Total vectors indexed
    count: usize,
}

impl VectorIndex {
    pub fn new(name: &str, dim: usize, metric: DistanceMetric, m: usize, ef_construction: usize) -> Self {
        Self {
            name: name.to_string(),
            dim,
            metric,
            hnsw: HnswIndex::new(dim, m, ef_construction),
            count: 0,
        }
    }

    pub fn insert(&mut self, id: NodeId, vector: Vec<f32>) {
        assert_eq!(vector.len(), self.dim);
        self.hnsw.insert(id, vector);
        self.count += 1;
    }

    pub fn search(&self, query: &[f32], k: usize, ef: usize) -> Vec<(NodeId, Score)> {
        assert_eq!(query.len(), self.dim);
        self.hnsw.search(query, k, ef)
    }

    pub fn len(&self) -> usize {
        self.count
    }
}
