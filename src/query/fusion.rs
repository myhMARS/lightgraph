// Score fusion methods for combining full-text and vector search results.

#[derive(Debug, Clone)]
pub enum FusionMethod {
    /// score = alpha * BM25 + beta * cosine_similarity
    WeightedSum { alpha: f32, beta: f32 },
    /// Reciprocal Rank Fusion (RRF): score = sum(1 / (k + rank_i))
    /// Works well when score distributions differ significantly.
    ReciprocalRankFusion { k: f32 },
    /// Convex combination: score = λ * BM25 + (1-λ) * cosine
    Convex { lambda: f32 },
}

impl Default for FusionMethod {
    fn default() -> Self {
        FusionMethod::WeightedSum { alpha: 1.0, beta: 0.0 }
    }
}
