// Hybrid Query Engine
//
// Combines: full-text + vector + property predicate + graph traversal
// in a single fused query. Predicate pre-filtering reduces the candidate
// set before expensive scoring operations.
//
// Pipeline: Predicate bitmap → (Full-text score + Vector score) → Fusion → Sort → Limit → [Traverse]

mod builder;
mod fusion;
mod plan;

pub use builder::QueryBuilder;
pub use fusion::FusionMethod;
pub use plan::{PlanOp, QueryPlan};

use crate::types::{NodeId, Score};
use crate::index::fulltext::FullTextIndex;
use crate::index::vector::VectorIndex;
use crate::index::property::{Predicate, PropertyIndex};
use roaring::RoaringBitmap;
use std::collections::HashMap;

/// Result from a hybrid query
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub node_id: NodeId,
    pub score: Score,
    pub path: Option<Vec<NodeId>>, // traversed path if any
}

/// Adaptive filter timing strategy.
#[derive(Debug, Clone)]
pub enum FilterTiming {
    /// Filter first (when predicate selectivity < 10%)
    Pre,
    /// Score first, filter later (when selectivity ≥ 10%)
    Post,
    /// Auto-detect based on estimated selectivity
    Adaptive,
}

/// Execution context holding all indexes and stores
pub struct QueryContext<'a> {
    pub ft_index: Option<&'a FullTextIndex>,
    pub vec_index: Option<&'a VectorIndex>,
    pub prop_index: &'a PropertyIndex,
    pub node_count: u64,
}

/// Hybrid search: score = α * BM25_score + β * cosine_score,
/// filtered by predicate bitmap, optionally traversed.
pub fn hybrid_search(
    ctx: &QueryContext,
    fulltext_query: Option<&str>,
    vector_query: Option<&[f32]>,
    vector_k: Option<usize>,
    vector_ef: Option<usize>,
    predicate: Option<&Predicate>,
    timing: FilterTiming,
    alpha: Score,
    beta: Score,
    limit: usize,
) -> Vec<QueryResult> {
    // Step 1: evaluate predicate → bitmap
    let filter_bm = predicate
        .map(|p| ctx.prop_index.evaluate(p))
        .unwrap_or_default();

    let selectivity = if ctx.node_count > 0 {
        filter_bm.len() as f32 / ctx.node_count as f32
    } else {
        0.0
    };

    let use_pre_filter = match timing {
        FilterTiming::Pre => true,
        FilterTiming::Post => false,
        FilterTiming::Adaptive => selectivity < 0.1,
    };

    let mut scores: HashMap<NodeId, Score> = HashMap::new();

    if use_pre_filter {
        // Pre-filter: only score candidates that pass the predicate
        let candidates: Vec<NodeId> = filter_bm.iter().map(|id| id as NodeId).collect();

        for &node_id in &candidates {
            let mut score = 0.0;

            if let Some(ref ft) = ctx.ft_index {
                if let Some(q) = fulltext_query {
                    let _ = q; // TODO: single-doc scoring
                    score += alpha * 1.0;
                }
            }
            if let Some(ref vi) = ctx.vec_index {
                if let Some(vq) = vector_query {
                    let _ = vq;
                    score += beta * 1.0; // TODO: single-doc vector score
                }
            }
            if score > 0.0 {
                scores.insert(node_id, score);
            }
        }
    } else {
        // Post-filter: score all, then intersect with predicate bitmap
        if let Some(ref ft) = ctx.ft_index {
            if let Some(q) = fulltext_query {
                for (id, s) in ft.search(q) {
                    *scores.entry(id).or_default() += alpha * s;
                }
            }
        }
        if let Some(ref vi) = ctx.vec_index {
            if let Some(vq) = vector_query {
                let k = vector_k.unwrap_or(50);
                let ef = vector_ef.unwrap_or(200);
                for (id, s) in vi.search(vq, k, ef) {
                    *scores.entry(id).or_default() += beta * s;
                }
            }
        }
        // Remove candidates outside filter
        if !filter_bm.is_empty() {
            scores.retain(|id, _| filter_bm.contains(*id as u32));
        }
    }

    let mut results: Vec<QueryResult> = scores
        .into_iter()
        .map(|(node_id, score)| QueryResult {
            node_id,
            score,
            path: None,
        })
        .collect();

    results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
    results.truncate(limit);
    results
}
