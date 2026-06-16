// Compiled query plan — immutable, sendable, cacheable.

use crate::index::property::Predicate;
use crate::query::builder::{Direction, TraversalClause};
use crate::query::fusion::FusionMethod;
use crate::query::query::FilterTiming;

/// Compiled execution plan — no string parsing at execute time.
#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub fulltext_prop: Option<String>,
    pub fulltext_text: Option<String>,
    pub vector_prop: Option<String>,
    pub vector_query: Option<Vec<f32>>,
    pub vector_k: usize,
    pub vector_ef: usize,
    pub predicate: Option<Predicate>,
    pub fusion: FusionMethod,
    pub filter_timing: FilterTiming,
    pub traversal: Option<TraversalClause>,
    pub limit: usize,
}

/// Operator nodes in the physical execution plan (volcano model).
#[derive(Debug)]
pub enum PlanOp {
    IndexScan {
        index_name: String,
        predicate: Predicate,
    },
    FullTextSearch {
        property: String,
        query: String,
    },
    VectorSearch {
        property: String,
        query: Vec<f32>,
        k: usize,
        ef: usize,
    },
    Filter {
        predicate: Predicate,
    },
    FuseSort {
        method: FusionMethod,
        limit: usize,
    },
    Expand {
        etype: String,
        direction: Direction,
        min_depth: usize,
        max_depth: usize,
    },
    Project {
        fields: Vec<String>,
    },
    Limit {
        count: usize,
    },
}
