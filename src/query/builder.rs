// Query builder — chainable DSL for constructing hybrid queries.
//
// Example:
//   QueryBuilder::new()
//     .fulltext("description", "机械键盘")
//     .vector("embedding", &my_vec, 20, 200)
//     .filter(eq("category", "外设").and(gt("price", 100.0)))
//     .fusion(FusionMethod::WeightedSum { alpha: 0.4, beta: 0.6 })
//     .traverse("SIMILAR_TO", Direction::Out, 1, None)
//     .limit(20)
//     .build()

use crate::index::property::Predicate;
use crate::query::plan::QueryPlan;
use crate::query::fusion::FusionMethod;
use super::FilterTiming;

pub struct QueryBuilder {
    fulltext_text: Option<String>,
    fulltext_prop: Option<String>,
    vector_prop: Option<String>,
    vector_query: Option<Vec<f32>>,
    vector_k: usize,
    vector_ef: usize,
    predicates: Vec<Predicate>,
    fusion: FusionMethod,
    filter_timing: FilterTiming,
    traversal: Option<TraversalClause>,
    limit: usize,
}

#[derive(Debug, Clone)]
pub struct TraversalClause {
    pub etype: String,
    pub direction: Direction,
    pub min_depth: usize,
    pub max_depth: usize,
    pub edge_predicates: Vec<Predicate>,
    pub node_predicates: Vec<Predicate>,
}

#[derive(Debug, Clone)]
pub enum Direction {
    Out,
    In,
    Both,
}

impl QueryBuilder {
    pub fn new() -> Self {
        Self {
            fulltext_text: None,
            fulltext_prop: None,
            vector_prop: None,
            vector_query: None,
            vector_k: 20,
            vector_ef: 200,
            predicates: Vec::new(),
            fusion: FusionMethod::WeightedSum { alpha: 1.0, beta: 0.0 },
            filter_timing: FilterTiming::Adaptive,
            traversal: None,
            limit: 20,
        }
    }

    pub fn fulltext(mut self, property: &str, query: &str) -> Self {
        self.fulltext_prop = Some(property.to_string());
        self.fulltext_text = Some(query.to_string());
        self
    }

    pub fn vector(mut self, property: &str, query: Vec<f32>, k: usize, ef: usize) -> Self {
        self.vector_prop = Some(property.to_string());
        self.vector_query = Some(query);
        self.vector_k = k;
        self.vector_ef = ef;
        self
    }

    pub fn filter(mut self, pred: Predicate) -> Self {
        self.predicates.push(pred);
        self
    }

    pub fn fusion(mut self, method: FusionMethod) -> Self {
        self.fusion = method;
        self
    }

    pub fn filter_timing(mut self, timing: FilterTiming) -> Self {
        self.filter_timing = timing;
        self
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.limit = n;
        self
    }

    pub fn build(self) -> QueryPlan {
        let combined_pred = self.predicates
            .into_iter()
            .reduce(|a, b| Predicate::And(Box::new(a), Box::new(b)));

        QueryPlan {
            fulltext_prop: self.fulltext_prop,
            fulltext_text: self.fulltext_text,
            vector_prop: self.vector_prop,
            vector_query: self.vector_query,
            vector_k: self.vector_k,
            vector_ef: self.vector_ef,
            predicate: combined_pred,
            fusion: self.fusion,
            filter_timing: self.filter_timing,
            traversal: self.traversal,
            limit: self.limit,
        }
    }
}
