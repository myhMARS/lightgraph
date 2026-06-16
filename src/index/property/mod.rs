// Property Index — BTreeMap-based eq/range with RoaringBitmap pushdown.
//
// Each (label, prop) pair has:
//   eq_index:   BTreeMap<Value, RoaringBitmap>  — O(log n) point lookup
//   range_index: BTreeMap<f64, RoaringBitmap>    — O(log n + k) range scan
//
// Predicate AST supports AND/OR/NOT composition at the bitmap level,
// avoiding fetching actual node data until the final result set.

use crate::types::NodeId;
use crate::storage::prop_store::Value;
use roaring::RoaringBitmap;
use std::collections::BTreeMap;
use dashmap::DashMap;

pub type LabelProp = (String, String); // (label, property_name)

pub struct PropertyIndex {
    eq_indexes: DashMap<LabelProp, BTreeMap<ValueWrapper, RoaringBitmap>>,
    range_indexes: DashMap<LabelProp, BTreeMap<ordered_float::OrderedFloat<f64>, RoaringBitmap>>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ValueWrapper(ordered_float::OrderedFloat<f64>); // value → wrapper for BTreeMap key

/// Predicate AST — compiled from Query DSL, evaluated against indexes.
#[derive(Debug, Clone)]
pub enum Predicate {
    /// property == value
    Eq(String, String, Value),
    /// property > value
    Gt(String, String, Value),
    /// property >= value
    Gte(String, String, Value),
    /// property < value
    Lt(String, String, Value),
    /// property <= value
    Lte(String, String, Value),
    /// property IN [values]
    In(String, String, Vec<Value>),
    /// Predicate AND Predicate
    And(Box<Predicate>, Box<Predicate>),
    /// Predicate OR Predicate
    Or(Box<Predicate>, Box<Predicate>),
    /// NOT Predicate
    Not(Box<Predicate>),
}

impl PropertyIndex {
    pub fn new() -> Self {
        Self {
            eq_indexes: DashMap::new(),
            range_indexes: DashMap::new(),
        }
    }

    /// Evaluate a predicate → RoaringBitmap of matching NodeIds.
    /// Pure bitmap operations — no node data access required.
    pub fn evaluate(&self, pred: &Predicate) -> RoaringBitmap {
        match pred {
            Predicate::Eq(label, prop, val) => {
                self.eq_indexes
                    .get(&(label.clone(), prop.clone()))
                    .and_then(|idx| idx.get(&ValueWrapper::from(val)).cloned())
                    .unwrap_or_default()
            }
            Predicate::Gt(label, prop, val) => {
                self.range_scan(label, prop, val.as_f64(), f64::MAX)
            }
            Predicate::Lt(label, prop, val) => {
                self.range_scan(label, prop, f64::MIN, val.as_f64())
            }
            Predicate::Gte(label, prop, val) => {
                self.range_scan(label, prop, val.as_f64(), f64::MAX)
            }
            Predicate::Lte(label, prop, val) => {
                self.range_scan(label, prop, f64::MIN, val.as_f64())
            }
            Predicate::In(label, prop, vals) => {
                vals.iter()
                    .map(|v| Self::evaluate(self, &Predicate::Eq(label.clone(), prop.clone(), v.clone())))
                    .fold(RoaringBitmap::new(), |a, b| a | b)
            }
            Predicate::And(a, b) => {
                Self::evaluate(self, a) & Self::evaluate(self, b)
            }
            Predicate::Or(a, b) => {
                Self::evaluate(self, a) | Self::evaluate(self, b)
            }
            Predicate::Not(a) => {
                // NOT is approximate — returns empty if no "all nodes" bitmap
                // In practice, `NOT(p)` needs a universe set to subtract from.
                // This is handled by the query engine with a universe bitmap.
                RoaringBitmap::new()
            }
        }
    }

    fn range_scan(&self, label: &str, prop: &str, lo: f64, hi: f64) -> RoaringBitmap {
        self.range_indexes
            .get(&(label.to_string(), prop.to_string()))
            .map(|idx| {
                let lo = ordered_float::OrderedFloat(lo);
                let hi = ordered_float::OrderedFloat(hi);
                idx.range(lo..=hi)
                    .fold(RoaringBitmap::new(), |acc, (_, bm)| acc | bm)
            })
            .unwrap_or_default()
    }
}
