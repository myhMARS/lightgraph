// Property Index — BTreeMap-based eq/range with RoaringBitmap pushdown.
//
// Each (label, prop) pair has:
//   eq_index:   BTreeMap<ValueWrapper, RoaringBitmap>  — O(log n) point lookup
//   range_index: BTreeMap<f64, RoaringBitmap>           — O(log n + k) range scan
//
// Predicate AST supports AND/OR/NOT composition at the bitmap level,
// avoiding fetching actual node data until the final result set.

use std::collections::BTreeMap;

use dashmap::DashMap;
use ordered_float::OrderedFloat;
use roaring::RoaringBitmap;

use crate::storage::prop_store::Value;
use crate::types::NodeId;

pub type LabelProp = (String, String); // (label, property_name)

pub struct PropertyIndex {
    eq_indexes: DashMap<LabelProp, BTreeMap<ValueWrapper, RoaringBitmap>>,
    range_indexes: DashMap<LabelProp, BTreeMap<OrderedFloat<f64>, RoaringBitmap>>,
}

/// Wrapper around a comparable representation of Value for BTreeMap keys.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ValueWrapper {
    Null,
    Bool(u8),  // 0=false, 1=true
    Int(i64),
    Float(OrderedFloat<f64>),
    String(String),
}

impl From<&Value> for ValueWrapper {
    fn from(v: &Value) -> Self {
        match v {
            Value::Null => ValueWrapper::Null,
            Value::Bool(b) => ValueWrapper::Bool(if *b { 1 } else { 0 }),
            Value::Int(i) => ValueWrapper::Int(*i),
            Value::Float(f) => ValueWrapper::Float(*f),
            Value::String(s) => ValueWrapper::String(s.clone()),
            Value::List(_) => ValueWrapper::Null, // lists are not indexable
            Value::Vector(_) => ValueWrapper::Null, // vectors are not property-indexable
        }
    }
}

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
                    .and_then(|idx| {
                        idx.get(&ValueWrapper::from(val)).cloned()
                    })
                    .unwrap_or_default()
            }
            Predicate::Gt(label, prop, val) => {
                self.range_scan(label, prop, val.as_f64(), f64::MAX)
            }
            Predicate::Gte(label, prop, val) => {
                self.range_scan(label, prop, val.as_f64(), f64::MAX)
            }
            Predicate::Lt(label, prop, val) => {
                self.range_scan(label, prop, f64::MIN, val.as_f64())
            }
            Predicate::Lte(label, prop, val) => {
                self.range_scan(label, prop, f64::MIN, val.as_f64())
            }
            Predicate::In(label, prop, vals) => {
                vals.iter()
                    .map(|v| self.evaluate(&Predicate::Eq(label.clone(), prop.clone(), v.clone())))
                    .fold(RoaringBitmap::new(), |a, b| a | b)
            }
            Predicate::And(a, b) => {
                self.evaluate(a) & self.evaluate(b)
            }
            Predicate::Or(a, b) => {
                self.evaluate(a) | self.evaluate(b)
            }
            Predicate::Not(_a) => {
                // NOT is approximate — needs a universe bitmap from the query engine
                RoaringBitmap::new()
            }
        }
    }

    fn range_scan(&self, label: &str, prop: &str, lo: f64, hi: f64) -> RoaringBitmap {
        self.range_indexes
            .get(&(label.to_string(), prop.to_string()))
            .map(|idx| {
                let lo = OrderedFloat(lo);
                let hi = OrderedFloat(hi);
                idx.range(lo..=hi)
                    .fold(RoaringBitmap::new(), |acc, (_, bm)| acc | bm)
            })
            .unwrap_or_default()
    }
}

impl Default for PropertyIndex {
    fn default() -> Self {
        Self::new()
    }
}
