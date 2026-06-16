//! Columnar property storage — one column per (label, property_name) pair.
//!
//! Advantages over row-based storage:
//! - SIMD-friendly scans when filtering a single property
//! - Better cache locality for predicate evaluation
//! - Compact representation

use crate::types::{NodeId, LabelId};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};

/// Supported value types in properties
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(ordered_float::OrderedFloat<f64>),
    String(String),
    List(Vec<Value>),
    Vector(Vec<f32>), // For embedding vectors
}

impl Value {
    pub fn as_f64(&self) -> f64 {
        match self {
            Value::Int(v) => *v as f64,
            Value::Float(v) => v.0,
            _ => 0.0,
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Value::String(s) => s.as_str(),
            _ => "",
        }
    }

    pub fn as_vec_f32(&self) -> Option<&Vec<f32>> {
        match self {
            Value::Vector(v) => Some(v),
            _ => None,
        }
    }
}

/// One column: stores values for a single property across all nodes of a label.
pub struct PropertyColumn {
    values: Vec<Option<Value>>, // index = row_id
}

impl PropertyColumn {
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }

    pub fn push(&mut self, val: Value) -> u32 {
        let row = self.values.len() as u32;
        self.values.push(Some(val));
        row
    }

    #[inline]
    pub fn get(&self, row: u32) -> Option<&Value> {
        self.values.get(row as usize).and_then(|v| v.as_ref())
    }
}

/// Property storage grouped by (label, property_name)
pub struct PropStore {
    columns: DashMap<(LabelId, String), PropertyColumn>,
}

impl PropStore {
    pub fn new() -> Self {
        Self { columns: DashMap::new() }
    }

    pub fn get_column(&self, label: LabelId, prop: &str) -> Option<dashmap::mapref::one::Ref<(LabelId, String), PropertyColumn>> {
        self.columns.get(&(label, prop.to_string()))
    }
}
