//! Columnar property storage — one column per (label, property_name) pair.
//!
//! ## Design
//! - Each column is a `Vec<Option<Value>>`, indexed by `row_id` (u32).
//! - Columns are stored in a `DashMap<(LabelId, String), RwLock<PropertyColumn>>`
//!   — lock-free reads for column lookup, fine-grained locking for writes.
//! - Write-through persistence: each mutation logs the serialized column to disk.
//! - Recovery replays the log to rebuild all columns.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

use dashmap::DashMap;
use parking_lot::RwLock;

use super::store_log::StoreLog;
use crate::types::LabelId;

/// Supported value types in properties.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(ordered_float::OrderedFloat<f64>),
    String(String),
    List(Vec<Value>),
    Vector(Vec<f32>),
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

    pub fn as_vec_f32(&self) -> Option<&[f32]> {
        match self {
            Value::Vector(v) => Some(v.as_slice()),
            _ => None,
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Int(i) => write!(f, "{}", i),
            Value::Float(v) => write!(f, "{}", v),
            Value::String(s) => write!(f, "\"{}\"", s),
            Value::List(vs) => write!(f, "{:?}", vs),
            Value::Vector(v) => write!(f, "vec[{}]", v.len()),
        }
    }
}

/// One column: stores values for a single property across all rows of a label.
pub struct PropertyColumn {
    values: Vec<Option<Value>>,
}

impl PropertyColumn {
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            values: Vec::with_capacity(cap),
        }
    }

    /// Append a value at the end, return its row index.
    pub fn push(&mut self, val: Option<Value>) -> u32 {
        let row = self.values.len() as u32;
        self.values.push(val);
        row
    }

    #[inline]
    pub fn get(&self, row: u32) -> Option<&Option<Value>> {
        self.values.get(row as usize)
    }

    /// Set a value at an existing row. Returns false if out of bounds.
    pub fn set(&mut self, row: u32, val: Option<Value>) -> bool {
        if (row as usize) < self.values.len() {
            self.values[row as usize] = val;
            true
        } else {
            false
        }
    }

    /// Extend to at least `min_len` rows (fill with None).
    pub fn extend_to(&mut self, min_len: usize) {
        if self.values.len() < min_len {
            self.values.resize(min_len, None);
        }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }
}

/// Label-level property schema. Tracks which columns exist and the row count.
struct LabelSchema {
    next_row: AtomicU32,
}

impl LabelSchema {
    fn new() -> Self {
        Self {
            next_row: AtomicU32::new(0),
        }
    }
}

/// Property storage grouped by (label_id, property_name).
pub struct PropStore {
    /// Column data: (LabelId, "prop_name") → PropertyColumn
    columns: DashMap<(LabelId, String), RwLock<PropertyColumn>>,
    /// Per-label row counter
    schemas: DashMap<LabelId, LabelSchema>,
    /// Persistent log (None = memory-only).
    log: Option<std::sync::Mutex<StoreLog>>,
}

impl PropStore {
    pub fn new() -> Self {
        Self {
            columns: DashMap::new(),
            schemas: DashMap::new(),
            log: None,
        }
    }

    /// Open a persistent PropStore.
    pub fn open(data_dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let log_path = data_dir.join("props.log");

        let mut store = Self {
            columns: DashMap::new(),
            schemas: DashMap::new(),
            log: None,
        };

        if log_path.exists() {
            super::store_log::replay_log(&log_path, |opcode, payload| {
                if opcode == 1 {
                    // Payload: [label_id: u32 LE][prop_name_len: u8][prop_name: bytes][values: bincode...]
                    if payload.len() < 6 { return; }
                    let label = u32::from_le_bytes(payload[..4].try_into().unwrap());
                    let name_len = payload[4] as usize;
                    if payload.len() < 5 + name_len { return; }
                    let prop_name = String::from_utf8_lossy(&payload[5..5+name_len]).to_string();
                    let val_data = &payload[5+name_len..];
                    if let Ok(values) = bincode::deserialize::<Vec<Option<Value>>>(val_data) {
                        let len = values.len() as u32;
                        let col = PropertyColumn { values };
                        store.columns.insert((label, prop_name.clone()), RwLock::new(col));
                        // Update row counter
                        store.schemas.entry(label).or_insert_with(LabelSchema::new)
                            .next_row.fetch_max(len, Ordering::SeqCst);
                    }
                }
            })?;
        }

        store.log = Some(std::sync::Mutex::new(StoreLog::open(&log_path)?));
        Ok(store)
    }

    /// Persist a column to disk.
    fn persist_column(&self, label: LabelId, prop_name: &str) {
        if let Some(ref log) = self.log {
            if let Some(col) = self.columns.get(&(label, prop_name.to_string())) {
                let col = col.read();
                let mut payload: Vec<u8> = Vec::new();
                payload.extend_from_slice(&label.to_le_bytes());
                payload.push(prop_name.len() as u8);
                payload.extend_from_slice(prop_name.as_bytes());
                if let Ok(val_data) = bincode::serialize(&col.values) {
                    payload.extend_from_slice(&val_data);
                    let _ = log.lock().unwrap().append_insert(&payload);
                }
            }
        }
    }

    // ── Row allocation ────────────────────────────────────────────

    /// Allocate a new row for the given label and return its row_id.
    pub fn alloc_row(&self, label: LabelId) -> u32 {
        let schema = self
            .schemas
            .entry(label)
            .or_insert_with(LabelSchema::new);
        schema.next_row.fetch_add(1, Ordering::SeqCst)
    }

    // ── Single-property access ────────────────────────────────────

    /// Insert a property value for a specific (label, prop) at a given row.
    /// Returns the row index.
    pub fn insert_prop(&self, label: LabelId, prop: &str, row: u32, value: Option<Value>) -> u32 {
        let key = (label, prop.to_string());
        let mut col = self
            .columns
            .entry(key)
            .or_insert_with(|| RwLock::new(PropertyColumn::new()));
        let mut col = col.write();
        col.extend_to(row as usize + 1);
        col.set(row, value);
        drop(col); // release RwLock before persist
        self.persist_column(label, prop);
        row
    }

    /// Get a single property value. Returns None if column or row doesn't exist.
    pub fn get_prop(&self, label: LabelId, prop: &str, row: u32) -> Option<Value> {
        let key = (label, prop.to_string());
        self.columns
            .get(&key)
            .and_then(|col| {
                let col = col.read();
                col.get(row).and_then(|v| v.clone())
            })
    }

    /// Set a single property value. Returns false if out of bounds.
    pub fn set_prop(&self, label: LabelId, prop: &str, row: u32, value: Option<Value>) -> bool {
        let key = (label, prop.to_string());
        let ok = match self.columns.get(&key) {
            Some(col) => col.write().set(row, value),
            None => false,
        };
        if ok {
            self.persist_column(label, prop);
        }
        ok
    }

    // ── Batch row insert ──────────────────────────────────────────

    /// Insert a full row of properties at once. Allocates the row and sets all
    /// given properties. Returns the row index.
    pub fn insert_row(
        &self,
        label: LabelId,
        props: &[(String, Value)],
    ) -> u32 {
        let row = self.alloc_row(label);

        for (prop_name, value) in props {
            self.insert_prop(label, prop_name, row, Some(value.clone()));
        }
        row
    }

    /// Get all properties for a given label and row.
    pub fn get_row(&self, label: LabelId, row: u32) -> Vec<(String, Value)> {
        let prefix = (label, String::new());
        let mut result = Vec::new();

        // Iterate all columns for this label (prefix scan on DashMap is not supported,
        // so we filter by key. For production, a secondary index by label would be needed.)
        for entry in self.columns.iter() {
            let (lbl, prop_name) = entry.key();
            if *lbl == label {
                let col = entry.value().read();
                if let Some(Some(val)) = col.get(row) {
                    result.push((prop_name.clone(), val.clone()));
                }
            }
        }
        result
    }

    // ── Statistics ────────────────────────────────────────────────

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn label_count(&self) -> usize {
        self.schemas.len()
    }

    pub fn row_count(&self, label: LabelId) -> u32 {
        self.schemas
            .get(&label)
            .map(|s| s.next_row.load(Ordering::SeqCst))
            .unwrap_or(0)
    }

    /// Get a reference to the column's read lock for bulk operations.
    pub fn get_column(
        &self,
        label: LabelId,
        prop: &str,
    ) -> Option<dashmap::mapref::one::Ref<'_, (LabelId, String), RwLock<PropertyColumn>>> {
        self.columns.get(&(label, prop.to_string()))
    }
}

impl Default for PropStore {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_store() -> PropStore {
        PropStore::new()
    }

    fn v_int(i: i64) -> Value {
        Value::Int(i)
    }

    fn v_str(s: &str) -> Value {
        Value::String(s.to_string())
    }

    // ──────────────── Single Property ────────────────

    #[test]
    fn test_insert_and_get_single_prop() {
        let store = mk_store();
        store.insert_prop(0, "name", 0, Some(v_str("Alice")));

        assert_eq!(store.get_prop(0, "name", 0), Some(v_str("Alice")));
        assert_eq!(store.get_prop(0, "name", 1), None);
        assert_eq!(store.get_prop(0, "missing", 0), None);
    }

    #[test]
    fn test_multi_row_properties() {
        let store = mk_store();

        store.insert_prop(0, "age", 0, Some(v_int(30)));
        store.insert_prop(0, "age", 1, Some(v_int(25)));
        store.insert_prop(0, "age", 2, Some(v_int(42)));

        assert_eq!(store.get_prop(0, "age", 0), Some(v_int(30)));
        assert_eq!(store.get_prop(0, "age", 1), Some(v_int(25)));
        assert_eq!(store.get_prop(0, "age", 2), Some(v_int(42)));
    }

    #[test]
    fn test_multi_column_per_label() {
        let store = mk_store();

        store.insert_prop(0, "name", 0, Some(v_str("Alice")));
        store.insert_prop(0, "age", 0, Some(v_int(30)));

        assert_eq!(store.get_prop(0, "name", 0), Some(v_str("Alice")));
        assert_eq!(store.get_prop(0, "age", 0), Some(v_int(30)));
    }

    #[test]
    fn test_multi_label_isolation() {
        let store = mk_store();

        store.insert_prop(0, "name", 0, Some(v_str("Alice")));
        store.insert_prop(1, "name", 0, Some(v_str("Bob")));

        assert_eq!(store.get_prop(0, "name", 0), Some(v_str("Alice")));
        assert_eq!(store.get_prop(1, "name", 0), Some(v_str("Bob")));
    }

    #[test]
    fn test_set_prop_overwrites() {
        let store = mk_store();

        store.insert_prop(0, "name", 0, Some(v_str("Alice")));
        assert!(store.set_prop(0, "name", 0, Some(v_str("Bob"))));
        assert_eq!(store.get_prop(0, "name", 0), Some(v_str("Bob")));
    }

    #[test]
    fn test_set_prop_returns_false_for_missing_column() {
        let store = mk_store();
        assert!(!store.set_prop(0, "name", 0, Some(v_str("X"))));
    }

    #[test]
    fn test_null_property_supported() {
        let store = mk_store();

        store.insert_prop(0, "nickname", 0, None);
        assert_eq!(store.get_prop(0, "nickname", 0), None);

        store.insert_prop(0, "nickname", 1, Some(v_str("Al")));
        assert_eq!(store.get_prop(0, "nickname", 0), None);
        assert_eq!(store.get_prop(0, "nickname", 1), Some(v_str("Al")));
    }

    // ──────────────── Row-level operations ────────────────

    #[test]
    fn test_insert_row() {
        let store = mk_store();

        let row = store.insert_row(0, &[
            ("name".into(), v_str("Alice")),
            ("age".into(), v_int(30)),
            ("city".into(), v_str("Beijing")),
        ]);

        assert_eq!(row, 0);
        let props = store.get_row(0, 0);
        assert_eq!(props.len(), 3);

        let mut props = props;
        props.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(props[0], ("age".to_string(), v_int(30)));
        assert_eq!(props[1], ("city".to_string(), v_str("Beijing")));
        assert_eq!(props[2], ("name".to_string(), v_str("Alice")));
    }

    #[test]
    fn test_insert_multiple_rows() {
        let store = mk_store();

        let r0 = store.insert_row(0, &[("name".into(), v_str("Alice"))]);
        let r1 = store.insert_row(0, &[("name".into(), v_str("Bob"))]);
        let r2 = store.insert_row(0, &[("name".into(), v_str("Charlie"))]);

        assert_eq!(r0, 0);
        assert_eq!(r1, 1);
        assert_eq!(r2, 2);

        assert_eq!(store.row_count(0), 3);
    }

    #[test]
    fn test_get_row_returns_only_set_props() {
        let store = mk_store();

        store.insert_prop(0, "name", 0, Some(v_str("Alice")));
        store.insert_prop(0, "age", 10, Some(v_int(30)));

        let r0 = store.get_row(0, 0);
        assert_eq!(r0.len(), 1);
        assert_eq!(r0[0], ("name".into(), v_str("Alice")));

        let r10 = store.get_row(0, 10);
        assert_eq!(r10.len(), 1);
        assert_eq!(r10[0], ("age".into(), v_int(30)));
    }

    // ──────────────── Value Types ────────────────

    #[test]
    fn test_all_value_types() {
        let store = mk_store();

        store.insert_prop(0, "null_val", 0, Some(Value::Null));
        store.insert_prop(0, "bool_val", 0, Some(Value::Bool(true)));
        store.insert_prop(0, "int_val", 0, Some(Value::Int(-42)));
        store.insert_prop(0, "float_val", 0, Some(Value::Float(ordered_float::OrderedFloat(3.14))));
        store.insert_prop(0, "str_val", 0, Some(Value::String("hello".into())));
        store.insert_prop(0, "list_val", 0, Some(Value::List(vec![v_int(1), v_int(2)])));
        store.insert_prop(0, "vec_val", 0, Some(Value::Vector(vec![0.1, 0.2, 0.3])));

        assert_eq!(store.get_prop(0, "null_val", 0), Some(Value::Null));
        assert_eq!(store.get_prop(0, "bool_val", 0), Some(Value::Bool(true)));
        assert_eq!(store.get_prop(0, "int_val", 0), Some(Value::Int(-42)));
        assert!(matches!(store.get_prop(0, "float_val", 0), Some(Value::Float(_))));
        assert_eq!(store.get_prop(0, "str_val", 0), Some(Value::String("hello".into())));
        assert_eq!(store.get_prop(0, "list_val", 0), Some(Value::List(vec![v_int(1), v_int(2)])));

        let vec_val = store.get_prop(0, "vec_val", 0);
        assert!(matches!(vec_val, Some(Value::Vector(_))));
        if let Some(Value::Vector(v)) = vec_val {
            assert_eq!(v, vec![0.1, 0.2, 0.3]);
        }
    }

    // ──────────────── Edge Cases ────────────────

    #[test]
    fn test_empty_store() {
        let store = mk_store();
        assert_eq!(store.column_count(), 0);
        assert_eq!(store.label_count(), 0);
        assert_eq!(store.row_count(0), 0);
    }

    #[test]
    fn test_large_row_count() {
        let store = mk_store();
        for i in 0..5000 {
            store.insert_row(0, &[("id".into(), v_int(i as i64))]);
        }
        assert_eq!(store.row_count(0), 5000);

        // Spot checks
        assert_eq!(store.get_prop(0, "id", 0), Some(v_int(0)));
        assert_eq!(store.get_prop(0, "id", 2500), Some(v_int(2500)));
        assert_eq!(store.get_prop(0, "id", 4999), Some(v_int(4999)));
        assert_eq!(store.get_prop(0, "id", 5000), None);
    }

    #[test]
    fn test_sparse_columns() {
        let store = mk_store();

        // Column "name": rows 0, 2, 4
        store.insert_prop(0, "name", 0, Some(v_str("A")));
        store.insert_prop(0, "name", 2, Some(v_str("C")));
        store.insert_prop(0, "name", 4, Some(v_str("E")));

        // Column "age": rows 1, 3, 5
        store.insert_prop(0, "age", 1, Some(v_int(10)));
        store.insert_prop(0, "age", 3, Some(v_int(30)));
        store.insert_prop(0, "age", 5, Some(v_int(50)));

        // Verify sparse reads
        assert_eq!(store.get_prop(0, "name", 0), Some(v_str("A")));
        assert_eq!(store.get_prop(0, "name", 1), None); // not set
        assert_eq!(store.get_prop(0, "name", 2), Some(v_str("C")));

        assert_eq!(store.get_prop(0, "age", 0), None); // not set
        assert_eq!(store.get_prop(0, "age", 1), Some(v_int(10)));
    }

    #[test]
    fn test_concurrent_row_insertion() {
        use std::sync::Arc;
        use std::thread;

        let store = Arc::new(PropStore::new());
        let mut handles = Vec::new();

        for t in 0..8 {
            let s = Arc::clone(&store);
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    s.insert_row(
                        (t as u32),
                        &[("seq".into(), v_int((t * 100 + i) as i64))],
                    );
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Each label should have 100 rows
        for t in 0..8 {
            assert_eq!(store.row_count(t as u32), 100);
        }
    }
}
