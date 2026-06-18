//! Columnar property storage — one column per (label, property_name) pair.
//!
//! ## Architecture
//!
//! Same as NodeStore/EdgeStore: DashMap cache + WalThread for persistence.
//! Each column mutation serializes the full column as a WAL record.

use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use parking_lot::RwLock;

use super::consistency::Consistency;
use super::wal_thread::WalThread;
use crate::types::LabelId;

/// Supported property value types.
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
        match self { Value::Int(v) => *v as f64, Value::Float(v) => v.0, _ => 0.0 }
    }
    pub fn as_str(&self) -> &str {
        match self { Value::String(s) => s.as_str(), _ => "" }
    }
    pub fn as_vec_f32(&self) -> Option<&[f32]> {
        match self { Value::Vector(v) => Some(v.as_slice()), _ => None }
    }
}

/// One column: stores values for a single property across all rows of a label.
pub struct PropertyColumn {
    values: Vec<Option<Value>>,
}

impl PropertyColumn {
    pub fn new() -> Self { Self { values: Vec::new() } }
    pub fn with_capacity(cap: usize) -> Self { Self { values: Vec::with_capacity(cap) } }
    pub fn push(&mut self, val: Option<Value>) -> u32 {
        let row = self.values.len() as u32;
        self.values.push(val);
        row
    }
    #[inline]
    pub fn get(&self, row: u32) -> Option<&Option<Value>> { self.values.get(row as usize) }
    pub fn set(&mut self, row: u32, val: Option<Value>) -> bool {
        if (row as usize) < self.values.len() { self.values[row as usize] = val; true }
        else { false }
    }
    pub fn extend_to(&mut self, min_len: usize) {
        if self.values.len() < min_len { self.values.resize(min_len, None); }
    }
    pub fn len(&self) -> usize { self.values.len() }
}

struct LabelSchema {
    next_row: AtomicU32,
}
impl LabelSchema {
    fn new() -> Self { Self { next_row: AtomicU32::new(0) } }
}

pub struct PropStore {
    columns: DashMap<(LabelId, String), RwLock<PropertyColumn>>,
    schemas: DashMap<LabelId, LabelSchema>,
    wal: Option<WalThread>,
}

impl PropStore {
    pub fn new() -> Self {
        Self { columns: DashMap::new(), schemas: DashMap::new(), wal: None }
    }

    /// Open with an explicit consistency contract.
    pub fn open(data_dir: &Path, consistency: Consistency) -> io::Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let wal_path = data_dir.join("props.wal");

        let mut store = Self { columns: DashMap::new(), schemas: DashMap::new(), wal: None };

        if wal_path.exists() && std::fs::metadata(&wal_path)?.len() > 4 {
            super::wal_thread::replay_wal(&wal_path, |opcode, _id, payload| {
                if opcode == 1 && payload.len() >= 6 {
                    let label = u32::from_le_bytes(payload[..4].try_into().unwrap());
                    let name_len = payload[4] as usize;
                    if payload.len() < 5 + name_len { return; }
                    let prop_name = String::from_utf8_lossy(&payload[5..5+name_len]).to_string();
                    let val_data = &payload[5+name_len..];
                    if let Ok(values) = bincode::deserialize::<Vec<Option<Value>>>(val_data) {
                        let len = values.len() as u32;
                        store.columns.insert((label, prop_name), RwLock::new(PropertyColumn { values }));
                        store.schemas.entry(label).or_insert_with(LabelSchema::new)
                            .next_row.fetch_max(len, Ordering::SeqCst);
                    }
                }
            })?;
        }

        store.wal = Some(WalThread::spawn(
            &data_dir.join("props.wal"), consistency.durability, consistency.wal_channel_capacity)?);
        Ok(store)
    }

    /// Serialize and send a column to the WAL thread.
    fn wal_send_column(&self, label: LabelId, prop_name: &str) {
        if let Some(ref wal) = self.wal {
            if let Some(col) = self.columns.get(&(label, prop_name.to_string())) {
                let col = col.read();
                let mut payload: Vec<u8> = Vec::new();
                payload.extend_from_slice(&label.to_le_bytes());
                payload.push(prop_name.len() as u8);
                payload.extend_from_slice(prop_name.as_bytes());
                if let Ok(val_data) = bincode::serialize(&col.values) {
                    payload.extend_from_slice(&val_data);
                    wal.send_insert(label as u64, payload);
                }
            }
        }
    }

    // ── API ────────────────────────────────────────────────────────

    pub fn alloc_row(&self, label: LabelId) -> u32 {
        self.schemas.entry(label).or_insert_with(LabelSchema::new)
            .next_row.fetch_add(1, Ordering::SeqCst)
    }

    pub fn insert_prop(&self, label: LabelId, prop: &str, row: u32, value: Option<Value>) -> u32 {
        let key = (label, prop.to_string());
        // Scope the DashMap RefMut so it's dropped before wal_send_column,
        // which also needs to access the DashMap (avoiding a shard-lock deadlock).
        {
            let col = self.columns.entry(key).or_insert_with(|| RwLock::new(PropertyColumn::new()));
            let mut col = col.write();
            col.extend_to(row as usize + 1);
            col.set(row, value);
        }
        self.wal_send_column(label, prop);
        row
    }

    pub fn get_prop(&self, label: LabelId, prop: &str, row: u32) -> Option<Value> {
        self.columns.get(&(label, prop.to_string()))
            .and_then(|col| col.read().get(row).and_then(|v| v.clone()))
    }

    pub fn set_prop(&self, label: LabelId, prop: &str, row: u32, value: Option<Value>) -> bool {
        let ok = match self.columns.get(&(label, prop.to_string())) {
            Some(col) => col.write().set(row, value),
            None => false,
        };
        if ok { self.wal_send_column(label, prop); }
        ok
    }

    pub fn insert_row(&self, label: LabelId, props: &[(String, Value)]) -> u32 {
        let row = self.alloc_row(label);
        for (prop_name, value) in props {
            self.insert_prop(label, prop_name, row, Some(value.clone()));
        }
        row
    }

    pub fn get_row(&self, label: LabelId, row: u32) -> Vec<(String, Value)> {
        let mut result = Vec::new();
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

    // ── Stats ─────────────────────────────────────────────────────

    pub fn column_count(&self) -> usize { self.columns.len() }
    pub fn label_count(&self) -> usize { self.schemas.len() }
    pub fn row_count(&self, label: LabelId) -> u32 {
        self.schemas.get(&label).map(|s| s.next_row.load(Ordering::SeqCst)).unwrap_or(0)
    }

    pub fn flush(&self) {
        if let Some(ref wal) = self.wal { wal.flush(); }
    }
}

// ── Tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn v(i: i64) -> Value { Value::Int(i) }
    fn s(t: &str) -> Value { Value::String(t.into()) }

    #[test]
    fn test_persist_and_recover() {
        let dir = TempDir::new().unwrap();
        let p = dir.path();
        {
            let st = PropStore::open(p, Consistency::immediate()).unwrap();
            st.insert_row(0, &[("name".into(), s("Alice")), ("age".into(), v(30))]);
            st.insert_row(0, &[("name".into(), s("Bob"))]);
            st.flush();
        }
        {
            let st = PropStore::open(p, Consistency::immediate()).unwrap();
            assert_eq!(st.row_count(0), 2);
            assert_eq!(st.get_prop(0, "name", 0), Some(s("Alice")));
            assert_eq!(st.get_prop(0, "age", 0), Some(v(30)));
            assert_eq!(st.get_prop(0, "name", 1), Some(s("Bob")));
        }
    }

    #[test]
    fn test_insert_and_get() {
        let st = PropStore::new();
        st.insert_prop(0, "name", 0, Some(s("Alice")));
        assert_eq!(st.get_prop(0, "name", 0), Some(s("Alice")));
        assert_eq!(st.get_prop(0, "missing", 0), None);
    }

    #[test]
    fn test_multi_label_isolation() {
        let st = PropStore::new();
        st.insert_prop(0, "name", 0, Some(s("Alice")));
        st.insert_prop(1, "name", 0, Some(s("Bob")));
        assert_eq!(st.get_prop(0, "name", 0), Some(s("Alice")));
        assert_eq!(st.get_prop(1, "name", 0), Some(s("Bob")));
    }

    #[test]
    fn test_concurrent_writes() {
        use std::sync::Arc;
        use std::thread;
        let st = Arc::new(PropStore::new());
        let mut hs = Vec::new();
        for t in 0..4 {
            let s = Arc::clone(&st);
            hs.push(thread::spawn(move || {
                for i in 0..100 {
                    s.insert_row(t, &[("seq".into(), v((t*100+i) as i64))]);
                }
            }));
        }
        for h in hs { h.join().unwrap(); }
        for t in 0..4 { assert_eq!(st.row_count(t), 100); }
    }
}
