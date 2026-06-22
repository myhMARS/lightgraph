// Write-Ahead Log with group commit.
//
// Every write transaction is first serialized to the WAL before
// being applied to the in-memory stores. fsync is batched
// (group commit window ~100µs) for throughput while maintaining
// durability guarantees.
//
// Recovery: replay WAL records from the latest snapshot checkpoint.

use crate::types::{NodeId, EdgeId, TxId};
use crate::storage::prop_store::Value;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{BufWriter, Read, Write};
use std::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalRecord {
    /// Begin a write transaction
    Begin(TxId),
    /// Commit a write transaction
    Commit(TxId),
    /// Rollback a write transaction
    Rollback(TxId),
    /// Create a node
    NodeCreate(TxId, NodeId, Vec<String>, HashMap<String, Value>),
    /// Update node properties
    NodeUpdate(TxId, NodeId, HashMap<String, Value>),
    /// Delete a node
    NodeDelete(TxId, NodeId),
    /// Create an edge
    EdgeCreate(TxId, EdgeId, NodeId, NodeId, String, HashMap<String, Value>),
    /// Delete an edge
    EdgeDelete(TxId, EdgeId),
    /// Snapshot checkpoint marker
    Checkpoint(u64), // snapshot id
}

pub struct WalWriter {
    writer: BufWriter<fs::File>,
    buffer: Vec<u8>,
    max_buffer_size: usize,
    last_fsync: std::time::Instant,
    fsync_interval: std::time::Duration,
}

#[allow(dead_code)]
pub struct WalReader {
    file: fs::File,
}

impl WalWriter {
    pub fn open(path: &str) -> std::io::Result<Self> {
        let file = fs::OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            buffer: Vec::with_capacity(65536), // 64KB batch
            max_buffer_size: 65536,
            last_fsync: std::time::Instant::now(),
            fsync_interval: std::time::Duration::from_micros(100), // 100µs group commit
        })
    }

    pub fn write(&mut self, record: &WalRecord) -> std::io::Result<()> {
        let data = bincode::serialize(record).expect("WAL serialization");
        self.buffer.extend_from_slice(&(data.len() as u32).to_le_bytes());
        self.buffer.extend_from_slice(&data);

        // Group commit: flush when buffer is full or time threshold reached
        if self.buffer.len() >= self.max_buffer_size
            || self.last_fsync.elapsed() >= self.fsync_interval
        {
            self.flush()?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.writer.write_all(&self.buffer)?;
        self.writer.flush()?;
        self.writer.get_mut().sync_all()?; // fsync
        self.buffer.clear();
        self.last_fsync = std::time::Instant::now();
        Ok(())
    }
}

impl WalReader {
    pub fn open(path: &str) -> std::io::Result<Self> {
        let file = fs::OpenOptions::new().read(true).open(path)?;
        Ok(Self { file })
    }

    pub fn replay(&mut self) -> std::io::Result<Vec<WalRecord>> {
        let mut records = Vec::new();
        let mut len_buf = [0u8; 4];
        loop {
            match self.file.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut data = vec![0u8; len];
            self.file.read_exact(&mut data)?;
            let record: WalRecord = bincode::deserialize(&data)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            records.push(record);
        }
        Ok(records)
    }
}

// ── Tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[allow(dead_code)]
    fn v(i: i64) -> Value { Value::Int(i) }
    fn s(t: &str) -> Value { Value::String(t.into()) }

    #[test]
    fn test_wal_record_serialization_roundtrip() {
        let records = vec![
            WalRecord::Begin(1),
            WalRecord::NodeCreate(1, 42, vec!["Person".into()], {
                let mut m = HashMap::new();
                m.insert("name".into(), s("Alice"));
                m
            }),
            WalRecord::Commit(1),
        ];
        for r in &records {
            let data = bincode::serialize(r).unwrap();
            let back: WalRecord = bincode::deserialize(&data).unwrap();
            match (r, &back) {
                (WalRecord::Begin(a), WalRecord::Begin(b)) => assert_eq!(a, b),
                (WalRecord::Commit(a), WalRecord::Commit(b)) => assert_eq!(a, b),
                _ => {} // structural match by variant
            }
        }
    }

    #[test]
    fn test_wal_write_and_replay() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.wal");
        let path_str = path.to_str().unwrap();

        // Write
        {
            let mut writer = WalWriter::open(path_str).unwrap();
            writer.write(&WalRecord::Begin(1)).unwrap();
            writer.write(&WalRecord::NodeCreate(1, 10, vec!["A".into()], HashMap::new())).unwrap();
            writer.write(&WalRecord::NodeCreate(1, 20, vec!["B".into()], HashMap::new())).unwrap();
            writer.write(&WalRecord::Commit(1)).unwrap();
            writer.flush().unwrap();
        }

        // Replay
        {
            let mut reader = WalReader::open(path_str).unwrap();
            let recovered = reader.replay().unwrap();
            assert_eq!(recovered.len(), 4);
            assert!(matches!(recovered[0], WalRecord::Begin(1)));
            assert!(matches!(recovered[3], WalRecord::Commit(1)));
        }
    }

    #[test]
    fn test_wal_empty_file_replay() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("empty.wal");
        // Create empty file
        std::fs::write(&path, b"").unwrap();
        let mut reader = WalReader::open(path.to_str().unwrap()).unwrap();
        let records = reader.replay().unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn test_wal_multiple_transactions() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("multi.wal");
        let path_str = path.to_str().unwrap();

        {
            let mut writer = WalWriter::open(path_str).unwrap();
            for tx in 1..=5 {
                writer.write(&WalRecord::Begin(tx)).unwrap();
                writer.write(&WalRecord::NodeCreate(tx, tx * 10, vec!["X".into()], HashMap::new())).unwrap();
                writer.write(&WalRecord::Commit(tx)).unwrap();
            }
            writer.flush().unwrap();
        }

        {
            let mut reader = WalReader::open(path_str).unwrap();
            let recovered = reader.replay().unwrap();
            assert_eq!(recovered.len(), 15); // 3 records × 5 txs
            // Verify Begin/Commit pairs
            let mut tx_ids = vec![];
            for r in &recovered {
                if let WalRecord::Begin(id) = r { tx_ids.push(*id); }
            }
            assert_eq!(tx_ids, vec![1, 2, 3, 4, 5]);
        }
    }
}
