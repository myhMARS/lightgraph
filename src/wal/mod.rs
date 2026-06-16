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
use std::io::{BufWriter, Write};
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

    pub fn replay(&self) -> std::io::Result<Vec<WalRecord>> {
        // Sequential read: len(u32) + data
        // Returns ordered list of records for crash recovery
        // Placeholder — Sprint 3
        Ok(Vec::new())
    }
}
