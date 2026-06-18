// FlatBuffers-based full snapshot for fast recovery.
//
// Every N minutes or when WAL exceeds a size threshold, a full
// snapshot is dumped. The snapshot uses FlatBuffers for zero-copy
// deserialization — mmap the file and access directly.

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[allow(dead_code)]
pub struct SnapshotManager {
    data_dir: PathBuf,
    current_snapshot_id: u64,
    wal_since_snapshot: u64, // bytes written to WAL since last snapshot
    snapshot_interval_ms: u64,
    snapshot_max_wal_bytes: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub latest_snapshot_id: u64,
    pub latest_snapshot_path: String,
    pub merged_wal_files: Vec<String>, // WAL files fully incorporated into snapshots
}

impl SnapshotManager {
    pub fn new(data_dir: &Path) -> Self {
        Self {
            data_dir: data_dir.to_path_buf(),
            current_snapshot_id: 0,
            wal_since_snapshot: 0,
            snapshot_interval_ms: 600_000,        // 10 minutes
            snapshot_max_wal_bytes: 536_870_912,  // 512 MB
        }
    }

    /// Check if a snapshot should be taken now.
    pub fn should_snapshot(&self) -> bool {
        self.wal_since_snapshot >= self.snapshot_max_wal_bytes
            || self.current_snapshot_id == 0
    }

    /// Write a full snapshot. Placeholder — Sprint 4.
    pub fn write_snapshot(&mut self) -> std::io::Result<u64> {
        let id = self.current_snapshot_id + 1;
        let _path = self.data_dir.join(format!("snapshot-{:04}.flat", id));

        // TODO: Serialize nodes, edges, and index metadata via FlatBuffers
        self.current_snapshot_id = id;
        self.wal_since_snapshot = 0;
        Ok(id)
    }

    /// Load latest snapshot via mmap (zero-copy).
    pub fn load_latest(&self) -> std::io::Result<()> {
        let _manifest = self.read_manifest()?;
        // TODO: mmap the flatbuffers file and reconstruct indexes
        Ok(())
    }

    fn read_manifest(&self) -> std::io::Result<Manifest> {
        let path = self.data_dir.join("manifest");
        let data = fs::read_to_string(path)?;
        Ok(serde_json::from_str(&data)?)
    }
}
