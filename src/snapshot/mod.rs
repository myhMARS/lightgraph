//! Full snapshot for fast recovery — Sprint 4.
//!
//! ## Architecture
//!
//! ```text
//! [Memory State] --> SnapshotWriter --> snapshot-N.bin
//!                                         |
//! [Recovery] SnapshotReader --> load snapshot --> replay WAL --> ready
//! ```
//!
//! - Snapshots use bincode serialization for speed (FlatBuffers schema in schema.fbs)
//! - Manifest tracks the latest snapshot + merged WAL files
//! - Recovery: load latest snapshot, replay WAL records since snapshot

use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::storage::Node;
use crate::storage::Edge;
use crate::storage::prop_store::Value;
use crate::types::LabelId;

// ── Manifest ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub latest_snapshot_id: u64,
    pub latest_snapshot_path: String,
    pub merged_wal_files: Vec<String>,
}

impl Manifest {
    pub fn empty() -> Self {
        Self {
            latest_snapshot_id: 0,
            latest_snapshot_path: String::new(),
            merged_wal_files: Vec::new(),
        }
    }

    pub fn read(data_dir: &Path) -> std::io::Result<Self> {
        let path = data_dir.join("manifest.json");
        match fs::read_to_string(&path) {
            Ok(data) => Ok(serde_json::from_str(&data)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::empty()),
            Err(e) => Err(e),
        }
    }

    pub fn write(&self, data_dir: &Path) -> std::io::Result<()> {
        let path = data_dir.join("manifest.json");
        let data = serde_json::to_string_pretty(self)?;
        fs::write(path, data)
    }
}

// ── Snapshot data format ─────────────────────────────────────────

/// Full snapshot of all stores at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotData {
    pub version: u32,
    pub snapshot_id: u64,
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
    /// Columnar properties: Vec<(label, prop_name, values)>
    pub prop_columns: Vec<(LabelId, String, Vec<Option<Value>>)>,
    /// Next ID counters
    pub next_node_id: u64,
    pub next_edge_id: u64,
}

// ── SnapshotWriter ────────────────────────────────────────────────

pub struct SnapshotWriter {
    data_dir: PathBuf,
}

impl SnapshotWriter {
    pub fn new(data_dir: &Path) -> Self {
        Self { data_dir: data_dir.to_path_buf() }
    }

    /// Write a full snapshot of node, edge, and property stores.
    pub fn write_snapshot(
        &self,
        snapshot_id: u64,
        nodes: &[Node],
        edges: &[Edge],
        prop_columns: &[(LabelId, String, Vec<Option<Value>>)],
        next_node_id: u64,
        next_edge_id: u64,
    ) -> std::io::Result<()> {
        let path = self.data_dir.join(format!("snapshot-{:04}.bin", snapshot_id));

        let data = SnapshotData {
            version: 1,
            snapshot_id,
            nodes: nodes.to_vec(),
            edges: edges.to_vec(),
            prop_columns: prop_columns.to_vec(),
            next_node_id,
            next_edge_id,
        };

        let file = File::create(&path)?;
        let mut writer = BufWriter::with_capacity(1_048_576, file); // 1 MB buffer
        let encoded = bincode::serialize(&data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        writer.write_all(&encoded)?;
        writer.flush()?;
        writer.get_mut().sync_all()?; // fsync

        Ok(())
    }
}

// ── SnapshotReader ────────────────────────────────────────────────

pub struct SnapshotReader {
    data_dir: PathBuf,
}

impl SnapshotReader {
    pub fn new(data_dir: &Path) -> Self {
        Self { data_dir: data_dir.to_path_buf() }
    }

    /// Read a snapshot by ID.
    pub fn read_snapshot(&self, snapshot_id: u64) -> std::io::Result<SnapshotData> {
        let path = self.data_dir.join(format!("snapshot-{:04}.bin", snapshot_id));
        let file = File::open(&path)?;
        let mut reader = BufReader::with_capacity(1_048_576, file);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        let data: SnapshotData = bincode::deserialize(&buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(data)
    }

    /// Load the latest snapshot according to the manifest.
    pub fn load_latest(&self, manifest: &Manifest) -> std::io::Result<Option<SnapshotData>> {
        if manifest.latest_snapshot_id == 0 {
            return Ok(None);
        }
        self.read_snapshot(manifest.latest_snapshot_id).map(Some)
    }
}

// ── SnapshotManager ───────────────────────────────────────────────

pub struct SnapshotManager {
    pub data_dir: PathBuf,
    manifest: Manifest,
    wal_bytes_since_snapshot: u64,
    snapshot_interval_bytes: u64,
}

impl SnapshotManager {
    pub fn new(data_dir: &Path) -> std::io::Result<Self> {
        let manifest = Manifest::read(data_dir)?;
        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            manifest,
            wal_bytes_since_snapshot: 0,
            snapshot_interval_bytes: 536_870_912, // 512 MB
        })
    }

    /// Track WAL bytes written since last snapshot.
    pub fn add_wal_bytes(&mut self, bytes: u64) {
        self.wal_bytes_since_snapshot += bytes;
    }

    /// Check if a snapshot should be taken.
    pub fn should_snapshot(&self) -> bool {
        self.manifest.latest_snapshot_id == 0
            || self.wal_bytes_since_snapshot >= self.snapshot_interval_bytes
    }

    /// Write a snapshot and update the manifest.
    pub fn take_snapshot(
        &mut self,
        nodes: &[Node],
        edges: &[Edge],
        prop_columns: &[(LabelId, String, Vec<Option<Value>>)],
        next_node_id: u64,
        next_edge_id: u64,
    ) -> std::io::Result<u64> {
        let id = self.manifest.latest_snapshot_id + 1;

        let writer = SnapshotWriter::new(&self.data_dir);
        writer.write_snapshot(id, nodes, edges, prop_columns, next_node_id, next_edge_id)?;

        // Update manifest
        self.manifest.latest_snapshot_id = id;
        self.manifest.latest_snapshot_path = format!("snapshot-{:04}.bin", id);
        self.manifest.write(&self.data_dir)?;

        self.wal_bytes_since_snapshot = 0;
        Ok(id)
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Mark WAL files as merged into snapshot (for cleanup).
    pub fn merge_wal(&mut self, wal_file: &str) -> std::io::Result<()> {
        self.manifest.merged_wal_files.push(wal_file.to_string());
        self.manifest.write(&self.data_dir)
    }

    /// Recover from the latest snapshot + WAL replay.
    /// Returns the recovered data and the snapshot ID (0 if no snapshot).
    pub fn recover(&self) -> std::io::Result<(Option<SnapshotData>, Vec<String>)> {
        let reader = SnapshotReader::new(&self.data_dir);
        let snapshot = reader.load_latest(&self.manifest)?;

        // Collect WAL files not yet merged into a snapshot
        let unmerged_wal: Vec<String> = if snapshot.is_some() {
            // WAL files that need replaying (those after the snapshot)
            let wal_files = ["nodes.wal", "edges.wal", "props.wal"];
            wal_files.iter()
                .filter(|f| {
                    !self.manifest.merged_wal_files.contains(&f.to_string())
                        && self.data_dir.join(f).exists()
                })
                .map(|f| f.to_string())
                .collect()
        } else {
            Vec::new()
        };

        Ok((snapshot, unmerged_wal))
    }
}

// ── Tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Node;
    use crate::storage::Edge;
    use crate::types::{EdgeId, LabelId, NodeId, TxId, MAX_TX_ID, NULL_EDGE};
    use tempfile::TempDir;

    fn make_node(id: NodeId, labels: Vec<LabelId>, props_row: u32, created_tx: TxId) -> Node {
        Node {
            id,
            labels,
            first_out: NULL_EDGE,
            first_in: NULL_EDGE,
            props_row,
            created_tx,
            deleted_tx: MAX_TX_ID,
        }
    }

    fn make_edge(id: EdgeId, src: NodeId, dst: NodeId, etype: LabelId) -> Edge {
        Edge {
            id,
            src,
            dst,
            etype,
            next_out: NULL_EDGE,
            next_in: NULL_EDGE,
            props_row: 0,
            created_tx: 1,
            deleted_tx: MAX_TX_ID,
        }
    }

    #[test]
    fn test_manifest_read_write() {
        let dir = TempDir::new().unwrap();
        let path = dir.path();

        // Empty read
        let m = Manifest::read(path).unwrap();
        assert_eq!(m.latest_snapshot_id, 0);

        // Write + read
        let mut m = Manifest::empty();
        m.latest_snapshot_id = 42;
        m.latest_snapshot_path = "snapshot-0042.bin".into();
        m.merged_wal_files.push("nodes.wal".into());
        m.write(path).unwrap();

        let m2 = Manifest::read(path).unwrap();
        assert_eq!(m2.latest_snapshot_id, 42);
        assert_eq!(m2.latest_snapshot_path, "snapshot-0042.bin");
        assert_eq!(m2.merged_wal_files, vec!["nodes.wal"]);
    }

    #[test]
    fn test_snapshot_write_and_read() {
        let dir = TempDir::new().unwrap();
        let path = dir.path();

        let nodes = vec![
            make_node(0, vec![1, 2], 0, 1),
            make_node(1, vec![3], 1, 2),
        ];
        let edges = vec![make_edge(100, 0, 1, 10)];
        let props: Vec<(u32, String, Vec<Option<Value>>)> = vec![
            (0, "name".into(), vec![Some(Value::String("Alice".into()))]),
        ];

        // Write
        let writer = SnapshotWriter::new(path);
        writer.write_snapshot(1, &nodes, &edges, &props, 2, 1).unwrap();

        // Read
        let reader = SnapshotReader::new(path);
        let data = reader.read_snapshot(1).unwrap();

        assert_eq!(data.snapshot_id, 1);
        assert_eq!(data.nodes.len(), 2);
        assert_eq!(data.nodes[0].id, 0);
        assert_eq!(data.nodes[0].labels, vec![1, 2]);
        assert_eq!(data.edges.len(), 1);
        assert_eq!(data.edges[0].src, 0);
        assert_eq!(data.edges[0].dst, 1);
        assert_eq!(data.next_node_id, 2);
        assert_eq!(data.next_edge_id, 1);
    }

    #[test]
    fn test_snapshot_manager_take_and_recover() {
        let dir = TempDir::new().unwrap();
        let path = dir.path();

        let nodes = vec![make_node(0, vec![1], 0, 1)];
        let edges = vec![];
        let props: Vec<(u32, String, Vec<Option<Value>>)> = vec![];

        // Take snapshot
        let mut mgr = SnapshotManager::new(path).unwrap();
        assert!(mgr.should_snapshot()); // first snapshot always needed
        let id = mgr.take_snapshot(&nodes, &edges, &props, 1, 0).unwrap();
        assert_eq!(id, 1);

        // Recover
        let (snap, unmerged) = mgr.recover().unwrap();
        assert!(snap.is_some());
        let data = snap.unwrap();
        assert_eq!(data.nodes.len(), 1);
        assert_eq!(data.nodes[0].id, 0);
        assert!(unmerged.is_empty()); // no WAL files yet
    }

    #[test]
    fn test_snapshot_manager_multiple_snapshots() {
        let dir = TempDir::new().unwrap();
        let path = dir.path();

        let nodes_a = vec![make_node(0, vec![1], 0, 1)];
        let nodes_b = vec![
            make_node(0, vec![1], 0, 1),
            make_node(1, vec![2], 1, 2),
        ];

        let mut mgr = SnapshotManager::new(path).unwrap();

        // First snapshot
        mgr.take_snapshot(&nodes_a, &[], &[], 1, 0).unwrap();
        mgr.add_wal_bytes(600_000_000);
        assert!(mgr.should_snapshot());

        // Second snapshot
        mgr.take_snapshot(&nodes_b, &[], &[], 2, 0).unwrap();

        // Recover latest
        let (snap, _) = mgr.recover().unwrap();
        let data = snap.unwrap();
        assert_eq!(data.nodes.len(), 2);
        assert_eq!(data.snapshot_id, 2);
    }

    #[test]
    fn test_snapshot_preserves_properties() {
        let dir = TempDir::new().unwrap();
        let path = dir.path();

        let nodes = vec![
            make_node(0, vec![0], 0, 1),
            make_node(1, vec![0], 1, 1),
        ];
        let props: Vec<(u32, String, Vec<Option<Value>>)> = vec![
            (0, "name".into(), vec![
                Some(Value::String("Alice".into())),
                Some(Value::String("Bob".into())),
            ]),
            (0, "age".into(), vec![
                Some(Value::Int(30)),
                Some(Value::Int(25)),
            ]),
        ];

        let writer = SnapshotWriter::new(path);
        writer.write_snapshot(1, &nodes, &[], &props, 2, 0).unwrap();

        let reader = SnapshotReader::new(path);
        let data = reader.read_snapshot(1).unwrap();

        assert_eq!(data.prop_columns.len(), 2);
        assert_eq!(data.prop_columns[0].0, 0); // label
        assert_eq!(data.prop_columns[0].1, "name");
        assert_eq!(data.prop_columns[1].1, "age");
        assert_eq!(data.prop_columns[0].2[0], Some(Value::String("Alice".into())));
        assert_eq!(data.prop_columns[1].2[0], Some(Value::Int(30)));
    }
}
