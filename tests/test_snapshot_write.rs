//! Snapshot write tests — Sprint 4
//!
//! Tests SnapshotWriter: full dump of nodes, edges, properties,
//! large data sets, incremental snapshots.

use lightgraph::snapshot::{SnapshotWriter, SnapshotReader, SnapshotManager};
use lightgraph::storage::Node;
use lightgraph::storage::Edge;
use lightgraph::storage::prop_store::Value;
use lightgraph::types::{MAX_TX_ID, NULL_EDGE};
use tempfile::TempDir;

fn node(id: u64, labels: Vec<u32>, created_tx: u64) -> Node {
    Node { id, labels, first_out: NULL_EDGE, first_in: NULL_EDGE,
           props_row: id as u32, created_tx, deleted_tx: MAX_TX_ID }
}

fn edge(id: u64, src: u64, dst: u64, etype: u32) -> Edge {
    Edge { id, src, dst, etype, next_out: NULL_EDGE, next_in: NULL_EDGE,
           props_row: 0, created_tx: 1, deleted_tx: MAX_TX_ID }
}

// ── Basic writes ────────────────────────────────────────────────────

#[test]
fn test_write_empty_snapshot() {
    let dir = TempDir::new().unwrap();
    let writer = SnapshotWriter::new(dir.path());
    writer.write_snapshot(1, &[], &[], &[], 0, 0).unwrap();
    assert!(dir.path().join("snapshot-0001.bin").exists());
}

#[test]
fn test_write_snapshot_with_data() {
    let dir = TempDir::new().unwrap();
    let nodes = vec![node(0, vec![1], 1), node(1, vec![2], 2)];
    let edges = vec![edge(100, 0, 1, 10)];
    let props = vec![
        (0u32, "k".to_string(), vec![Some(Value::Int(42))]),
    ];

    let writer = SnapshotWriter::new(dir.path());
    writer.write_snapshot(1, &nodes, &edges, &props, 2, 1).unwrap();

    let path = dir.path().join("snapshot-0001.bin");
    assert!(path.exists());
    assert!(std::fs::metadata(&path).unwrap().len() > 0);
}

#[test]
fn test_write_large_snapshot() {
    let dir = TempDir::new().unwrap();
    let nodes: Vec<_> = (0..1000).map(|i| node(i, vec![1], 1)).collect();
    let edges: Vec<_> = (0..500).map(|i| edge(i, i, i + 1, 1)).collect();

    let writer = SnapshotWriter::new(dir.path());
    writer.write_snapshot(1, &nodes, &edges, &[], 1000, 500).unwrap();

    // Verify it can be read back
    let reader = SnapshotReader::new(dir.path());
    let data = reader.read_snapshot(1).unwrap();
    assert_eq!(data.nodes.len(), 1000);
    assert_eq!(data.edges.len(), 500);
}

// ── Multiple snapshots ──────────────────────────────────────────────

#[test]
fn test_multiple_snapshots_accumulate() {
    let dir = TempDir::new().unwrap();

    let nodes_1 = vec![node(0, vec![1], 1)];
    let nodes_2 = vec![node(0, vec![1], 1), node(1, vec![2], 2)];

    let writer = SnapshotWriter::new(dir.path());
    writer.write_snapshot(1, &nodes_1, &[], &[], 1, 0).unwrap();
    writer.write_snapshot(2, &nodes_2, &[], &[], 2, 0).unwrap();

    assert!(dir.path().join("snapshot-0001.bin").exists());
    assert!(dir.path().join("snapshot-0002.bin").exists());

    let reader = SnapshotReader::new(dir.path());
    assert_eq!(reader.read_snapshot(1).unwrap().nodes.len(), 1);
    assert_eq!(reader.read_snapshot(2).unwrap().nodes.len(), 2);
}

// ── Manager lifecycle ───────────────────────────────────────────────

#[test]
fn test_manager_should_snapshot_rules() {
    let dir = TempDir::new().unwrap();
    let mut mgr = SnapshotManager::new(dir.path()).unwrap();

    // No snapshot yet — should snapshot
    assert!(mgr.should_snapshot());

    // After taking snapshot, should not need one
    mgr.take_snapshot(&[node(0, vec![1], 1)], &[], &[], 1, 0).unwrap();
    assert!(!mgr.should_snapshot());

    // After adding WAL bytes beyond threshold
    mgr.add_wal_bytes(600_000_000);
    assert!(mgr.should_snapshot());
}

#[test]
fn test_manager_manifest_persistence() {
    let dir = TempDir::new().unwrap();

    {
        let mut mgr = SnapshotManager::new(dir.path()).unwrap();
        mgr.take_snapshot(&[node(0, vec![1], 1)], &[], &[], 1, 0).unwrap();
        mgr.merge_wal("nodes.wal").unwrap();
    }

    // New manager should read persisted manifest
    {
        let mgr = SnapshotManager::new(dir.path()).unwrap();
        let m = mgr.manifest();
        assert_eq!(m.latest_snapshot_id, 1);
        assert!(m.merged_wal_files.contains(&"nodes.wal".to_string()));
    }
}

// ── Properties in snapshots ─────────────────────────────────────────

#[test]
fn test_snapshot_preserves_all_property_types() {
    let dir = TempDir::new().unwrap();
    let nodes = vec![node(0, vec![0], 1)];
    let props = vec![
        (0u32, "str_col".to_string(), vec![
            Some(Value::String("hello".into())),
        ]),
        (0u32, "int_col".to_string(), vec![
            Some(Value::Int(42)),
        ]),
        (0u32, "bool_col".to_string(), vec![
            Some(Value::Bool(true)),
        ]),
        (0u32, "null_col".to_string(), vec![None]),
    ];

    let writer = SnapshotWriter::new(dir.path());
    writer.write_snapshot(1, &nodes, &[], &props, 1, 0).unwrap();

    let reader = SnapshotReader::new(dir.path());
    let data = reader.read_snapshot(1).unwrap();

    assert_eq!(data.prop_columns.len(), 4);
}
