//! Snapshot read/recovery tests — Sprint 4
//!
//! Tests SnapshotReader: reading snapshots back, recovery
//! from snapshot-only state, and partial recovery scenarios.

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

// ── Basic reads ─────────────────────────────────────────────────────

#[test]
fn test_read_nonexistent_snapshot() {
    let dir = TempDir::new().unwrap();
    let reader = SnapshotReader::new(dir.path());
    let result = reader.read_snapshot(999);
    assert!(result.is_err());
}

#[test]
fn test_read_snapshot_roundtrip() {
    let dir = TempDir::new().unwrap();

    let nodes = vec![
        node(0, vec![1, 2], 1),
        node(1, vec![3], 2),
    ];
    let edges = vec![
        Edge { id: 100, src: 0, dst: 1, etype: 10,
               next_out: NULL_EDGE, next_in: NULL_EDGE,
               props_row: 0, created_tx: 1, deleted_tx: MAX_TX_ID },
    ];
    let props = vec![
        (0u32, "name".to_string(), vec![Some(Value::String("Alice".into()))]),
    ];

    let writer = SnapshotWriter::new(dir.path());
    writer.write_snapshot(5, &nodes, &edges, &props, 2, 1).unwrap();

    let reader = SnapshotReader::new(dir.path());
    let data = reader.read_snapshot(5).unwrap();

    assert_eq!(data.snapshot_id, 5);
    assert_eq!(data.version, 1);
    assert_eq!(data.nodes.len(), 2);

    // Verify node details
    assert_eq!(data.nodes[0].id, 0);
    assert_eq!(data.nodes[0].labels, vec![1, 2]);
    assert_eq!(data.nodes[1].props_row, 1);

    // Verify edge details
    assert_eq!(data.edges.len(), 1);
    assert_eq!(data.edges[0].src, 0);
    assert_eq!(data.edges[0].dst, 1);

    // Verify counters
    assert_eq!(data.next_node_id, 2);
    assert_eq!(data.next_edge_id, 1);
}

// ── Recovery from snapshot ──────────────────────────────────────────

#[test]
fn test_recover_from_snapshot_no_wal() {
    let dir = TempDir::new().unwrap();

    // Create a snapshot
    let nodes = vec![node(0, vec![1], 1), node(1, vec![2], 2)];
    let mut mgr = SnapshotManager::new(dir.path()).unwrap();
    mgr.take_snapshot(&nodes, &[], &[], 2, 0).unwrap();

    // Recover
    let (snap, unmerged) = mgr.recover().unwrap();
    assert!(snap.is_some());
    assert_eq!(snap.unwrap().nodes.len(), 2);
    assert!(unmerged.is_empty());
}

#[test]
fn test_recover_without_snapshot_returns_none() {
    let dir = TempDir::new().unwrap();
    let mgr = SnapshotManager::new(dir.path()).unwrap();
    let (snap, _) = mgr.recover().unwrap();
    assert!(snap.is_none());
}

// ── Snapshot versioning ─────────────────────────────────────────────

#[test]
fn test_snapshot_versions_are_independent() {
    let dir = TempDir::new().unwrap();

    let writer = SnapshotWriter::new(dir.path());
    writer.write_snapshot(1, &[node(0, vec![1], 1)], &[], &[], 1, 0).unwrap();
    writer.write_snapshot(2, &[node(0, vec![1], 1), node(1, vec![2], 2)], &[], &[], 2, 0).unwrap();

    let reader = SnapshotReader::new(dir.path());
    let v1 = reader.read_snapshot(1).unwrap();
    let v2 = reader.read_snapshot(2).unwrap();

    assert_eq!(v1.nodes.len(), 1);
    assert_eq!(v2.nodes.len(), 2);
    assert_ne!(v1.snapshot_id, v2.snapshot_id);
}

// ── Edge preservation ───────────────────────────────────────────────

#[test]
fn test_snapshot_preserves_edge_chains() {
    let dir = TempDir::new().unwrap();

    // Linked list: e1 -> e2 -> NULL
    let edges = vec![
        Edge { id: 1, src: 0, dst: 1, etype: 1,
               next_out: 2, next_in: NULL_EDGE,
               props_row: 0, created_tx: 1, deleted_tx: MAX_TX_ID },
        Edge { id: 2, src: 0, dst: 2, etype: 1,
               next_out: NULL_EDGE, next_in: NULL_EDGE,
               props_row: 0, created_tx: 2, deleted_tx: MAX_TX_ID },
    ];

    let writer = SnapshotWriter::new(dir.path());
    writer.write_snapshot(1, &[], &edges, &[], 0, 2).unwrap();

    let reader = SnapshotReader::new(dir.path());
    let data = reader.read_snapshot(1).unwrap();

    assert_eq!(data.edges[0].next_out, 2);
    assert_eq!(data.edges[1].next_out, NULL_EDGE);
}
