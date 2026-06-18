//! MVCC snapshot isolation tests — Sprint 2
//!
//! Tests: multi-version visibility, snapshot correctness,
//! version chain traversal, interaction with compaction.

use lightgraph::transaction::Database;
use lightgraph::types::MAX_TX_ID;

// ── Basics ──────────────────────────────────────────────────────────

#[test]
fn test_mvcc_initial_snapshot_is_zero() {
    let db = Database::memory();
    let rx = db.begin_read();
    assert_eq!(rx.snapshot(), 0);
}

#[test]
fn test_mvcc_snapshot_advances_after_commit() {
    let db = Database::memory();
    let snap0 = db.begin_read().snapshot();

    let tx = db.begin_write();
    tx.create_node(vec![0], 0);
    tx.commit().unwrap();

    let snap1 = db.begin_read().snapshot();
    assert!(snap1 > snap0);
}

#[test]
fn test_mvcc_old_snapshot_does_not_see_new_data() {
    let db = Database::memory();

    // Start a read with current snapshot
    let rx = db.begin_read();
    let snap = rx.snapshot();
    drop(rx);

    // Write new data
    let tx = db.begin_write();
    let id = tx.create_node(vec![0], 0);
    tx.commit().unwrap();

    // Read at old snapshot — should NOT see the new node
    // (We use get_node which filters by snapshot)
    let rx = db.begin_read();
    let snap_after = rx.snapshot();
    assert!(snap_after > snap);
    // The new node was created at tx > snap, so:
    let n = db.nodes.get(id).unwrap();
    assert!(n.is_alive(snap_after));
    assert!(!n.is_alive(snap));
}

// ── Multi-version across writers ───────────────────────────────────

#[test]
fn test_mvcc_multiple_writers_serialized() {
    let db = Database::memory();

    // Writer 1 creates a node at tx=t1
    let t1 = db.begin_write();
    let a = t1.create_node(vec![1], 0);
    t1.commit().unwrap();

    // Writer 2 creates another node at tx=t2
    let t2 = db.begin_write();
    let b = t2.create_node(vec![2], 0);
    t2.commit().unwrap();

    // Both visible at latest snapshot
    let rx = db.begin_read();
    assert!(rx.get_node(a).is_some());
    assert!(rx.get_node(b).is_some());
}

#[test]
fn test_mvcc_delete_creates_new_version() {
    let db = Database::memory();

    let tx = db.begin_write();
    let id = tx.create_node(vec![0], 0);
    tx.commit().unwrap();

    // Snapshot before delete sees the node
    let snap_before = db.begin_read().snapshot();

    let tx = db.begin_write();
    tx.delete_node(id);
    tx.commit().unwrap();

    // Old snapshot still sees it, new snapshot does not
    let n = db.nodes.get(id).unwrap();
    assert!(n.is_alive(snap_before));
    assert!(!n.is_alive(db.begin_read().snapshot()));
}

// NOTE: test_mvcc_update_preserves_created_tx removed — covered by
// test_node_update_labels / test_node_update_labels_idempotent in test_crud.rs

// ── Visibility edge cases ──────────────────────────────────────────

#[test]
fn test_mvcc_created_at_snapshot_is_visible() {
    let db = Database::memory();

    let tx = db.begin_write();
    let id = tx.create_node(vec![0], 0);
    let created_tx = tx.tx_id();
    tx.commit().unwrap();

    // Node created at tx `created_tx`, should be visible at that snapshot
    let n = db.nodes.get(id).unwrap();
    assert!(n.is_alive(created_tx));
    assert!(n.is_alive(created_tx + 1));
}

#[test]
fn test_mvcc_deleted_at_snapshot_is_invisible() {
    let db = Database::memory();

    let tx = db.begin_write();
    let id = tx.create_node(vec![0], 0);
    let _created_tx = tx.tx_id();
    tx.commit().unwrap();

    let tx = db.begin_write();
    tx.delete_node(id);
    let deleted_tx = tx.tx_id();
    tx.commit().unwrap();

    let n = db.nodes.get(id).unwrap();
    assert!(n.is_alive(deleted_tx - 1)); // still visible just before
    assert!(!n.is_alive(deleted_tx));     // invisible at delete tx
    assert!(!n.is_alive(deleted_tx + 1)); // invisible after
}

#[test]
fn test_mvcc_node_never_visible_before_creation() {
    let db = Database::memory();

    let tx = db.begin_write();
    let id = tx.create_node(vec![0], 0);
    let created_tx = tx.tx_id();
    tx.commit().unwrap();

    let n = db.nodes.get(id).unwrap();
    // Not visible before creation
    assert!(!n.is_alive(0));
    assert!(!n.is_alive(created_tx - 1));
    // Visible starting from creation
    assert!(n.is_alive(created_tx));
}

#[test]
fn test_mvcc_alive_node_has_max_deleted_tx() {
    let db = Database::memory();
    let tx = db.begin_write();
    let id = tx.create_node(vec![0], 0);
    tx.commit().unwrap();

    let n = db.nodes.get(id).unwrap();
    assert_eq!(n.deleted_tx, MAX_TX_ID);
    assert!(n.is_alive(u64::MAX - 1)); // alive even at snapshot near max
}
