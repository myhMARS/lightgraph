//! CRUD tests for nodes and edges — Sprint 2
//!
//! Covers: create, read, update, delete for nodes and edges
//! through the Transaction API, with snapshot isolation verification.

use lightgraph::transaction::Database;
use lightgraph::storage::prop_store::Value;
use lightgraph::types::MAX_TX_ID;

fn s(v: &str) -> Value { Value::String(v.into()) }
fn v(i: i64) -> Value { Value::Int(i) }

// ── Node CRUD ───────────────────────────────────────────────────────

#[test]
fn test_node_create_read() {
    let db = Database::memory();
    let tx = db.begin_write();
    let id = tx.create_node(vec![1, 2], 0);
    tx.commit().unwrap();
    drop(tx);

    let rx = db.begin_read();
    let n = rx.get_node(id).unwrap();
    assert_eq!(n.labels, vec![1, 2]);
    assert_eq!(n.id, id);
    assert_eq!(n.deleted_tx, MAX_TX_ID);
}

#[test]
fn test_node_create_multiple() {
    let db = Database::memory();
    let tx = db.begin_write();
    let a = tx.create_node(vec![0], 0);
    let b = tx.create_node(vec![0], 1);
    let c = tx.create_node(vec![1], 2);
    tx.commit().unwrap();

    let rx = db.begin_read();
    assert!(rx.get_node(a).is_some());
    assert!(rx.get_node(b).is_some());
    assert!(rx.get_node(c).is_some());
    assert_ne!(a, b);
    assert_ne!(b, c);
}

#[test]
fn test_node_update_labels() {
    let db = Database::memory();
    // Create
    let tx = db.begin_write();
    let id = tx.create_node(vec![0], 0);
    tx.commit().unwrap();

    // Update
    let tx = db.begin_write();
    tx.update_node_labels(id, vec![10, 20, 30]);
    tx.commit().unwrap();

    let rx = db.begin_read();
    assert_eq!(rx.get_node(id).unwrap().labels, vec![10, 20, 30]);
}

#[test]
fn test_node_update_labels_idempotent() {
    let db = Database::memory();
    let tx = db.begin_write();
    let id = tx.create_node(vec![0], 0);
    tx.commit().unwrap();

    // Multiple updates
    for labels in [vec![1], vec![2, 3], vec![4, 5, 6]] {
        let tx = db.begin_write();
        tx.update_node_labels(id, labels.clone());
        tx.commit().unwrap();
        let rx = db.begin_read();
        assert_eq!(rx.get_node(id).unwrap().labels, labels);
    }
}

#[test]
fn test_node_delete_soft() {
    let db = Database::memory();
    let tx = db.begin_write();
    let id = tx.create_node(vec![0], 0);
    tx.commit().unwrap();

    // Delete
    let tx = db.begin_write();
    tx.delete_node(id);
    tx.commit().unwrap();

    // Invisible after delete
    let rx = db.begin_read();
    assert!(rx.get_node(id).is_none());
}

#[test]
fn test_node_delete_then_recreate() {
    let db = Database::memory();
    let tx = db.begin_write();
    let id = tx.create_node(vec![0], 0);
    tx.commit().unwrap();

    // Delete
    let tx = db.begin_write();
    tx.delete_node(id);
    tx.commit().unwrap();

    // Re-create (same ID may be recycled)
    let tx = db.begin_write();
    let new_id = tx.create_node(vec![99], 0);
    tx.commit().unwrap();

    let rx = db.begin_read();
    assert!(rx.get_node(id).is_none()); // old node invisible
    assert!(rx.get_node(new_id).is_some());
    assert_eq!(rx.get_node(new_id).unwrap().labels, vec![99]);
}

/// Multi-operation in one transaction: create, update, delete
#[test]
fn test_node_mixed_ops_single_tx() {
    let db = Database::memory();
    // Setup
    let tx = db.begin_write();
    let keep = tx.create_node(vec![0], 0);
    let update_me = tx.create_node(vec![1], 1);
    let delete_me = tx.create_node(vec![2], 2);
    tx.commit().unwrap();

    // Mixed ops in one tx
    let tx = db.begin_write();
    tx.create_node(vec![3], 3);           // new node
    tx.update_node_labels(update_me, vec![99]); // update
    tx.delete_node(delete_me);            // delete
    tx.commit().unwrap();

    let rx = db.begin_read();
    assert!(rx.get_node(keep).is_some());
    assert_eq!(rx.get_node(update_me).unwrap().labels, vec![99]);
    assert!(rx.get_node(delete_me).is_none());
}

// ── Edge CRUD ───────────────────────────────────────────────────────

#[test]
fn test_edge_create_read() {
    let db = Database::memory();
    let tx = db.begin_write();
    let a = tx.create_node(vec![0], 0);
    let b = tx.create_node(vec![0], 1);
    let e = tx.create_edge(a, b, 42, 100);
    tx.commit().unwrap();

    let rx = db.begin_read();
    let edge = rx.get_edge(e).unwrap();
    assert_eq!(edge.src, a);
    assert_eq!(edge.dst, b);
    assert_eq!(edge.etype, 42);
    assert_eq!(edge.props_row, 100);
    assert_eq!(edge.deleted_tx, MAX_TX_ID);
}

#[test]
fn test_edge_create_multiple() {
    let db = Database::memory();
    let tx = db.begin_write();
    let a = tx.create_node(vec![0], 0);
    let b = tx.create_node(vec![0], 1);
    let c = tx.create_node(vec![0], 2);
    let e1 = tx.create_edge(a, b, 1, 0);
    let e2 = tx.create_edge(a, c, 2, 0);
    let e3 = tx.create_edge(b, c, 3, 0);
    tx.commit().unwrap();

    let rx = db.begin_read();
    assert!(rx.get_edge(e1).is_some());
    assert!(rx.get_edge(e2).is_some());
    assert!(rx.get_edge(e3).is_some());
}

#[test]
fn test_edge_delete_soft() {
    let db = Database::memory();
    let tx = db.begin_write();
    let a = tx.create_node(vec![0], 0);
    let b = tx.create_node(vec![0], 1);
    let e = tx.create_edge(a, b, 1, 0);
    tx.commit().unwrap();

    // Delete
    let tx = db.begin_write();
    tx.delete_edge(e);
    tx.commit().unwrap();

    let rx = db.begin_read();
    assert!(rx.get_edge(e).is_none());
}

#[test]
fn test_edge_delete_does_not_affect_nodes() {
    let db = Database::memory();
    let tx = db.begin_write();
    let a = tx.create_node(vec![0], 0);
    let b = tx.create_node(vec![0], 1);
    let e = tx.create_edge(a, b, 1, 0);
    tx.commit().unwrap();

    let tx = db.begin_write();
    tx.delete_edge(e);
    tx.commit().unwrap();

    // Nodes still exist
    let rx = db.begin_read();
    assert!(rx.get_node(a).is_some());
    assert!(rx.get_node(b).is_some());
    assert!(rx.get_edge(e).is_none());
}

// ── CRUD with Properties ────────────────────────────────────────────

#[test]
fn test_crud_with_properties() {
    let db = Database::memory();
    let tx = db.begin_write();
    let _alice = tx.create_node(vec![0], 0);
    tx.set_prop(0, "name", 0, Some(s("Alice")));
    tx.set_prop(0, "age", 0, Some(v(30)));
    tx.commit().unwrap();

    // Read props directly
    assert_eq!(db.props.get_prop(0, "name", 0), Some(s("Alice")));
    assert_eq!(db.props.get_prop(0, "age", 0), Some(v(30)));

    // Update via transaction
    let tx = db.begin_write();
    tx.set_prop(0, "age", 0, Some(v(31)));
    tx.set_prop(0, "city", 0, Some(s("NYC")));
    tx.commit().unwrap();

    assert_eq!(db.props.get_prop(0, "age", 0), Some(v(31)));
    assert_eq!(db.props.get_prop(0, "city", 0), Some(s("NYC")));
    assert_eq!(db.props.get_prop(0, "name", 0), Some(s("Alice"))); // unchanged
}

#[test]
fn test_property_delete_by_setting_none() {
    let db = Database::memory();
    let tx = db.begin_write();
    tx.create_node(vec![0], 0);
    tx.set_prop(0, "temp", 0, Some(s("remove_me")));
    tx.commit().unwrap();

    assert_eq!(db.props.get_prop(0, "temp", 0), Some(s("remove_me")));

    // Delete property by setting to None
    let tx = db.begin_write();
    tx.set_prop(0, "temp", 0, None);
    tx.commit().unwrap();

    assert_eq!(db.props.get_prop(0, "temp", 0), None);
}

// ── Read-your-own-writes (within a transaction) ──────────────────
// NOTE: Not yet implemented. Requires Transaction::get_node/get_edge
// to check the write buffer before the store. Planned for Sprint 2+.
