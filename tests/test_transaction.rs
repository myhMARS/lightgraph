//! Integration tests for transactions — Sprint 2
//!
//! Tests: snapshot isolation, atomic commit, rollback, conflict detection.

use lightgraph::transaction::Database;
use lightgraph::storage::prop_store::Value;

fn v(i: i64) -> Value { Value::Int(i) }
fn s(t: &str) -> Value { Value::String(t.into()) }

// ── Basic CRUD via Transaction ──────────────────────────────────

#[test]
fn test_create_and_read_node() {
    let db = Database::memory();

    let tx = db.begin_write();
    let id = tx.create_node(vec![1], 0);
    tx.commit().unwrap();

    // Read after commit
    let rx = db.begin_read();
    let node = rx.get_node(id).unwrap();
    assert_eq!(node.labels, vec![1]);
    assert!(node.is_alive(rx.snapshot()));
}

#[test]
fn test_create_node_with_properties() {
    let db = Database::memory();

    let tx = db.begin_write();
    let _id = tx.create_node(vec![0], 0);
    tx.set_prop(0, "name", 0, Some(s("Alice")));
    tx.set_prop(0, "age", 0, Some(v(30)));
    tx.commit().unwrap();

    assert_eq!(db.props.get_prop(0, "name", 0), Some(s("Alice")));
    assert_eq!(db.props.get_prop(0, "age", 0), Some(v(30)));
}

#[test]
fn test_create_edge_between_nodes() {
    let db = Database::memory();

    let tx = db.begin_write();
    let a = tx.create_node(vec![0], 0);
    let b = tx.create_node(vec![0], 1);
    let e = tx.create_edge(a, b, 10, 100);
    tx.commit().unwrap();

    let rx = db.begin_read();
    let edge = rx.get_edge(e).unwrap();
    assert_eq!(edge.src, a);
    assert_eq!(edge.dst, b);
    assert_eq!(edge.etype, 10);
}

// ── Snapshot Isolation ──────────────────────────────────────────

#[test]
fn test_snapshot_does_not_see_uncommitted() {
    let db = Database::memory();

    // Writer inserts but does NOT commit
    let tx = db.begin_write();
    let id = tx.create_node(vec![1], 0);

    // Reader should NOT see it
    let rx = db.begin_read();
    assert!(rx.get_node(id).is_none());
}

#[test]
fn test_snapshot_sees_committed_only() {
    let db = Database::memory();

    // Commit first batch
    let tx1 = db.begin_write();
    let a = tx1.create_node(vec![0], 0);
    tx1.commit().unwrap();

    // Write second batch but don't commit
    let tx2 = db.begin_write();
    let b = tx2.create_node(vec![0], 1);

    // Reader sees only committed (a, not b)
    let rx = db.begin_read();
    assert!(rx.get_node(a).is_some());
    assert!(rx.get_node(b).is_none());
}

// ── Rollback ────────────────────────────────────────────────────

#[test]
fn test_rollback_discards_writes() {
    let db = Database::memory();

    let tx = db.begin_write();
    let id = tx.create_node(vec![0], 0);
    tx.rollback();

    // Should not exist after rollback
    let rx = db.begin_read();
    assert!(rx.get_node(id).is_none());
}

#[test]
fn test_rollback_does_not_affect_committed() {
    let db = Database::memory();

    // Commit some data
    let tx1 = db.begin_write();
    let a = tx1.create_node(vec![0], 0);
    tx1.commit().unwrap();

    // Write + rollback
    let tx2 = db.begin_write();
    let _b = tx2.create_node(vec![0], 1);
    tx2.rollback();

    // Committed data still there
    let rx = db.begin_read();
    assert!(rx.get_node(a).is_some());
}

// ── Atomic Multi-Store Commit ───────────────────────────────────

#[test]
fn test_atomic_node_edge_commit() {
    let db = Database::memory();

    // Write node + edge + properties in one transaction
    let tx = db.begin_write();
    let alice = tx.create_node(vec![0], 0);
    let bob = tx.create_node(vec![0], 1);
    let knows = tx.create_edge(alice, bob, 1, 100);
    tx.set_prop(0, "name", 0, Some(s("Alice")));
    tx.set_prop(0, "name", 1, Some(s("Bob")));
    tx.commit().unwrap();

    // All should be visible atomically
    let rx = db.begin_read();
    assert!(rx.get_node(alice).is_some());
    assert!(rx.get_node(bob).is_some());
    assert!(rx.get_edge(knows).is_some());
    assert_eq!(db.props.get_prop(0, "name", 0), Some(s("Alice")));
    assert_eq!(db.props.get_prop(0, "name", 1), Some(s("Bob")));
}

// ── Delete via Transaction ──────────────────────────────────────

#[test]
fn test_soft_delete_via_transaction() {
    let db = Database::memory();

    // Create
    let tx = db.begin_write();
    let id = tx.create_node(vec![0], 0);
    tx.commit().unwrap();

    // Delete
    let tx = db.begin_write();
    tx.delete_node(id);
    tx.commit().unwrap();

    // Should be invisible after delete commit
    let rx = db.begin_read();
    assert!(rx.get_node(id).is_none());
}

// ── Concurrent Writers ──────────────────────────────────────────

#[test]
fn test_concurrent_independent_writes() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    let db = Arc::new(Database::memory());
    let barrier = Arc::new(Barrier::new(4));
    let mut handles = Vec::new();

    for t in 0..4 {
        let db = Arc::clone(&db);
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            b.wait();
            for i in 0..50 {
                let tx = db.begin_write();
                tx.create_node(vec![t], (t * 50 + i) as u32);
                tx.commit().unwrap();
            }
        }));
    }

    for h in handles { h.join().unwrap(); }
}

#[test]
fn test_auto_rollback_on_drop() {
    let db = Database::memory();

    {
        let tx = db.begin_write();
        let _id = tx.create_node(vec![0], 0);
        // tx dropped without commit → auto rollback
    }

    let rx = db.begin_read();
    assert!(rx.get_node(0).is_none());
}
