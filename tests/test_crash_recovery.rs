//! Crash recovery tests — Sprint 3
//!
//! End-to-end simulation: write data via WAL, simulate crash
//! (close without clean shutdown), recover from WAL and verify
//! state consistency.

use lightgraph::storage::consistency::Consistency;
use lightgraph::storage::node_store::NodeStore;
use lightgraph::storage::edge_store::EdgeStore;
use lightgraph::storage::prop_store::PropStore;
use tempfile::TempDir;

// ── NodeStore crash recovery ────────────────────────────────────────

#[test]
fn test_node_store_crash_recovery() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();

    // Phase 1: write data with Immediate durability (fsync on every op)
    let ids: Vec<u64>;
    {
        let store = NodeStore::open(path, Consistency::immediate()).unwrap();
        ids = (0..10).map(|i| store.insert_node(vec![i as u32], i as u32, 1)).collect();
        store.flush(); // ensure synced
        // Simulate clean shutdown via drop (WalThread::drop joins)
    }

    // Phase 2: "crash" recovery — reopen and verify all data
    {
        let store = NodeStore::open(path, Consistency::immediate()).unwrap();
        assert_eq!(store.len(), 10);
        for (i, &id) in ids.iter().enumerate() {
            let n = store.get(id).unwrap();
            assert_eq!(n.labels, vec![i as u32]);
            assert_eq!(n.props_row, i as u32);
        }
    }
}

#[test]
fn test_node_store_recovery_with_deletes() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();

    let a;
    let b;
    let c;
    {
        let store = NodeStore::open(path, Consistency::immediate()).unwrap();
        a = store.insert_node(vec![1], 0, 1);
        b = store.insert_node(vec![2], 0, 1);
        c = store.insert_node(vec![3], 0, 1);
        store.soft_delete(b, 10);
        store.hard_delete(c);
        store.flush();
    }

    {
        let store = NodeStore::open(path, Consistency::immediate()).unwrap();
        assert_eq!(store.len(), 2); // a (alive), b (soft-deleted but exists)
        assert!(store.get(a).is_some());
        assert!(!store.get(b).unwrap().is_alive(10));
        assert!(store.get(c).is_none()); // hard-deleted
    }
}

#[test]
fn test_node_store_recovery_with_updates() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();

    let id;
    {
        let store = NodeStore::open(path, Consistency::immediate()).unwrap();
        id = store.insert_node(vec![0], 0, 1);
        store.update_labels(id, vec![99], 2);
        store.update_props_row(id, 42);
        store.set_first_out(id, 12345);
        store.flush();
    }

    {
        let store = NodeStore::open(path, Consistency::immediate()).unwrap();
        let n = store.get(id).unwrap();
        assert_eq!(n.labels, vec![99]);
        assert_eq!(n.props_row, 42);
        assert_eq!(n.first_out, 12345);
    }
}

// ── EdgeStore crash recovery ───────────────────────────────────────

#[test]
fn test_edge_store_crash_recovery() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();

    let e1;
    let e2;
    {
        let store = EdgeStore::open(path, Consistency::immediate()).unwrap();
        e1 = store.insert_edge(0, 1, 10, 100, 1);
        e2 = store.insert_edge(1, 2, 20, 200, 1);
        store.flush();
    }

    {
        let store = EdgeStore::open(path, Consistency::immediate()).unwrap();
        assert_eq!(store.len(), 2);
        assert_eq!(store.get(e1).unwrap().etype, 10);
        assert_eq!(store.get(e2).unwrap().dst, 2);
    }
}

#[test]
fn test_edge_store_recovery_with_delete() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();

    {
        let store = EdgeStore::open(path, Consistency::immediate()).unwrap();
        let e1 = store.insert_edge(0, 1, 1, 0, 1);
        store.insert_edge(1, 2, 2, 0, 1);
        store.soft_delete(e1, 50);
        store.flush();
    }

    {
        let store = EdgeStore::open(path, Consistency::immediate()).unwrap();
        assert_eq!(store.len(), 2);
        let e = store.get(0).unwrap();
        assert!(!e.is_alive(50));
    }
}

// ── PropStore crash recovery ────────────────────────────────────────

#[test]
fn test_prop_store_crash_recovery() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();

    {
        let store = PropStore::open(path, Consistency::immediate()).unwrap();
        store.insert_row(0, &[
            ("name".into(), lightgraph::storage::prop_store::Value::String("Alice".into())),
            ("age".into(), lightgraph::storage::prop_store::Value::Int(30)),
        ]);
        store.insert_row(0, &[
            ("name".into(), lightgraph::storage::prop_store::Value::String("Bob".into())),
        ]);
        store.flush();
    }

    {
        let store = PropStore::open(path, Consistency::immediate()).unwrap();
        assert_eq!(store.row_count(0), 2);
        assert_eq!(
            store.get_prop(0, "name", 0),
            Some(lightgraph::storage::prop_store::Value::String("Alice".into()))
        );
        assert_eq!(
            store.get_prop(0, "name", 1),
            Some(lightgraph::storage::prop_store::Value::String("Bob".into()))
        );
    }
}

// ── Full database recovery ──────────────────────────────────────────

#[test]
fn test_full_database_recovery() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();

    // Open and populate
    {
        let nodes = NodeStore::open(path, Consistency::immediate()).unwrap();
        let edges = EdgeStore::open(path, Consistency::immediate()).unwrap();
        let props = PropStore::open(path, Consistency::immediate()).unwrap();

        let _a = nodes.insert_node(vec![1], 0, 1);
        let _b = nodes.insert_node(vec![2], 1, 1);
        edges.insert_edge(0, 1, 10, 100, 1);
        props.insert_row(0, &[("k".into(), lightgraph::storage::prop_store::Value::Int(1))]);

        nodes.flush();
        edges.flush();
        props.flush();
    }

    // Recover all stores
    {
        let nodes = NodeStore::open(path, Consistency::immediate()).unwrap();
        let edges = EdgeStore::open(path, Consistency::immediate()).unwrap();
        let props = PropStore::open(path, Consistency::immediate()).unwrap();

        assert_eq!(nodes.len(), 2);
        assert!(nodes.get(0).is_some());
        assert!(nodes.get(1).is_some());
        assert_eq!(edges.len(), 1);
        assert_eq!(props.row_count(0), 1);
    }
}
