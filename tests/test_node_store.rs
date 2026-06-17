//! Integration tests for NodeStore — Sprint 1
//!
//! These tests exercise the NodeStore from the crate-external perspective,
//! as a user of the library would.

use lightgraph::storage::node_store::NodeStore;

// ── Helper ────────────────────────────────────────────────────────

fn populate(store: &NodeStore, count: u64, tx: u64) -> Vec<u64> {
    let mut ids = Vec::new();
    for i in 0..count {
        let id = store.insert_node(vec![(i % 5) as u32], i as u32, tx);
        ids.push(id);
    }
    ids
}

// ── Basic lifecycle ───────────────────────────────────────────────

#[test]
fn test_create_read_update_delete_lifecycle() {
    let store = NodeStore::new();

    // Create
    let id = store.insert_node(vec![10], 99, 1);
    assert!(store.contains(id));

    // Read
    let node = store.get(id).unwrap();
    assert_eq!(node.labels, vec![10]);
    assert_eq!(node.props_row, 99);
    assert_eq!(node.created_tx, 1);
    drop(node);

    // Update
    assert!(store.update_labels(id, vec![20, 30]));
    let node = store.get(id).unwrap();
    assert_eq!(node.labels, vec![20, 30]);
    drop(node);

    assert!(store.update_props_row(id, 42));
    let node = store.get(id).unwrap();
    assert_eq!(node.props_row, 42);
    drop(node);

    // Soft-delete
    assert!(store.soft_delete(id, 100));
    let node = store.get(id).unwrap();
    assert!(!node.is_alive(100));
    drop(node);

    // Hard-delete
    assert!(store.hard_delete(id));
    assert!(store.get(id).is_none());
}

// ── MVCC snapshot isolation ───────────────────────────────────────

#[test]
fn test_snapshot_isolation_across_timeline() {
    let store = NodeStore::new();

    // Transaction 1 creates n0, n1
    let n0 = store.insert_node(vec![0], 0, 1);
    let n1 = store.insert_node(vec![1], 1, 1);

    // Transaction 2 updates n0
    store.update_labels(n0, vec![99]);

    // Transaction 3 deletes n1
    store.soft_delete(n1, 3);

    // Transaction 4 creates n2
    let n2 = store.insert_node(vec![2], 2, 4);

    // Snapshot at tx=1 sees n0,n1 only
    let snap1 = store.visible_nodes(1);
    assert_eq!(snap1.len(), 2);
    assert!(snap1.contains(&n0));
    assert!(snap1.contains(&n1));

    // Snapshot at tx=2 sees n0(updated), n1
    let snap2 = store.visible_nodes(2);
    assert_eq!(snap2.len(), 2);

    // Snapshot at tx=4 sees n0(updated), n2 (n1 was deleted at tx 3)
    let snap4 = store.visible_nodes(4);
    assert_eq!(snap4.len(), 2);
    assert!(snap4.contains(&n0));
    assert!(snap4.contains(&n2));
    assert!(!snap4.contains(&n1)); // deleted
}

// ── Compaction ────────────────────────────────────────────────────

#[test]
fn test_compaction_cleans_up_old_versions() {
    let store = NodeStore::new();

    let ids = populate(&store, 10, 1);

    // Soft-delete first 5 at tx 10
    for &id in &ids[..5] {
        store.soft_delete(id, 10);
    }

    // Soft-delete rest at tx 20
    for &id in &ids[5..] {
        store.soft_delete(id, 20);
    }

    assert_eq!(store.len(), 10); // all still physically present

    // Compact with oldest_active_tx = 15
    let removed = store.compact(15);
    assert_eq!(removed, 5); // first 5 deleted at tx 10 < 15
    assert_eq!(store.len(), 5); // 5 still remain
}

// ── ID recycling ──────────────────────────────────────────────────

#[test]
fn test_id_recycling_full_cycle() {
    let store = NodeStore::new();

    // Create 100 nodes first, THEN delete them all.
    // This pushes 100 distinct IDs into the free list.
    let mut ids: Vec<u64> = Vec::new();
    for _ in 0..100 {
        let id = store.insert_node(vec![0], 0, 1);
        ids.push(id);
    }
    for id in &ids {
        store.hard_delete(*id);
    }

    assert_eq!(store.free_count(), 100);
    assert_eq!(store.len(), 0);

    // Re-create 100 nodes — all should get recycled IDs (all < 100).
    for _ in 0..100 {
        let id = store.insert_node(vec![1], 0, 2);
        assert!(id < 100, "Expected recycled ID < 100, got {}", id);
    }

    assert_eq!(store.free_count(), 0);
}

// ── Edge head management ─────────────────────────────────────────

#[test]
fn test_edge_chain_heads() {
    let store = NodeStore::new();
    let a = store.insert_node(vec![0], 0, 1);
    let b = store.insert_node(vec![0], 0, 1);

    store.set_first_out(a, 100);
    store.set_first_in(a, 200);
    store.set_first_out(b, 300);

    assert_eq!(store.get(a).unwrap().first_out, 100);
    assert_eq!(store.get(a).unwrap().first_in, 200);
    assert_eq!(store.get(b).unwrap().first_out, 300);
    assert_eq!(store.get(b).unwrap().first_in, u64::MAX); // unchanged
}
