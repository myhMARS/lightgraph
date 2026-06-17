//! Integration tests for EdgeStore — Sprint 1
//!
//! Tests bidirectional adjacency-list traversal, linking/unlinking,
//! MVCC soft-delete, and ID recycling from outside the crate.

use lightgraph::storage::edge_store::EdgeStore;
use lightgraph::types::NULL_EDGE;

#[test]
fn test_full_lifecycle_with_chains() {
    let store = EdgeStore::new();

    // Insert three edges from node 0 to nodes 1, 2, 3
    let e1 = store.insert_edge(0, 1, 10, 100, 1);
    let e2 = store.insert_edge(0, 2, 10, 101, 1);
    let e3 = store.insert_edge(0, 3, 10, 102, 1);

    // Link into chains (e1 first, e2 second, e3 third → chain: e3→e2→e1→NULL)
    let mut src_out = NULL_EDGE;
    let mut dummy = NULL_EDGE;
    store.link_into_chains(e3, &mut src_out, &mut dummy);
    store.link_into_chains(e2, &mut src_out, &mut dummy);
    store.link_into_chains(e1, &mut src_out, &mut dummy);

    // Traverse out
    let chain = store.out_edges(src_out);
    assert_eq!(chain, vec![e1, e2, e3]);

    // Soft-delete middle edge
    store.soft_delete(e2, 100);

    // Middle edge gone for transactions at tx ≥ 100
    let visible = store.out_edges_visible(src_out, 200);
    assert_eq!(visible, vec![e1, e3]);

    // Unlink e2 fully
    store.unlink_from_chains(e2, &mut src_out, &mut dummy);
    let after_unlink = store.out_edges(src_out);
    assert_eq!(after_unlink, vec![e1, e3]);
}

#[test]
fn test_bidirectional_chains() {
    let store = EdgeStore::new();

    let e1 = store.insert_edge(0, 1, 0, 0, 1);
    let e2 = store.insert_edge(0, 1, 0, 0, 1);

    let mut src_out = NULL_EDGE;
    let mut dst_in = NULL_EDGE;

    store.link_into_chains(e2, &mut src_out, &mut dst_in);
    store.link_into_chains(e1, &mut src_out, &mut dst_in);

    // Outgoing from src
    assert_eq!(store.out_edges(src_out), vec![e1, e2]);
    // Incoming to dst
    assert_eq!(store.in_edges(dst_in), vec![e1, e2]);
}

#[test]
fn test_mvcc_snapshot_traversal() {
    let store = EdgeStore::new();

    // Create edges at different txs
    let e_old = store.insert_edge(0, 1, 0, 0, 1);
    let e_new = store.insert_edge(0, 2, 0, 0, 50);

    let mut src_out = NULL_EDGE;
    let mut dummy = NULL_EDGE;
    store.link_into_chains(e_new, &mut src_out, &mut dummy);
    store.link_into_chains(e_old, &mut src_out, &mut dummy);

    // At tx 30, only e_old is visible
    let vis = store.out_edges_visible(src_out, 30);
    assert_eq!(vis, vec![e_old]);
}
