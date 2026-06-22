//! Storage performance benchmark — Sprint 1-4 perf baseline
//!
//! Measures: insert throughput, read latency, concurrent scaling,
//! WAL write throughput, snapshot speed.

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use lightgraph::storage::node_store::NodeStore;
use lightgraph::storage::edge_store::EdgeStore;
use lightgraph::storage::prop_store::PropStore;
use lightgraph::transaction::Database;

fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

// ═══════════════════════════════════════════════════════════════════
// NodeStore benchmarks
// ═══════════════════════════════════════════════════════════════════

#[test]
fn perf_node_insert_sequential() {
    let store = NodeStore::with_capacity(1_000_000);
    let n = 100_000;
    let start = Instant::now();
    for i in 0..n {
        store.insert_node(vec![1], i as u32, 1);
    }
    let ms = elapsed_ms(start);
    let ops_per_sec = n as f64 / (ms / 1000.0);
    assert_eq!(store.len(), n as usize);
    println!("NodeStore::insert  sequential: {:.0} ops/s ({:.1} ms for {} inserts)", ops_per_sec, ms, n);
}

#[test]
fn perf_node_read_random() {
    let store = NodeStore::with_capacity(100_000);
    for i in 0..100_000u64 { store.insert_node(vec![1], i as u32, 1); }

    let n = 1_000_000;
    let start = Instant::now();
    let mut found = 0u64;
    for i in 0..n {
        if store.get(i % 100_000).is_some() { found += 1; }
    }
    let ms = elapsed_ms(start);
    let ops_per_sec = n as f64 / (ms / 1000.0);
    assert_eq!(found, n as u64);
    println!("NodeStore::get     random:    {:.0} ops/s ({:.1} ms for {} reads)", ops_per_sec, ms, n);
}

#[test]
fn perf_node_concurrent_insert() {
    let store = Arc::new(NodeStore::with_capacity(1_000_000));
    let threads = 8;
    let per_thread = 12_500u64;
    let barrier = Arc::new(Barrier::new(threads + 1));
    let mut handles = Vec::new();

    let start = Instant::now();
    for _t in 0..threads {
        let s = Arc::clone(&store);
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            b.wait();
            for i in 0..per_thread {
                s.insert_node(vec![1], i as u32, 1);
            }
        }));
    }
    barrier.wait();
    for h in handles { h.join().unwrap(); }

    let ms = elapsed_ms(start);
    let total = threads as u64 * per_thread;
    let ops_per_sec = total as f64 / (ms / 1000.0);
    println!("NodeStore::insert concurrent(8): {:.0} ops/s ({:.1} ms for {} inserts)", ops_per_sec, ms, total);
    assert_eq!(store.len(), total as usize);
}

// ═══════════════════════════════════════════════════════════════════
// EdgeStore benchmarks
// ═══════════════════════════════════════════════════════════════════

#[test]
fn perf_edge_insert_sequential() {
    let store = EdgeStore::with_capacity(1_000_000);
    let n = 100_000u64;
    let start = Instant::now();
    for i in 0..n {
        store.insert_edge(i, i + 1, 1, i as u32, 1);
    }
    let ms = elapsed_ms(start);
    let ops_per_sec = n as f64 / (ms / 1000.0);
    println!("EdgeStore::insert  sequential: {:.0} ops/s ({:.1} ms for {} inserts)", ops_per_sec, ms, n);
    assert_eq!(store.len(), n as usize);
}

#[test]
fn perf_edge_read_random() {
    let store = EdgeStore::with_capacity(100_000);
    for i in 0..100_000u64 { store.insert_edge(i, i + 1, 1, i as u32, 1); }

    let n = 1_000_000;
    let start = Instant::now();
    let mut found = 0u64;
    for i in 0..n {
        if store.get(i % 100_000).is_some() { found += 1; }
    }
    let ms = elapsed_ms(start);
    let ops_per_sec = n as f64 / (ms / 1000.0);
    println!("EdgeStore::get     random:    {:.0} ops/s ({:.1} ms for {} reads)", ops_per_sec, ms, n);
    assert_eq!(found, n as u64);
}

// ═══════════════════════════════════════════════════════════════════
// PropStore benchmarks
// ═══════════════════════════════════════════════════════════════════

#[test]
fn perf_prop_insert_bulk() {
    let store = PropStore::new();
    let n = 10_000u32;
    let start = Instant::now();
    for i in 0..n {
        store.insert_row(0, &[
            ("id".into(), lightgraph::storage::prop_store::Value::Int(i as i64)),
            ("name".into(), lightgraph::storage::prop_store::Value::String(format!("user_{}", i))),
            ("active".into(), lightgraph::storage::prop_store::Value::Bool(i % 2 == 0)),
        ]);
    }
    let ms = elapsed_ms(start);
    let ops_per_sec = n as f64 / (ms / 1000.0);
    println!("PropStore::insert  bulk(3col): {:.0} ops/s ({:.1} ms for {} rows)", ops_per_sec, ms, n);
    assert_eq!(store.row_count(0), n);
}

#[test]
fn perf_prop_read_random() {
    let store = PropStore::new();
    for i in 0..10_000u32 {
        store.insert_row(0, &[("v".into(), lightgraph::storage::prop_store::Value::Int(i as i64))]);
    }

    let n = 100_000;
    let start = Instant::now();
    let mut found = 0u64;
    for i in 0..n {
        if store.get_prop(0, "v", i % 10_000).is_some() { found += 1; }
    }
    let ms = elapsed_ms(start);
    let ops_per_sec = n as f64 / (ms / 1000.0);
    println!("PropStore::get     random:    {:.0} ops/s ({:.1} ms for {} reads)", ops_per_sec, ms, n);
    assert_eq!(found, n as u64);
}

// ═══════════════════════════════════════════════════════════════════
// Transaction commit throughput
// ═══════════════════════════════════════════════════════════════════

#[test]
fn perf_transaction_commit() {
    let db = Database::memory();
    let n = 10_000;
    let start = Instant::now();
    for _i in 0..n {
        let tx = db.begin_write();
        tx.create_node(vec![1], 0);
        tx.commit().unwrap();
        drop(tx);
    }
    let ms = elapsed_ms(start);
    let tps = n as f64 / (ms / 1000.0);
    println!("Transaction::commit           {:.0} tps ({:.1} ms for {} commits)", tps, ms, n);
}

#[test]
fn perf_transaction_batch_commit() {
    let db = Database::memory();
    let batch_size = 100;
    let batches = 100;
    let start = Instant::now();
    for _b in 0..batches {
        let tx = db.begin_write();
        for _i in 0..batch_size {
            tx.create_node(vec![1], 0);
        }
        tx.commit().unwrap();
        drop(tx);
    }
    let ms = elapsed_ms(start);
    let total = batch_size * batches;
    let tps = total as f64 / (ms / 1000.0);
    println!("Transaction::commit batch(100): {:.0} node/s ({:.1} ms for {} nodes)", tps, ms, total);
}

// ═══════════════════════════════════════════════════════════════════
// WAL write throughput (sync mode)
// ═══════════════════════════════════════════════════════════════════

#[test]
fn perf_wal_write_sync() {
    use tempfile::TempDir;
    use lightgraph::storage::consistency::Consistency;

    let dir = TempDir::new().unwrap();
    let path = dir.path();
    let n = 1_000;

    let store = NodeStore::open(path, Consistency::immediate()).unwrap();
    let start = Instant::now();
    for i in 0..n {
        store.insert_node(vec![1], i as u32, 1);
    }
    store.flush();
    let ms = elapsed_ms(start);
    let ops_per_sec = n as f64 / (ms / 1000.0);
    println!("WAL write (fsync each):        {:.0} ops/s ({:.1} ms for {} writes)", ops_per_sec, ms, n);
}

#[test]
fn perf_wal_write_batched() {
    use tempfile::TempDir;
    use lightgraph::storage::consistency::Consistency;

    let dir = TempDir::new().unwrap();
    let path = dir.path();
    let n = 10_000;

    let store = NodeStore::open(path, Consistency::balanced()).unwrap();
    let start = Instant::now();
    for i in 0..n {
        store.insert_node(vec![1], i as u32, 1);
    }
    store.flush();
    let ms = elapsed_ms(start);
    let ops_per_sec = n as f64 / (ms / 1000.0);
    println!("WAL write (batched 5ms/64KB):  {:.0} ops/s ({:.1} ms for {} writes)", ops_per_sec, ms, n);
}
