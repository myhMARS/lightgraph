use lightgraph::storage::node_store::NodeStore;
use lightgraph::storage::edge_store::EdgeStore;
use lightgraph::storage::prop_store::PropStore;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

const ROUNDS: usize = 5;

fn median(data: &mut [f64]) -> f64 {
    data.sort_by(|a, b| a.partial_cmp(b).unwrap());
    data[data.len() / 2]
}

#[test]
fn bench_nodes_concurrent_read() {
    let store = Arc::new(NodeStore::with_capacity(1_000_000));
    for i in 0..100_000u64 { store.insert_node(vec![1], i as u32, 1); }
    for threads in [1, 2, 4, 8, 16] {
        let mut times = Vec::new();
        let total: u64 = 100_000;
        for _ in 0..ROUNDS {
            let barrier = Arc::new(Barrier::new(threads + 1));
            let mut handles = Vec::new();
            let start = Instant::now();
            for t in 0..threads {
                let s = Arc::clone(&store);
                let b = Arc::clone(&barrier);
                let n = (total / threads as u64) as usize;
                handles.push(thread::spawn(move || {
                    b.wait();
                    let base = t * n;
                    for i in base..base + n { let _ = s.get(i as u64); }
                }));
            }
            barrier.wait();
            for h in handles { h.join().unwrap(); }
            times.push(start.elapsed().as_secs_f64());
        }
        let ops = total as f64 / median(&mut times);
        println!("NodeStore READ  | {:>2} threads | {:>12.1} ops/s", threads, ops);
    }
}

#[test]
fn bench_nodes_concurrent_write() {
    for threads in [1, 2, 4, 8, 16] {
        let mut times = Vec::new();
        let total: u64 = 20_000;
        for _ in 0..ROUNDS {
            let store = Arc::new(NodeStore::with_capacity(1_000_000));
            let barrier = Arc::new(Barrier::new(threads + 1));
            let mut handles = Vec::new();
            let start = Instant::now();
            let n = (total / threads as u64) as usize;
            for t in 0..threads {
                let s = Arc::clone(&store);
                let b = Arc::clone(&barrier);
                handles.push(thread::spawn(move || {
                    b.wait();
                    for i in 0..n {
                        s.insert_node(vec![1], (t as u64 * n as u64 + i as u64) as u32, 1);
                    }
                }));
            }
            barrier.wait();
            for h in handles { h.join().unwrap(); }
            times.push(start.elapsed().as_secs_f64());
        }
        let ops = total as f64 / median(&mut times);
        println!("NodeStore WRITE | {:>2} threads | {:>12.1} ops/s", threads, ops);
    }
}

#[test]
fn bench_nodes_concurrent_mixed() {
    for threads in [4, 8, 16] {
        let mut times = Vec::new();
        let n: u64 = 5000;
        let total = n * threads as u64;
        for _ in 0..ROUNDS {
            let store = Arc::new(NodeStore::with_capacity(1_000_000));
            for _ in 0..50_000u64 { store.insert_node(vec![1], 0, 1); }
            let barrier = Arc::new(Barrier::new(threads + 1));
            let mut handles = Vec::new();
            let start = Instant::now();
            for t in 0..threads {
                let s = Arc::clone(&store);
                let b = Arc::clone(&barrier);
                handles.push(thread::spawn(move || {
                    b.wait();
                    for i in 0..n {
                        if i % 3 == 0 { s.insert_node(vec![1], 0, 2); }
                        else {
                            let key = ((i + t as u64 * n) % 50_000) as u64;
                            let _ = s.get(key);
                        }
                    }
                }));
            }
            barrier.wait();
            for h in handles { h.join().unwrap(); }
            times.push(start.elapsed().as_secs_f64());
        }
        let ops = total as f64 / median(&mut times);
        println!("NodeStore MIXED | {:>2} threads | {:>12.1} ops/s (70R/30W)", threads, ops);
    }
}

#[test]
fn bench_edges_concurrent_read() {
    let store = Arc::new(EdgeStore::with_capacity(1_000_000));
    for i in 0..100_000u64 { store.insert_edge(i, i + 1, 0, 0, 1); }
    for threads in [1, 2, 4, 8, 16] {
        let mut times = Vec::new();
        let total: u64 = 100_000;
        for _ in 0..ROUNDS {
            let barrier = Arc::new(Barrier::new(threads + 1));
            let mut handles = Vec::new();
            let start = Instant::now();
            for t in 0..threads {
                let s = Arc::clone(&store);
                let b = Arc::clone(&barrier);
                let n = (total / threads as u64) as usize;
                handles.push(thread::spawn(move || {
                    b.wait();
                    let base = t * n;
                    for i in base..base + n { let _ = s.get(i as u64); }
                }));
            }
            barrier.wait();
            for h in handles { h.join().unwrap(); }
            times.push(start.elapsed().as_secs_f64());
        }
        let ops = total as f64 / median(&mut times);
        println!("EdgeStore READ  | {:>2} threads | {:>12.1} ops/s", threads, ops);
    }
}

#[test]
fn bench_edges_concurrent_write() {
    for threads in [1, 2, 4, 8, 16] {
        let mut times = Vec::new();
        let total: u64 = 20_000;
        for _ in 0..ROUNDS {
            let store = Arc::new(EdgeStore::with_capacity(1_000_000));
            let barrier = Arc::new(Barrier::new(threads + 1));
            let mut handles = Vec::new();
            let start = Instant::now();
            let n = (total / threads as u64) as usize;
            for _t in 0..threads {
                let s = Arc::clone(&store);
                let b = Arc::clone(&barrier);
                handles.push(thread::spawn(move || {
                    b.wait();
                    for _ in 0..n { s.insert_edge(1, 2, 0, 0, 1); }
                }));
            }
            barrier.wait();
            for h in handles { h.join().unwrap(); }
            times.push(start.elapsed().as_secs_f64());
        }
        let ops = total as f64 / median(&mut times);
        println!("EdgeStore WRITE | {:>2} threads | {:>12.1} ops/s", threads, ops);
    }
}

#[test]
fn bench_edges_concurrent_mixed() {
    for threads in [4, 8, 16] {
        let mut times = Vec::new();
        let n: u64 = 5000;
        let total = n * threads as u64;
        for _ in 0..ROUNDS {
            let store = Arc::new(EdgeStore::with_capacity(1_000_000));
            for _ in 0..50_000u64 { store.insert_edge(0, 1, 0, 0, 1); }
            let barrier = Arc::new(Barrier::new(threads + 1));
            let mut handles = Vec::new();
            let start = Instant::now();
            for t in 0..threads {
                let s = Arc::clone(&store);
                let b = Arc::clone(&barrier);
                handles.push(thread::spawn(move || {
                    b.wait();
                    for i in 0..n {
                        if i % 3 == 0 { s.insert_edge(1, 2, 0, 0, 1); }
                        else {
                            let key = ((i + t as u64 * n) % 50_000) as u64;
                            let _ = s.get(key);
                        }
                    }
                }));
            }
            barrier.wait();
            for h in handles { h.join().unwrap(); }
            times.push(start.elapsed().as_secs_f64());
        }
        let ops = total as f64 / median(&mut times);
        println!("EdgeStore MIXED | {:>2} threads | {:>12.1} ops/s (70R/30W)", threads, ops);
    }
}

#[test]
fn bench_props_concurrent_write() {
    for threads in [1, 2, 4, 8, 16] {
        let mut times = Vec::new();
        let total: u64 = 5000;
        for _ in 0..ROUNDS {
            let store = Arc::new(PropStore::new());
            let barrier = Arc::new(Barrier::new(threads + 1));
            let mut handles = Vec::new();
            let start = Instant::now();
            let n = (total / threads as u64) as usize;
            for t in 0..threads {
                let s = Arc::clone(&store);
                let b = Arc::clone(&barrier);
                let label: u32 = t as u32;
                handles.push(thread::spawn(move || {
                    b.wait();
                    for i in 0..n {
                        s.insert_row(label, &[
                            ("name".into(), lightgraph::storage::prop_store::Value::String(format!("u{}", i))),
                            ("age".into(), lightgraph::storage::prop_store::Value::Int(i as i64)),
                        ]);
                    }
                }));
            }
            barrier.wait();
            for h in handles { h.join().unwrap(); }
            times.push(start.elapsed().as_secs_f64());
        }
        let ops = total as f64 / median(&mut times);
        println!("PropStore WRITE | {:>2} threads | {:>12.1} ops/s", threads, ops);
    }
}
