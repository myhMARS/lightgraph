//! Real performance benchmarks — measured, not estimated.
//!
//! Run: cargo test --test bench_real --release -- --nocapture --test-threads=1

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;
use lightgraph::transaction::Database;
use lightgraph::storage::prop_store::Value;

fn v(i: i64) -> Value { Value::Int(i) }

// ── Helpers ──────────────────────────────────────────────────────

fn median_us(data: &mut [f64]) -> f64 {
    data.sort_by(|a, b| a.partial_cmp(b).unwrap());
    data[data.len() / 2]
}

fn percentile(data: &mut [f64], p: f64) -> f64 {
    data.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((data.len() as f64) * p / 100.0) as usize;
    data[idx.min(data.len() - 1)]
}

/// Run a benchmark and report results.
fn run_bench(name: &str, threads: &[usize], mut f: impl FnMut(usize) -> (usize, Vec<f64>)) {
    println!("\n=== {} ===", name);
    println!("{:>12} {:>12} {:>12} {:>12} {:>12}",
             "threads", "ops", "ops/s", "p50(µs)", "p99(µs)");
    for &t in threads {
        let (total_ops, mut lats_us) = f(t);
        let ops = lats_us.len() as f64;
        let total_time_s = lats_us.iter().sum::<f64>() / 1_000_000.0 / t as f64;
        let throughput = ops / total_time_s;
        let p50 = percentile(&mut lats_us, 50.0);
        let p99 = percentile(&mut lats_us, 99.0);
        println!("{:>12} {:>12} {:>12.0} {:>12.0} {:>12.0}",
                 t, total_ops, throughput, p50, p99);
    }
}

// ── Benchmarks ───────────────────────────────────────────────────

#[test]
fn bench_single_thread_write() {
    let db = Database::memory();
    let n = 50_000;
    let mut lats = Vec::with_capacity(n);

    let start = Instant::now();
    for i in 0..n {
        let t0 = Instant::now();
        let tx = db.begin_write();
        tx.create_node(vec![1], i as u32);
        tx.set_prop(1, "val", i as u32, Some(v(i as i64)));
        tx.commit().unwrap();
        lats.push(t0.elapsed().as_secs_f64() * 1_000_000.0);
    }
    let elapsed = start.elapsed().as_secs_f64();

    let throughput = n as f64 / elapsed;
    println!("=== Single-thread Write (50K nodes + props) ===");
    println!("  total:    {:.2}s", elapsed);
    println!("  ops:      {}", n);
    println!("  ops/s:    {:.0}", throughput);
    println!("  p50:      {:.0}µs", percentile(&mut lats, 50.0));
    println!("  p99:      {:.0}µs", percentile(&mut lats, 99.0));
}

#[test]
fn bench_single_thread_read() {
    let db = Database::memory();
    // Pre-populate 100K nodes
    for i in 0..100_000 {
        let tx = db.begin_write();
        tx.create_node(vec![1], i as u32);
        tx.commit().unwrap();
    }

    let n = 500_000;
    let mut lats = Vec::with_capacity(n);

    let start = Instant::now();
    for _ in 0..n {
        let t0 = Instant::now();
        let rx = db.begin_read();
        let _ = rx.get_node((n as u64 % 100_000) as u64);
        lats.push(t0.elapsed().as_secs_f64() * 1_000_000.0);
    }
    let elapsed = start.elapsed().as_secs_f64();

    let throughput = n as f64 / elapsed;
    println!("\n=== Single-thread Read (500K gets) ===");
    println!("  total:    {:.2}s", elapsed);
    println!("  ops:      {}", n);
    println!("  ops/s:    {:.0}", throughput);
    println!("  p50:      {:.0}µs", percentile(&mut lats, 50.0));
    println!("  p99:      {:.0}µs", percentile(&mut lats, 99.0));
}

#[test]
fn bench_concurrent_reads() {
    let db = Arc::new(Database::memory());
    // Pre-populate
    for i in 0..50_000u64 {
        let tx = db.begin_write();
        tx.create_node(vec![1], i as u32);
        tx.commit().unwrap();
    }

    println!("\n=== Concurrent Reads (50K pre-loaded nodes) ===");
    println!("{:>12} {:>12} {:>12} {:>12} {:>12}",
             "threads", "ops", "ops/s", "p50(µs)", "p99(µs)");

    for threads in [1, 2, 4, 8, 16] {
        let ops_per_thread = 100_000 / threads;
        let mut all_lats = Vec::new();
        let barrier = Arc::new(Barrier::new(threads + 1));
        let mut handles = Vec::new();

        for t in 0..threads {
            let db = Arc::clone(&db);
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                b.wait();
                let mut lats = Vec::with_capacity(ops_per_thread);
                for i in 0..ops_per_thread {
                    let t0 = Instant::now();
                    let rx = db.begin_read();
                    let _ = rx.get_node(((t * ops_per_thread + i) as u64 % 50_000) as u64);
                    lats.push(t0.elapsed().as_secs_f64() * 1_000_000.0);
                }
                lats
            }));
        }
        barrier.wait();
        for h in handles {
            all_lats.extend(h.join().unwrap());
        }

        let p50 = percentile(&mut all_lats, 50.0);
        let p99 = percentile(&mut all_lats, 99.0);
        let total_ops: usize = all_lats.len();
        let avg_lat = all_lats.iter().sum::<f64>() / all_lats.len() as f64;
        let throughput = 1_000_000.0 / avg_lat * threads as f64;
        println!("{:>12} {:>12} {:>12.0} {:>12.0} {:>12.0}",
                 threads, total_ops, throughput, p50, p99);
    }
}

#[test]
fn bench_concurrent_writes() {
    println!("\n=== Concurrent Writes (node + property) ===");
    println!("{:>12} {:>12} {:>12} {:>12} {:>12}",
             "threads", "ops", "ops/s", "p50(µs)", "p99(µs)");

    for threads in [1, 2, 4, 8] {
        let ops_per_thread = 5000;
        let mut all_lats = Vec::new();
        let barrier = Arc::new(Barrier::new(threads + 1));
        let mut handles = Vec::new();

        for t in 0..threads {
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let db = Database::memory();
                b.wait();
                let mut lats = Vec::with_capacity(ops_per_thread);
                for i in 0..ops_per_thread {
                    let t0 = Instant::now();
                    let tx = db.begin_write();
                    let id = tx.create_node(vec![1], (t * ops_per_thread + i) as u32);
                    tx.set_prop(1, "val", id as u32, Some(v(id as i64)));
                    tx.commit().unwrap();
                    lats.push(t0.elapsed().as_secs_f64() * 1_000_000.0);
                }
                lats
            }));
        }
        barrier.wait();
        for h in handles {
            all_lats.extend(h.join().unwrap());
        }

        let p50 = percentile(&mut all_lats, 50.0);
        let p99 = percentile(&mut all_lats, 99.0);
        let total_ops: usize = all_lats.len();
        let avg_lat = all_lats.iter().sum::<f64>() / all_lats.len() as f64;
        let throughput = 1_000_000.0 / avg_lat * threads as f64;
        println!("{:>12} {:>12} {:>12.0} {:>12.0} {:>12.0}",
                 threads, total_ops, throughput, p50, p99);
    }
}

#[test]
fn bench_mixed_read_write() {
    println!("\n=== Mixed 70R/30W ===");
    println!("{:>12} {:>12} {:>12} {:>12} {:>12}",
             "threads", "ops", "ops/s", "p50(µs)", "p99(µs)");

    for threads in [4, 8] {
        let ops_per_thread = 5000usize;
        let mut all_lats = Vec::new();
        let barrier = Arc::new(Barrier::new(threads + 1));
        let mut handles = Vec::new();
        let db = Arc::new(Database::memory());

        // Pre-populate 10K nodes
        for i in 0..10_000u64 {
            let tx = db.begin_write();
            tx.create_node(vec![1], i as u32);
            tx.commit().unwrap();
        }

        for _ in 0..threads {
            let db = Arc::clone(&db);
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                b.wait();
                let mut lats = Vec::with_capacity(ops_per_thread);
                for i in 0..ops_per_thread {
                    let t0 = Instant::now();
                    if i % 3 == 0 {
                        // Write (30%)
                        let tx = db.begin_write();
                        tx.create_node(vec![1], 0);
                        tx.commit().unwrap();
                    } else {
                        // Read (70%)
                        let rx = db.begin_read();
                        let _ = rx.get_node((i as u64 % 10_000) as u64);
                    }
                    lats.push(t0.elapsed().as_secs_f64() * 1_000_000.0);
                }
                lats
            }));
        }
        barrier.wait();
        for h in handles {
            all_lats.extend(h.join().unwrap());
        }

        let p50 = percentile(&mut all_lats, 50.0);
        let p99 = percentile(&mut all_lats, 99.0);
        let total_ops: usize = all_lats.len();
        let avg_lat = all_lats.iter().sum::<f64>() / all_lats.len() as f64;
        let throughput = 1_000_000.0 / avg_lat * threads as f64;
        println!("{:>12} {:>12} {:>12.0} {:>12.0} {:>12.0}",
                 threads, total_ops, throughput, p50, p99);
    }
}

#[test]
fn bench_transaction_batch_size() {
    let db = Database::memory();
    println!("\n=== Transaction Batch Size Impact (single thread) ===");
    println!("{:>12} {:>12} {:>12} {:>12}",
             "batch_size", "total_writes", "ops/s", "µs/write");

    for batch in [1, 10, 100, 1000] {
        let total_writes = batch * 100;
        let start = Instant::now();
        for _ in 0..100 {
            let tx = db.begin_write();
            for i in 0..batch {
                tx.create_node(vec![1], 0);
            }
            tx.commit().unwrap();
        }
        let elapsed = start.elapsed().as_secs_f64();
        let ops = total_writes as f64 / elapsed;
        let us_per = elapsed * 1_000_000.0 / total_writes as f64;
        println!("{:>12} {:>12} {:>12.0} {:>12.1}",
                 batch, total_writes, ops, us_per);
    }
}
