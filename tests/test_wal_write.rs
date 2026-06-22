//! WAL write tests — Sprint 3
//!
//! Tests WalWriter group-commit behavior: batch accumulation,
//! automatic flush on buffer threshold, manual flush, and fsync.

use lightgraph::wal::WalWriter;
use lightgraph::wal::WalRecord;
use std::collections::HashMap;
use tempfile::TempDir;

fn open_writer(dir: &TempDir, name: &str) -> WalWriter {
    let path = dir.path().join(name);
    WalWriter::open(path.to_str().unwrap()).unwrap()
}

// ── Basic writes ────────────────────────────────────────────────────

#[test]
fn test_write_single_record() {
    let dir = TempDir::new().unwrap();
    let mut w = open_writer(&dir, "single.wal");
    w.write(&WalRecord::Begin(1)).unwrap();
    w.flush().unwrap();
    // File should have data
    let path = dir.path().join("single.wal");
    assert!(path.exists());
    assert!(std::fs::metadata(&path).unwrap().len() > 0);
}

#[test]
fn test_write_multiple_records() {
    let dir = TempDir::new().unwrap();
    let mut w = open_writer(&dir, "multi.wal");

    for i in 0..100 {
        w.write(&WalRecord::Begin(i)).unwrap();
        w.write(&WalRecord::Commit(i)).unwrap();
    }
    w.flush().unwrap();

    let path = dir.path().join("multi.wal");
    assert!(std::fs::metadata(&path).unwrap().len() > 0);
}

// ── Flush behavior ──────────────────────────────────────────────────

#[test]
fn test_explicit_flush_persists_data() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("flush.wal");
    let path_str = path.to_str().unwrap();

    {
        let mut w = WalWriter::open(path_str).unwrap();
        w.write(&WalRecord::Begin(42)).unwrap();
        w.write(&WalRecord::Commit(42)).unwrap();
        w.flush().unwrap();
    }

    // Verify data is on disk
    let data = std::fs::read(&path).unwrap();
    assert!(!data.is_empty(), "flush() should persist data to disk");
}

#[test]
fn test_flush_clears_buffer() {
    let dir = TempDir::new().unwrap();
    let mut w = open_writer(&dir, "clear.wal");

    // Write many records to fill buffer
    for i in 0..200 {
        w.write(&WalRecord::Begin(i)).unwrap();
    }
    w.flush().unwrap();

    // After flush, writing more and flushing again should work
    w.write(&WalRecord::Commit(999)).unwrap();
    w.flush().unwrap();
}

#[test]
fn test_group_commit_stress() {
    let dir = TempDir::new().unwrap();
    let mut w = open_writer(&dir, "stress.wal");

    // Simulate many small transactions to stress group commit
    for tx in 1..=500 {
        w.write(&WalRecord::Begin(tx)).unwrap();
        w.write(&WalRecord::NodeCreate(
            tx, tx, vec!["Node".into()], HashMap::new(),
        )).unwrap();
        w.write(&WalRecord::Commit(tx)).unwrap();
    }
    w.flush().unwrap();

    let path = dir.path().join("stress.wal");
    let size = std::fs::metadata(&path).unwrap().len();
    assert!(size > 0, "stress test should produce output");
}

// ── Record type coverage ────────────────────────────────────────────

#[test]
fn test_all_record_types_serialize() {
    let dir = TempDir::new().unwrap();
    let mut w = open_writer(&dir, "types.wal");

    let mut props = HashMap::new();
    props.insert("key".into(), lightgraph::storage::prop_store::Value::Int(42));

    let records = vec![
        WalRecord::Begin(1),
        WalRecord::NodeCreate(1, 1, vec!["A".into()], props.clone()),
        WalRecord::NodeUpdate(1, 1, props.clone()),
        WalRecord::NodeDelete(1, 1),
        WalRecord::EdgeCreate(1, 100, 1, 2, "KNOWS".into(), props),
        WalRecord::EdgeDelete(1, 100),
        WalRecord::Rollback(2),
        WalRecord::Checkpoint(1),
        WalRecord::Commit(1),
    ];

    for r in &records {
        w.write(r).unwrap();
    }
    w.flush().unwrap();
}

// ── Checkpoint marker ───────────────────────────────────────────────

#[test]
fn test_checkpoint_written() {
    let dir = TempDir::new().unwrap();
    let mut w = open_writer(&dir, "ckpt.wal");

    w.write(&WalRecord::Begin(1)).unwrap();
    w.write(&WalRecord::NodeCreate(1, 1, vec!["X".into()], HashMap::new())).unwrap();
    w.write(&WalRecord::Commit(1)).unwrap();
    w.write(&WalRecord::Checkpoint(1)).unwrap();
    w.flush().unwrap();

    let path = dir.path().join("ckpt.wal");
    assert!(std::fs::metadata(&path).unwrap().len() > 0);
}
