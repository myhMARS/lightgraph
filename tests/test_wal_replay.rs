//! WAL replay tests — Sprint 3
//!
//! Tests WalReader sequential replay: write-then-read roundtrip,
//! multiple transactions, partial writes, and recovery consistency.

use lightgraph::wal::{WalReader, WalWriter, WalRecord};
use std::collections::HashMap;
use tempfile::TempDir;

fn write_and_replay(dir: &TempDir, name: &str, records: &[WalRecord]) -> Vec<WalRecord> {
    let path = dir.path().join(name);
    let path_str = path.to_str().unwrap();

    // Write phase
    {
        let mut w = WalWriter::open(path_str).unwrap();
        for r in records {
            w.write(r).unwrap();
        }
        w.flush().unwrap();
    }

    // Replay phase
    let mut reader = WalReader::open(path_str).unwrap();
    reader.replay().unwrap()
}

// ── Roundtrip ───────────────────────────────────────────────────────

#[test]
fn test_roundtrip_single_transaction() {
    let dir = TempDir::new().unwrap();
    let records = vec![
        WalRecord::Begin(1),
        WalRecord::NodeCreate(1, 42, vec!["Person".into()], HashMap::new()),
        WalRecord::Commit(1),
    ];
    let recovered = write_and_replay(&dir, "rt.wal", &records);
    assert_eq!(recovered.len(), 3);
    assert!(matches!(recovered[0], WalRecord::Begin(1)));
    assert!(matches!(recovered[2], WalRecord::Commit(1)));
}

#[test]
fn test_roundtrip_multiple_transactions() {
    let dir = TempDir::new().unwrap();
    let mut records = Vec::new();
    for tx in 1..=10 {
        records.push(WalRecord::Begin(tx));
        records.push(WalRecord::NodeCreate(tx, tx * 10, vec!["N".into()], HashMap::new()));
        records.push(WalRecord::Commit(tx));
    }
    let recovered = write_and_replay(&dir, "multi.wal", &records);
    assert_eq!(recovered.len(), 30);

    // Verify all Begin/Commit pairs
    let begins: Vec<u64> = recovered.iter()
        .filter_map(|r| if let WalRecord::Begin(id) = r { Some(*id) } else { None })
        .collect();
    assert_eq!(begins, (1..=10).collect::<Vec<_>>());
}

#[test]
fn test_roundtrip_all_record_types() {
    let dir = TempDir::new().unwrap();
    let mut props = HashMap::new();
    props.insert("k".into(), lightgraph::storage::prop_store::Value::String("v".into()));

    let records = vec![
        WalRecord::Begin(1),
        WalRecord::NodeCreate(1, 1, vec!["A".into()], props.clone()),
        WalRecord::NodeUpdate(1, 1, props.clone()),
        WalRecord::EdgeCreate(1, 100, 1, 2, "E".into(), props.clone()),
        WalRecord::EdgeDelete(1, 100),
        WalRecord::NodeDelete(1, 1),
        WalRecord::Rollback(2),
        WalRecord::Checkpoint(1),
        WalRecord::Commit(1),
    ];
    let recovered = write_and_replay(&dir, "types.wal", &records);
    assert_eq!(recovered.len(), records.len());

    // Verify record counts by type
    let begins = recovered.iter().filter(|r| matches!(r, WalRecord::Begin(_))).count();
    let commits = recovered.iter().filter(|r| matches!(r, WalRecord::Commit(_))).count();
    let rollbacks = recovered.iter().filter(|r| matches!(r, WalRecord::Rollback(_))).count();
    let checkpoints = recovered.iter().filter(|r| matches!(r, WalRecord::Checkpoint(_))).count();
    assert_eq!(begins, 1);
    assert_eq!(commits, 1);
    assert_eq!(rollbacks, 1);
    assert_eq!(checkpoints, 1);
}

// ── Empty / Edge cases ──────────────────────────────────────────────

#[test]
fn test_replay_empty_file() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("empty.wal");
    std::fs::write(&path, b"").unwrap();

    let mut reader = WalReader::open(path.to_str().unwrap()).unwrap();
    let recovered = reader.replay().unwrap();
    assert!(recovered.is_empty());
}

#[test]
fn test_replay_large_transaction() {
    let dir = TempDir::new().unwrap();
    let mut records = vec![WalRecord::Begin(1)];
    for i in 1..=1000 {
        records.push(WalRecord::NodeCreate(1, i, vec!["N".into()], HashMap::new()));
    }
    records.push(WalRecord::Commit(1));

    let recovered = write_and_replay(&dir, "large.wal", &records);
    assert_eq!(recovered.len(), 1002);
}

// ── Recovery consistency ────────────────────────────────────────────

#[test]
fn test_replay_preserves_record_order() {
    let dir = TempDir::new().unwrap();
    let records: Vec<_> = (1..=50).map(|i| {
        if i % 2 == 0 {
            WalRecord::Begin(i)
        } else {
            WalRecord::Commit(i)
        }
    }).collect();

    let recovered = write_and_replay(&dir, "order.wal", &records);

    // Odd ids are Commit, even ids are Begin (from construction above)
    for (i, r) in recovered.iter().enumerate() {
        let expected_id = (i + 1) as u64;
        if expected_id % 2 == 0 {
            assert!(matches!(r, WalRecord::Begin(id) if *id == expected_id),
                "Expected Begin({}) at position {}", expected_id, i);
        } else {
            assert!(matches!(r, WalRecord::Commit(id) if *id == expected_id),
                "Expected Commit({}) at position {}", expected_id, i);
        }
    }
}

#[test]
fn test_replay_includes_rollback_transactions() {
    let dir = TempDir::new().unwrap();
    let records = vec![
        WalRecord::Begin(1),
        WalRecord::NodeCreate(1, 1, vec!["X".into()], HashMap::new()),
        WalRecord::Rollback(1),  // rolled back, not committed
    ];
    let recovered = write_and_replay(&dir, "rollback.wal", &records);
    assert_eq!(recovered.len(), 3);
    // Recovery should include rollback marker so recovery logic can skip
    assert!(matches!(recovered[2], WalRecord::Rollback(1)));
}
