//! Consistency Contract — explicit durability semantics.
//!
//! Instead of "data might be lost" as an implicit implementation detail,
//! LightGraph makes durability an **explicit, user-chosen contract**.
//!
//! ## Durability Levels
//!
//! | Level | `flush()` latency | Crash window | Throughput | Use case |
//! |-------|-------------------|-------------|------------|----------|
//! | `Immediate` | ~2ms | 0 | ~500 ops/s | Financial records, auth |
//! | `Batch { interval_ms, bytes }` | ~0 | interval_ms | ~40K ops/s | User content, messages |
//! | `Async` | ~0 | unbounded | ~1M ops/s | Analytics, caches, ML features |
//!
//! ## Guarantees (all levels)
//!
//! **G1 — Write ordering**: writes to the same store are totally ordered
//! in the WAL. Recovery replays them in order.
//!
//! **G2 — flush() is the sync point**: after `store.flush()` returns,
//! all writes submitted before the flush are durable on disk.
//! (Under `Immediate`, every write implies a flush.)
//!
//! **G3 — Atomic recovery**: on restart, the database recovers to a state
//! consistent with the last completed fsync. Writes that were in the
//! channel but not fsynced are lost.
//!
//! **G4 — No partial writes**: each WAL record is self-contained.
//! A crash during append never produces a corrupt record.
//!
//! ## Non-guarantees
//!
//! - **Gap loss**: under `Batch`, writes in the un-synced window are lost together.
//!   The system does NOT support "some but not all" of the window.
//! - **Cross-store atomicity**: `NodeStore.flush()` and `EdgeStore.flush()`
//!   are independent. To atomically persist a node+edge write, use
//!   transactions (Sprint 2).
//! - **Disk corruption**: no checksums yet (planned Sprint 3).
//!
//! ## Example
//!
//! ```ignore
//! use lightgraph::storage::consistency::{Consistency, Durability};
//!
//! // Financial data — no loss tolerated
//! let store = NodeStore::open(path, Consistency {
//!     durability: Durability::Immediate,
//!     ..Consistency::default()
//! })?;
//!
//! // Social media posts — 5ms loss OK, high throughput
//! let store = NodeStore::open(path, Consistency {
//!     durability: Durability::Batch { interval_ms: 5, bytes: 65536 },
//!     ..Consistency::default()
//! })?;
//!
//! store.insert_node(...);
//! store.flush(); // ← explicit sync point: everything before this is now durable
//! ```

use std::time::Duration;

/// Durability level — how aggressively to fsync.
///
/// This is the core of the consistency contract.
/// The user chooses their trade-off between durability and throughput.
#[derive(Debug, Clone, PartialEq)]
pub enum Durability {
    /// Every write is fsynced before returning.
    /// Crash window: 0. Throughput: ~500 ops/s.
    Immediate,

    /// Writes are batched and fsynced periodically.
    ///
    /// - `interval_ms`: max time between fsyncs. Crash window = this value.
    /// - `bytes`: max buffer size before forced fsync.
    ///
    /// Crash window: at most `interval_ms` of writes.
    /// Throughput: ~40K ops/s at 5ms interval.
    Batch {
        interval_ms: u64,
        bytes: usize,
    },

    /// Writes go to the WAL thread but fsync is never guaranteed.
    /// The WAL thread still writes to the OS page cache.
    /// Data survives a process crash but NOT a power failure.
    ///
    /// Crash window: unbounded on power loss; zero on process restart.
    /// Throughput: ~400K+ ops/s.
    Async,
}

impl Default for Durability {
    fn default() -> Self {
        Durability::Batch { interval_ms: 5, bytes: 65536 }
    }
}

impl Durability {
    /// Human-readable description of the crash window.
    pub fn crash_window_description(&self) -> String {
        match self {
            Durability::Immediate => "zero (every write is fsynced)".into(),
            Durability::Batch { interval_ms, .. } => {
                format!("at most {}ms of writes", interval_ms)
            }
            Durability::Async => "unbounded on power loss; zero on process restart".into(),
        }
    }

    /// Approximate single-thread write throughput for this level.
    pub fn expected_throughput(&self) -> &'static str {
        match self {
            Durability::Immediate => "~500 ops/s",
            Durability::Batch { .. } => "~40,000 ops/s",
            Durability::Async => "~400,000+ ops/s",
        }
    }

    pub(crate) fn batch_timeout(&self) -> Duration {
        match self {
            Durability::Immediate => Duration::ZERO,  // fsync on every write
            Durability::Batch { interval_ms, .. } => Duration::from_millis(*interval_ms),
            Durability::Async => Duration::MAX,  // never fsync
        }
    }

    pub(crate) fn batch_bytes(&self) -> usize {
        match self {
            Durability::Immediate => 1,  // fsync every byte
            Durability::Batch { bytes, .. } => *bytes,
            Durability::Async => usize::MAX,  // never fsync on size
        }
    }

    pub(crate) fn fsync_enabled(&self) -> bool {
        !matches!(self, Durability::Async)
    }
}

/// Full consistency contract for a store.
///
/// Captures all guarantees the store makes to the user.
#[derive(Debug, Clone)]
pub struct Consistency {
    /// Durability level — the core trade-off.
    pub durability: Durability,

    /// Channel capacity for the WAL thread.
    /// Larger capacity = more buffering = higher throughput but larger crash window.
    pub wal_channel_capacity: usize,
}

impl Default for Consistency {
    fn default() -> Self {
        Self {
            durability: Durability::default(),
            wal_channel_capacity: 4096,
        }
    }
}

impl Consistency {
    /// Maximum durability — every write fsynced. For financial data.
    pub fn immediate() -> Self {
        Self { durability: Durability::Immediate, wal_channel_capacity: 1024 }
    }

    /// Balanced — 5ms batch window. For most user-facing apps.
    pub fn balanced() -> Self {
        Self::default()
    }

    /// High throughput — no fsync. For analytics, caches, ML embeddings.
    pub fn high_throughput() -> Self {
        Self { durability: Durability::Async, wal_channel_capacity: 8192 }
    }

    /// Custom batch window.
    pub fn batch(interval_ms: u64, bytes: usize) -> Self {
        Self {
            durability: Durability::Batch { interval_ms, bytes },
            wal_channel_capacity: 4096,
        }
    }
}
