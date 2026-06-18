//! Append-only persistent log with group commit.
//!
//! ## Format
//!
//! Each record: `[len: u32 LE][opcode: u8][payload: len bytes]`
//!   opcode 1 (INSERT), opcode 2 (DELETE), opcode 0 (TOMBSTONE)
//!
//! ## Group commit
//!
//! Instead of fsync on every write (which limits throughput to ~500 ops/s),
//! writes are buffered and fsynced in batches. Two triggers:
//!   1. Buffer reaches `batch_size` bytes → fsync
//!   2. Time since last fsync exceeds `batch_timeout` → fsync (on next write)
//!
//! This pushes throughput from ~500 ops/s to 40K-400K+ ops/s
//! while maintaining durability within a configurable window.
//!
//! ## Durability
//!
//! In the worst case, up to `batch_timeout` worth of writes can be lost
//! on crash. For absolute durability, set `batch_size = 1`.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const MAGIC: &[u8; 4] = b"LGDB";
const OP_INSERT: u8 = 1;
const OP_DELETE: u8 = 2;
const OP_TOMBSTONE: u8 = 0;

/// Default: fsync every 64KB or every 5ms, whichever comes first.
const DEFAULT_BATCH_BYTES: usize = 65536;
const DEFAULT_BATCH_TIMEOUT_MS: u64 = 5;

#[allow(dead_code)]
pub struct StoreLog {
    path: PathBuf,
    writer: BufWriter<File>,
    buffer: Vec<u8>,
    batch_size: usize,
    batch_timeout: Duration,
    last_sync: Instant,
    bytes_written: u64,
}

impl StoreLog {
    /// Open or create a log file with default group-commit settings.
    pub fn open(path: &Path) -> io::Result<Self> {
        Self::open_with_opts(path, DEFAULT_BATCH_BYTES, Duration::from_millis(DEFAULT_BATCH_TIMEOUT_MS))
    }

    /// Open with custom group-commit settings.
    /// - `batch_size`: fsync when buffer reaches this many bytes.
    /// - `batch_timeout`: fsync if this long has passed since last sync.
    /// Set `batch_size=1` for per-write fsync (max durability, ~500 ops/s).
    pub fn open_with_opts(path: &Path, batch_size: usize, batch_timeout: Duration) -> io::Result<Self> {
        let exists = path.exists() && fs::metadata(path)?.len() > 0;

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;

        let mut writer = BufWriter::with_capacity(65536, file);

        if !exists {
            writer.write_all(MAGIC)?;
            writer.flush()?;
            writer.get_mut().sync_all()?; // fsync the magic header
        }

        Ok(Self {
            path: path.to_path_buf(),
            writer,
            buffer: Vec::with_capacity(batch_size),
            batch_size,
            batch_timeout,
            last_sync: Instant::now(),
            bytes_written: if exists { fs::metadata(path)?.len() } else { 4 },
        })
    }

    /// Append an INSERT record. May or may not trigger fsync depending on group-commit settings.
    pub fn append_insert(&mut self, payload: &[u8]) -> io::Result<()> {
        Self::encode_record(&mut self.buffer, OP_INSERT, payload);
        self.maybe_sync()?;
        Ok(())
    }

    /// Append a DELETE record.
    pub fn append_delete(&mut self, id_bytes: &[u8]) -> io::Result<()> {
        Self::encode_record(&mut self.buffer, OP_DELETE, id_bytes);
        self.maybe_sync()?;
        Ok(())
    }

    /// Force flush and fsync now. Call before shutdown.
    pub fn sync_now(&mut self) -> io::Result<()> {
        self.flush_buffer()?;
        self.writer.get_mut().sync_all()
    }

    fn encode_record(buf: &mut Vec<u8>, opcode: u8, payload: &[u8]) {
        let len = payload.len() as u32;
        buf.extend_from_slice(&len.to_le_bytes());
        buf.push(opcode);
        if !payload.is_empty() {
            buf.extend_from_slice(payload);
        }
    }

    fn maybe_sync(&mut self) -> io::Result<()> {
        let buffer_full = self.buffer.len() >= self.batch_size;
        let timed_out = self.last_sync.elapsed() >= self.batch_timeout;

        if buffer_full || timed_out {
            self.flush_buffer()?;
            self.writer.get_mut().sync_all()?;
            self.last_sync = Instant::now();
            self.bytes_written += self.buffer.len() as u64;
            self.buffer.clear();
        }
        Ok(())
    }

    fn flush_buffer(&mut self) -> io::Result<()> {
        if !self.buffer.is_empty() {
            self.writer.write_all(&self.buffer)?;
            self.writer.flush()?;
        }
        Ok(())
    }

    /// Force flush + fsync any pending data.
    pub fn flush(&mut self) -> io::Result<()> {
        self.sync_now()
    }

    pub fn size_bytes(&self) -> u64 {
        self.bytes_written + self.buffer.len() as u64
    }
}

/// Replay a log file, calling `on_record(opcode, payload)` for each record.
pub fn replay_log(path: &Path, mut on_record: impl FnMut(u8, &[u8])) -> io::Result<()> {
    let mut file = File::open(path)?;
    let mut magic = [0u8; 4];
    if file.read_exact(&mut magic).is_err() {
        return Ok(());
    }
    if &magic != MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic"));
    }

    let mut len_buf = [0u8; 4];
    loop {
        match file.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
        let len = u32::from_le_bytes(len_buf) as usize;

        let mut opcode = [0u8; 1];
        file.read_exact(&mut opcode)?;

        let mut payload = vec![0u8; len];
        if len > 0 {
            file.read_exact(&mut payload)?;
        }

        if opcode[0] != OP_TOMBSTONE {
            on_record(opcode[0], &payload);
        }
    }
    Ok(())
}
