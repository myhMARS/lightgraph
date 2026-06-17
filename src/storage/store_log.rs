//! Append-only persistent log for store records.
//!
//! ## Format
//!
//! Each record: `[len: u32 LE][opcode: u8][payload: len bytes]`
//!
//! - `opcode 1 (INSERT)`: payload = bincode-serialized record
//! - `opcode 2 (DELETE)`: payload = empty (len=0)
//! - `opcode 0 (TOMBSTONE)`: skip this record (from compaction)
//!
//! ## Usage
//!
//! - `append(opcode, payload)`: write to log, fsync
//! - `replay(f)`: iterate records, call `f(opcode, payload)` for each
//! - `compact(path, live_set)`: write new log with only live records

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// Magic bytes at start of log file: "LGDB"
const MAGIC: &[u8; 4] = b"LGDB";
const OP_INSERT: u8 = 1;
const OP_DELETE: u8 = 2;
const OP_TOMBSTONE: u8 = 0;

pub struct StoreLog {
    path: PathBuf,
    writer: BufWriter<File>,
    bytes_written: u64,
}

impl StoreLog {
    /// Open or create a log file. If the file exists and has data,
    /// append mode is used. Otherwise a new file is created with magic header.
    pub fn open(path: &Path) -> io::Result<Self> {
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
        }

        Ok(Self {
            path: path.to_path_buf(),
            writer,
            bytes_written: if exists { fs::metadata(path)?.len() } else { 4 },
        })
    }

    /// Append an INSERT record. `payload` is the serialized record.
    pub fn append_insert(&mut self, payload: &[u8]) -> io::Result<()> {
        Self::write_record(&mut self.writer, OP_INSERT, payload)?;
        self.writer.get_mut().sync_all()?; // fsync
        self.bytes_written += 5 + payload.len() as u64;
        Ok(())
    }

    /// Append a DELETE record. `id_bytes` identifies the deleted record.
    pub fn append_delete(&mut self, id_bytes: &[u8]) -> io::Result<()> {
        Self::write_record(&mut self.writer, OP_DELETE, id_bytes)?;
        self.writer.get_mut().sync_all()?;
        self.bytes_written += 5 + id_bytes.len() as u64;
        Ok(())
    }

    fn write_record(w: &mut impl Write, opcode: u8, payload: &[u8]) -> io::Result<()> {
        let len = payload.len() as u32;
        w.write_all(&len.to_le_bytes())?;
        w.write_all(&[opcode])?;
        if !payload.is_empty() {
            w.write_all(payload)?;
        }
        w.flush()?;
        Ok(())
    }

    /// Current size of the log file in bytes.
    pub fn size_bytes(&self) -> u64 {
        self.bytes_written
    }

    /// Flush any buffered writes.
    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

/// Replay a log file, calling `on_record(opcode, payload)` for each record.
pub fn replay_log(path: &Path, mut on_record: impl FnMut(u8, &[u8])) -> io::Result<()> {
    let mut file = File::open(path)?;
    let mut magic = [0u8; 4];
    if file.read_exact(&mut magic).is_err() {
        return Ok(()); // empty file
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

/// Compact a log file: write a new file containing only the records
/// whose IDs are in `live_set`. The new file replaces the old one.
pub fn compact_log(
    path: &Path,
    live_set: &dashmap::DashSet<u64>,
    serializer: impl Fn(u64) -> Option<Vec<u8>>,
) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    let mut new_log = StoreLog::open(&tmp)?;

    for id in live_set.iter() {
        if let Some(payload) = serializer(*id) {
            new_log.append_insert(&payload)?;
        }
    }

    fs::rename(&tmp, path)?;
    Ok(())
}
