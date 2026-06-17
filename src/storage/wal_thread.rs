//! Dedicated WAL thread — the single writer to the append-only log.
//!
//! ## Architecture
//!
//! ```
//! Business threads (N)          WAL Thread (1)
//! ┌──────────┐                 ┌──────────────────┐
//! │ insert    │                 │  recv(channel)   │
//! │ to DashMap│                 │  serialize batch │
//! │           │                 │  append to file  │
//! │ send(cmd) ├────────────────►│  group fsync     │
//! └──────────┘                 └──────────────────┘
//! ```
//!
//! - Business threads never touch disk — zero I/O latency.
//! - WAL thread serializes in batches for better throughput.
//! - Data loss window: commands in channel not yet fsynced.
//!   At most ~5ms of writes (configurable).
//! - `flush()` drains the channel and fsyncs immediately.
//!   Call this on transaction commit for synchronous durability.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam::channel::{Sender, Receiver, bounded};

use super::consistency::Durability;

/// A command sent to the WAL thread.
pub enum WalCmd {
    /// Insert/update: (id, serialized_record)
    Insert(u64, Vec<u8>),
    /// Delete: (id)
    Delete(u64),
    /// Force fsync now. Sender sends back () when done.
    Flush(Sender<()>),
    /// Shutdown. Sender sends back () when done.
    Shutdown(Sender<()>),
}

const MAGIC: &[u8; 4] = b"LGDB";

pub struct WalThread {
    tx: Sender<WalCmd>,
    handle: Option<JoinHandle<()>>,
    /// Approximate bytes pending in channel/disk buffer.
    pending_bytes: Arc<AtomicU64>,
    running: Arc<AtomicBool>,
}

impl WalThread {
    /// Spawn the WAL thread with an explicit durability contract.
    ///
    /// - `log_path`: path to the WAL file.
    /// - `durability`: the consistency contract — defines fsync behavior.
    /// - `channel_cap`: channel buffer size.
    pub fn spawn(
        log_path: &Path,
        durability: Durability,
        channel_cap: usize,
    ) -> io::Result<Self> {
        if let Some(parent) = log_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let exists = log_path.exists() && std::fs::metadata(&log_path)?.len() > 0;

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)?;
        let mut writer = BufWriter::with_capacity(durability.batch_bytes() * 2, file);

        if !exists {
            writer.write_all(MAGIC)?;
            writer.flush()?;
            writer.get_mut().sync_all()?;
        }

        let (tx, rx) = bounded::<WalCmd>(channel_cap);
        let pending = Arc::new(AtomicU64::new(0));
        let running = Arc::new(AtomicBool::new(true));
        let pending_clone = Arc::clone(&pending);
        let running_clone = Arc::clone(&running);

        let batch_bytes = durability.batch_bytes();
        let fsync_enabled = durability.fsync_enabled();
        // Channel recv timeout: use batch interval, but minimum 1ms to avoid busy loop
        let recv_timeout = durability.batch_timeout().max(Duration::from_millis(1));

        let handle = thread::spawn(move || {
            wal_loop(rx, &mut writer, batch_bytes, recv_timeout, durability.batch_timeout(), fsync_enabled, &pending_clone, &running_clone);
        });

        Ok(Self {
            tx,
            handle: Some(handle),
            pending_bytes: pending,
            running,
        })
    }

    /// Send an insert command. Non-blocking if channel has capacity.
    pub fn send_insert(&self, id: u64, record: Vec<u8>) {
        self.pending_bytes.fetch_add(record.len() as u64 + 9, Ordering::Relaxed);
        // Ignore send error: WAL thread is shutting down
        let _ = self.tx.try_send(WalCmd::Insert(id, record));
    }

    /// Send a delete command.
    pub fn send_delete(&self, id: u64) {
        self.pending_bytes.fetch_add(9, Ordering::Relaxed);
        let _ = self.tx.try_send(WalCmd::Delete(id));
    }

    /// Flush: drain the channel and fsync. Blocks until done.
    /// Call on transaction commit for synchronous durability.
    pub fn flush(&self) {
        let (reply_tx, reply_rx) = bounded(1);
        let _ = self.tx.send(WalCmd::Flush(reply_tx));
        let _ = reply_rx.recv();
        self.pending_bytes.store(0, Ordering::Relaxed);
    }

    /// Approximate bytes not yet fsynced.
    pub fn pending_bytes(&self) -> u64 {
        self.pending_bytes.load(Ordering::Relaxed)
    }

    /// Shutdown: drain + fsync + stop thread. Blocks until done.
    pub fn shutdown(mut self) {
        self.running.store(false, Ordering::SeqCst);
        let (reply_tx, reply_rx) = bounded(1);
        let _ = self.tx.send(WalCmd::Shutdown(reply_tx));
        let _ = reply_rx.recv();
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for WalThread {
    fn drop(&mut self) {
        // Best-effort shutdown
        self.running.store(false, Ordering::SeqCst);
    }
}

/// The WAL thread main loop.
/// - `recv_timeout`: how long to wait for channel messages (≥1ms to avoid busy loop)
/// - `fsync_timeout`: how frequently to fsync (can be 0 for Immediate)
fn wal_loop(
    rx: Receiver<WalCmd>,
    writer: &mut BufWriter<File>,
    batch_size: usize,
    recv_timeout: Duration,
    fsync_timeout: Duration,
    fsync_enabled: bool,
    pending: &AtomicU64,
    running: &AtomicBool,
) {
    let mut buffer: Vec<u8> = Vec::with_capacity(batch_size);
    let mut last_sync = Instant::now();

    loop {
        // Block until next command or timeout
        match rx.recv_timeout(recv_timeout) {
            Ok(cmd) => {
                match cmd {
                    WalCmd::Insert(id, record) => {
                        encode_insert(&mut buffer, id, &record);
                    }
                    WalCmd::Delete(id) => {
                        encode_delete(&mut buffer, id);
                    }
                    WalCmd::Flush(reply) => {
                        // Drain any other pending commands, then sync
                        flush_and_sync(writer, &mut buffer, pending, fsync_enabled);
                        last_sync = Instant::now();
                        let _ = reply.send(());
                        continue;
                    }
                    WalCmd::Shutdown(reply) => {
                        flush_and_sync(writer, &mut buffer, pending, fsync_enabled);
                        let _ = reply.send(());
                        return;
                    }
                }
            }
            Err(_) => {
                // Timeout — check if we should sync
                if !running.load(Ordering::SeqCst) && buffer.is_empty() {
                    return;
                }
            }
        }

        // Sync if buffer full or timed out
        let buffer_full = buffer.len() >= batch_size;
        let timed_out = last_sync.elapsed() >= fsync_timeout;

        if (buffer_full || timed_out) && !buffer.is_empty() {
            flush_and_sync(writer, &mut buffer, pending, fsync_enabled);
            last_sync = Instant::now();
        }
    }
}

fn encode_insert(buf: &mut Vec<u8>, id: u64, record: &[u8]) {
    // Record: [total_len: u32 LE][opcode: u8=1][id: u64 LE][payload...]
    let total = 1 + 8 + record.len(); // opcode + id + payload
    buf.extend_from_slice(&(total as u32).to_le_bytes());
    buf.push(1u8); // INSERT
    buf.extend_from_slice(&id.to_le_bytes());
    buf.extend_from_slice(record);
}

fn encode_delete(buf: &mut Vec<u8>, id: u64) {
    let total = 1 + 8; // opcode + id
    buf.extend_from_slice(&(total as u32).to_le_bytes());
    buf.push(2u8); // DELETE
    buf.extend_from_slice(&id.to_le_bytes());
}

fn flush_and_sync(writer: &mut BufWriter<File>, buffer: &mut Vec<u8>, pending: &AtomicU64, fsync: bool) {
    if buffer.is_empty() {
        return;
    }
    if let Err(e) = writer.write_all(buffer) {
        eprintln!("WAL write error: {}", e);
    }
    if let Err(e) = writer.flush() {
        eprintln!("WAL flush error: {}", e);
    }
    if fsync {
        if let Err(e) = writer.get_mut().sync_all() {
            eprintln!("WAL fsync error: {}", e);
        }
    }
    pending.store(0, Ordering::Relaxed);
    buffer.clear();
}

/// Replay a WAL log file, calling `on_record(opcode, id, payload)` for each record.
pub fn replay_wal(path: &Path, mut on_record: impl FnMut(u8, u64, &[u8])) -> io::Result<()> {
    use std::io::Read;

    let mut file = File::open(path)?;
    let mut magic = [0u8; 4];
    if file.read_exact(&mut magic).is_err() {
        return Ok(());
    }
    if &magic != MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad wal magic"));
    }

    let mut len_buf = [0u8; 4];
    loop {
        match file.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
        let total = u32::from_le_bytes(len_buf) as usize;
        if total < 9 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "record too short"));
        }

        let mut header = [0u8; 9];
        file.read_exact(&mut header)?;
        let opcode = header[0];
        let id = u64::from_le_bytes(header[1..9].try_into().unwrap());
        let payload_len = total - 9;
        let mut payload = vec![0u8; payload_len];
        if payload_len > 0 {
            file.read_exact(&mut payload)?;
        }

        on_record(opcode, id, &payload);
    }
    Ok(())
}
