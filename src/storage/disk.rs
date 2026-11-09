//! On-disk storage: a segmented write-ahead log, an atomically replaced
//! hard-state file, and checksummed snapshot files.
//!
//! ```text
//! data-dir/
//!   LOCK                               held while the process runs
//!   meta                               node id, cluster id, term, vote
//!   wal-00000000000000000001.seg       [header][record][record]...
//!   snap-<last_index>-<last_term>.snap
//! record := [payload_len u32][crc32 u32][term u64][index u64][data]
//! ```

use super::Storage;
use crate::codec::*;
use crate::types::*;
use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

const WAL_MAGIC: &[u8; 8] = b"RKVWAL01";
const META_MAGIC: &[u8; 8] = b"RKVMET01";
const SNAP_MAGIC: &[u8; 8] = b"RKVSNP01";
const WAL_HEADER: usize = 8 + 8 + 4;
const RECORD_HEADER: usize = 8;
const MIN_PAYLOAD: usize = 16;
pub const MAX_RECORD: usize = 64 * 1024 * 1024;

/// Stop the process on an I/O failure. See the note in `storage/mod.rs`.
fn fatal(ctx: &str, e: io::Error) -> ! {
    eprintln!("level=ERROR event=fatal_io ctx={ctx:?} err={e}");
    std::process::abort();
}

extern "C" {
    fn fsync(fd: i32) -> i32;
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    let f = File::open(dir)?;
    match f.sync_all() {
        // On macOS, sync_all uses F_FULLFSYNC, which some file systems reject
        // for directories (EINVAL, ENOTTY, ENOTSUP). Plain fsync still applies.
        Err(e) if cfg!(target_os = "macos") && matches!(e.raw_os_error(), Some(22 | 25 | 45 | 102)) => {
            use std::os::fd::AsRawFd;
            if unsafe { fsync(f.as_raw_fd()) } == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        }
        other => other,
    }
}

fn corrupt(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

#[derive(Clone, Debug)]
pub struct DiskOptions {
    pub node_id: NodeId,
    pub cluster_id: u64,
    pub segment_bytes: u64,
    pub skip_fsync: bool,
}

impl Default for DiskOptions {
    fn default() -> Self {
        DiskOptions { node_id: 1, cluster_id: 1, segment_bytes: 16 * 1024 * 1024, skip_fsync: false }
    }
}

struct Segment {
    first_index: Index,
    path: PathBuf,
}

#[derive(Default, Clone, Copy, Debug)]
pub struct DiskStats {
    pub fsyncs: u64,
    pub fsync_micros: u64,
    pub bytes_written: u64,
}

pub struct DiskStorage {
    dir: PathBuf,
    opts: DiskOptions,
    _lock: File,
    hs: HardState,
    snap_meta: SnapshotMeta,
    snap_data: Option<Arc<Vec<u8>>>,
    entries: VecDeque<Entry>,
    /// File offset of each entry in `entries`, within its own segment.
    offsets: VecDeque<u64>,
    segments: Vec<Segment>,
    file: File,
    file_len: u64,
    buf: Vec<u8>,
    needs_sync: bool,
    pub stats: DiskStats,
}

fn seg_path(dir: &Path, first: Index) -> PathBuf {
    dir.join(format!("wal-{first:020}.seg"))
}

fn snap_path(dir: &Path, m: SnapshotMeta) -> PathBuf {
    dir.join(format!("snap-{:020}-{:020}.snap", m.last_index, m.last_term))
}

fn create_segment(dir: &Path, first: Index) -> io::Result<(File, PathBuf)> {
    let path = seg_path(dir, first);
    let mut file = OpenOptions::new().create(true).truncate(true).read(true).write(true).open(&path)?;
    let mut h = Writer::with_capacity(WAL_HEADER);
    h.buf.extend_from_slice(WAL_MAGIC);
    h.u64(first);
    let crc = crc32(&h.buf);
    h.u32(crc);
    file.write_all(&h.buf)?;
    file.sync_all()?;
    sync_dir(dir)?;
    Ok((file, path))
}

fn encode_record(out: &mut Vec<u8>, e: &Entry) {
    let payload_len = MIN_PAYLOAD + e.data.len();
    let start = out.len();
    out.extend_from_slice(&(payload_len as u32).to_le_bytes());
    out.extend_from_slice(&[0u8; 4]);
    out.extend_from_slice(&e.term.to_le_bytes());
    out.extend_from_slice(&e.index.to_le_bytes());
    out.extend_from_slice(&e.data);
    let crc = crc32(&out[start + RECORD_HEADER..]);
    out[start + 4..start + 8].copy_from_slice(&crc.to_le_bytes());
}

enum Parsed {
    Record { term: Term, index: Index, data: Vec<u8>, next: usize },
    End,
    Bad { checksum_failed_at_full_length: Option<usize> },
}

fn parse_record(b: &[u8], off: usize) -> Parsed {
    if off == b.len() {
        return Parsed::End;
    }
    if b.len() - off < RECORD_HEADER {
        return Parsed::Bad { checksum_failed_at_full_length: None };
    }
    let len = u32::from_le_bytes(b[off..off + 4].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(b[off + 4..off + 8].try_into().unwrap());
    if !(MIN_PAYLOAD..=MAX_RECORD).contains(&len) || b.len() - off - RECORD_HEADER < len {
        return Parsed::Bad { checksum_failed_at_full_length: None };
    }
    let start = off + RECORD_HEADER;
    let payload = &b[start..start + len];
    if crc32(payload) != crc {
        return Parsed::Bad { checksum_failed_at_full_length: Some(start + len) };
    }
    Parsed::Record {
        term: u64::from_le_bytes(payload[0..8].try_into().unwrap()),
        index: u64::from_le_bytes(payload[8..16].try_into().unwrap()),
        data: payload[16..].to_vec(),
        next: start + len,
    }
}

fn valid_header(b: &[u8]) -> Option<Index> {
    if b.len() < WAL_HEADER || &b[..8] != WAL_MAGIC {
        return None;
    }
    let crc = u32::from_le_bytes(b[16..20].try_into().unwrap());
    if crc32(&b[..16]) != crc {
        return None;
    }
    Some(u64::from_le_bytes(b[8..16].try_into().unwrap()))
}

fn load_meta(path: &Path) -> io::Result<Option<(NodeId, u64, HardState)>> {
    let b = match fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    if b.len() < 12 || &b[..8] != META_MAGIC {
        return Err(corrupt("meta file has a bad header".into()));
    }
    let (body, tail) = b.split_at(b.len() - 4);
    if crc32(body) != u32::from_le_bytes(tail.try_into().unwrap()) {
        return Err(corrupt("meta file failed its checksum".into()));
    }
    let mut r = Reader::new(&body[8..]);
    let parse =
        |r: &mut Reader| -> DResult<(u64, u64, u64, Option<u64>)> { Ok((r.u64()?, r.u64()?, r.u64()?, r.opt_u64()?)) };
    let (node, cluster, term, voted_for) = parse(&mut r).map_err(|e| corrupt(e.to_string()))?;
    Ok(Some((node, cluster, HardState { term, voted_for })))
}

fn store_meta(dir: &Path, node: NodeId, cluster: u64, hs: HardState) -> io::Result<()> {
    let mut w = Writer::with_capacity(64);
    w.buf.extend_from_slice(META_MAGIC);
    w.u64(node);
    w.u64(cluster);
    w.u64(hs.term);
    w.opt_u64(hs.voted_for);
    let crc = crc32(&w.buf);
    w.u32(crc);
    let tmp = dir.join("meta.tmp");
    let mut f = OpenOptions::new().create(true).truncate(true).write(true).open(&tmp)?;
    f.write_all(&w.buf)?;
    f.sync_all()?;
    fs::rename(&tmp, dir.join("meta"))?;
    sync_dir(dir)
}

/// Write a snapshot file atomically. A free function so a background thread
/// can do the slow part without blocking heartbeats.
pub fn write_snapshot_file(dir: &Path, meta: SnapshotMeta, data: &[u8]) -> io::Result<()> {
    let path = snap_path(dir, meta);
    let tmp = path.with_extension("tmp");
    let mut w = Writer::with_capacity(44);
    w.buf.extend_from_slice(SNAP_MAGIC);
    w.u64(meta.last_index);
    w.u64(meta.last_term);
    w.u64(data.len() as u64);
    w.u32(crc32(data));
    let hcrc = crc32(&w.buf);
    w.u32(hcrc);
    let mut f = OpenOptions::new().create(true).truncate(true).write(true).open(&tmp)?;
    f.write_all(&w.buf)?;
    f.write_all(data)?;
    f.sync_all()?;
    fs::rename(&tmp, &path)?;
    sync_dir(dir)
}

fn read_snapshot_file(path: &Path) -> io::Result<(SnapshotMeta, Vec<u8>)> {
    let mut f = File::open(path)?;
    let mut h = [0u8; 40];
    f.read_exact(&mut h)?;
    if &h[..8] != SNAP_MAGIC || crc32(&h[..36]) != u32::from_le_bytes(h[36..40].try_into().unwrap()) {
        return Err(corrupt(format!("{}: bad snapshot header", path.display())));
    }
    let meta = SnapshotMeta {
        last_index: u64::from_le_bytes(h[8..16].try_into().unwrap()),
        last_term: u64::from_le_bytes(h[16..24].try_into().unwrap()),
    };
    let len = u64::from_le_bytes(h[24..32].try_into().unwrap()) as usize;
    let want = u32::from_le_bytes(h[32..36].try_into().unwrap());
    let mut data = Vec::with_capacity(len);
    f.read_to_end(&mut data)?;
    if data.len() != len || crc32(&data) != want {
        return Err(corrupt(format!("{}: snapshot failed its checksum", path.display())));
    }
    Ok((meta, data))
}

/// All files with `prefix`/`suffix` in `dir`, sorted by name. The fixed-width
/// numbers in the names make lexical order equal numeric order.
fn list(dir: &Path, prefix: &str, suffix: &str) -> io::Result<Vec<PathBuf>> {
    let mut v = Vec::new();
    for e in fs::read_dir(dir)? {
        let p = e?.path();
        if let Some(n) = p.file_name().and_then(|s| s.to_str()) {
            if n.starts_with(prefix) && n.ends_with(suffix) {
                v.push(p);
            }
        }
    }
    v.sort();
    Ok(v)
}

fn load_newest_snapshot(dir: &Path) -> io::Result<Option<(SnapshotMeta, Vec<u8>)>> {
    for p in list(dir, "snap-", ".snap")?.into_iter().rev() {
        match read_snapshot_file(&p) {
            Ok(x) => return Ok(Some(x)),
            Err(e) => eprintln!("level=WARN event=snapshot_unreadable path={p:?} err={e}"),
        }
    }
    Ok(None)
}

impl DiskStorage {
    /// Open or create a data directory, running crash recovery.
    ///
    /// Returns an error, rather than guessing, when the data shows damage that
    /// a crash cannot explain. A replica that restarts with silently missing
    /// entries can break the quorum guarantee for writes already acknowledged.
    pub fn open(dir: impl AsRef<Path>, opts: DiskOptions) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let lock = OpenOptions::new().create(true).truncate(false).write(true).open(dir.join("LOCK"))?;
        lock.try_lock().map_err(|e| {
            io::Error::new(io::ErrorKind::WouldBlock, format!("data dir {} is in use: {e}", dir.display()))
        })?;

        for tmp in list(&dir, "", ".tmp")? {
            fs::remove_file(tmp)?;
        }

        let wal_files = list(&dir, "wal-", ".seg")?;
        let hs = match load_meta(&dir.join("meta"))? {
            Some((node, cluster, hs)) => {
                if node != opts.node_id || cluster != opts.cluster_id {
                    return Err(corrupt(format!(
                        "data dir belongs to node {node} of cluster {cluster}, not node {} of cluster {}",
                        opts.node_id, opts.cluster_id
                    )));
                }
                hs
            }
            None => {
                if !wal_files.is_empty() {
                    return Err(corrupt("WAL exists but the meta file is gone; term and vote were lost".into()));
                }
                let hs = HardState::default();
                store_meta(&dir, opts.node_id, opts.cluster_id, hs)?;
                hs
            }
        };

        let snap = load_newest_snapshot(&dir)?;
        let (snap_meta, snap_data) = match snap {
            Some((m, d)) => (m, Some(Arc::new(d))),
            None => (SnapshotMeta::default(), None),
        };

        let mut entries: VecDeque<Entry> = VecDeque::new();
        let mut offsets: VecDeque<u64> = VecDeque::new();
        let mut segments: Vec<Segment> = Vec::new();
        let mut tail_len = 0u64;

        for (i, path) in wal_files.iter().enumerate() {
            let is_last = i + 1 == wal_files.len();
            let b = fs::read(path)?;
            let first = match valid_header(&b) {
                Some(f) => f,
                None if is_last => {
                    // The crash hit while creating this segment. It held nothing.
                    fs::remove_file(path)?;
                    sync_dir(&dir)?;
                    break;
                }
                None => return Err(corrupt(format!("{}: bad segment header", path.display()))),
            };
            if let Some(last) = entries.back() {
                if first != last.index + 1 {
                    return Err(corrupt(format!(
                        "{}: starts at {first} but previous entry is {}",
                        path.display(),
                        last.index
                    )));
                }
            }
            let mut off = WAL_HEADER;
            let mut expect = first;
            loop {
                match parse_record(&b, off) {
                    Parsed::Record { term, index, data, next } => {
                        if index != expect {
                            return Err(corrupt(format!("{}: expected index {expect}, found {index}", path.display())));
                        }
                        if let Some(prev) = entries.back() {
                            if term < prev.term {
                                return Err(corrupt(format!("{}: term decreased at {index}", path.display())));
                            }
                        }
                        entries.push_back(Entry { term, index, data: data.into() });
                        offsets.push_back(off as u64);
                        expect += 1;
                        off = next;
                    }
                    Parsed::End => break,
                    Parsed::Bad { checksum_failed_at_full_length } => {
                        if !is_last {
                            return Err(corrupt(format!("{}: bad record at offset {off}", path.display())));
                        }
                        // A torn final write has nothing valid after it. A bad
                        // record followed by a good one is bit rot in data we
                        // may have acknowledged, so it must not be cut away.
                        if let Some(n) = checksum_failed_at_full_length {
                            if n < b.len() && matches!(parse_record(&b, n), Parsed::Record { .. }) {
                                return Err(corrupt(format!(
                                    "{}: corrupt record at offset {off} followed by valid data",
                                    path.display()
                                )));
                            }
                        }
                        eprintln!(
                            "level=WARN event=wal_torn_tail path={path:?} valid_bytes={off} dropped_bytes={}",
                            b.len() - off
                        );
                        let f = OpenOptions::new().write(true).open(path)?;
                        f.set_len(off as u64)?;
                        f.sync_all()?;
                        break;
                    }
                }
            }
            tail_len = off as u64;
            segments.push(Segment { first_index: first, path: path.clone() });
        }

        // Reconcile the log with the snapshot.
        let s = snap_meta.last_index;
        let log_first = entries.front().map(|e| e.index);
        let log_last = entries.back().map(|e| e.index);
        let mut discard = false;
        if s > 0 {
            match (log_first, log_last) {
                (Some(f), Some(l)) => {
                    if f > s + 1 {
                        return Err(corrupt(format!("gap: snapshot ends at {s}, log starts at {f}")));
                    }
                    if l < s {
                        discard = true;
                    } else if f <= s {
                        let t = entries[(s - f) as usize].term;
                        if t != snap_meta.last_term {
                            discard = true;
                        }
                    }
                }
                _ => {
                    if let Some(seg) = segments.first() {
                        if seg.first_index != s + 1 {
                            discard = true;
                        }
                    }
                }
            }
        } else if let Some(f) = log_first {
            if f != 1 {
                return Err(corrupt(format!("no snapshot, but log starts at {f}; the prefix is gone")));
            }
        }
        if discard {
            // An interrupted InstallSnapshot: the snapshot wins.
            for seg in segments.iter().rev() {
                fs::remove_file(&seg.path)?;
            }
            sync_dir(&dir)?;
            segments.clear();
            entries.clear();
            offsets.clear();
        }

        let (file, file_len) = if let Some(seg) = segments.last() {
            let mut f = OpenOptions::new().read(true).write(true).open(&seg.path)?;
            f.seek(SeekFrom::Start(tail_len))?;
            (f, tail_len)
        } else {
            let first = s + 1;
            let (f, path) = create_segment(&dir, first)?;
            segments.push(Segment { first_index: first, path });
            let mut f = f;
            f.seek(SeekFrom::End(0))?;
            (f, WAL_HEADER as u64)
        };

        Ok(DiskStorage {
            dir,
            opts,
            _lock: lock,
            hs,
            snap_meta,
            snap_data,
            entries,
            offsets,
            segments,
            file,
            file_len,
            buf: Vec::with_capacity(1 << 20),
            needs_sync: false,
            stats: DiskStats::default(),
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn flush_and_sync(&mut self) {
        if !self.buf.is_empty() {
            if let Err(e) = self.file.write_all(&self.buf) {
                fatal("wal write", e);
            }
            self.file_len += self.buf.len() as u64;
            self.stats.bytes_written += self.buf.len() as u64;
            self.buf.clear();
            self.needs_sync = true;
        }
        if self.needs_sync {
            if !self.opts.skip_fsync {
                let t = Instant::now();
                if let Err(e) = self.file.sync_data() {
                    fatal("wal fsync", e);
                }
                self.stats.fsyncs += 1;
                self.stats.fsync_micros += t.elapsed().as_micros() as u64;
            }
            self.needs_sync = false;
        }
    }

    fn rotate(&mut self, first: Index) {
        self.flush_and_sync();
        match create_segment(&self.dir, first) {
            Ok((mut f, path)) => {
                if let Err(e) = f.seek(SeekFrom::End(0)) {
                    fatal("wal seek", e);
                }
                self.file = f;
                self.file_len = WAL_HEADER as u64;
                self.segments.push(Segment { first_index: first, path });
            }
            Err(e) => fatal("wal rotate", e),
        }
    }

    fn reset_wal(&mut self, first: Index) {
        self.buf.clear();
        self.needs_sync = false;
        self.entries.clear();
        self.offsets.clear();
        // Newest first: a crash part-way leaves a contiguous prefix, which the
        // snapshot check at recovery discards. Oldest first could leave a gap.
        for seg in self.segments.iter().rev() {
            if let Err(e) = fs::remove_file(&seg.path) {
                fatal("wal reset", e);
            }
        }
        self.segments.clear();
        if let Err(e) = sync_dir(&self.dir) {
            fatal("wal reset dirsync", e);
        }
        self.rotate(first);
    }

    fn clean_old_snapshots(&self) {
        if let Ok(files) = list(&self.dir, "snap-", ".snap") {
            let n = files.len();
            for p in files.into_iter().take(n.saturating_sub(2)) {
                let _ = fs::remove_file(p);
            }
        }
    }
}

impl Storage for DiskStorage {
    fn hard_state(&self) -> HardState {
        self.hs
    }

    fn save_hard_state(&mut self, hs: HardState) {
        if let Err(e) = store_meta(&self.dir, self.opts.node_id, self.opts.cluster_id, hs) {
            fatal("meta write", e);
        }
        self.hs = hs;
    }

    fn snapshot_meta(&self) -> SnapshotMeta {
        self.snap_meta
    }

    fn snapshot_data(&mut self) -> Arc<Vec<u8>> {
        self.snap_data.clone().unwrap_or_default()
    }

    fn first_index(&self) -> Index {
        self.entries.front().map(|e| e.index).unwrap_or(self.last_index() + 1)
    }

    fn last_index(&self) -> Index {
        self.entries.back().map(|e| e.index).unwrap_or(self.snap_meta.last_index)
    }

    fn term_at(&self, index: Index) -> Option<Term> {
        if index == self.snap_meta.last_index {
            return Some(self.snap_meta.last_term);
        }
        let first = self.entries.front()?.index;
        if index < first {
            return None;
        }
        self.entries.get((index - first) as usize).map(|e| e.term)
    }

    fn entries(&self, lo: Index, hi: Index, max_bytes: usize) -> Vec<Entry> {
        let mut out = Vec::new();
        let Some(first) = self.entries.front().map(|e| e.index) else { return out };
        if lo < first {
            return out;
        }
        let mut total = 0;
        for e in self.entries.iter().skip((lo - first) as usize) {
            if e.index > hi || (!out.is_empty() && total + e.size() > max_bytes) {
                break;
            }
            total += e.size();
            out.push(e.clone());
        }
        out
    }

    fn append(&mut self, entries: &[Entry]) {
        for e in entries {
            debug_assert_eq!(e.index, self.last_index() + 1);
            if self.file_len + self.buf.len() as u64 >= self.opts.segment_bytes {
                self.rotate(e.index);
            }
            let offset = self.file_len + self.buf.len() as u64;
            encode_record(&mut self.buf, e);
            self.entries.push_back(e.clone());
            self.offsets.push_back(offset);
        }
    }

    fn truncate_from(&mut self, index: Index) {
        self.flush_and_sync();
        let first = match self.entries.front() {
            Some(e) => e.index,
            None => return,
        };
        if index > self.last_index() {
            return;
        }
        assert!(index >= first, "truncating into the compacted log");
        let pos = (index - first) as usize;
        let offset = self.offsets[pos];
        while self.segments.len() > 1 && self.segments.last().unwrap().first_index > index {
            let seg = self.segments.pop().unwrap();
            if let Err(e) = fs::remove_file(&seg.path) {
                fatal("wal truncate remove", e);
            }
        }
        if let Err(e) = sync_dir(&self.dir) {
            fatal("wal truncate dirsync", e);
        }
        let path = self.segments.last().unwrap().path.clone();
        let result = (|| -> io::Result<File> {
            let mut f = OpenOptions::new().read(true).write(true).open(&path)?;
            f.set_len(offset)?;
            f.sync_all()?;
            f.seek(SeekFrom::End(0))?;
            Ok(f)
        })();
        match result {
            Ok(f) => self.file = f,
            Err(e) => fatal("wal truncate", e),
        }
        self.file_len = offset;
        self.entries.truncate(pos);
        self.offsets.truncate(pos);
    }

    fn sync(&mut self) {
        self.flush_and_sync();
    }

    fn install_snapshot(&mut self, meta: SnapshotMeta, data: Arc<Vec<u8>>) {
        self.flush_and_sync();
        if let Err(e) = write_snapshot_file(&self.dir, meta, &data) {
            fatal("snapshot write", e);
        }
        let keep = self.term_at(meta.last_index) == Some(meta.last_term)
            && self.entries.front().map(|e| e.index <= meta.last_index + 1).unwrap_or(false);
        self.snap_meta = meta;
        self.snap_data = Some(data);
        if !keep {
            self.reset_wal(meta.last_index + 1);
        }
        self.clean_old_snapshots();
    }

    fn adopt_snapshot(&mut self, meta: SnapshotMeta, data: Arc<Vec<u8>>) {
        if meta.last_index <= self.snap_meta.last_index {
            return;
        }
        self.snap_meta = meta;
        self.snap_data = Some(data);
        self.clean_old_snapshots();
    }

    fn compact(&mut self, upto: Index) {
        let upto = upto.min(self.snap_meta.last_index);
        // Keep at least the current segment. Oldest first so a crash part-way
        // still leaves a contiguous suffix.
        let mut removed = false;
        while self.segments.len() > 1 && self.segments[1].first_index <= upto + 1 {
            let seg = self.segments.remove(0);
            if let Err(e) = fs::remove_file(&seg.path) {
                fatal("wal compact", e);
            }
            removed = true;
        }
        if removed {
            if let Err(e) = sync_dir(&self.dir) {
                fatal("wal compact dirsync", e);
            }
        }
        while self.entries.front().map(|e| e.index <= upto).unwrap_or(false) {
            self.entries.pop_front();
            self.offsets.pop_front();
        }
    }
}
