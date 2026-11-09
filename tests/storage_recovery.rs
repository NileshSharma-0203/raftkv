//! Crash recovery of the on-disk log, including a torn write at every byte.

use raftkv::storage::disk::*;
use raftkv::storage::Storage;
use raftkv::types::*;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let p = std::env::temp_dir().join(format!("raftkv-test-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn opts() -> DiskOptions {
    DiskOptions::default()
}

fn entry(term: Term, index: Index) -> Entry {
    Entry::new(term, index, format!("value-{index}-term-{term}").into_bytes())
}

fn open(dir: &Path) -> DiskStorage {
    DiskStorage::open(dir, opts()).unwrap()
}

fn all(s: &DiskStorage) -> Vec<Entry> {
    s.entries(s.first_index(), s.last_index(), usize::MAX)
}

fn wal_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().map(|x| x == "seg").unwrap_or(false))
        .collect();
    v.sort();
    v
}

fn write_entries(dir: &Path, n: u64) -> Vec<Entry> {
    let mut s = open(dir);
    let v: Vec<Entry> = (1..=n).map(|i| entry(1, i)).collect();
    s.append(&v);
    s.sync();
    v
}

#[test]
fn entries_survive_reopen() {
    let d = TempDir::new("reopen");
    let v = write_entries(&d.0, 20);
    assert_eq!(all(&open(&d.0)), v);
}

#[test]
fn hard_state_survives_reopen() {
    let d = TempDir::new("hs");
    {
        let mut s = open(&d.0);
        s.save_hard_state(HardState { term: 7, voted_for: Some(3) });
    }
    assert_eq!(open(&d.0).hard_state(), HardState { term: 7, voted_for: Some(3) });
}

#[test]
fn unsynced_entries_are_not_written_early() {
    let d = TempDir::new("unsynced");
    write_entries(&d.0, 3);
    {
        let mut s = open(&d.0);
        s.append(&[entry(1, 4)]);
        // Dropped without sync: the process "crashed".
    }
    assert_eq!(open(&d.0).last_index(), 3);
}

#[test]
fn torn_write_at_every_byte_recovers_a_clean_prefix() {
    let d = TempDir::new("torn-src");
    let v = write_entries(&d.0, 6);
    let wal = wal_files(&d.0).remove(0);
    let name = wal.file_name().unwrap().to_owned();
    let full = fs::read(&wal).unwrap();
    let meta = fs::read(d.0.join("meta")).unwrap();

    for cut in 0..=full.len() {
        let t = TempDir::new(&format!("torn-{cut}"));
        fs::write(t.0.join("meta"), &meta).unwrap();
        fs::write(t.0.join(&name), &full[..cut]).unwrap();
        let mut s = DiskStorage::open(&t.0, opts()).unwrap_or_else(|e| panic!("cut at {cut}: {e}"));
        let got = all(&s);
        assert!(got.len() <= v.len(), "cut at {cut}");
        assert_eq!(got, v[..got.len()], "cut at {cut} recovered wrong data");

        // The recovered log must keep working.
        let next = entry(2, got.len() as u64 + 1);
        s.append(std::slice::from_ref(&next));
        s.sync();
        drop(s);
        let again = all(&open(&t.0));
        assert_eq!(again.len(), got.len() + 1, "cut at {cut}");
        assert_eq!(again.last().unwrap(), &next, "cut at {cut}");
    }
}

#[test]
fn every_single_bit_flip_is_detected() {
    let d = TempDir::new("flip-src");
    let v = write_entries(&d.0, 4);
    let wal = wal_files(&d.0).remove(0);
    let name = wal.file_name().unwrap().to_owned();
    let full = fs::read(&wal).unwrap();
    let meta = fs::read(d.0.join("meta")).unwrap();
    let mut refused = 0;
    let mut shortened = 0;
    for byte in 0..full.len() {
        let mut b = full.clone();
        b[byte] ^= 0x10;
        let t = TempDir::new(&format!("flip-{byte}"));
        fs::write(t.0.join("meta"), &meta).unwrap();
        fs::write(t.0.join(&name), &b).unwrap();
        match DiskStorage::open(&t.0, opts()) {
            Err(_) => refused += 1,
            Ok(s) => {
                let got = all(&s);
                assert_eq!(got, v[..got.len()], "flip at byte {byte} produced wrong data");
                assert!(got.len() < v.len(), "flip at byte {byte} went unnoticed");
                shortened += 1;
            }
        }
    }
    assert!(refused > 0, "corruption before the tail must refuse to start");
    assert!(shortened > 0, "corruption in the final record looks like a torn write");
}

#[test]
fn corruption_before_the_tail_refuses_to_start() {
    let d = TempDir::new("midrot");
    write_entries(&d.0, 5);
    let wal = wal_files(&d.0).remove(0);
    let mut b = fs::read(&wal).unwrap();
    // Inside the first record's data, well away from the tail.
    b[20 + 8 + 16 + 2] ^= 0xFF;
    fs::write(&wal, &b).unwrap();
    let err = DiskStorage::open(&d.0, opts()).err().expect("must refuse");
    assert!(err.to_string().contains("followed by valid data"), "{err}");
}

#[test]
fn log_spans_segments_and_recovers() {
    let d = TempDir::new("segments");
    let o = DiskOptions { segment_bytes: 200, ..opts() };
    let v: Vec<Entry> = (1..=50).map(|i| entry(1, i)).collect();
    {
        let mut s = DiskStorage::open(&d.0, o.clone()).unwrap();
        s.append(&v);
        s.sync();
    }
    assert!(wal_files(&d.0).len() > 3);
    let s = DiskStorage::open(&d.0, o).unwrap();
    assert_eq!(all(&s), v);
}

#[test]
fn truncate_across_segments_is_durable() {
    let d = TempDir::new("truncate");
    let o = DiskOptions { segment_bytes: 200, ..opts() };
    {
        let mut s = DiskStorage::open(&d.0, o.clone()).unwrap();
        s.append(&(1..=40).map(|i| entry(1, i)).collect::<Vec<_>>());
        s.sync();
        s.truncate_from(11);
        s.append(&(11..=15).map(|i| entry(2, i)).collect::<Vec<_>>());
        s.sync();
    }
    let s = DiskStorage::open(&d.0, o).unwrap();
    assert_eq!(s.last_index(), 15);
    assert_eq!(s.term_at(10), Some(1));
    assert_eq!(s.term_at(11), Some(2));
    assert_eq!(s.term_at(15), Some(2));
}

#[test]
fn missing_middle_segment_refuses_to_start() {
    let d = TempDir::new("gap");
    let o = DiskOptions { segment_bytes: 200, ..opts() };
    {
        let mut s = DiskStorage::open(&d.0, o.clone()).unwrap();
        s.append(&(1..=40).map(|i| entry(1, i)).collect::<Vec<_>>());
        s.sync();
    }
    let files = wal_files(&d.0);
    fs::remove_file(&files[files.len() / 2]).unwrap();
    assert!(DiskStorage::open(&d.0, o).is_err());
}

#[test]
fn compaction_removes_whole_segments_and_recovers() {
    let d = TempDir::new("compact");
    let o = DiskOptions { segment_bytes: 200, ..opts() };
    {
        let mut s = DiskStorage::open(&d.0, o.clone()).unwrap();
        s.append(&(1..=40).map(|i| entry(1, i)).collect::<Vec<_>>());
        s.sync();
        let meta = SnapshotMeta { last_index: 30, last_term: 1 };
        write_snapshot_file(&d.0, meta, b"state").unwrap();
        s.adopt_snapshot(meta, Arc::new(b"state".to_vec()));
        let before = wal_files(&d.0).len();
        s.compact(30);
        assert!(wal_files(&d.0).len() < before);
        assert_eq!(s.first_index(), 31);
    }
    let mut s = DiskStorage::open(&d.0, o).unwrap();
    assert_eq!(s.snapshot_meta().last_index, 30);
    assert_eq!(*s.snapshot_data(), b"state".to_vec());
    assert_eq!(s.last_index(), 40);
    assert_eq!(s.term_at(40), Some(1));
}

#[test]
fn install_snapshot_with_conflicting_log_discards_it() {
    let d = TempDir::new("install-discard");
    let mut s = open(&d.0);
    s.append(&(1..=10).map(|i| entry(1, i)).collect::<Vec<_>>());
    s.sync();
    s.install_snapshot(SnapshotMeta { last_index: 5, last_term: 9 }, Arc::new(b"s".to_vec()));
    assert_eq!(s.last_index(), 5);
    assert_eq!(s.first_index(), 6);
    s.append(&[entry(9, 6)]);
    s.sync();
    drop(s);
    let s = open(&d.0);
    assert_eq!(s.last_index(), 6);
    assert_eq!(s.term_at(5), Some(9));
}

#[test]
fn install_snapshot_with_matching_log_keeps_the_tail() {
    let d = TempDir::new("install-keep");
    let mut s = open(&d.0);
    s.append(&(1..=10).map(|i| entry(1, i)).collect::<Vec<_>>());
    s.sync();
    s.install_snapshot(SnapshotMeta { last_index: 5, last_term: 1 }, Arc::new(b"s".to_vec()));
    assert_eq!(s.last_index(), 10);
}

#[test]
fn crash_during_install_snapshot_lets_the_snapshot_win() {
    let d = TempDir::new("install-crash");
    write_entries(&d.0, 10);
    // The snapshot reached disk, then the crash hit before the log was reset.
    write_snapshot_file(&d.0, SnapshotMeta { last_index: 20, last_term: 5 }, b"new").unwrap();
    let s = open(&d.0);
    assert_eq!(s.last_index(), 20);
    assert_eq!(s.first_index(), 21);
    assert_eq!(s.term_at(20), Some(5));
}

#[test]
fn corrupt_newest_snapshot_falls_back_to_older() {
    let d = TempDir::new("snap-fallback");
    write_entries(&d.0, 10);
    let older = SnapshotMeta { last_index: 5, last_term: 1 };
    let newer = SnapshotMeta { last_index: 8, last_term: 1 };
    write_snapshot_file(&d.0, older, b"older").unwrap();
    write_snapshot_file(&d.0, newer, b"newer").unwrap();
    let p = d.0.join(format!("snap-{:020}-{:020}.snap", 8, 1));
    let mut b = fs::read(&p).unwrap();
    let n = b.len();
    b[n - 1] ^= 1;
    fs::write(&p, b).unwrap();
    let mut s = open(&d.0);
    assert_eq!(s.snapshot_meta(), older);
    assert_eq!(*s.snapshot_data(), b"older".to_vec());
    assert_eq!(s.last_index(), 10);
}

#[test]
fn data_dir_cannot_be_opened_twice() {
    let d = TempDir::new("lock");
    let _first = open(&d.0);
    assert!(DiskStorage::open(&d.0, opts()).is_err());
}

#[test]
fn wrong_identity_is_refused() {
    let d = TempDir::new("identity");
    drop(open(&d.0));
    assert!(DiskStorage::open(&d.0, DiskOptions { node_id: 2, ..opts() }).is_err());
    assert!(DiskStorage::open(&d.0, DiskOptions { cluster_id: 2, ..opts() }).is_err());
}

#[test]
fn lost_meta_file_refuses_to_start() {
    let d = TempDir::new("no-meta");
    write_entries(&d.0, 3);
    fs::remove_file(d.0.join("meta")).unwrap();
    let err = DiskStorage::open(&d.0, opts()).err().expect("must refuse");
    assert!(err.to_string().contains("meta"), "{err}");
}
