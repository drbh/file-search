use crate::scan::{scan, FileFilter, ResultBatch, ScanOptions};
use ahash::AHashMap as HashMap;
use crossbeam_channel as channel;
use memmap2::Mmap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const CONTENT_MAGIC: [u8; 8] = *b"FSCNTV1\0";
const CONTENT_VERSION: u32 = 1;
const HEADER_SIZE: usize = 112;
const PATH_ENTRY_SIZE: usize = 16;
const GRAM_ENTRY_SIZE: usize = 24;
const FLAG_INCLUDE_BINARY: u64 = 1;
const TRIGRAM_SPACE: usize = 1 << 24;

#[derive(Debug)]
pub struct ContentBuildStats {
    pub workers: usize,
    pub files_indexed: usize,
    pub files_scanned: usize,
    pub files_skipped_binary: usize,
    pub files_skipped_too_large: usize,
    pub total_bytes_indexed: u64,
    pub scan_duration: Duration,
    pub total_duration: Duration,
    pub snapshot_bytes: u64,
    pub index_dir: PathBuf,
}

#[derive(Debug)]
pub struct ContentQueryStats {
    pub matches: usize,
    pub candidates: usize,
    pub duration: Duration,
}

#[derive(Debug)]
pub struct ContentIndexStatus {
    pub root: String,
    pub index_dir: PathBuf,
    pub snapshot_exists: bool,
    pub files_indexed: usize,
    pub grams_indexed: usize,
    pub snapshot_size_bytes: u64,
    pub snapshot_created_unix: u64,
    pub include_binary: bool,
    pub max_file_size: u64,
}

struct ContentIndexPaths {
    root: String,
    dir: PathBuf,
    snapshot: PathBuf,
    snapshot_prev: PathBuf,
    root_file: PathBuf,
}

struct ContentSnapshot {
    mmap: Mmap,
    file_count: usize,
    path_table_off: usize,
    path_arena_off: usize,
    path_arena_len: usize,
    gram_count: usize,
    gram_table_off: usize,
    postings_off: usize,
    postings_len: usize,
    created_unix: u64,
    include_binary: bool,
    max_file_size: u64,
}

#[inline]
fn io_other(msg: impl Into<String>) -> io::Error {
    io::Error::other(msg.into())
}

#[inline]
fn put_u32(buf: &mut [u8], off: usize, value: u32) {
    buf[off..off + 4].copy_from_slice(&value.to_le_bytes());
}

#[inline]
fn put_u64(buf: &mut [u8], off: usize, value: u64) {
    buf[off..off + 8].copy_from_slice(&value.to_le_bytes());
}

#[inline]
fn read_u32(buf: &[u8], off: usize) -> io::Result<u32> {
    let bytes = buf
        .get(off..off + 4)
        .ok_or_else(|| io_other("corrupt content index: short u32"))?;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

#[inline]
fn read_u64(buf: &[u8], off: usize) -> io::Result<u64> {
    let bytes = buf
        .get(off..off + 8)
        .ok_or_else(|| io_other("corrupt content index: short u64"))?;
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
}

#[inline]
fn fnv1a64(s: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for b in s.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[inline]
fn normalize_root(path: &Path) -> String {
    let mut p = path.to_string_lossy().to_string();
    while p.ends_with('/') && p.len() > 1 {
        p.pop();
    }
    if p.is_empty() {
        "/".to_string()
    } else {
        p
    }
}

fn index_home() -> io::Result<PathBuf> {
    if let Ok(home) = std::env::var("FILE_SEARCH_INDEX_HOME") {
        return Ok(PathBuf::from(home));
    }
    let home = std::env::var("HOME").map_err(|_| io_other("HOME not set"))?;
    Ok(PathBuf::from(home).join(".file-search").join("indexes"))
}

fn content_paths_for_root(root_path: &Path) -> io::Result<ContentIndexPaths> {
    let canonical = fs::canonicalize(root_path)?;
    let root = normalize_root(&canonical);
    let hash = fnv1a64(&root);
    let dir = index_home()?.join(format!("{hash:016x}")).join("content");
    Ok(ContentIndexPaths {
        root,
        snapshot: dir.join("snapshot.bin"),
        snapshot_prev: dir.join("snapshot.prev.bin"),
        root_file: dir.join("root.txt"),
        dir,
    })
}

fn ensure_content_dir(paths: &ContentIndexPaths) -> io::Result<()> {
    fs::create_dir_all(&paths.dir)?;
    if paths.root_file.exists() {
        let existing = fs::read_to_string(&paths.root_file)?.trim().to_string();
        if existing != paths.root {
            return Err(io_other(format!(
                "content index hash collision: {} != {}",
                existing, paths.root
            )));
        }
    } else {
        fs::write(&paths.root_file, format!("{}\n", paths.root))?;
    }
    Ok(())
}

#[inline]
fn is_binary(bytes: &[u8]) -> bool {
    bytes.contains(&0)
}

#[inline]
fn trigram_value(a: u8, b: u8, c: u8) -> u32 {
    ((a as u32) << 16) | ((b as u32) << 8) | (c as u32)
}

#[inline]
fn trigram_idx_parts(gram: u32) -> (usize, u64) {
    let idx = gram as usize;
    let word = idx >> 6;
    let bit = idx & 63;
    (word, 1u64 << bit)
}

struct TrigramDeduper {
    bits: Vec<u64>,
    touched: Vec<u32>,
}

impl TrigramDeduper {
    fn new() -> Self {
        Self {
            bits: vec![0u64; TRIGRAM_SPACE / 64],
            touched: Vec::with_capacity(4096),
        }
    }

    #[inline]
    fn clear_touched(&mut self) {
        for gram in self.touched.drain(..) {
            let (word, mask) = trigram_idx_parts(gram);
            self.bits[word] &= !mask;
        }
    }
}

#[inline]
fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    if needle.len() > haystack.len() {
        return false;
    }
    haystack.windows(needle.len()).any(|w| w == needle)
}

fn write_snapshot_atomic(
    paths: &ContentIndexPaths,
    path_entries: &[(u64, u32)],
    path_arena: &[u8],
    gram_entries: &[(u32, u64, u32)],
    postings: &[u32],
    include_binary: bool,
    max_file_size: u64,
) -> io::Result<u64> {
    const STREAM_BUFFER_BYTES: usize = 8 * 1024 * 1024;
    const CHUNK_BUFFER_BYTES: usize = 256 * 1024;

    #[inline]
    fn write_path_entries_chunked<W: Write>(
        writer: &mut W,
        entries: &[(u64, u32)],
    ) -> io::Result<()> {
        let mut buf = vec![0u8; CHUNK_BUFFER_BYTES];
        let entries_per_chunk = (buf.len() / PATH_ENTRY_SIZE).max(1);
        for chunk in entries.chunks(entries_per_chunk) {
            let mut off = 0usize;
            for (path_off, path_len) in chunk {
                buf[off..off + 8].copy_from_slice(&path_off.to_le_bytes());
                buf[off + 8..off + 12].copy_from_slice(&path_len.to_le_bytes());
                buf[off + 12..off + 16].copy_from_slice(&0u32.to_le_bytes());
                off += PATH_ENTRY_SIZE;
            }
            writer.write_all(&buf[..off])?;
        }
        Ok(())
    }

    #[inline]
    fn write_gram_entries_chunked<W: Write>(
        writer: &mut W,
        entries: &[(u32, u64, u32)],
    ) -> io::Result<()> {
        let mut buf = vec![0u8; CHUNK_BUFFER_BYTES];
        let entries_per_chunk = (buf.len() / GRAM_ENTRY_SIZE).max(1);
        for chunk in entries.chunks(entries_per_chunk) {
            let mut off = 0usize;
            for (gram, post_off, post_len) in chunk {
                buf[off..off + 4].copy_from_slice(&gram.to_le_bytes());
                buf[off + 4..off + 8].copy_from_slice(&0u32.to_le_bytes());
                buf[off + 8..off + 16].copy_from_slice(&post_off.to_le_bytes());
                buf[off + 16..off + 20].copy_from_slice(&post_len.to_le_bytes());
                buf[off + 20..off + 24].copy_from_slice(&0u32.to_le_bytes());
                off += GRAM_ENTRY_SIZE;
            }
            writer.write_all(&buf[..off])?;
        }
        Ok(())
    }

    #[inline]
    fn write_postings_chunked<W: Write>(writer: &mut W, postings: &[u32]) -> io::Result<()> {
        let mut buf = vec![0u8; CHUNK_BUFFER_BYTES];
        let ids_per_chunk = (buf.len() / 4).max(1);
        for chunk in postings.chunks(ids_per_chunk) {
            let mut off = 0usize;
            for id in chunk {
                buf[off..off + 4].copy_from_slice(&id.to_le_bytes());
                off += 4;
            }
            writer.write_all(&buf[..off])?;
        }
        Ok(())
    }

    ensure_content_dir(paths)?;

    let tmp = paths.dir.join("snapshot.bin.tmp");
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp)?;
    let mut writer = BufWriter::with_capacity(STREAM_BUFFER_BYTES, file);

    writer.write_all(&[0u8; HEADER_SIZE])?;

    write_path_entries_chunked(&mut writer, path_entries)?;
    writer.write_all(path_arena)?;

    write_gram_entries_chunked(&mut writer, gram_entries)?;
    write_postings_chunked(&mut writer, postings)?;

    writer.flush()?;
    let mut file = writer.into_inner().map_err(|e| e.into_error())?;
    let snapshot_bytes = file.metadata()?.len();
    let path_table_bytes = u64::try_from(path_entries.len() * PATH_ENTRY_SIZE).unwrap();
    let path_arena_bytes = u64::try_from(path_arena.len()).unwrap();
    let gram_table_bytes = u64::try_from(gram_entries.len() * GRAM_ENTRY_SIZE).unwrap();
    let postings_bytes = u64::try_from(postings.len() * 4).unwrap();
    let path_arena_off = HEADER_SIZE as u64 + path_table_bytes;
    let gram_table_off = path_arena_off + path_arena_bytes;
    let postings_off = gram_table_off + gram_table_bytes;
    let created_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let mut header = [0u8; HEADER_SIZE];
    header[0..8].copy_from_slice(&CONTENT_MAGIC);
    put_u32(&mut header, 8, CONTENT_VERSION);
    put_u64(&mut header, 16, path_entries.len() as u64);
    put_u64(&mut header, 24, HEADER_SIZE as u64);
    put_u64(&mut header, 32, path_table_bytes);
    put_u64(&mut header, 40, path_arena_off);
    put_u64(&mut header, 48, path_arena_bytes);
    put_u64(&mut header, 56, gram_table_off);
    put_u64(&mut header, 64, gram_table_bytes);
    put_u64(&mut header, 72, postings_off);
    put_u64(&mut header, 80, postings_bytes);
    put_u64(&mut header, 88, created_unix);
    put_u64(&mut header, 96, max_file_size);
    let mut flags = 0u64;
    if include_binary {
        flags |= FLAG_INCLUDE_BINARY;
    }
    put_u64(&mut header, 104, flags);

    file.seek(SeekFrom::Start(0))?;
    file.write_all(&header)?;
    file.sync_all()?;
    drop(file);

    if paths.snapshot_prev.exists() {
        fs::remove_file(&paths.snapshot_prev)?;
    }
    if paths.snapshot.exists() {
        fs::rename(&paths.snapshot, &paths.snapshot_prev)?;
    }
    fs::rename(tmp, &paths.snapshot)?;
    File::open(&paths.dir)?.sync_all()?;
    Ok(snapshot_bytes)
}

impl ContentSnapshot {
    fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        if mmap.len() < HEADER_SIZE {
            return Err(io_other("corrupt content snapshot: too small"));
        }
        if mmap[0..8] != CONTENT_MAGIC {
            return Err(io_other("corrupt content snapshot: bad magic"));
        }
        let version = read_u32(&mmap, 8)?;
        if version != CONTENT_VERSION {
            return Err(io_other(format!(
                "unsupported content snapshot version: {}",
                version
            )));
        }
        let file_count = read_u64(&mmap, 16)? as usize;
        let path_table_off = read_u64(&mmap, 24)? as usize;
        let path_table_bytes = read_u64(&mmap, 32)? as usize;
        let path_arena_off = read_u64(&mmap, 40)? as usize;
        let path_arena_len = read_u64(&mmap, 48)? as usize;
        let gram_table_off = read_u64(&mmap, 56)? as usize;
        let gram_table_bytes = read_u64(&mmap, 64)? as usize;
        let postings_off = read_u64(&mmap, 72)? as usize;
        let postings_len = read_u64(&mmap, 80)? as usize;
        let created_unix = read_u64(&mmap, 88)?;
        let max_file_size = read_u64(&mmap, 96)?;
        let flags = read_u64(&mmap, 104)?;

        if path_table_bytes != file_count.saturating_mul(PATH_ENTRY_SIZE) {
            return Err(io_other(
                "corrupt content snapshot: path table size mismatch",
            ));
        }
        if gram_table_bytes % GRAM_ENTRY_SIZE != 0 {
            return Err(io_other("corrupt content snapshot: gram table alignment"));
        }
        let gram_count = gram_table_bytes / GRAM_ENTRY_SIZE;
        if postings_len % 4 != 0 {
            return Err(io_other("corrupt content snapshot: postings alignment"));
        }

        let check = |off: usize, len: usize| -> bool {
            off <= mmap.len() && len <= mmap.len().saturating_sub(off)
        };
        if !check(path_table_off, path_table_bytes)
            || !check(path_arena_off, path_arena_len)
            || !check(gram_table_off, gram_table_bytes)
            || !check(postings_off, postings_len)
        {
            return Err(io_other("corrupt content snapshot: out of bounds"));
        }

        Ok(Self {
            mmap,
            file_count,
            path_table_off,
            path_arena_off,
            path_arena_len,
            gram_count,
            gram_table_off,
            postings_off,
            postings_len,
            created_unix,
            include_binary: (flags & FLAG_INCLUDE_BINARY) != 0,
            max_file_size,
        })
    }

    #[inline]
    fn path_at(&self, file_id: u32) -> io::Result<&str> {
        let idx = file_id as usize;
        if idx >= self.file_count {
            return Err(io_other("file id out of bounds"));
        }
        let off = self.path_table_off + idx * PATH_ENTRY_SIZE;
        let path_off = read_u64(&self.mmap, off)? as usize;
        let path_len = read_u32(&self.mmap, off + 8)? as usize;
        if path_off + path_len > self.path_arena_len {
            return Err(io_other("corrupt content snapshot: path bounds"));
        }
        let start = self.path_arena_off + path_off;
        let end = start + path_len;
        std::str::from_utf8(&self.mmap[start..end])
            .map_err(|_| io_other("corrupt content snapshot: invalid utf-8 path"))
    }

    #[inline]
    fn gram_entry(&self, idx: usize) -> io::Result<(u32, usize, usize)> {
        if idx >= self.gram_count {
            return Err(io_other("gram index out of bounds"));
        }
        let off = self.gram_table_off + idx * GRAM_ENTRY_SIZE;
        let gram = read_u32(&self.mmap, off)?;
        let post_off_u32 = read_u64(&self.mmap, off + 8)? as usize;
        let post_len_u32 = read_u32(&self.mmap, off + 16)? as usize;
        let byte_off = self.postings_off + post_off_u32.saturating_mul(4);
        let byte_len = post_len_u32.saturating_mul(4);
        if byte_off + byte_len > self.postings_off + self.postings_len {
            return Err(io_other("corrupt content snapshot: posting bounds"));
        }
        Ok((gram, byte_off, post_len_u32))
    }

    fn postings_for_gram(&self, gram: u32) -> io::Result<Option<Vec<u32>>> {
        if self.gram_count == 0 {
            return Ok(None);
        }
        let mut lo = 0usize;
        let mut hi = self.gram_count;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let (mid_gram, _, _) = self.gram_entry(mid)?;
            if mid_gram < gram {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo >= self.gram_count {
            return Ok(None);
        }
        let (found, byte_off, len) = self.gram_entry(lo)?;
        if found != gram {
            return Ok(None);
        }
        let mut out = Vec::with_capacity(len);
        for i in 0..len {
            let pos = byte_off + i * 4;
            out.push(read_u32(&self.mmap, pos)?);
        }
        Ok(Some(out))
    }
}

#[inline]
fn intersect_sorted(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = Vec::with_capacity(a.len().min(b.len()));
    let mut i = 0usize;
    let mut j = 0usize;
    while i < a.len() && j < b.len() {
        let av = a[i];
        let bv = b[j];
        if av == bv {
            out.push(av);
            i += 1;
            j += 1;
        } else if av < bv {
            i += 1;
        } else {
            j += 1;
        }
    }
    out
}

struct ContentWorkerOutput {
    postings_map: HashMap<u32, Vec<u32>>,
    indexed_paths: Vec<(u32, String)>,
    files_scanned: usize,
    files_indexed: usize,
    files_skipped_binary: usize,
    files_skipped_too_large: usize,
    total_bytes_indexed: u64,
}

#[inline]
fn resolve_content_workers(requested: usize, task_count: usize) -> usize {
    if task_count == 0 {
        return 1;
    }
    let auto = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    let base = if requested == 0 { auto } else { requested };
    base.max(1).min(task_count)
}

fn run_content_worker(
    work_rx: channel::Receiver<String>,
    next_file_id: Arc<AtomicU32>,
    max_file_size: u64,
    include_binary: bool,
) -> ContentWorkerOutput {
    let mut postings_map: HashMap<u32, Vec<u32>> = HashMap::new();
    let mut indexed_paths: Vec<(u32, String)> = Vec::new();
    let mut scratch_grams: Vec<u32> = Vec::new();
    let mut deduper = TrigramDeduper::new();
    let mut read_buf = vec![0u8; 256 * 1024];

    let mut files_scanned = 0usize;
    let mut files_indexed = 0usize;
    let mut files_skipped_binary = 0usize;
    let mut files_skipped_too_large = 0usize;
    let mut total_bytes_indexed = 0u64;

    for path in &work_rx {
        files_scanned += 1;
        let mut file = match File::open(&path) {
            Ok(f) => f,
            Err(_) => continue,
        };
        let metadata = match file.metadata() {
            Ok(m) => m,
            Err(_) => continue,
        };
        if !metadata.is_file() {
            continue;
        }
        if metadata.len() > max_file_size {
            files_skipped_too_large += 1;
            continue;
        }

        scratch_grams.clear();
        let mut prev0 = 0u8;
        let mut prev1 = 0u8;
        let mut have = 0usize;
        let mut saw_binary = false;
        let mut read_failed = false;

        loop {
            let n = match file.read(&mut read_buf) {
                Ok(0) => break,
                Ok(n) => n,
                Err(_) => {
                    read_failed = true;
                    break;
                }
            };
            for &byte in &read_buf[..n] {
                if !include_binary && byte == 0 {
                    saw_binary = true;
                    break;
                }
                if have < 2 {
                    if have == 0 {
                        prev0 = byte;
                    } else {
                        prev1 = byte;
                    }
                    have += 1;
                    continue;
                }

                let gram = trigram_value(prev0, prev1, byte);
                let (word, mask) = trigram_idx_parts(gram);
                if deduper.bits[word] & mask == 0 {
                    deduper.bits[word] |= mask;
                    deduper.touched.push(gram);
                    scratch_grams.push(gram);
                }
                prev0 = prev1;
                prev1 = byte;
            }
            if saw_binary {
                break;
            }
        }

        if read_failed {
            deduper.clear_touched();
            scratch_grams.clear();
            continue;
        }
        if saw_binary {
            files_skipped_binary += 1;
            deduper.clear_touched();
            scratch_grams.clear();
            continue;
        }

        let file_id = next_file_id.fetch_add(1, Ordering::Relaxed);
        indexed_paths.push((file_id, path));

        for gram in &scratch_grams {
            postings_map.entry(*gram).or_default().push(file_id);
        }
        deduper.clear_touched();

        total_bytes_indexed = total_bytes_indexed.saturating_add(metadata.len());
        files_indexed += 1;
    }

    ContentWorkerOutput {
        postings_map,
        indexed_paths,
        files_scanned,
        files_indexed,
        files_skipped_binary,
        files_skipped_too_large,
        total_bytes_indexed,
    }
}

pub fn build_content_index(
    root: &Path,
    max_depth: usize,
    scan_options: ScanOptions,
    max_file_size: u64,
    include_binary: bool,
    requested_workers: usize,
) -> io::Result<ContentBuildStats> {
    let started = Instant::now();
    let paths = content_paths_for_root(root)?;
    ensure_content_dir(&paths)?;

    let root_str = paths.root.clone();
    let scan_handle = scan(&root_str, true, max_depth, FileFilter::All, scan_options);
    let mut all_paths: Vec<String> = Vec::new();
    for batch in &scan_handle.receiver {
        if let ResultBatch::Paths(paths_batch) = batch {
            all_paths.extend(paths_batch);
        }
    }
    let scan_stats = scan_handle.wait_for_completion();

    let workers = resolve_content_workers(requested_workers, all_paths.len());
    let (work_tx, work_rx) = channel::bounded::<String>(workers.saturating_mul(1024).max(1024));
    let next_file_id = Arc::new(AtomicU32::new(0));

    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let rx = work_rx.clone();
        let file_id_ctr = Arc::clone(&next_file_id);
        handles.push(std::thread::spawn(move || {
            run_content_worker(rx, file_id_ctr, max_file_size, include_binary)
        }));
    }
    drop(work_rx);

    for path in all_paths {
        if work_tx.send(path).is_err() {
            break;
        }
    }
    drop(work_tx);

    let mut postings_map: HashMap<u32, Vec<u32>> = HashMap::new();
    let mut indexed_paths: Vec<(u32, String)> = Vec::new();

    let mut files_scanned = 0usize;
    let mut files_indexed = 0usize;
    let mut files_skipped_binary = 0usize;
    let mut files_skipped_too_large = 0usize;
    let mut total_bytes_indexed = 0u64;

    for handle in handles {
        let mut output = handle
            .join()
            .map_err(|_| io_other("content index worker thread panicked"))?;
        files_scanned += output.files_scanned;
        files_indexed += output.files_indexed;
        files_skipped_binary += output.files_skipped_binary;
        files_skipped_too_large += output.files_skipped_too_large;
        total_bytes_indexed = total_bytes_indexed.saturating_add(output.total_bytes_indexed);
        indexed_paths.append(&mut output.indexed_paths);
        for (gram, mut ids) in output.postings_map {
            postings_map.entry(gram).or_default().append(&mut ids);
        }
    }

    if indexed_paths.len() > u32::MAX as usize {
        return Err(io_other("too many files for u32 file ids"));
    }
    indexed_paths.sort_unstable_by_key(|(file_id, _)| *file_id);
    for (expected, (file_id, _)) in indexed_paths.iter().enumerate() {
        if *file_id != expected as u32 {
            return Err(io_other(
                "content index internal error: non-contiguous file ids",
            ));
        }
    }

    let mut path_entries: Vec<(u64, u32)> = Vec::with_capacity(indexed_paths.len());
    let arena_cap = indexed_paths
        .iter()
        .map(|(_, path)| path.len())
        .sum::<usize>();
    let mut path_arena: Vec<u8> = Vec::with_capacity(arena_cap);

    for (_, path) in indexed_paths {
        let path_off =
            u64::try_from(path_arena.len()).map_err(|_| io_other("path arena too large"))?;
        let path_len = u32::try_from(path.len()).map_err(|_| io_other("path too long"))?;
        path_entries.push((path_off, path_len));
        path_arena.extend_from_slice(path.as_bytes());
    }

    let mut gram_postings: Vec<(u32, Vec<u32>)> = postings_map.into_iter().collect();
    gram_postings.sort_unstable_by_key(|(gram, _)| *gram);

    if !gram_postings.is_empty() {
        let finalize_workers = resolve_content_workers(workers, gram_postings.len());
        let chunk_size = (gram_postings.len() + finalize_workers - 1) / finalize_workers;
        std::thread::scope(|scope| {
            for chunk in gram_postings.chunks_mut(chunk_size.max(1)) {
                scope.spawn(move || {
                    for (_, ids) in chunk {
                        ids.sort_unstable();
                    }
                });
            }
        });
    }

    let postings_cap = gram_postings
        .iter()
        .map(|(_, ids)| ids.len())
        .sum::<usize>();
    let mut gram_entries: Vec<(u32, u64, u32)> = Vec::with_capacity(gram_postings.len());
    let mut postings: Vec<u32> = Vec::with_capacity(postings_cap);

    for (gram, ids) in gram_postings {
        let off = u64::try_from(postings.len()).unwrap();
        let len = u32::try_from(ids.len()).unwrap();
        postings.extend_from_slice(&ids);
        gram_entries.push((gram, off, len));
    }

    let snapshot_bytes = write_snapshot_atomic(
        &paths,
        &path_entries,
        &path_arena,
        &gram_entries,
        &postings,
        include_binary,
        max_file_size,
    )?;

    Ok(ContentBuildStats {
        workers,
        files_indexed,
        files_scanned,
        files_skipped_binary,
        files_skipped_too_large,
        total_bytes_indexed,
        scan_duration: scan_stats.duration,
        total_duration: started.elapsed(),
        snapshot_bytes,
        index_dir: paths.dir,
    })
}

fn query_candidates(snapshot: &ContentSnapshot, needle: &[u8]) -> io::Result<Vec<u32>> {
    if needle.len() < 3 {
        return Ok((0..snapshot.file_count as u32).collect());
    }

    let mut grams: Vec<u32> = needle
        .windows(3)
        .map(|w| trigram_value(w[0], w[1], w[2]))
        .collect();
    grams.sort_unstable();
    grams.dedup();

    let mut lists: Vec<Vec<u32>> = Vec::with_capacity(grams.len());
    for gram in grams {
        let Some(list) = snapshot.postings_for_gram(gram)? else {
            return Ok(Vec::new());
        };
        lists.push(list);
    }
    if lists.is_empty() {
        return Ok(Vec::new());
    }
    lists.sort_unstable_by_key(|l| l.len());

    let mut acc = lists.remove(0);
    for list in lists {
        acc = intersect_sorted(&acc, &list);
        if acc.is_empty() {
            break;
        }
    }
    Ok(acc)
}

pub fn query_content_index(
    root: &Path,
    needle: &str,
    list_mode: bool,
    max_results: Option<usize>,
) -> io::Result<ContentQueryStats> {
    if needle.is_empty() {
        return Err(io_other("content query cannot be empty"));
    }
    let started = Instant::now();
    let paths = content_paths_for_root(root)?;
    if !paths.snapshot.exists() {
        return Err(io_other(
            "content index snapshot not found; run --content-index-build",
        ));
    }

    let snapshot = ContentSnapshot::open(&paths.snapshot)?;
    let candidates = query_candidates(&snapshot, needle.as_bytes())?;

    let mut matches = 0usize;
    for file_id in candidates.iter().copied() {
        let path = snapshot.path_at(file_id)?;
        let bytes = match fs::read(path) {
            Ok(b) => b,
            Err(_) => continue,
        };
        if bytes.len() as u64 > snapshot.max_file_size {
            continue;
        }
        if !snapshot.include_binary && is_binary(&bytes) {
            continue;
        }
        if contains_bytes(&bytes, needle.as_bytes()) {
            matches += 1;
            if list_mode {
                println!("{path}");
            }
            if max_results.is_some_and(|m| matches >= m) {
                break;
            }
        }
    }

    Ok(ContentQueryStats {
        matches,
        candidates: candidates.len(),
        duration: started.elapsed(),
    })
}

pub fn content_index_status(root: &Path) -> io::Result<ContentIndexStatus> {
    let paths = content_paths_for_root(root)?;
    let snapshot_exists = paths.snapshot.exists();
    if !snapshot_exists {
        return Ok(ContentIndexStatus {
            root: paths.root,
            index_dir: paths.dir,
            snapshot_exists: false,
            files_indexed: 0,
            grams_indexed: 0,
            snapshot_size_bytes: 0,
            snapshot_created_unix: 0,
            include_binary: false,
            max_file_size: 0,
        });
    }

    let snapshot = ContentSnapshot::open(&paths.snapshot)?;
    let meta = fs::metadata(&paths.snapshot)?;
    Ok(ContentIndexStatus {
        root: paths.root,
        index_dir: paths.dir,
        snapshot_exists: true,
        files_indexed: snapshot.file_count,
        grams_indexed: snapshot.gram_count,
        snapshot_size_bytes: meta.len(),
        snapshot_created_unix: snapshot.created_unix,
        include_binary: snapshot.include_binary,
        max_file_size: snapshot.max_file_size,
    })
}
