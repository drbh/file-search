use crate::scan::{scan, FileFilter, ResultBatch, ScanOptions};
use memmap2::Mmap;
use notify::{recommended_watcher, Event, EventKind, RecursiveMode, Watcher};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const SNAP_MAGIC: [u8; 8] = *b"FSIDXV1\0";
const SNAP_VERSION: u32 = 1;
const SNAP_HEADER_SIZE: usize = 64;
const SNAP_ENTRY_SIZE: usize = 16;

const DELTA_MAGIC: [u8; 8] = *b"FSDLT1\0\0";
const DELTA_OP_ADD: u8 = b'A';
const DELTA_OP_REMOVE_EXACT: u8 = b'D';
const DELTA_OP_REMOVE_PREFIX: u8 = b'R';

const DELTA_AUTO_COMPACT_BYTES: u64 = 32 * 1024 * 1024;
const DELTA_AUTO_COMPACT_OPS: u64 = 200_000;

#[derive(Debug)]
pub struct BuildStats {
    pub files: usize,
    pub scan_duration: Duration,
    pub total_duration: Duration,
    pub snapshot_bytes: u64,
    pub index_dir: PathBuf,
}

#[derive(Debug)]
pub struct QueryStats {
    pub files: usize,
    pub duration: Duration,
    pub auto_compacted: bool,
}

#[derive(Debug)]
pub struct CompactStats {
    pub files: usize,
    pub duration: Duration,
    pub snapshot_bytes: u64,
}

#[derive(Debug)]
pub struct IndexStatus {
    pub root: String,
    pub index_dir: PathBuf,
    pub snapshot_exists: bool,
    pub snapshot_files: usize,
    pub snapshot_size_bytes: u64,
    pub snapshot_created_unix: u64,
    pub delta_exists: bool,
    pub delta_size_bytes: u64,
    pub delta_ops: u64,
}

#[derive(Default)]
struct DeltaState {
    adds: HashMap<String, u64>,
    exact_removes: HashMap<String, u64>,
    remove_prefixes: Vec<(String, u64)>,
    op_count: u64,
}

struct Snapshot {
    mmap: Mmap,
    count: usize,
    table_offset: usize,
    arena_offset: usize,
    arena_len: usize,
    created_unix: u64,
}

struct IndexPaths {
    root: String,
    dir: PathBuf,
    snapshot: PathBuf,
    snapshot_prev: PathBuf,
    delta: PathBuf,
    root_file: PathBuf,
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
        .ok_or_else(|| io_other("corrupt index: short u32"))?;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

#[inline]
fn read_u64(buf: &[u8], off: usize) -> io::Result<u64> {
    let bytes = buf
        .get(off..off + 8)
        .ok_or_else(|| io_other("corrupt index: short u64"))?;
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
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

#[inline]
fn fnv1a64(s: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for b in s.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn index_home() -> io::Result<PathBuf> {
    if let Ok(home) = std::env::var("FILE_SEARCH_INDEX_HOME") {
        return Ok(PathBuf::from(home));
    }
    let home = std::env::var("HOME").map_err(|_| io_other("HOME not set"))?;
    Ok(PathBuf::from(home).join(".file-search").join("indexes"))
}

fn index_paths_for_root(root_path: &Path) -> io::Result<IndexPaths> {
    let canonical = fs::canonicalize(root_path)?;
    let root = normalize_root(&canonical);
    let hash = fnv1a64(&root);
    let dir = index_home()?.join(format!("{hash:016x}"));
    Ok(IndexPaths {
        root,
        snapshot: dir.join("snapshot.bin"),
        snapshot_prev: dir.join("snapshot.prev.bin"),
        delta: dir.join("delta.bin"),
        root_file: dir.join("root.txt"),
        dir,
    })
}

fn ensure_index_dir(paths: &IndexPaths) -> io::Result<()> {
    fs::create_dir_all(&paths.dir)?;
    if paths.root_file.exists() {
        let existing = fs::read_to_string(&paths.root_file)?.trim().to_string();
        if existing != paths.root {
            return Err(io_other(format!(
                "index hash collision: {} != {}",
                existing, paths.root
            )));
        }
    } else {
        fs::write(&paths.root_file, format!("{}\n", paths.root))?;
    }
    Ok(())
}

fn open_delta_append(paths: &IndexPaths) -> io::Result<BufWriter<File>> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(&paths.delta)?;
    let len = file.metadata()?.len();
    if len == 0 {
        file.write_all(&DELTA_MAGIC)?;
        file.flush()?;
    } else {
        let mut magic = [0u8; DELTA_MAGIC.len()];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut magic)?;
        if magic != DELTA_MAGIC {
            return Err(io_other("corrupt delta log: bad magic"));
        }
        file.seek(SeekFrom::End(0))?;
    }
    Ok(BufWriter::new(file))
}

fn reset_delta(paths: &IndexPaths) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&paths.delta)?;
    file.write_all(&DELTA_MAGIC)?;
    file.sync_all()?;
    Ok(())
}

fn append_delta_record(writer: &mut BufWriter<File>, op: u8, path: &str) -> io::Result<()> {
    writer.write_all(&[op])?;
    writer.write_all(&(path.len() as u32).to_le_bytes())?;
    writer.write_all(path.as_bytes())?;
    Ok(())
}

fn path_matches_prefix(candidate: &str, prefix: &str) -> bool {
    if candidate == prefix {
        return true;
    }
    candidate
        .strip_prefix(prefix)
        .is_some_and(|rest| rest.starts_with('/'))
}

fn path_file_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn load_delta(paths: &IndexPaths) -> io::Result<DeltaState> {
    if !paths.delta.exists() {
        return Ok(DeltaState::default());
    }

    let data = fs::read(&paths.delta)?;
    if data.is_empty() {
        return Ok(DeltaState::default());
    }
    if data.len() < DELTA_MAGIC.len() || data[..DELTA_MAGIC.len()] != DELTA_MAGIC {
        return Err(io_other("corrupt delta log: bad magic"));
    }

    let mut state = DeltaState::default();
    let mut p = DELTA_MAGIC.len();
    let mut idx = 0u64;
    while p + 5 <= data.len() {
        let op = data[p];
        p += 1;
        let len = u32::from_le_bytes(data[p..p + 4].try_into().unwrap()) as usize;
        p += 4;
        if p + len > data.len() {
            break;
        }
        let path = String::from_utf8_lossy(&data[p..p + len]).to_string();
        p += len;

        match op {
            DELTA_OP_ADD => {
                state.adds.insert(path, idx);
            }
            DELTA_OP_REMOVE_EXACT => {
                state.exact_removes.insert(path, idx);
            }
            DELTA_OP_REMOVE_PREFIX => {
                state.remove_prefixes.push((path, idx));
            }
            _ => {}
        }
        idx += 1;
    }
    state.op_count = idx;
    Ok(state)
}

fn max_remove_idx(path: &str, state: &DeltaState) -> Option<u64> {
    let mut max_idx = state.exact_removes.get(path).copied();
    for (prefix, idx) in &state.remove_prefixes {
        if path_matches_prefix(path, prefix) {
            max_idx = Some(max_idx.map_or(*idx, |cur| cur.max(*idx)));
        }
    }
    max_idx
}

fn push_index_entry(
    entries: &mut Vec<(u64, u32)>,
    arena: &mut Vec<u8>,
    path: &str,
) -> io::Result<()> {
    let off = arena.len();
    let len = path.len();
    let off_u64 = u64::try_from(off).map_err(|_| io_other("path arena too large"))?;
    let len_u32 = u32::try_from(len).map_err(|_| io_other("path too long"))?;
    arena.extend_from_slice(path.as_bytes());
    entries.push((off_u64, len_u32));
    Ok(())
}

fn write_snapshot_atomic(
    paths: &IndexPaths,
    entries: &[(u64, u32)],
    arena: &[u8],
) -> io::Result<u64> {
    ensure_index_dir(paths)?;

    let tmp = paths.dir.join("snapshot.bin.tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp)?;

    file.write_all(&vec![0u8; SNAP_HEADER_SIZE])?;

    for (off, len) in entries {
        file.write_all(&off.to_le_bytes())?;
        file.write_all(&len.to_le_bytes())?;
        file.write_all(&0u32.to_le_bytes())?;
    }
    file.write_all(arena)?;

    let snapshot_bytes = file.metadata()?.len();
    let entry_bytes = u64::try_from(entries.len() * SNAP_ENTRY_SIZE).unwrap();
    let arena_bytes = u64::try_from(arena.len()).unwrap();
    let created_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let mut header = [0u8; SNAP_HEADER_SIZE];
    header[0..8].copy_from_slice(&SNAP_MAGIC);
    put_u32(&mut header, 8, SNAP_VERSION);
    put_u64(&mut header, 16, entries.len() as u64);
    put_u64(&mut header, 24, SNAP_HEADER_SIZE as u64);
    put_u64(&mut header, 32, entry_bytes);
    put_u64(&mut header, 40, SNAP_HEADER_SIZE as u64 + entry_bytes);
    put_u64(&mut header, 48, arena_bytes);
    put_u64(&mut header, 56, created_unix);

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

    let dir_file = File::open(&paths.dir)?;
    dir_file.sync_all()?;
    Ok(snapshot_bytes)
}

impl Snapshot {
    fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        if mmap.len() < SNAP_HEADER_SIZE {
            return Err(io_other("corrupt snapshot: too small"));
        }
        if mmap[0..8] != SNAP_MAGIC {
            return Err(io_other("corrupt snapshot: bad magic"));
        }
        let version = read_u32(&mmap, 8)?;
        if version != SNAP_VERSION {
            return Err(io_other(format!(
                "unsupported snapshot version: {}",
                version
            )));
        }
        let count = read_u64(&mmap, 16)? as usize;
        let table_offset = read_u64(&mmap, 24)? as usize;
        let table_len = read_u64(&mmap, 32)? as usize;
        let arena_offset = read_u64(&mmap, 40)? as usize;
        let arena_len = read_u64(&mmap, 48)? as usize;
        let created_unix = read_u64(&mmap, 56)?;

        if table_len != count.saturating_mul(SNAP_ENTRY_SIZE) {
            return Err(io_other("corrupt snapshot: entry table size mismatch"));
        }
        if table_offset + table_len > mmap.len() || arena_offset + arena_len > mmap.len() {
            return Err(io_other("corrupt snapshot: bounds"));
        }

        Ok(Self {
            mmap,
            count,
            table_offset,
            arena_offset,
            arena_len,
            created_unix,
        })
    }

    #[inline]
    fn path_at(&self, idx: usize) -> io::Result<&str> {
        if idx >= self.count {
            return Err(io_other("snapshot index out of bounds"));
        }
        let off = self.table_offset + idx * SNAP_ENTRY_SIZE;
        let path_off = read_u64(&self.mmap, off)? as usize;
        let path_len = read_u32(&self.mmap, off + 8)? as usize;
        if path_off + path_len > self.arena_len {
            return Err(io_other("corrupt snapshot: path bounds"));
        }
        let start = self.arena_offset + path_off;
        let end = start + path_len;
        let bytes = &self.mmap[start..end];
        std::str::from_utf8(bytes).map_err(|_| io_other("corrupt snapshot: invalid utf-8"))
    }
}

fn compact_index_inner(paths: &IndexPaths) -> io::Result<CompactStats> {
    if !paths.snapshot.exists() {
        return Err(io_other("index snapshot not found; run --index-build"));
    }
    let started = Instant::now();
    let snapshot = Snapshot::open(&paths.snapshot)?;
    let mut delta = load_delta(paths)?;

    if delta.op_count == 0 {
        return Ok(CompactStats {
            files: snapshot.count,
            duration: started.elapsed(),
            snapshot_bytes: fs::metadata(&paths.snapshot)?.len(),
        });
    }

    let mut entries: Vec<(u64, u32)> = Vec::with_capacity(snapshot.count);
    let mut arena: Vec<u8> = Vec::with_capacity(snapshot.arena_len + delta.adds.len() * 64);

    for idx in 0..snapshot.count {
        let path = snapshot.path_at(idx)?;
        let add_idx = delta.adds.remove(path);
        let remove_idx = max_remove_idx(path, &delta);
        let keep = match add_idx {
            Some(add) => match remove_idx {
                Some(rm) => add > rm,
                None => true,
            },
            None => remove_idx.is_none(),
        };
        if keep {
            push_index_entry(&mut entries, &mut arena, path)?;
        }
    }

    let mut extras: Vec<(String, u64)> = std::mem::take(&mut delta.adds).into_iter().collect();
    extras.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    for (path, add_idx) in extras {
        let remove_idx = max_remove_idx(&path, &delta);
        let keep = match remove_idx {
            Some(rm) => add_idx > rm,
            None => true,
        };
        if keep {
            push_index_entry(&mut entries, &mut arena, &path)?;
        }
    }

    let snapshot_bytes = write_snapshot_atomic(paths, &entries, &arena)?;
    reset_delta(paths)?;

    Ok(CompactStats {
        files: entries.len(),
        duration: started.elapsed(),
        snapshot_bytes,
    })
}

pub fn build_index(
    root: &Path,
    max_depth: usize,
    scan_options: ScanOptions,
) -> io::Result<BuildStats> {
    let started = Instant::now();
    let paths = index_paths_for_root(root)?;
    ensure_index_dir(&paths)?;

    let root_str = paths.root.clone();
    let scan_handle = scan(&root_str, true, max_depth, FileFilter::All, scan_options);
    let mut entries: Vec<(u64, u32)> = Vec::new();
    let mut arena: Vec<u8> = Vec::new();

    for batch in &scan_handle.receiver {
        if let ResultBatch::Paths(paths_batch) = batch {
            for path in paths_batch {
                push_index_entry(&mut entries, &mut arena, &path)?;
            }
        }
    }
    let scan_stats = scan_handle.wait_for_completion();
    let snapshot_bytes = write_snapshot_atomic(&paths, &entries, &arena)?;
    reset_delta(&paths)?;

    Ok(BuildStats {
        files: entries.len(),
        scan_duration: scan_stats.duration,
        total_duration: started.elapsed(),
        snapshot_bytes,
        index_dir: paths.dir,
    })
}

pub fn query_index(
    root: &Path,
    filter: &FileFilter,
    list_mode: bool,
    max_results: Option<usize>,
    auto_compact: bool,
) -> io::Result<QueryStats> {
    let started = Instant::now();
    let paths = index_paths_for_root(root)?;
    if !paths.snapshot.exists() {
        return Err(io_other("index snapshot not found; run --index-build"));
    }

    let mut auto_compacted = false;
    if auto_compact {
        let delta_size = fs::metadata(&paths.delta).map(|m| m.len()).unwrap_or(0);
        let delta = load_delta(&paths)?;
        if delta.op_count >= DELTA_AUTO_COMPACT_OPS || delta_size >= DELTA_AUTO_COMPACT_BYTES {
            let _ = compact_index_inner(&paths)?;
            auto_compacted = true;
        }
    }

    let snapshot = Snapshot::open(&paths.snapshot)?;
    let mut delta = load_delta(&paths)?;

    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());
    let mut total = 0usize;

    'outer: for idx in 0..snapshot.count {
        let path = snapshot.path_at(idx)?;
        let add_idx = delta.adds.remove(path);
        let remove_idx = max_remove_idx(path, &delta);
        let present = match add_idx {
            Some(add) => match remove_idx {
                Some(rm) => add > rm,
                None => true,
            },
            None => remove_idx.is_none(),
        };
        if !present {
            continue;
        }
        if !filter.matches(path_file_name(path)) {
            continue;
        }

        total += 1;
        if list_mode {
            writeln!(writer, "{path}")?;
        }
        if max_results.is_some_and(|max| total >= max) {
            break 'outer;
        }
    }

    if max_results.map(|max| total < max).unwrap_or(true) {
        let mut extras: Vec<(String, u64)> = std::mem::take(&mut delta.adds).into_iter().collect();
        extras.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        for (path, add_idx) in extras {
            let remove_idx = max_remove_idx(&path, &delta);
            if remove_idx.is_some_and(|rm| add_idx <= rm) {
                continue;
            }
            if !filter.matches(path_file_name(&path)) {
                continue;
            }
            total += 1;
            if list_mode {
                writeln!(writer, "{path}")?;
            }
            if max_results.is_some_and(|max| total >= max) {
                break;
            }
        }
    }
    writer.flush()?;

    Ok(QueryStats {
        files: total,
        duration: started.elapsed(),
        auto_compacted,
    })
}

pub fn compact_index(root: &Path) -> io::Result<CompactStats> {
    let paths = index_paths_for_root(root)?;
    compact_index_inner(&paths)
}

pub fn index_status(root: &Path) -> io::Result<IndexStatus> {
    let paths = index_paths_for_root(root)?;
    let snapshot_exists = paths.snapshot.exists();
    let (snapshot_files, snapshot_size_bytes, snapshot_created_unix) = if snapshot_exists {
        let snap = Snapshot::open(&paths.snapshot)?;
        let meta = fs::metadata(&paths.snapshot)?;
        (snap.count, meta.len(), snap.created_unix)
    } else {
        (0, 0, 0)
    };

    let delta_exists = paths.delta.exists();
    let delta_size_bytes = if delta_exists {
        fs::metadata(&paths.delta)?.len()
    } else {
        0
    };
    let delta_ops = if delta_exists {
        load_delta(&paths)?.op_count
    } else {
        0
    };

    Ok(IndexStatus {
        root: paths.root,
        index_dir: paths.dir,
        snapshot_exists,
        snapshot_files,
        snapshot_size_bytes,
        snapshot_created_unix,
        delta_exists,
        delta_size_bytes,
        delta_ops,
    })
}

fn event_to_ops(event: &Event) -> Vec<(u8, String)> {
    let mut ops = Vec::with_capacity(event.paths.len());
    for path in &event.paths {
        let path_str = path.to_string_lossy().to_string();
        match event.kind {
            EventKind::Create(_) | EventKind::Modify(_) => {
                if path.is_file() {
                    ops.push((DELTA_OP_ADD, path_str));
                }
            }
            EventKind::Remove(_) => {
                ops.push((DELTA_OP_REMOVE_PREFIX, path_str));
            }
            _ => {
                if path.is_file() {
                    ops.push((DELTA_OP_ADD, path_str));
                } else if !path.exists() {
                    ops.push((DELTA_OP_REMOVE_PREFIX, path_str));
                }
            }
        }
    }
    ops
}

pub fn watch_index(root: &Path) -> io::Result<()> {
    let paths = index_paths_for_root(root)?;
    ensure_index_dir(&paths)?;
    let mut writer = open_delta_append(&paths)?;

    let (tx, rx) = std::sync::mpsc::channel::<notify::Result<Event>>();
    let mut watcher = recommended_watcher(move |res| {
        let _ = tx.send(res);
    })
    .map_err(|e| io_other(format!("watcher init failed: {}", e)))?;
    watcher
        .watch(root, RecursiveMode::Recursive)
        .map_err(|e| io_other(format!("watch start failed: {}", e)))?;

    eprintln!("Watching {} for index delta updates", paths.root);
    let mut pending = 0usize;

    loop {
        match rx.recv() {
            Ok(Ok(event)) => {
                for (op, path) in event_to_ops(&event) {
                    append_delta_record(&mut writer, op, &path)?;
                    pending += 1;
                }
            }
            Ok(Err(err)) => {
                eprintln!("watch error: {}", err);
            }
            Err(_) => break,
        }

        if pending > 0 {
            writer.flush()?;
            pending = 0;
        }
    }

    writer.flush()?;
    Ok(())
}
