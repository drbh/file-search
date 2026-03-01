use crossbeam_channel::unbounded;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{mem, ptr};

#[repr(C)]
struct attrlist {
    bitmapcount: u16,
    reserved: u16,
    commonattr: u32,
    volattr: u32,
    dirattr: u32,
    fileattr: u32,
    forkattr: u32,
}
#[repr(C)]
struct attrreference {
    off: i32,
    len: u32,
}

#[link(name = "System")]
extern "C" {
    fn getattrlistbulk(
        dirfd: i32,
        al: *const attrlist,
        buf: *mut core::ffi::c_void,
        size: usize,
        opts: u64,
    ) -> isize;
    fn openat(dfd: i32, path: *const i8, oflag: i32, ...) -> i32;
    fn close(fd: i32) -> i32;
    fn fcntl(fd: i32, cmd: i32, ...) -> i32;
    fn getdtablesize() -> i32;
}

#[inline]
unsafe fn read_u32_unaligned(p: *const u8) -> u32 {
    ptr::read_unaligned(p.cast())
}

#[inline]
unsafe fn read_attrref_unaligned(p: *const u8) -> attrreference {
    ptr::read_unaligned(p.cast())
}

const ATTR_BIT_MAP_COUNT: u16 = 5;
const ATTR_CMN_NAME: u32 = 0x00000001;
const ATTR_CMN_OBJTYPE: u32 = 0x00000008;
const ATTR_CMN_RETURNED_ATTRS: u32 = 0x80000000;

const O_RDONLY: i32 = 0x0000;
const O_DIRECTORY: i32 = 0x100000;
const O_CLOEXEC: i32 = 0x01000000;
const AT_FDCWD: i32 = -2;
const F_RDAHEAD: i32 = 45;
const INITIAL_BULK_BUF_SIZE: usize = 4 << 20;
const MAX_BULK_BUF_SIZE: usize = 16 << 20;
const BULK_EXPAND_SLACK: usize = 128 << 10; // grow when buffer nearly full
const DEFAULT_FD_LIMIT: usize = 1024;
const FD_HEADROOM: usize = 64;
const FD_MIN_LIMIT: usize = 64;
const FD_MAX_LIMIT: usize = 65536;
const COUNT_RESULT_BATCH_SIZE: usize = 1 << 18;
const PATH_RESULT_BATCH_SIZE: usize = 4096;

// Darwin vtype constants
const VREG: u32 = 1;
const VDIR: u32 = 2;

static GETATTR_CALLS: AtomicU64 = AtomicU64::new(0);
static GETATTR_ENTRIES: AtomicU64 = AtomicU64::new(0);
static OPENAT_CALLS: AtomicU64 = AtomicU64::new(0);
static OPENAT_ABS_CALLS: AtomicU64 = AtomicU64::new(0);
static OPENAT_REL_CALLS: AtomicU64 = AtomicU64::new(0);
static OPENAT_FAILS: AtomicU64 = AtomicU64::new(0);
static OPENAT_NANOS: AtomicU64 = AtomicU64::new(0);
static GETATTR_NANOS: AtomicU64 = AtomicU64::new(0);
static GETATTR_ERRORS: AtomicU64 = AtomicU64::new(0);
static RDAHEAD_CALLS: AtomicU64 = AtomicU64::new(0);
static RDAHEAD_FAILS: AtomicU64 = AtomicU64::new(0);
static RDAHEAD_NANOS: AtomicU64 = AtomicU64::new(0);
static CLOSE_CALLS: AtomicU64 = AtomicU64::new(0);

#[derive(Default)]
struct LocalSysStats {
    getattr_calls: u64,
    getattr_entries: u64,
    getattr_errors: u64,
    openat_calls: u64,
    openat_abs_calls: u64,
    openat_rel_calls: u64,
    openat_fails: u64,
    openat_nanos: u64,
    getattr_nanos: u64,
    rdahead_calls: u64,
    rdahead_fails: u64,
    rdahead_nanos: u64,
    close_calls: u64,
}

#[inline]
fn flush_local_sys_stats(stats: &LocalSysStats) {
    GETATTR_CALLS.fetch_add(stats.getattr_calls, Ordering::Relaxed);
    GETATTR_ENTRIES.fetch_add(stats.getattr_entries, Ordering::Relaxed);
    GETATTR_ERRORS.fetch_add(stats.getattr_errors, Ordering::Relaxed);
    OPENAT_CALLS.fetch_add(stats.openat_calls, Ordering::Relaxed);
    OPENAT_ABS_CALLS.fetch_add(stats.openat_abs_calls, Ordering::Relaxed);
    OPENAT_REL_CALLS.fetch_add(stats.openat_rel_calls, Ordering::Relaxed);
    OPENAT_FAILS.fetch_add(stats.openat_fails, Ordering::Relaxed);
    OPENAT_NANOS.fetch_add(stats.openat_nanos, Ordering::Relaxed);
    GETATTR_NANOS.fetch_add(stats.getattr_nanos, Ordering::Relaxed);
    RDAHEAD_CALLS.fetch_add(stats.rdahead_calls, Ordering::Relaxed);
    RDAHEAD_FAILS.fetch_add(stats.rdahead_fails, Ordering::Relaxed);
    RDAHEAD_NANOS.fetch_add(stats.rdahead_nanos, Ordering::Relaxed);
    CLOSE_CALLS.fetch_add(stats.close_calls, Ordering::Relaxed);
}

#[inline]
unsafe fn timed_openat(
    dfd: i32,
    path: *const i8,
    oflag: i32,
    absolute: bool,
    collect_syscall_stats: bool,
    stats: &mut LocalSysStats,
) -> i32 {
    if collect_syscall_stats {
        let start = Instant::now();
        let fd = openat(dfd, path, oflag);
        stats.openat_calls += 1;
        if absolute {
            stats.openat_abs_calls += 1;
        } else {
            stats.openat_rel_calls += 1;
        }
        if fd < 0 {
            stats.openat_fails += 1;
        }
        stats.openat_nanos += start.elapsed().as_nanos() as u64;
        fd
    } else {
        openat(dfd, path, oflag)
    }
}

#[inline]
unsafe fn timed_getattrlistbulk(
    dirfd: i32,
    al: *const attrlist,
    buf: *mut core::ffi::c_void,
    size: usize,
    collect_syscall_stats: bool,
    stats: &mut LocalSysStats,
) -> isize {
    if collect_syscall_stats {
        let start = Instant::now();
        let n = getattrlistbulk(dirfd, al, buf, size, 0);
        stats.getattr_calls += 1;
        if n > 0 {
            stats.getattr_entries += n as u64;
        } else if n < 0 {
            stats.getattr_errors += 1;
        }
        stats.getattr_nanos += start.elapsed().as_nanos() as u64;
        n
    } else {
        getattrlistbulk(dirfd, al, buf, size, 0)
    }
}

#[inline]
unsafe fn timed_set_rdahead(fd: i32, collect_syscall_stats: bool, stats: &mut LocalSysStats) {
    if collect_syscall_stats {
        let start = Instant::now();
        let rc = fcntl(fd, F_RDAHEAD, 1);
        stats.rdahead_calls += 1;
        if rc < 0 {
            stats.rdahead_fails += 1;
        }
        stats.rdahead_nanos += start.elapsed().as_nanos() as u64;
    } else {
        let _ = fcntl(fd, F_RDAHEAD, 1);
    }
}

#[derive(Clone)]
pub struct ScanOptions {
    pub include_hidden_dirs: bool,
    pub prune_defaults: bool,
    pub ignore_dir_names: Vec<String>,
    pub ignore_path_prefixes: Vec<String>,
}

#[inline]
fn is_default_prune_dir(name: &str) -> bool {
    matches!(
        name,
        ".git"
            | ".hg"
            | ".svn"
            | "node_modules"
            | ".next"
            | "__pycache__"
            | ".venv"
            | "venv"
            | "target"
            | "dist"
            | "build"
            | ".cache"
            | ".mypy_cache"
            | ".pytest_cache"
            | ".ruff_cache"
            | ".tox"
            | ".nox"
            | ".gradle"
            | ".idea"
            | ".vscode"
            | ".pnpm-store"
            | ".yarn"
            | ".sass-cache"
            | ".parcel-cache"
            | ".turbo"
            | ".angular"
            | "Pods"
            | "DerivedData"
            | "bazel-out"
            | "bazel-bin"
            | "bazel-testlogs"
            | ".terraform"
            | ".serverless"
            | ".aws-sam"
    )
}

#[inline]
fn matches_path_prefix(candidate: &str, prefix: &str) -> bool {
    if candidate == prefix {
        return true;
    }
    candidate
        .strip_prefix(prefix)
        .is_some_and(|rest| rest.starts_with('/'))
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self {
            include_hidden_dirs: false,
            prune_defaults: true,
            ignore_dir_names: Vec::new(),
            ignore_path_prefixes: Vec::new(),
        }
    }
}

impl ScanOptions {
    #[inline]
    fn should_skip_dir(&self, parent_path: &str, name: &str) -> bool {
        if !self.include_hidden_dirs && name.starts_with('.') {
            return true;
        }
        if self.prune_defaults && is_default_prune_dir(name) {
            return true;
        }
        if !self.ignore_dir_names.is_empty() && self.ignore_dir_names.iter().any(|dir| dir == name)
        {
            return true;
        }
        if self.ignore_path_prefixes.is_empty() {
            return false;
        }

        let mut child_path = String::with_capacity(parent_path.len() + name.len() + 1);
        if parent_path == "/" {
            child_path.push('/');
            child_path.push_str(name);
        } else {
            child_path.push_str(parent_path);
            child_path.push('/');
            child_path.push_str(name);
        }
        self.ignore_path_prefixes
            .iter()
            .any(|prefix| matches_path_prefix(&child_path, prefix))
    }
}

struct WorkItem {
    path: String,
    dirfd: Option<RawFd>,
    depth: usize,
}

enum WorkMsg {
    Dir(WorkItem),
    Shutdown,
}

struct PathScratch {
    buf: String,
    base_len: usize,
}

impl PathScratch {
    fn new() -> Self {
        Self {
            buf: String::with_capacity(1024),
            base_len: 0,
        }
    }

    fn set_base(&mut self, base: &str) {
        self.buf.clear();
        self.buf.push_str(base);
        if self.buf != "/" && !self.buf.ends_with('/') {
            self.buf.push('/');
        }
        self.base_len = self.buf.len();
    }

    fn compose<'a>(&'a mut self, name: &str) -> &'a str {
        self.buf.truncate(self.base_len);
        self.buf.push_str(name);
        &self.buf
    }
}

pub enum ResultBatch {
    Count(usize),
    Paths(Vec<String>),
}

struct FdBudget {
    limit: usize,
    in_use: AtomicUsize,
}

impl FdBudget {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            in_use: AtomicUsize::new(0),
        }
    }

    fn try_acquire(&self) -> bool {
        let mut current = self.in_use.load(Ordering::Relaxed);
        loop {
            if current >= self.limit {
                return false;
            }
            match self.in_use.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(next) => current = next,
            }
        }
    }

    fn release(&self) {
        self.in_use.fetch_sub(1, Ordering::Release);
    }
}

#[inline]
fn mark_work_complete(
    active_count: &AtomicUsize,
    work_tx: &crossbeam_channel::Sender<WorkMsg>,
    workers: usize,
) {
    if active_count.fetch_sub(1, Ordering::AcqRel) == 1 {
        for _ in 0..workers {
            let _ = work_tx.send(WorkMsg::Shutdown);
        }
    }
}

/// File filter configuration
#[derive(Clone)]
pub enum FileFilter {
    /// Match all regular files
    All,
    /// Match when all provided clauses pass (logical AND)
    Composite {
        extensions: Option<Vec<Box<[u8]>>>,
        contains_all: Vec<Box<[u8]>>,
        prefix: Option<Box<[u8]>>,
        suffix: Option<Box<[u8]>>,
    },
}

#[inline]
fn ascii_fold(byte: u8) -> u8 {
    if byte.is_ascii_uppercase() {
        byte + 32
    } else {
        byte
    }
}

#[inline]
fn eq_ascii_fold(input: &[u8], pattern_lower: &[u8]) -> bool {
    if input.len() != pattern_lower.len() {
        return false;
    }
    input
        .iter()
        .zip(pattern_lower.iter())
        .all(|(a, b)| ascii_fold(*a) == *b)
}

#[inline]
fn starts_with_ascii_fold(input: &[u8], prefix_lower: &[u8]) -> bool {
    if prefix_lower.len() > input.len() {
        return false;
    }
    input[..prefix_lower.len()]
        .iter()
        .zip(prefix_lower.iter())
        .all(|(a, b)| ascii_fold(*a) == *b)
}

#[inline]
fn ends_with_ascii_fold(input: &[u8], suffix_lower: &[u8]) -> bool {
    if suffix_lower.len() > input.len() {
        return false;
    }
    let start = input.len() - suffix_lower.len();
    input[start..]
        .iter()
        .zip(suffix_lower.iter())
        .all(|(a, b)| ascii_fold(*a) == *b)
}

#[inline]
fn contains_ascii_fold(input: &[u8], needle_lower: &[u8]) -> bool {
    if needle_lower.is_empty() {
        return true;
    }
    if needle_lower.len() > input.len() {
        return false;
    }

    let last_start = input.len() - needle_lower.len();
    for i in 0..=last_start {
        if ascii_fold(input[i]) != needle_lower[0] {
            continue;
        }
        let mut matched = true;
        for j in 1..needle_lower.len() {
            if ascii_fold(input[i + j]) != needle_lower[j] {
                matched = false;
                break;
            }
        }
        if matched {
            return true;
        }
    }
    false
}

impl FileFilter {
    #[inline]
    fn matches_extension(name_bytes: &[u8], exts: &[Box<[u8]>]) -> bool {
        let Some(dot) = name_bytes.iter().rposition(|&b| b == b'.') else {
            return false;
        };
        let ext = &name_bytes[dot + 1..];
        if ext.is_empty() {
            return false;
        }
        exts.iter().any(|e| eq_ascii_fold(ext, e))
    }

    #[inline]
    fn matches_suffix(name_bytes: &[u8], suffix: &[u8]) -> bool {
        // Match suffix before extension
        let base = if let Some(dot) = name_bytes.iter().rposition(|&b| b == b'.') {
            &name_bytes[..dot]
        } else {
            name_bytes
        };
        ends_with_ascii_fold(base, suffix)
    }

    #[inline]
    pub fn matches_bytes(&self, name_bytes: &[u8]) -> bool {
        match self {
            FileFilter::All => true,
            FileFilter::Composite {
                extensions,
                contains_all,
                prefix,
                suffix,
            } => {
                if let Some(exts) = extensions {
                    if !Self::matches_extension(name_bytes, exts) {
                        return false;
                    }
                }
                for needle in contains_all {
                    if !contains_ascii_fold(name_bytes, needle) {
                        return false;
                    }
                }
                if let Some(pfx) = prefix {
                    if !starts_with_ascii_fold(name_bytes, pfx) {
                        return false;
                    }
                }
                if let Some(sfx) = suffix {
                    if !Self::matches_suffix(name_bytes, sfx) {
                        return false;
                    }
                }
                true
            }
        }
    }
}

unsafe fn worker(
    work_rx: crossbeam_channel::Receiver<WorkMsg>,
    work_tx: crossbeam_channel::Sender<WorkMsg>,
    result_tx: crossbeam_channel::Sender<ResultBatch>,
    active_count: Arc<AtomicUsize>,
    fd_budget: Arc<FdBudget>,
    workers: usize,
    list_mode: bool,
    max_depth: usize,
    filter: FileFilter,
    scan_options: Arc<ScanOptions>,
    collect_syscall_stats: bool,
) {
    // Thread-local reusable buffers
    let mut buf = vec![0u8; INITIAL_BULK_BUF_SIZE];
    let mut sys_stats = LocalSysStats::default();
    let enable_rdahead = std::env::var_os("FILE_SEARCH_DISABLE_RDAHEAD").is_none();

    // Only request what we need: NAME + OBJTYPE + RETURNED_ATTRS (required for getattrlistbulk)
    let al = attrlist {
        bitmapcount: ATTR_BIT_MAP_COUNT,
        reserved: 0,
        commonattr: ATTR_CMN_RETURNED_ATTRS | ATTR_CMN_NAME | ATTR_CMN_OBJTYPE,
        volattr: 0,
        dirattr: 0,
        fileattr: 0,
        forkattr: 0,
    };
    let mut path_buf = PathScratch::new();
    let mut dir_cbuf: Vec<u8> = Vec::with_capacity(1024); // for openat (NUL-terminated)
    let mut name_cbuf: Vec<u8> = Vec::with_capacity(1024);
    let mut pending_count: usize = 0;
    let mut pending_paths: Vec<String> = Vec::with_capacity(1024);

    loop {
        let work = match work_rx.recv() {
            Ok(WorkMsg::Dir(w)) => w,
            Ok(WorkMsg::Shutdown) => break,
            Err(_) => break,
        };

        let WorkItem { path, dirfd, depth } = work;

        let mut held_fd = false;
        let dirfd = match dirfd {
            Some(fd) => {
                held_fd = true;
                fd
            }
            None => {
                // Open by absolute path
                dir_cbuf.clear();
                if path.is_empty() {
                    dir_cbuf.extend_from_slice(b"/");
                } else {
                    dir_cbuf.extend_from_slice(path.as_bytes());
                }
                dir_cbuf.push(0);
                let fd = timed_openat(
                    AT_FDCWD,
                    dir_cbuf.as_ptr() as *const i8,
                    O_RDONLY | O_DIRECTORY | O_CLOEXEC,
                    true,
                    collect_syscall_stats,
                    &mut sys_stats,
                );
                if fd < 0 {
                    mark_work_complete(&active_count, &work_tx, workers);
                    continue;
                }
                fd
            }
        };
        path_buf.set_base(&path);

        // Enable directory read-ahead for better performance
        if enable_rdahead {
            timed_set_rdahead(dirfd, collect_syscall_stats, &mut sys_stats);
        }

        loop {
            let n = timed_getattrlistbulk(
                dirfd,
                &al,
                buf.as_mut_ptr().cast(),
                buf.len(),
                collect_syscall_stats,
                &mut sys_stats,
            );
            if n < 0 {
                // syscall error; stop scanning this directory
                break;
            }
            if n == 0 {
                // End of directory
                break;
            }

            // n == number of records, not bytes
            let mut p = 0usize;
            for _ in 0..(n as usize) {
                let base = buf.as_ptr().add(p);

                // [u32 reclen]
                let reclen = read_u32_unaligned(base) as usize;

                // skip returned_attrs (attribute_set_t = 5 * u32)
                let name_ref_ptr = base.add(4 + 20);
                let name_ref = read_attrref_unaligned(name_ref_ptr);

                // objtype right after attrreference
                let objtype = read_u32_unaligned(name_ref_ptr.add(mem::size_of::<attrreference>()));

                // Name bytes are at (name_ref_ptr + name_ref.off) with byte length name_ref.len
                // Calculate offset relative to buffer start
                let name_start =
                    (name_ref_ptr as usize - buf.as_ptr() as usize) + (name_ref.off as usize);
                let name_len = name_ref.len as usize;

                // Trim a trailing NUL if present; avoid CStr and extra validation work.
                let name_bytes = &buf[name_start..name_start + name_len];
                let name_bytes = if !name_bytes.is_empty() && *name_bytes.last().unwrap() == 0 {
                    &name_bytes[..name_len - 1]
                } else {
                    name_bytes
                };

                if name_bytes == b"." || name_bytes == b".." {
                    p += reclen;
                    continue;
                }

                if objtype == VDIR {
                    // SAFETY: APFS filenames are UTF-8; skip per-entry validation on hot path.
                    let name = std::str::from_utf8_unchecked(name_bytes);
                    if scan_options.should_skip_dir(&path, name) {
                        p += reclen;
                        continue;
                    }
                    // Check max_depth: 0 means unlimited, otherwise respect the limit
                    let next_depth = depth + 1;
                    if max_depth > 0 && next_depth > max_depth {
                        p += reclen;
                        continue;
                    }
                    let mut next_fd: Option<RawFd> = None;

                    if fd_budget.try_acquire() {
                        name_cbuf.clear();
                        name_cbuf.extend_from_slice(name.as_bytes());
                        name_cbuf.push(0);
                        let cfd = timed_openat(
                            dirfd,
                            name_cbuf.as_ptr() as *const i8,
                            O_RDONLY | O_DIRECTORY | O_CLOEXEC,
                            false,
                            collect_syscall_stats,
                            &mut sys_stats,
                        );
                        if cfd >= 0 {
                            next_fd = Some(cfd);
                        } else {
                            fd_budget.release();
                        }
                    }
                    // If we can't acquire fd budget, next_fd stays None and we'll open by path later

                    // Increment counter BEFORE sending to queue to prevent race
                    active_count.fetch_add(1, Ordering::Relaxed);
                    let composed = path_buf.compose(name).to_owned();
                    let child = WorkItem {
                        path: composed,
                        dirfd: next_fd,
                        depth: next_depth,
                    };
                    if let Err(err) = work_tx.send(WorkMsg::Dir(child)) {
                        if let WorkMsg::Dir(failed_child) = err.0 {
                            if let Some(fd) = failed_child.dirfd {
                                let _ = close(fd);
                                if collect_syscall_stats {
                                    sys_stats.close_calls += 1;
                                }
                                fd_budget.release();
                            }
                        }
                        mark_work_complete(&active_count, &work_tx, workers);
                    }
                } else if objtype == VREG && filter.matches_bytes(name_bytes) {
                    if list_mode {
                        // SAFETY: APFS filenames are UTF-8; skip per-entry validation on hot path.
                        let name = std::str::from_utf8_unchecked(name_bytes);
                        let composed = path_buf.compose(name).to_owned();
                        pending_paths.push(composed);
                        if pending_paths.len() >= PATH_RESULT_BATCH_SIZE {
                            let batch = std::mem::take(&mut pending_paths);
                            let _ = result_tx.send(ResultBatch::Paths(batch));
                            pending_paths = Vec::with_capacity(PATH_RESULT_BATCH_SIZE);
                        }
                    } else {
                        pending_count += 1;
                        if pending_count >= COUNT_RESULT_BATCH_SIZE {
                            let _ = result_tx.send(ResultBatch::Count(pending_count));
                            pending_count = 0;
                        }
                    }
                }

                p += reclen; // advance to next record
            }

            if p + BULK_EXPAND_SLACK >= buf.len() && buf.len() < MAX_BULK_BUF_SIZE {
                let new_len = (buf.len() * 2).min(MAX_BULK_BUF_SIZE);
                buf.resize(new_len, 0);
            }
        }

        let _ = close(dirfd);
        if collect_syscall_stats {
            sys_stats.close_calls += 1;
        }
        if held_fd {
            fd_budget.release();
        }

        // Decrement counter for this directory finishing
        // (subdirs were already incremented when queued)
        mark_work_complete(&active_count, &work_tx, workers);
    }

    if list_mode {
        if !pending_paths.is_empty() {
            let batch = std::mem::take(&mut pending_paths);
            let _ = result_tx.send(ResultBatch::Paths(batch));
        }
    } else if pending_count != 0 {
        let _ = result_tx.send(ResultBatch::Count(pending_count));
    }

    if collect_syscall_stats {
        flush_local_sys_stats(&sys_stats);
    }
}

fn detect_fd_limit() -> usize {
    let raw = unsafe { getdtablesize() };
    let base = if raw > 0 {
        raw as usize
    } else {
        DEFAULT_FD_LIMIT
    };
    base.saturating_sub(FD_HEADROOM)
        .clamp(FD_MIN_LIMIT, FD_MAX_LIMIT)
}

fn resolve_scan_workers() -> usize {
    if let Ok(raw) = std::env::var("FILE_SEARCH_SCAN_WORKERS") {
        if let Ok(parsed) = raw.parse::<usize>() {
            return parsed.max(1);
        }
    }

    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    if cfg!(target_arch = "aarch64") {
        cores.min(7).max(2)
    } else {
        cores.min(8).max(2)
    }
}

/// Runtime statistics from scanning
#[derive(Debug)]
pub struct ScanStats {
    pub duration: Duration,
    pub getattr_calls: u64,
    pub getattr_entries: u64,
    pub getattr_errors: u64,
    pub openat_calls: u64,
    pub openat_abs_calls: u64,
    pub openat_rel_calls: u64,
    pub openat_fails: u64,
    pub openat_ms: f64,
    pub getattr_ms: f64,
    pub rdahead_calls: u64,
    pub rdahead_fails: u64,
    pub rdahead_ms: f64,
    pub close_calls: u64,
}

impl ScanStats {
    pub fn avg_entries_per_call(&self) -> f64 {
        if self.getattr_calls > 0 {
            self.getattr_entries as f64 / self.getattr_calls as f64
        } else {
            0.0
        }
    }
}

/// Result of starting a scan operation
pub struct ScanHandle {
    pub receiver: crossbeam_channel::Receiver<ResultBatch>,
    start_time: std::time::Instant,
}

impl ScanHandle {
    /// Wait for the scan to complete and return statistics
    pub fn wait_for_completion(self) -> ScanStats {
        // Drain the receiver to ensure all workers complete
        while self.receiver.recv().is_ok() {}

        let openat_nanos = OPENAT_NANOS.load(Ordering::Relaxed);
        let getattr_nanos = GETATTR_NANOS.load(Ordering::Relaxed);
        let rdahead_nanos = RDAHEAD_NANOS.load(Ordering::Relaxed);

        ScanStats {
            duration: self.start_time.elapsed(),
            getattr_calls: GETATTR_CALLS.load(Ordering::Relaxed),
            getattr_entries: GETATTR_ENTRIES.load(Ordering::Relaxed),
            getattr_errors: GETATTR_ERRORS.load(Ordering::Relaxed),
            openat_calls: OPENAT_CALLS.load(Ordering::Relaxed),
            openat_abs_calls: OPENAT_ABS_CALLS.load(Ordering::Relaxed),
            openat_rel_calls: OPENAT_REL_CALLS.load(Ordering::Relaxed),
            openat_fails: OPENAT_FAILS.load(Ordering::Relaxed),
            openat_ms: openat_nanos as f64 / 1_000_000.0,
            getattr_ms: getattr_nanos as f64 / 1_000_000.0,
            rdahead_calls: RDAHEAD_CALLS.load(Ordering::Relaxed),
            rdahead_fails: RDAHEAD_FAILS.load(Ordering::Relaxed),
            rdahead_ms: rdahead_nanos as f64 / 1_000_000.0,
            close_calls: CLOSE_CALLS.load(Ordering::Relaxed),
        }
    }
}

/// Scan a directory tree for files matching a filter
///
/// # Arguments
///
/// * `root_path` - The root directory to scan
/// * `list_mode` - If true, sends individual paths; if false, only sends counts
/// * `max_depth` - Maximum depth to traverse (0 = unlimited)
/// * `filter` - File filter to apply
///
/// # Returns
///
/// Returns a `ScanHandle` containing a receiver for `ResultBatch` items
pub fn scan(
    root_path: &str,
    list_mode: bool,
    max_depth: usize,
    filter: FileFilter,
    options: ScanOptions,
    collect_syscall_stats: bool,
) -> ScanHandle {
    let start_time = std::time::Instant::now();

    // Reset global counters
    GETATTR_CALLS.store(0, Ordering::Relaxed);
    GETATTR_ENTRIES.store(0, Ordering::Relaxed);
    GETATTR_ERRORS.store(0, Ordering::Relaxed);
    OPENAT_CALLS.store(0, Ordering::Relaxed);
    OPENAT_ABS_CALLS.store(0, Ordering::Relaxed);
    OPENAT_REL_CALLS.store(0, Ordering::Relaxed);
    OPENAT_FAILS.store(0, Ordering::Relaxed);
    OPENAT_NANOS.store(0, Ordering::Relaxed);
    GETATTR_NANOS.store(0, Ordering::Relaxed);
    RDAHEAD_CALLS.store(0, Ordering::Relaxed);
    RDAHEAD_FAILS.store(0, Ordering::Relaxed);
    RDAHEAD_NANOS.store(0, Ordering::Relaxed);
    CLOSE_CALLS.store(0, Ordering::Relaxed);

    let mut root_path = root_path.to_string();
    while root_path.ends_with('/') && root_path.len() > 1 {
        root_path.pop();
    }
    if root_path.is_empty() {
        root_path.push('/');
    }

    // NUL-terminated bytes for openat
    let mut root_c = root_path.as_bytes().to_vec();
    root_c.push(0);

    let fd_limit = detect_fd_limit();
    let fd_budget = Arc::new(FdBudget::new(fd_limit));

    // Ensure root directory can be opened up-front
    let mut root_stats = LocalSysStats::default();
    let root_fd = unsafe {
        let fd = timed_openat(
            AT_FDCWD,
            root_c.as_ptr() as *const i8,
            O_RDONLY | O_DIRECTORY | O_CLOEXEC,
            true,
            collect_syscall_stats,
            &mut root_stats,
        );
        if fd < 0 {
            if collect_syscall_stats {
                flush_local_sys_stats(&root_stats);
            }
            eprintln!("cannot open root: {root_path}");
            let (_, rx) = unbounded::<ResultBatch>();
            return ScanHandle {
                receiver: rx,
                start_time,
            };
        }
        fd
    };

    // Worker count: cap to 8; avoid extra contention on APFS metadata
    let workers = resolve_scan_workers();

    let (work_tx, work_rx) = unbounded::<WorkMsg>(); // unbounded to prevent deadlock on deep trees
    let (result_tx, result_rx) = unbounded::<ResultBatch>();
    let active = Arc::new(AtomicUsize::new(1)); // root is in-flight
    let scan_options = Arc::new(options);

    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let rx = work_rx.clone();
        let tx = work_tx.clone();
        let rtx = result_tx.clone();
        let ac = active.clone();
        let fd_mgr = fd_budget.clone();
        let flt = filter.clone();
        let opts = scan_options.clone();
        handles.push(std::thread::spawn(move || unsafe {
            worker(
                rx,
                tx,
                rtx,
                ac,
                fd_mgr,
                workers,
                list_mode,
                max_depth,
                flt,
                opts,
                collect_syscall_stats,
            )
        }));
    }

    // Coordinator thread: waits for workers to finish then closes result channel
    std::thread::spawn(move || {
        for h in handles {
            let _ = h.join();
        }
        drop(result_tx); // Close the channel when all workers are done
    });

    // Kick off with root (depth 1)
    let root_item = if fd_budget.try_acquire() {
        WorkMsg::Dir(WorkItem {
            path: root_path,
            dirfd: Some(root_fd),
            depth: 1,
        })
    } else {
        let _ = unsafe { close(root_fd) };
        if collect_syscall_stats {
            root_stats.close_calls += 1;
        }
        WorkMsg::Dir(WorkItem {
            path: root_path,
            dirfd: None,
            depth: 1,
        })
    };
    if collect_syscall_stats {
        flush_local_sys_stats(&root_stats);
    }
    work_tx.send(root_item).unwrap();
    drop(work_tx); // allow workers to terminate when queue drains

    ScanHandle {
        receiver: result_rx,
        start_time,
    }
}
