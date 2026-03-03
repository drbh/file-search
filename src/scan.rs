use crossbeam_channel::unbounded;
use crossbeam_deque::{Injector, Steal, Stealer, Worker as DequeWorker};
use libc::dirent;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[link(name = "System")]
extern "C" {
    #[link_name = "openat$NOCANCEL"]
    fn c_openat_nocancel(dfd: i32, path: *const i8, oflag: i32, ...) -> i32;
    fn close(fd: i32) -> i32;
    fn getdtablesize() -> i32;
    #[link_name = "__getdirentries64"]
    fn c_getdirentries64(
        fd: i32,
        buf: *mut core::ffi::c_void,
        nbytes: usize,
        basep: *mut libc::off_t,
    ) -> isize;
    fn getrlimit(resource: i32, rlp: *mut rlimit) -> i32;
    fn setrlimit(resource: i32, rlp: *const rlimit) -> i32;
}

const O_RDONLY: i32 = 0x0000;
const O_DIRECTORY: i32 = 0x100000;
const O_CLOEXEC: i32 = 0x01000000;
const AT_FDCWD: i32 = -2;
const DEFAULT_FD_LIMIT: usize = 1024;
const FD_HEADROOM: usize = 64;
const FD_MIN_LIMIT: usize = 64;
const FD_MAX_LIMIT: usize = 262144;
const COUNT_RESULT_BATCH_SIZE: usize = 1 << 18;
const PATH_RESULT_BATCH_SIZE: usize = 4096;
const LOCAL_WORK_STACK_LIMIT_DEFAULT: usize = 32;
const DIRENT_BUF_SIZE: usize = 256 << 10;
const RLIMIT_NOFILE: i32 = 8;
const RLIM_INFINITY: u64 = !0;

#[repr(C)]
struct rlimit {
    rlim_cur: u64,
    rlim_max: u64,
}

const DT_UNKNOWN: u8 = 0;
const DT_DIR: u8 = 4;
const DT_REG: u8 = 8;
static OPENAT_CALLS: AtomicU64 = AtomicU64::new(0);
static OPENAT_ABS_CALLS: AtomicU64 = AtomicU64::new(0);
static OPENAT_REL_CALLS: AtomicU64 = AtomicU64::new(0);
static OPENAT_FAILS: AtomicU64 = AtomicU64::new(0);
static OPENAT_NANOS: AtomicU64 = AtomicU64::new(0);
static FSTATAT_CALLS: AtomicU64 = AtomicU64::new(0);
static FSTATAT_FAILS: AtomicU64 = AtomicU64::new(0);
static FSTATAT_NANOS: AtomicU64 = AtomicU64::new(0);
static CLOSE_CALLS: AtomicU64 = AtomicU64::new(0);
static FD_BUDGET_MISSES: AtomicU64 = AtomicU64::new(0);
static LOCAL_STACK_PUSHES: AtomicU64 = AtomicU64::new(0);
static GLOBAL_QUEUE_SPILLS: AtomicU64 = AtomicU64::new(0);
static CANCEL_SKIPPED_DIRS: AtomicU64 = AtomicU64::new(0);

#[derive(Default)]
struct LocalSysStats {
    fstatat_calls: u64,
    fstatat_fails: u64,
    fstatat_nanos: u64,
    openat_calls: u64,
    openat_abs_calls: u64,
    openat_rel_calls: u64,
    openat_fails: u64,
    openat_nanos: u64,
    close_calls: u64,
    fd_budget_misses: u64,
    local_stack_pushes: u64,
    global_queue_spills: u64,
    cancel_skipped_dirs: u64,
}

#[inline]
fn flush_local_sys_stats(stats: &LocalSysStats) {
    FSTATAT_CALLS.fetch_add(stats.fstatat_calls, Ordering::Relaxed);
    FSTATAT_FAILS.fetch_add(stats.fstatat_fails, Ordering::Relaxed);
    FSTATAT_NANOS.fetch_add(stats.fstatat_nanos, Ordering::Relaxed);
    OPENAT_CALLS.fetch_add(stats.openat_calls, Ordering::Relaxed);
    OPENAT_ABS_CALLS.fetch_add(stats.openat_abs_calls, Ordering::Relaxed);
    OPENAT_REL_CALLS.fetch_add(stats.openat_rel_calls, Ordering::Relaxed);
    OPENAT_FAILS.fetch_add(stats.openat_fails, Ordering::Relaxed);
    OPENAT_NANOS.fetch_add(stats.openat_nanos, Ordering::Relaxed);
    CLOSE_CALLS.fetch_add(stats.close_calls, Ordering::Relaxed);
    FD_BUDGET_MISSES.fetch_add(stats.fd_budget_misses, Ordering::Relaxed);
    LOCAL_STACK_PUSHES.fetch_add(stats.local_stack_pushes, Ordering::Relaxed);
    GLOBAL_QUEUE_SPILLS.fetch_add(stats.global_queue_spills, Ordering::Relaxed);
    CANCEL_SKIPPED_DIRS.fetch_add(stats.cancel_skipped_dirs, Ordering::Relaxed);
}

#[inline]
fn is_cancelled(cancel_flag: &Option<Arc<AtomicBool>>) -> bool {
    cancel_flag
        .as_ref()
        .is_some_and(|flag| flag.load(Ordering::Relaxed))
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
        let fd = c_openat_nocancel(dfd, path, oflag);
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
        c_openat_nocancel(dfd, path, oflag)
    }
}

#[inline]
unsafe fn timed_fstatat(
    dfd: i32,
    path: *const i8,
    stat_buf: *mut libc::stat,
    flags: i32,
    collect_syscall_stats: bool,
    stats: &mut LocalSysStats,
) -> i32 {
    if collect_syscall_stats {
        let start = Instant::now();
        let rc = libc::fstatat(dfd, path, stat_buf, flags);
        stats.fstatat_calls += 1;
        if rc < 0 {
            stats.fstatat_fails += 1;
        }
        stats.fstatat_nanos += start.elapsed().as_nanos() as u64;
        rc
    } else {
        libc::fstatat(dfd, path, stat_buf, flags)
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
fn mark_work_complete(active_count: &AtomicUsize) {
    active_count.fetch_sub(1, Ordering::AcqRel);
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

#[inline]
fn emit_match(
    list_mode: bool,
    path_buf: &mut PathScratch,
    name: &str,
    pending_paths: &mut Vec<String>,
    pending_count: &mut usize,
    result_tx: &crossbeam_channel::Sender<ResultBatch>,
) {
    if list_mode {
        let composed = path_buf.compose(name).to_owned();
        pending_paths.push(composed);
        if pending_paths.len() >= PATH_RESULT_BATCH_SIZE {
            let batch = std::mem::take(pending_paths);
            let _ = result_tx.send(ResultBatch::Paths(batch));
            *pending_paths = Vec::with_capacity(PATH_RESULT_BATCH_SIZE);
        }
    } else {
        *pending_count += 1;
        if *pending_count >= COUNT_RESULT_BATCH_SIZE {
            let _ = result_tx.send(ResultBatch::Count(*pending_count));
            *pending_count = 0;
        }
    }
}

#[inline]
unsafe fn schedule_child_directory(
    parent_fd: i32,
    name: &str,
    depth: usize,
    max_depth: usize,
    path_buf: &mut PathScratch,
    local_deque: &DequeWorker<WorkItem>,
    global_injector: &Injector<WorkItem>,
    dispatch_counter: &mut usize,
    local_stack_limit: usize,
    fd_budget: &FdBudget,
    name_cbuf: &mut Vec<u8>,
    active_count: &AtomicUsize,
    collect_syscall_stats: bool,
    sys_stats: &mut LocalSysStats,
) {
    let next_depth = depth + 1;
    if max_depth > 0 && next_depth > max_depth {
        return;
    }

    let mut next_fd: Option<RawFd> = None;
    if fd_budget.try_acquire() {
        name_cbuf.clear();
        name_cbuf.extend_from_slice(name.as_bytes());
        name_cbuf.push(0);
        let cfd = timed_openat(
            parent_fd,
            name_cbuf.as_ptr() as *const i8,
            O_RDONLY | O_DIRECTORY | O_CLOEXEC,
            false,
            collect_syscall_stats,
            sys_stats,
        );
        if cfd >= 0 {
            next_fd = Some(cfd);
        } else {
            fd_budget.release();
        }
    } else if collect_syscall_stats {
        sys_stats.fd_budget_misses += 1;
    }

    // Increment counter BEFORE queueing to prevent race with completion.
    active_count.fetch_add(1, Ordering::Relaxed);
    let child = WorkItem {
        path: path_buf.compose(name).to_owned(),
        dirfd: next_fd,
        depth: next_depth,
    };

    *dispatch_counter = dispatch_counter.wrapping_add(1);
    if *dispatch_counter % local_stack_limit == 0 {
        global_injector.push(child);
        if collect_syscall_stats {
            sys_stats.global_queue_spills += 1;
        }
    } else {
        local_deque.push(child);
        if collect_syscall_stats {
            sys_stats.local_stack_pushes += 1;
        }
    }
}

unsafe fn worker(
    worker_index: usize,
    local_deque: DequeWorker<WorkItem>,
    stealers: Arc<[Stealer<WorkItem>]>,
    global_injector: Arc<Injector<WorkItem>>,
    result_tx: crossbeam_channel::Sender<ResultBatch>,
    active_count: Arc<AtomicUsize>,
    fd_budget: Arc<FdBudget>,
    list_mode: bool,
    max_depth: usize,
    filter: FileFilter,
    local_stack_limit: usize,
    scan_options: Arc<ScanOptions>,
    cancel_flag: Option<Arc<AtomicBool>>,
    collect_syscall_stats: bool,
) {
    // Thread-local reusable buffers
    let mut dirent_buf = vec![0u8; DIRENT_BUF_SIZE];
    let mut sys_stats = LocalSysStats::default();
    let mut path_buf = PathScratch::new();
    let mut dir_cbuf: Vec<u8> = Vec::with_capacity(1024); // for openat (NUL-terminated)
    let mut name_cbuf: Vec<u8> = Vec::with_capacity(1024);
    let mut pending_count: usize = 0;
    let mut pending_paths: Vec<String> = Vec::with_capacity(1024);
    let mut dispatch_counter: usize = worker_index;
    let has_cancel = cancel_flag.is_some();
    let mut idle_polls: u32 = 0;

    loop {
        let work = loop {
            if let Some(local) = local_deque.pop() {
                idle_polls = 0;
                break Some(local);
            }
            match global_injector.steal_batch_and_pop(&local_deque) {
                Steal::Success(item) => {
                    idle_polls = 0;
                    break Some(item);
                }
                Steal::Retry => continue,
                Steal::Empty => {}
            }

            let mut stolen = None;
            for (idx, stealer) in stealers.iter().enumerate() {
                if idx == worker_index {
                    continue;
                }
                match stealer.steal_batch_and_pop(&local_deque) {
                    Steal::Success(item) => {
                        stolen = Some(item);
                        break;
                    }
                    Steal::Retry => continue,
                    Steal::Empty => {}
                }
            }
            if stolen.is_some() {
                idle_polls = 0;
                break stolen;
            }

            if active_count.load(Ordering::Acquire) == 0 {
                break None;
            }

            // Back off once workers are idle to reduce scheduler churn.
            idle_polls = idle_polls.saturating_add(1);
            if idle_polls <= 8 {
                std::hint::spin_loop();
            } else if idle_polls <= 64 {
                std::thread::yield_now();
            } else {
                std::thread::sleep(Duration::from_micros(50));
            }
        };
        let Some(work) = work else {
            break;
        };

        if has_cancel && is_cancelled(&cancel_flag) {
            if let Some(fd) = work.dirfd {
                let _ = close(fd);
                if collect_syscall_stats {
                    sys_stats.close_calls += 1;
                }
                fd_budget.release();
            }
            if collect_syscall_stats {
                sys_stats.cancel_skipped_dirs += 1;
            }
            mark_work_complete(&active_count);
            continue;
        }

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
                    mark_work_complete(&active_count);
                    continue;
                }
                fd
            }
        };
        path_buf.set_base(&path);
        let mut basep: libc::off_t = 0;
        loop {
            if has_cancel && is_cancelled(&cancel_flag) {
                break;
            }

            let nread = c_getdirentries64(
                dirfd,
                dirent_buf.as_mut_ptr().cast(),
                dirent_buf.len(),
                &mut basep,
            );
            if nread <= 0 {
                break;
            }

            let nread = nread as usize;
            let mut p = 0usize;
            while p < nread {
                let ent = &*(dirent_buf.as_ptr().add(p) as *const dirent);
                let reclen = ent.d_reclen as usize;
                if reclen == 0 || p + reclen > nread {
                    break;
                }

                let name_len = ent.d_namlen as usize;
                if name_len == 0 {
                    p += reclen;
                    continue;
                }
                let name_bytes =
                    std::slice::from_raw_parts(ent.d_name.as_ptr() as *const u8, name_len);
                if name_bytes == b"." || name_bytes == b".." {
                    p += reclen;
                    continue;
                }

                if ent.d_type == DT_DIR {
                    // SAFETY: APFS filenames are UTF-8; skip per-entry validation on hot path.
                    let name = std::str::from_utf8_unchecked(name_bytes);
                    if scan_options.should_skip_dir(&path, name) {
                        p += reclen;
                        continue;
                    }
                    schedule_child_directory(
                        dirfd,
                        name,
                        depth,
                        max_depth,
                        &mut path_buf,
                        &local_deque,
                        &global_injector,
                        &mut dispatch_counter,
                        local_stack_limit,
                        &fd_budget,
                        &mut name_cbuf,
                        &active_count,
                        collect_syscall_stats,
                        &mut sys_stats,
                    );
                } else if ent.d_type == DT_REG {
                    if filter.matches_bytes(name_bytes) {
                        // SAFETY: APFS filenames are UTF-8; skip per-entry validation on hot path.
                        let name = std::str::from_utf8_unchecked(name_bytes);
                        emit_match(
                            list_mode,
                            &mut path_buf,
                            name,
                            &mut pending_paths,
                            &mut pending_count,
                            &result_tx,
                        );
                    }
                } else if ent.d_type == DT_UNKNOWN {
                    // Unknown type: classify with fstatat to avoid open+close probing.
                    name_cbuf.clear();
                    name_cbuf.extend_from_slice(name_bytes);
                    name_cbuf.push(0);
                    let mut st: libc::stat = std::mem::zeroed();
                    let stat_rc = timed_fstatat(
                        dirfd,
                        name_cbuf.as_ptr() as *const i8,
                        &mut st,
                        libc::AT_SYMLINK_NOFOLLOW,
                        collect_syscall_stats,
                        &mut sys_stats,
                    );
                    if stat_rc == 0 {
                        let mode = st.st_mode as libc::mode_t;
                        let file_type = mode & (libc::S_IFMT as libc::mode_t);
                        if file_type == (libc::S_IFDIR as libc::mode_t) {
                            // SAFETY: APFS filenames are UTF-8; skip per-entry validation on hot path.
                            let name = std::str::from_utf8_unchecked(name_bytes);
                            if !scan_options.should_skip_dir(&path, name) {
                                schedule_child_directory(
                                    dirfd,
                                    name,
                                    depth,
                                    max_depth,
                                    &mut path_buf,
                                    &local_deque,
                                    &global_injector,
                                    &mut dispatch_counter,
                                    local_stack_limit,
                                    &fd_budget,
                                    &mut name_cbuf,
                                    &active_count,
                                    collect_syscall_stats,
                                    &mut sys_stats,
                                );
                            }
                        } else if file_type == (libc::S_IFREG as libc::mode_t)
                            && filter.matches_bytes(name_bytes)
                        {
                            // SAFETY: APFS filenames are UTF-8; skip per-entry validation on hot path.
                            let name = std::str::from_utf8_unchecked(name_bytes);
                            emit_match(
                                list_mode,
                                &mut path_buf,
                                name,
                                &mut pending_paths,
                                &mut pending_count,
                                &result_tx,
                            );
                        }
                    } else if filter.matches_bytes(name_bytes) {
                        // Preserve prior fallback behavior on stat errors.
                        let name = std::str::from_utf8_unchecked(name_bytes);
                        emit_match(
                            list_mode,
                            &mut path_buf,
                            name,
                            &mut pending_paths,
                            &mut pending_count,
                            &result_tx,
                        );
                    }
                }

                p += reclen;
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
        mark_work_complete(&active_count);
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

fn raise_nofile_soft_limit() {
    unsafe {
        let mut lim = rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if getrlimit(RLIMIT_NOFILE, &mut lim) != 0 {
            return;
        }
        let mut desired = lim.rlim_max;
        if desired == RLIM_INFINITY {
            desired = FD_MAX_LIMIT as u64;
        }
        desired = desired.min(FD_MAX_LIMIT as u64);
        if desired <= lim.rlim_cur {
            return;
        }
        let bumped = rlimit {
            rlim_cur: desired,
            rlim_max: lim.rlim_max,
        };
        let _ = setrlimit(RLIMIT_NOFILE, &bumped);
    }
}

fn detect_fd_limit() -> usize {
    let mut lim = rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    let raw = unsafe {
        if getrlimit(RLIMIT_NOFILE, &mut lim) == 0 {
            lim.rlim_cur as i32
        } else {
            getdtablesize()
        }
    };
    let base = if raw > 0 {
        raw as usize
    } else {
        DEFAULT_FD_LIMIT
    };
    base.saturating_sub(FD_HEADROOM)
        .clamp(FD_MIN_LIMIT, FD_MAX_LIMIT)
}

fn resolve_scan_workers(fd_limit: usize) -> usize {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    let core_limited = if cfg!(target_arch = "aarch64") {
        // Metadata-heavy directory scans tend to saturate before full core count.
        cores.min(6).max(2)
    } else {
        cores.min(8).max(2)
    };

    // If file descriptor headroom is tight, cap workers to reduce metadata contention.
    if fd_limit < 2048 {
        core_limited.min(2)
    } else if fd_limit < 8192 {
        core_limited.min(4)
    } else {
        core_limited
    }
}

/// Runtime statistics from scanning
#[derive(Debug)]
pub struct ScanStats {
    pub duration: Duration,
    pub fstatat_calls: u64,
    pub fstatat_fails: u64,
    pub fstatat_ms: f64,
    pub openat_calls: u64,
    pub openat_abs_calls: u64,
    pub openat_rel_calls: u64,
    pub openat_fails: u64,
    pub openat_ms: f64,
    pub close_calls: u64,
    pub fd_budget_misses: u64,
    pub local_stack_pushes: u64,
    pub global_queue_spills: u64,
    pub cancel_skipped_dirs: u64,
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
        let fstatat_nanos = FSTATAT_NANOS.load(Ordering::Relaxed);

        ScanStats {
            duration: self.start_time.elapsed(),
            fstatat_calls: FSTATAT_CALLS.load(Ordering::Relaxed),
            fstatat_fails: FSTATAT_FAILS.load(Ordering::Relaxed),
            fstatat_ms: fstatat_nanos as f64 / 1_000_000.0,
            openat_calls: OPENAT_CALLS.load(Ordering::Relaxed),
            openat_abs_calls: OPENAT_ABS_CALLS.load(Ordering::Relaxed),
            openat_rel_calls: OPENAT_REL_CALLS.load(Ordering::Relaxed),
            openat_fails: OPENAT_FAILS.load(Ordering::Relaxed),
            openat_ms: openat_nanos as f64 / 1_000_000.0,
            close_calls: CLOSE_CALLS.load(Ordering::Relaxed),
            fd_budget_misses: FD_BUDGET_MISSES.load(Ordering::Relaxed),
            local_stack_pushes: LOCAL_STACK_PUSHES.load(Ordering::Relaxed),
            global_queue_spills: GLOBAL_QUEUE_SPILLS.load(Ordering::Relaxed),
            cancel_skipped_dirs: CANCEL_SKIPPED_DIRS.load(Ordering::Relaxed),
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
    cancel_flag: Option<Arc<AtomicBool>>,
    collect_syscall_stats: bool,
) -> ScanHandle {
    let start_time = std::time::Instant::now();
    raise_nofile_soft_limit();

    // Reset global counters
    FSTATAT_CALLS.store(0, Ordering::Relaxed);
    FSTATAT_FAILS.store(0, Ordering::Relaxed);
    FSTATAT_NANOS.store(0, Ordering::Relaxed);
    OPENAT_CALLS.store(0, Ordering::Relaxed);
    OPENAT_ABS_CALLS.store(0, Ordering::Relaxed);
    OPENAT_REL_CALLS.store(0, Ordering::Relaxed);
    OPENAT_FAILS.store(0, Ordering::Relaxed);
    OPENAT_NANOS.store(0, Ordering::Relaxed);
    CLOSE_CALLS.store(0, Ordering::Relaxed);
    FD_BUDGET_MISSES.store(0, Ordering::Relaxed);
    LOCAL_STACK_PUSHES.store(0, Ordering::Relaxed);
    GLOBAL_QUEUE_SPILLS.store(0, Ordering::Relaxed);
    CANCEL_SKIPPED_DIRS.store(0, Ordering::Relaxed);

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

    // Worker count: cap to avoid excess metadata contention.
    let workers = resolve_scan_workers(fd_limit);
    let local_stack_limit = LOCAL_WORK_STACK_LIMIT_DEFAULT;

    let global_injector = Arc::new(Injector::<WorkItem>::new());
    let mut local_deques = Vec::with_capacity(workers);
    for _ in 0..workers {
        local_deques.push(DequeWorker::new_lifo());
    }
    let stealers = Arc::<[Stealer<WorkItem>]>::from(
        local_deques
            .iter()
            .map(|deque| deque.stealer())
            .collect::<Vec<_>>(),
    );

    // Kick off with root (depth 1)
    let root_item = if fd_budget.try_acquire() {
        WorkItem {
            path: root_path,
            dirfd: Some(root_fd),
            depth: 1,
        }
    } else {
        let _ = unsafe { close(root_fd) };
        if collect_syscall_stats {
            root_stats.close_calls += 1;
        }
        WorkItem {
            path: root_path,
            dirfd: None,
            depth: 1,
        }
    };
    if collect_syscall_stats {
        flush_local_sys_stats(&root_stats);
    }
    global_injector.push(root_item);

    let (result_tx, result_rx) = unbounded::<ResultBatch>();
    let active = Arc::new(AtomicUsize::new(1)); // root is in-flight
    let scan_options = Arc::new(options);

    let mut handles = Vec::with_capacity(workers);
    for (worker_index, local_deque) in local_deques.into_iter().enumerate() {
        let rtx = result_tx.clone();
        let ac = active.clone();
        let deques = stealers.clone();
        let injector = global_injector.clone();
        let fd_mgr = fd_budget.clone();
        let flt = filter.clone();
        let stack_limit = local_stack_limit;
        let opts = scan_options.clone();
        let cancel = cancel_flag.clone();
        handles.push(std::thread::spawn(move || unsafe {
            worker(
                worker_index,
                local_deque,
                deques,
                injector,
                rtx,
                ac,
                fd_mgr,
                list_mode,
                max_depth,
                flt,
                stack_limit,
                opts,
                cancel,
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

    ScanHandle {
        receiver: result_rx,
        start_time,
    }
}
