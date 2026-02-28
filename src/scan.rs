use crossbeam_channel::{unbounded, RecvTimeoutError};
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
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
const FD_MAX_LIMIT: usize = 4096;

// Darwin vtype constants
const VREG: u32 = 1;
const VDIR: u32 = 2;

static GETATTR_CALLS: AtomicU64 = AtomicU64::new(0);
static GETATTR_ENTRIES: AtomicU64 = AtomicU64::new(0);
static OPENAT_CALLS: AtomicU64 = AtomicU64::new(0);
static OPENAT_NANOS: AtomicU64 = AtomicU64::new(0);
static GETATTR_NANOS: AtomicU64 = AtomicU64::new(0);
static CLOSE_CALLS: AtomicU64 = AtomicU64::new(0);

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
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(next) => current = next,
            }
        }
    }

    fn release(&self) {
        self.in_use.fetch_sub(1, Ordering::SeqCst);
    }
}

/// File filter configuration
#[derive(Clone)]
pub enum FileFilter {
    /// Match all regular files
    All,
    /// Match files with specific extensions (case-insensitive)
    Extensions(Vec<String>),
    /// Match files containing a substring (case-insensitive)
    Contains(String),
    /// Match files with a prefix
    Prefix(String),
    /// Match files with a suffix (before extension)
    Suffix(String),
}

impl FileFilter {
    #[inline]
    fn matches(&self, name: &str) -> bool {
        match self {
            FileFilter::All => true,
            FileFilter::Extensions(exts) => {
                let bytes = name.as_bytes();
                let Some(dot) = bytes.iter().rposition(|&b| b == b'.') else {
                    return false;
                };
                let ext = &bytes[dot + 1..];
                if ext.is_empty() {
                    return false;
                }
                // Convert to lowercase for comparison
                let ext_lower: String = ext.iter().map(|&b| b.to_ascii_lowercase() as char).collect();
                exts.iter().any(|e| e == &ext_lower)
            }
            FileFilter::Contains(pattern) => {
                name.to_ascii_lowercase().contains(&pattern.to_ascii_lowercase())
            }
            FileFilter::Prefix(prefix) => {
                name.to_ascii_lowercase().starts_with(&prefix.to_ascii_lowercase())
            }
            FileFilter::Suffix(suffix) => {
                // Match suffix before extension
                let base = if let Some(dot) = name.rfind('.') {
                    &name[..dot]
                } else {
                    name
                };
                base.to_ascii_lowercase().ends_with(&suffix.to_ascii_lowercase())
            }
        }
    }
}

#[inline]
fn should_skip_dir(_parent_path: &str, _name: &str) -> bool {
    // matches!(name, ".git" | ".hg" | ".svn" | "node_modules")
    false
}

unsafe fn worker(
    work_rx: crossbeam_channel::Receiver<WorkItem>,
    work_tx: crossbeam_channel::Sender<WorkItem>,
    result_tx: crossbeam_channel::Sender<ResultBatch>,
    active_count: Arc<AtomicUsize>,
    fd_budget: Arc<FdBudget>,
    list_mode: bool,
    max_depth: usize,
    filter: FileFilter,
) {
    // Thread-local reusable buffers
    let mut buf = vec![0u8; INITIAL_BULK_BUF_SIZE];

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
        // Wait briefly for work; exit when everything drained
        let work = match work_rx.recv_timeout(Duration::from_millis(5)) {
            Ok(w) => w,
            Err(RecvTimeoutError::Timeout) => {
                if active_count.load(Ordering::SeqCst) == 0 {
                    break;
                } else {
                    continue;
                }
            }
            Err(RecvTimeoutError::Disconnected) => break,
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
                let start = std::time::Instant::now();
                let fd = openat(
                    AT_FDCWD,
                    dir_cbuf.as_ptr() as *const i8,
                    O_RDONLY | O_DIRECTORY | O_CLOEXEC,
                );
                OPENAT_CALLS.fetch_add(1, Ordering::Relaxed);
                OPENAT_NANOS.fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                if fd < 0 {
                    active_count.fetch_sub(1, Ordering::SeqCst);
                    continue;
                }
                fd
            }
        };
        path_buf.set_base(&path);

        // Enable directory read-ahead for better performance
        let _ = fcntl(dirfd, F_RDAHEAD, 1);

        loop {
            let start = std::time::Instant::now();
            let n = getattrlistbulk(dirfd, &al, buf.as_mut_ptr().cast(), buf.len(), 0);
            if n > 0 {
                GETATTR_CALLS.fetch_add(1, Ordering::Relaxed);
                GETATTR_ENTRIES.fetch_add(n as u64, Ordering::Relaxed);
                GETATTR_NANOS.fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }
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
                let name_start = (name_ref_ptr as usize - buf.as_ptr() as usize) + (name_ref.off as usize);
                let name_len = name_ref.len as usize;

                // Trim a trailing NUL if present; avoid CStr and extra validation work.
                let name_bytes = &buf[name_start..name_start + name_len];
                let name_bytes = if !name_bytes.is_empty() && *name_bytes.last().unwrap() == 0 {
                    &name_bytes[..name_len - 1]
                } else {
                    name_bytes
                };

                // SAFETY: APFS filenames are UTF-8; skip per-entry validation on hot path.
                let name = std::str::from_utf8_unchecked(name_bytes);

                if name == "." || name == ".." {
                    p += reclen;
                    continue;
                }

                if objtype == VDIR {
                    if should_skip_dir(&path, name) {
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
                        let start = std::time::Instant::now();
                        let cfd = openat(
                            dirfd,
                            name_cbuf.as_ptr() as *const i8,
                            O_RDONLY | O_DIRECTORY | O_CLOEXEC,
                        );
                        OPENAT_CALLS.fetch_add(1, Ordering::Relaxed);
                        OPENAT_NANOS.fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        if cfd >= 0 {
                            next_fd = Some(cfd);
                        } else {
                            fd_budget.release();
                        }
                    }
                    // If we can't acquire fd budget, next_fd stays None and we'll open by path later

                    // Increment counter BEFORE sending to queue to prevent race
                    active_count.fetch_add(1, Ordering::SeqCst);
                    let composed = path_buf.compose(name).to_owned();
                    if work_tx
                        .send(WorkItem {
                            path: composed,
                            dirfd: next_fd,
                            depth: next_depth,
                        })
                        .is_err()
                    {
                        if let Some(fd) = next_fd {
                            let _ = close(fd);
                            fd_budget.release();
                        }
                        active_count.fetch_sub(1, Ordering::SeqCst);
                    }
                } else if objtype == VREG && filter.matches(name) {
                    if list_mode {
                        let composed = path_buf.compose(name).to_owned();
                        pending_paths.push(composed);
                        if pending_paths.len() >= 2048 {
                            let batch = std::mem::take(&mut pending_paths);
                            let _ = result_tx.send(ResultBatch::Paths(batch));
                            pending_paths = Vec::with_capacity(2048);
                        }
                    } else {
                        pending_count += 1;
                        if pending_count >= 1 << 14 {
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
        CLOSE_CALLS.fetch_add(1, Ordering::Relaxed);
        if held_fd {
            fd_budget.release();
        }

        // Decrement counter for this directory finishing
        // (subdirs were already incremented when queued)
        active_count.fetch_sub(1, Ordering::SeqCst);
    }

    if list_mode {
        if !pending_paths.is_empty() {
            let batch = std::mem::take(&mut pending_paths);
            let _ = result_tx.send(ResultBatch::Paths(batch));
        }
    } else if pending_count != 0 {
        let _ = result_tx.send(ResultBatch::Count(pending_count));
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

/// Runtime statistics from scanning
#[derive(Debug)]
pub struct ScanStats {
    pub duration: Duration,
    pub getattr_calls: u64,
    pub getattr_entries: u64,
    pub openat_calls: u64,
    pub openat_ms: f64,
    pub getattr_ms: f64,
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

        ScanStats {
            duration: self.start_time.elapsed(),
            getattr_calls: GETATTR_CALLS.load(Ordering::Relaxed),
            getattr_entries: GETATTR_ENTRIES.load(Ordering::Relaxed),
            openat_calls: OPENAT_CALLS.load(Ordering::Relaxed),
            openat_ms: openat_nanos as f64 / 1_000_000.0,
            getattr_ms: getattr_nanos as f64 / 1_000_000.0,
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
pub fn scan(root_path: &str, list_mode: bool, max_depth: usize, filter: FileFilter) -> ScanHandle {
    let start_time = std::time::Instant::now();

    // Reset global counters
    GETATTR_CALLS.store(0, Ordering::Relaxed);
    GETATTR_ENTRIES.store(0, Ordering::Relaxed);
    OPENAT_CALLS.store(0, Ordering::Relaxed);
    OPENAT_NANOS.store(0, Ordering::Relaxed);
    GETATTR_NANOS.store(0, Ordering::Relaxed);
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
    let root_fd = unsafe {
        let fd = openat(
            AT_FDCWD,
            root_c.as_ptr() as *const i8,
            O_RDONLY | O_DIRECTORY | O_CLOEXEC,
        );
        if fd < 0 {
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
    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .min(8)
        .max(2);

    let (work_tx, work_rx) = unbounded::<WorkItem>(); // unbounded to prevent deadlock on deep trees
    let (result_tx, result_rx) = unbounded::<ResultBatch>();
    let active = Arc::new(AtomicUsize::new(1)); // root is in-flight

    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let rx = work_rx.clone();
        let tx = work_tx.clone();
        let rtx = result_tx.clone();
        let ac = active.clone();
        let fd_mgr = fd_budget.clone();
        let flt = filter.clone();
        handles.push(std::thread::spawn(move || unsafe {
            worker(rx, tx, rtx, ac, fd_mgr, list_mode, max_depth, flt)
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
        WorkItem {
            path: root_path,
            dirfd: Some(root_fd),
            depth: 1,
        }
    } else {
        let _ = unsafe { close(root_fd) };
        WorkItem {
            path: root_path,
            dirfd: None,
            depth: 1,
        }
    };
    work_tx.send(root_item).unwrap();
    drop(work_tx); // allow workers to terminate when queue drains

    ScanHandle {
        receiver: result_rx,
        start_time,
    }
}
