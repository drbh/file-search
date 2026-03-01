use crate::scan::{scan, FileFilter, ResultBatch, ScanOptions};
use crossbeam_channel as channel;
use std::collections::HashSet;
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

const CONTENT_READ_CHUNK_SIZE: usize = 256 * 1024;

#[derive(Debug)]
pub struct ContentQueryStats {
    pub matches: usize,
    pub candidates: usize,
    pub duration: Duration,
    pub scan_duration: Duration,
    pub dispatch_duration: Duration,
    pub worker_join_duration: Duration,
    pub files_seen: usize,
    pub files_opened: usize,
    pub open_errors: usize,
    pub read_errors: usize,
    pub skipped_too_large: usize,
    pub skipped_binary: usize,
    pub bytes_read: u64,
    pub read_calls: u64,
}

struct ContentLiveWorkerOutput {
    matches: usize,
    candidates: usize,
    listed_paths: Vec<String>,
    files_seen: usize,
    files_opened: usize,
    open_errors: usize,
    read_errors: usize,
    skipped_too_large: usize,
    skipped_binary: usize,
    bytes_read: u64,
    read_calls: u64,
}

enum FileSearchStatus {
    Matched,
    NoMatch,
    TooLarge,
    Binary,
}

struct FileSearchOutcome {
    status: FileSearchStatus,
    bytes_read: u64,
    read_calls: u64,
}

#[inline]
fn resolve_content_workers(requested: usize) -> usize {
    if requested > 0 {
        return requested.max(1);
    }

    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);

    if cfg!(target_arch = "aarch64") {
        // On Apple Silicon cold scans, too many content workers contend with directory scanning.
        // Keep auto workers intentionally low; users can still override with -w.
        cores.min(3).max(2)
    } else {
        cores.min(4).max(1)
    }
}

#[inline]
fn try_take_match_slot(counter: &AtomicUsize, max_results: Option<usize>) -> bool {
    if let Some(limit) = max_results {
        let mut cur = counter.load(Ordering::Relaxed);
        loop {
            if cur >= limit {
                return false;
            }
            match counter.compare_exchange_weak(cur, cur + 1, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return true,
                Err(next) => cur = next,
            }
        }
    } else {
        counter.fetch_add(1, Ordering::Relaxed);
        true
    }
}

#[inline]
fn clamp_list_path_segments(path: &str, root: &Path, segments: usize) -> String {
    let Ok(relative) = Path::new(path).strip_prefix(root) else {
        return path.to_string();
    };
    let mut out = root.to_path_buf();
    for comp in relative.components().take(segments) {
        out.push(comp.as_os_str());
    }
    out.to_string_lossy().to_string()
}

fn search_file_for_needle(
    file: &mut File,
    needle_finder: &memchr::memmem::Finder<'_>,
    needle_len: usize,
    include_binary: bool,
    max_file_size: u64,
    read_buf: &mut [u8],
    overlap_buf: &mut Vec<u8>,
    boundary_buf: &mut Vec<u8>,
) -> io::Result<FileSearchOutcome> {
    let overlap_len = needle_len.saturating_sub(1);
    overlap_buf.clear();
    boundary_buf.clear();

    let mut bytes_read = 0u64;
    let mut read_calls = 0u64;

    loop {
        let n = file.read(read_buf)?;
        if n == 0 {
            return Ok(FileSearchOutcome {
                status: FileSearchStatus::NoMatch,
                bytes_read,
                read_calls,
            });
        }

        read_calls += 1;
        bytes_read += n as u64;
        if bytes_read > max_file_size {
            return Ok(FileSearchOutcome {
                status: FileSearchStatus::TooLarge,
                bytes_read,
                read_calls,
            });
        }

        let chunk = &read_buf[..n];

        if !include_binary && chunk.contains(&0) {
            return Ok(FileSearchOutcome {
                status: FileSearchStatus::Binary,
                bytes_read,
                read_calls,
            });
        }

        if needle_finder.find(chunk).is_some() {
            return Ok(FileSearchOutcome {
                status: FileSearchStatus::Matched,
                bytes_read,
                read_calls,
            });
        }

        if overlap_len > 0 && !overlap_buf.is_empty() {
            let head_len = overlap_len.min(chunk.len());
            boundary_buf.clear();
            boundary_buf.extend_from_slice(overlap_buf.as_slice());
            boundary_buf.extend_from_slice(&chunk[..head_len]);
            if needle_finder.find(boundary_buf.as_slice()).is_some() {
                return Ok(FileSearchOutcome {
                    status: FileSearchStatus::Matched,
                    bytes_read,
                    read_calls,
                });
            }
        }

        if overlap_len > 0 {
            overlap_buf.clear();
            let tail_len = overlap_len.min(chunk.len());
            overlap_buf.extend_from_slice(&chunk[n - tail_len..]);
        }
    }
}

fn run_content_live_worker(
    work_rx: channel::Receiver<String>,
    needle: Arc<Vec<u8>>,
    include_binary: bool,
    max_file_size: u64,
    max_results: Option<usize>,
    global_matches: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    list_mode: bool,
    segments: Option<usize>,
) -> ContentLiveWorkerOutput {
    let mut matches = 0usize;
    let mut candidates = 0usize;
    let mut listed_paths: Vec<String> = Vec::new();

    let mut files_seen = 0usize;
    let mut files_opened = 0usize;
    let mut open_errors = 0usize;
    let mut read_errors = 0usize;
    let mut skipped_too_large = 0usize;
    let mut skipped_binary = 0usize;
    let mut bytes_read = 0u64;
    let mut read_calls = 0u64;

    let finder = memchr::memmem::Finder::new(needle.as_slice());
    let needle_len = needle.len();
    let overlap_cap = needle_len.saturating_sub(1);
    let mut read_buf = vec![0u8; CONTENT_READ_CHUNK_SIZE];
    let mut overlap_buf: Vec<u8> = Vec::with_capacity(overlap_cap);
    let mut boundary_buf: Vec<u8> = Vec::with_capacity(overlap_cap.saturating_mul(2));

    for path in &work_rx {
        if stop.load(Ordering::Relaxed) {
            continue;
        }

        files_seen += 1;
        let mut file = match File::open(&path) {
            Ok(file) => {
                files_opened += 1;
                file
            }
            Err(_) => {
                open_errors += 1;
                continue;
            }
        };

        let outcome = match search_file_for_needle(
            &mut file,
            &finder,
            needle_len,
            include_binary,
            max_file_size,
            &mut read_buf,
            &mut overlap_buf,
            &mut boundary_buf,
        ) {
            Ok(outcome) => outcome,
            Err(_) => {
                read_errors += 1;
                continue;
            }
        };

        bytes_read += outcome.bytes_read;
        read_calls += outcome.read_calls;

        match outcome.status {
            FileSearchStatus::TooLarge => {
                skipped_too_large += 1;
                continue;
            }
            FileSearchStatus::Binary => {
                skipped_binary += 1;
                continue;
            }
            FileSearchStatus::NoMatch => {
                candidates += 1;
                continue;
            }
            FileSearchStatus::Matched => {
                candidates += 1;
            }
        }

        if !try_take_match_slot(&global_matches, max_results) {
            stop.store(true, Ordering::Relaxed);
            continue;
        }

        if list_mode {
            if segments.is_none() {
                println!("{path}");
            } else {
                listed_paths.push(path);
            }
        }
        matches += 1;

        if max_results.is_some_and(|limit| global_matches.load(Ordering::Relaxed) >= limit) {
            stop.store(true, Ordering::Relaxed);
        }
    }

    ContentLiveWorkerOutput {
        matches,
        candidates,
        listed_paths,
        files_seen,
        files_opened,
        open_errors,
        read_errors,
        skipped_too_large,
        skipped_binary,
        bytes_read,
        read_calls,
    }
}

pub fn query_content_live(
    root: &Path,
    max_depth: usize,
    scan_options: ScanOptions,
    file_filter: FileFilter,
    needle: &str,
    list_mode: bool,
    segments: Option<usize>,
    max_results: Option<usize>,
    max_file_size: u64,
    include_binary: bool,
    requested_workers: usize,
    collect_scan_stats: bool,
) -> io::Result<ContentQueryStats> {
    if needle.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "content query cannot be empty",
        ));
    }

    let started = Instant::now();
    let workers = resolve_content_workers(requested_workers);
    let root_str = root.to_string_lossy().to_string();
    let scan_handle = scan(
        &root_str,
        true,
        max_depth,
        file_filter,
        scan_options,
        collect_scan_stats,
    );
    let (work_tx, work_rx) = channel::bounded::<String>(workers.saturating_mul(1024).max(1024));

    let needle = Arc::new(needle.as_bytes().to_vec());
    let global_matches = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let mut handles = Vec::with_capacity(workers);
    for _ in 0..workers {
        let rx = work_rx.clone();
        let n = Arc::clone(&needle);
        let gm = Arc::clone(&global_matches);
        let stop_flag = Arc::clone(&stop);
        handles.push(std::thread::spawn(move || {
            run_content_live_worker(
                rx,
                n,
                include_binary,
                max_file_size,
                max_results,
                gm,
                stop_flag,
                list_mode,
                segments,
            )
        }));
    }
    drop(work_rx);

    let dispatch_started = Instant::now();
    let mut send_failed = false;
    for batch in &scan_handle.receiver {
        if send_failed || stop.load(Ordering::Relaxed) {
            continue;
        }
        if let ResultBatch::Paths(paths_batch) = batch {
            for path in paths_batch {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                if work_tx.send(path).is_err() {
                    send_failed = true;
                    break;
                }
            }
        }
    }
    let dispatch_duration = dispatch_started.elapsed();
    drop(work_tx);
    let scan_stats = scan_handle.wait_for_completion();

    let worker_join_started = Instant::now();
    let mut matches = 0usize;
    let mut candidates = 0usize;
    let mut files_seen = 0usize;
    let mut files_opened = 0usize;
    let mut open_errors = 0usize;
    let mut read_errors = 0usize;
    let mut skipped_too_large = 0usize;
    let mut skipped_binary = 0usize;
    let mut bytes_read = 0u64;
    let mut read_calls = 0u64;
    let mut clamped_set: HashSet<String> = HashSet::new();
    for handle in handles {
        let mut output = handle
            .join()
            .map_err(|_| io::Error::other("content live search worker thread panicked"))?;
        matches += output.matches;
        candidates += output.candidates;
        files_seen += output.files_seen;
        files_opened += output.files_opened;
        open_errors += output.open_errors;
        read_errors += output.read_errors;
        skipped_too_large += output.skipped_too_large;
        skipped_binary += output.skipped_binary;
        bytes_read += output.bytes_read;
        read_calls += output.read_calls;

        if list_mode {
            let Some(segment_count) = segments else {
                continue;
            };
            for path in output.listed_paths.drain(..) {
                clamped_set.insert(clamp_list_path_segments(&path, root, segment_count));
            }
        }
    }
    let worker_join_duration = worker_join_started.elapsed();

    if list_mode {
        let Some(_) = segments else {
            return Ok(ContentQueryStats {
                matches,
                candidates,
                duration: started.elapsed(),
                scan_duration: scan_stats.duration,
                dispatch_duration,
                worker_join_duration,
                files_seen,
                files_opened,
                open_errors,
                read_errors,
                skipped_too_large,
                skipped_binary,
                bytes_read,
                read_calls,
            });
        };

        let mut paths: Vec<String> = clamped_set.into_iter().collect();
        paths.sort_unstable();
        for path in paths {
            println!("{path}");
        }
    }

    Ok(ContentQueryStats {
        matches,
        candidates,
        duration: started.elapsed(),
        scan_duration: scan_stats.duration,
        dispatch_duration,
        worker_join_duration,
        files_seen,
        files_opened,
        open_errors,
        read_errors,
        skipped_too_large,
        skipped_binary,
        bytes_read,
        read_calls,
    })
}
