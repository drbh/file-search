use crate::scan::{scan, FileFilter, ResultBatch, ScanOptions};
use crossbeam_channel as channel;
use std::fs;
use std::io;
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct ContentQueryStats {
    pub matches: usize,
    pub candidates: usize,
    pub duration: Duration,
}

struct ContentLiveWorkerOutput {
    matches: usize,
    candidates: usize,
}

#[inline]
fn resolve_content_workers(requested: usize) -> usize {
    let auto = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    let base = if requested == 0 { auto } else { requested };
    base.max(1)
}

#[inline]
fn is_binary(bytes: &[u8]) -> bool {
    bytes.contains(&0)
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

fn run_content_live_worker(
    work_rx: channel::Receiver<String>,
    needle: Arc<Vec<u8>>,
    include_binary: bool,
    max_file_size: u64,
    max_results: Option<usize>,
    global_matches: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    list_mode: bool,
) -> ContentLiveWorkerOutput {
    let mut matches = 0usize;
    let mut candidates = 0usize;

    for path in &work_rx {
        if stop.load(Ordering::Relaxed) {
            continue;
        }

        let metadata = match fs::metadata(&path) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if !metadata.is_file() {
            continue;
        }
        if metadata.len() > max_file_size {
            continue;
        }

        let bytes = match fs::read(&path) {
            Ok(b) => b,
            Err(_) => continue,
        };
        if !include_binary && is_binary(&bytes) {
            continue;
        }
        candidates += 1;

        if !contains_bytes(&bytes, needle.as_slice()) {
            continue;
        }

        if !try_take_match_slot(&global_matches, max_results) {
            stop.store(true, Ordering::Relaxed);
            continue;
        }

        if list_mode {
            println!("{path}");
        }
        matches += 1;

        if max_results.is_some_and(|limit| global_matches.load(Ordering::Relaxed) >= limit) {
            stop.store(true, Ordering::Relaxed);
        }
    }

    ContentLiveWorkerOutput {
        matches,
        candidates,
    }
}

pub fn query_content_live(
    root: &Path,
    max_depth: usize,
    scan_options: ScanOptions,
    needle: &str,
    list_mode: bool,
    max_results: Option<usize>,
    max_file_size: u64,
    include_binary: bool,
    requested_workers: usize,
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
    let scan_handle = scan(&root_str, true, max_depth, FileFilter::All, scan_options);
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
            )
        }));
    }
    drop(work_rx);

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
    drop(work_tx);
    let _scan_stats = scan_handle.wait_for_completion();

    let mut matches = 0usize;
    let mut candidates = 0usize;
    for handle in handles {
        let output = handle
            .join()
            .map_err(|_| io::Error::other("content live search worker thread panicked"))?;
        matches += output.matches;
        candidates += output.candidates;
    }

    Ok(ContentQueryStats {
        matches,
        candidates,
        duration: started.elapsed(),
    })
}
