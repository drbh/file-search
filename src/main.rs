mod content;
mod index;
mod scan;

use clap::Parser;
use content::{build_content_index, content_index_status, query_content_index, query_content_live};
use index::{build_index, compact_index, index_status, query_index, watch_index};
use scan::{scan, FileFilter, ResultBatch, ScanOptions};
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(name = "file-search")]
#[command(about = "Fast file search using macOS getattrlistbulk", long_about = None)]
struct Cli {
    /// Directory to search (defaults to current directory)
    #[arg(default_value = ".")]
    path: String,

    /// List file paths instead of just counting
    #[arg(short, long)]
    list: bool,

    /// Maximum depth to traverse (0 = unlimited)
    #[arg(short, long, default_value = "0")]
    depth: usize,

    /// File extensions to match (comma-separated, e.g., "rs,toml,md")
    #[arg(short, long, value_delimiter = ',')]
    ext: Option<Vec<String>>,

    /// Match files containing this string
    #[arg(short = 'c', long)]
    contains: Option<String>,

    /// Match files starting with this prefix
    #[arg(short = 'p', long)]
    prefix: Option<String>,

    /// Match files ending with this suffix (before extension)
    #[arg(short = 'S', long)]
    suffix: Option<String>,

    /// Show statistics after scan
    #[arg(short, long)]
    stats: bool,

    /// Include hidden directories (names starting with '.')
    #[arg(long)]
    hidden: bool,

    /// Disable default directory pruning (.git, node_modules, target, etc.)
    #[arg(long)]
    no_default_prunes: bool,

    /// Additional directory names to exclude (comma-separated)
    #[arg(long, value_delimiter = ',')]
    exclude_dir: Option<Vec<String>>,

    /// Path to ignore config file (defaults to <scan-root>/.file-search-ignore)
    #[arg(long)]
    ignore_config: Option<String>,

    /// Query using on-disk index instead of live filesystem walk
    #[arg(long)]
    use_index: bool,

    /// Build or rebuild index from a full scan
    #[arg(long)]
    index_build: bool,

    /// Compact snapshot + delta log into a new snapshot
    #[arg(long)]
    index_compact: bool,

    /// Show index status
    #[arg(long)]
    index_status: bool,

    /// Watch filesystem changes and append to index delta log
    #[arg(long)]
    index_watch: bool,

    /// Disable automatic compaction of large delta logs during indexed query
    #[arg(long)]
    no_index_auto_compact: bool,

    /// Stop after returning this many matches
    #[arg(long)]
    max_results: Option<usize>,

    /// Build or rebuild content trigram index
    #[arg(long)]
    content_index_build: bool,

    /// Search literal text within files using content index
    #[arg(long)]
    content_search: Option<String>,

    /// Search literal text via live filesystem scan (no persistent index)
    #[arg(long)]
    content_search_live: Option<String>,

    /// Show content index status
    #[arg(long)]
    content_index_status: bool,

    /// Max file size for content indexing (bytes)
    #[arg(long, default_value_t = 8 * 1024 * 1024)]
    content_max_file_size: u64,

    /// Include binary files in content index/search
    #[arg(long)]
    content_include_binary: bool,

    /// Number of worker threads for content indexing (0 = auto)
    #[arg(long, default_value_t = 0)]
    content_workers: usize,

    /// Quiet mode - only output file paths or count
    #[arg(short, long)]
    quiet: bool,
}

#[derive(Default)]
struct IgnoreRules {
    dir_names: Vec<String>,
    path_prefixes: Vec<String>,
}

fn normalize_prefix(raw: &str, scan_root: &Path) -> Option<String> {
    if raw.is_empty() {
        return None;
    }

    let path = if raw.starts_with("~/") {
        expand_tilde(raw)
    } else if raw.starts_with('/') {
        PathBuf::from(raw)
    } else {
        scan_root.join(raw)
    };

    let mut normalized = path.to_string_lossy().to_string();
    while normalized.ends_with('/') && normalized.len() > 1 {
        normalized.pop();
    }

    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn load_ignore_rules(config_path: &Path, scan_root: &Path) -> io::Result<IgnoreRules> {
    let file = File::open(config_path)?;
    let reader = BufReader::new(file);
    let mut rules = IgnoreRules::default();

    for line in reader.lines() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if let Some(rest) = line.strip_prefix("dir:") {
            let name = rest.trim();
            if !name.is_empty() {
                rules.dir_names.push(name.to_string());
            }
            continue;
        }

        if let Some(rest) = line.strip_prefix("path:") {
            if let Some(prefix) = normalize_prefix(rest.trim(), scan_root) {
                rules.path_prefixes.push(prefix);
            }
            continue;
        }

        if line.contains('/') {
            if let Some(prefix) = normalize_prefix(line, scan_root) {
                rules.path_prefixes.push(prefix);
            }
        } else {
            rules.dir_names.push(line.to_string());
        }
    }

    Ok(rules)
}

fn expand_tilde(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    PathBuf::from(path)
}

fn run() -> io::Result<()> {
    let cli = Cli::parse();

    // Build filter from CLI options
    let filter = if let Some(exts) = &cli.ext {
        let normalized: Vec<Box<[u8]>> = exts
            .iter()
            .map(|e| {
                e.trim_start_matches('.')
                    .to_ascii_lowercase()
                    .into_bytes()
                    .into_boxed_slice()
            })
            .filter(|e| !e.is_empty())
            .collect();
        FileFilter::Extensions(normalized)
    } else if let Some(pattern) = &cli.contains {
        FileFilter::Contains(pattern.to_ascii_lowercase().into_bytes().into_boxed_slice())
    } else if let Some(prefix) = &cli.prefix {
        FileFilter::Prefix(prefix.to_ascii_lowercase().into_bytes().into_boxed_slice())
    } else if let Some(suffix) = &cli.suffix {
        FileFilter::Suffix(suffix.to_ascii_lowercase().into_bytes().into_boxed_slice())
    } else {
        FileFilter::All
    };

    // Resolve to absolute path
    let path =
        std::fs::canonicalize(&cli.path).unwrap_or_else(|_| std::path::PathBuf::from(&cli.path));
    let path_str = path.to_string_lossy().to_string();

    let mut ignore_dir_names = cli.exclude_dir.clone().unwrap_or_default();
    let mut ignore_path_prefixes = Vec::new();
    let ignore_config_path = cli
        .ignore_config
        .as_deref()
        .map(expand_tilde)
        .unwrap_or_else(|| path.join(".file-search-ignore"));
    let explicit_ignore_config = cli.ignore_config.is_some();

    if ignore_config_path.exists() {
        match load_ignore_rules(&ignore_config_path, &path) {
            Ok(mut rules) => {
                ignore_dir_names.append(&mut rules.dir_names);
                ignore_path_prefixes.append(&mut rules.path_prefixes);
            }
            Err(err) => eprintln!(
                "warning: failed to load ignore config {}: {}",
                ignore_config_path.display(),
                err
            ),
        }
    } else if explicit_ignore_config {
        eprintln!(
            "warning: ignore config not found: {}",
            ignore_config_path.display()
        );
    }

    ignore_dir_names.sort_unstable();
    ignore_dir_names.dedup();
    ignore_path_prefixes.sort_unstable();
    ignore_path_prefixes.dedup();

    let scan_options = ScanOptions {
        include_hidden_dirs: cli.hidden,
        prune_defaults: !cli.no_default_prunes,
        ignore_dir_names,
        ignore_path_prefixes,
    };

    let mode_count = usize::from(cli.use_index)
        + usize::from(cli.index_build)
        + usize::from(cli.index_compact)
        + usize::from(cli.index_status)
        + usize::from(cli.index_watch)
        + usize::from(cli.content_index_build)
        + usize::from(cli.content_index_status)
        + usize::from(cli.content_search.is_some())
        + usize::from(cli.content_search_live.is_some());
    if mode_count > 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "choose at most one index mode flag",
        ));
    }

    if cli.index_build {
        let build = build_index(&path, cli.depth, scan_options)?;
        if !cli.quiet {
            eprintln!("Indexed root: {}", path_str);
            eprintln!("files indexed: {}", build.files);
            eprintln!("scan duration: {:.2?}", build.scan_duration);
            eprintln!("total duration: {:.2?}", build.total_duration);
            eprintln!("snapshot bytes: {}", build.snapshot_bytes);
            eprintln!("index dir: {}", build.index_dir.display());
        }
        return Ok(());
    }

    if cli.index_compact {
        let compact = compact_index(&path)?;
        if !cli.quiet {
            eprintln!("Compacted index for {}", path_str);
            eprintln!("files: {}", compact.files);
            eprintln!("duration: {:.2?}", compact.duration);
            eprintln!("snapshot bytes: {}", compact.snapshot_bytes);
        }
        return Ok(());
    }

    if cli.index_status {
        let status = index_status(&path)?;
        eprintln!("root: {}", status.root);
        eprintln!("index dir: {}", status.index_dir.display());
        eprintln!("snapshot exists: {}", status.snapshot_exists);
        eprintln!("snapshot files: {}", status.snapshot_files);
        eprintln!("snapshot size: {}", status.snapshot_size_bytes);
        eprintln!("snapshot created unix: {}", status.snapshot_created_unix);
        eprintln!("delta exists: {}", status.delta_exists);
        eprintln!("delta size: {}", status.delta_size_bytes);
        eprintln!("delta ops: {}", status.delta_ops);
        return Ok(());
    }

    if cli.index_watch {
        if !cli.quiet {
            eprintln!("Starting index watcher for {}", path_str);
        }
        watch_index(&path)?;
        return Ok(());
    }

    if cli.content_index_build {
        let build = build_content_index(
            &path,
            cli.depth,
            scan_options,
            cli.content_max_file_size,
            cli.content_include_binary,
            cli.content_workers,
        )?;
        if !cli.quiet {
            eprintln!("Built content index for {}", path_str);
            eprintln!("workers: {}", build.workers);
            eprintln!("files scanned: {}", build.files_scanned);
            eprintln!("files indexed: {}", build.files_indexed);
            eprintln!("skipped binary: {}", build.files_skipped_binary);
            eprintln!("skipped too large: {}", build.files_skipped_too_large);
            eprintln!("bytes indexed: {}", build.total_bytes_indexed);
            eprintln!("scan duration: {:.2?}", build.scan_duration);
            eprintln!("total duration: {:.2?}", build.total_duration);
            eprintln!("snapshot bytes: {}", build.snapshot_bytes);
            eprintln!("index dir: {}", build.index_dir.display());
        }
        return Ok(());
    }

    if cli.content_index_status {
        let status = content_index_status(&path)?;
        eprintln!("root: {}", status.root);
        eprintln!("index dir: {}", status.index_dir.display());
        eprintln!("snapshot exists: {}", status.snapshot_exists);
        eprintln!("files indexed: {}", status.files_indexed);
        eprintln!("grams indexed: {}", status.grams_indexed);
        eprintln!("snapshot size: {}", status.snapshot_size_bytes);
        eprintln!("snapshot created unix: {}", status.snapshot_created_unix);
        eprintln!("include binary: {}", status.include_binary);
        eprintln!("max file size: {}", status.max_file_size);
        return Ok(());
    }

    if let Some(needle) = &cli.content_search {
        if !cli.quiet {
            eprintln!("Content search via index: {}", path_str);
        }
        let query = query_content_index(&path, needle, cli.list, cli.max_results)?;
        if !cli.quiet {
            eprintln!(
                "\nFound {} matching files in {:.2?} ({} candidates)",
                query.matches, query.duration, query.candidates
            );
        } else if !cli.list {
            println!("{}", query.matches);
        }
        return Ok(());
    }

    if let Some(needle) = &cli.content_search_live {
        if !cli.quiet {
            eprintln!("Content search via live scan: {}", path_str);
        }
        let query = query_content_live(
            &path,
            cli.depth,
            scan_options,
            needle,
            cli.list,
            cli.max_results,
            cli.content_max_file_size,
            cli.content_include_binary,
            cli.content_workers,
        )?;
        if !cli.quiet {
            eprintln!(
                "\nFound {} matching files in {:.2?} ({} candidates)",
                query.matches, query.duration, query.candidates
            );
        } else if !cli.list {
            println!("{}", query.matches);
        }
        return Ok(());
    }

    if cli.use_index {
        if !cli.quiet {
            eprintln!("Querying index: {}", path_str);
        }
        let query = query_index(
            &path,
            &filter,
            cli.list,
            cli.max_results,
            !cli.no_index_auto_compact,
        )?;
        if !cli.quiet {
            eprintln!("\nFound {} files in {:.2?}", query.files, query.duration);
            if query.auto_compacted {
                eprintln!("(auto-compacted delta log)");
            }
        } else if !cli.list {
            println!("{}", query.files);
        }
        return Ok(());
    }

    if !cli.quiet {
        eprintln!("Scanning: {}", path_str);
    }

    let handle = scan(&path_str, cli.list, cli.depth, filter, scan_options);

    let mut total: usize = 0;
    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());

    for batch in &handle.receiver {
        match batch {
            ResultBatch::Count(n) => {
                total += n;
            }
            ResultBatch::Paths(paths) => {
                total += paths.len();
                for p in paths {
                    let _ = writeln!(writer, "{}", p);
                }
            }
        }
    }
    let _ = writer.flush();

    let stats = handle.wait_for_completion();

    if !cli.quiet {
        eprintln!("\nFound {} files in {:.2?}", total, stats.duration);
    } else if !cli.list {
        println!("{}", total);
    }

    if cli.stats {
        eprintln!("\n--- Statistics ---");
        eprintln!("getattrlistbulk calls: {}", stats.getattr_calls);
        eprintln!("entries returned:      {}", stats.getattr_entries);
        eprintln!("avg entries/call:      {:.1}", stats.avg_entries_per_call());
        eprintln!("getattr errors:        {}", stats.getattr_errors);
        eprintln!("openat calls:          {}", stats.openat_calls);
        eprintln!(
            "openat abs/rel:        {}/{}",
            stats.openat_abs_calls, stats.openat_rel_calls
        );
        eprintln!("openat failures:       {}", stats.openat_fails);
        eprintln!("close calls:           {}", stats.close_calls);
        eprintln!("openat time:           {:.2} ms", stats.openat_ms);
        eprintln!("getattr time:          {:.2} ms", stats.getattr_ms);
        eprintln!("rdahead calls:         {}", stats.rdahead_calls);
        eprintln!("rdahead failures:      {}", stats.rdahead_fails);
        eprintln!("rdahead time:          {:.2} ms", stats.rdahead_ms);
    }
    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {}", err);
        std::process::exit(1);
    }
}
