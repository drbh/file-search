mod content;
mod scan;

use clap::Parser;
use content::query_content_live;
use scan::{scan, FileFilter, ResultBatch, ScanOptions};
use std::collections::HashSet;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(name = "f")]
#[command(about = "Fast file search using macOS getattrlistbulk", long_about = None)]
struct Cli {
    /// Directory to search (defaults to current directory)
    #[arg(default_value = ".")]
    path: String,

    /// Filename substring filter (positional)
    #[arg(value_name = "NAME")]
    name: Option<String>,

    /// List file paths instead of just counting
    #[arg(short, long)]
    list: bool,

    /// Clamp listed paths to at most N path segments below the input root and deduplicate
    #[arg(short = 'g', long)]
    segments: Option<usize>,

    /// Maximum depth to traverse (0 = unlimited)
    #[arg(short, long, default_value = "0")]
    depth: usize,

    /// File extensions to match (comma-separated, e.g., "rs,toml,md")
    #[arg(short, long, value_delimiter = ',')]
    ext: Option<Vec<String>>,

    /// Match filename substring (repeatable)
    #[arg(short = 'n', long = "name")]
    name_filter: Vec<String>,

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
    #[arg(short = 'x', long, value_delimiter = ',')]
    exclude_dir: Option<Vec<String>>,

    /// Path to ignore config file (defaults to <scan-root>/.file-search-ignore)
    #[arg(long)]
    ignore_config: Option<String>,

    /// Stop after returning this many content matches
    #[arg(long)]
    limit: Option<usize>,

    /// Search literal text inside files via live scan
    #[arg(short = 't', long)]
    text: Option<String>,

    /// Max file size for text search (bytes)
    #[arg(short = 'm', long, default_value_t = 8 * 1024 * 1024)]
    max_size: u64,

    /// Include binary files in text search
    #[arg(short = 'b', long)]
    binary: bool,

    /// Worker threads for text search (0 = auto)
    #[arg(short = 'w', long, default_value_t = 0)]
    workers: usize,

    /// Suppress diagnostic stderr output (including --stats)
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

fn run() -> io::Result<()> {
    let cli = Cli::parse();
    if cli.segments.is_some() && !cli.list {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--segments requires --list",
        ));
    }
    let mut path_arg = cli.path.clone();
    let mut implicit_name: Option<String> = None;
    if cli.name.is_none() && !path_arg.contains('/') {
        let candidate_path = PathBuf::from(&path_arg);
        if !candidate_path.exists() {
            implicit_name = Some(path_arg.clone());
            path_arg = ".".to_string();
        }
    }

    // Build filter from CLI options (all supplied clauses are AND-ed together)
    let extensions = cli.ext.as_ref().map(|exts| {
        exts.iter()
            .map(|e| {
                e.trim_start_matches('.')
                    .to_ascii_lowercase()
                    .into_bytes()
                    .into_boxed_slice()
            })
            .filter(|e| !e.is_empty())
            .collect::<Vec<_>>()
    });
    let mut contains_all: Vec<Box<[u8]>> = Vec::new();
    if let Some(term) = &implicit_name {
        if !term.is_empty() {
            contains_all.push(term.to_ascii_lowercase().into_bytes().into_boxed_slice());
        }
    }
    if let Some(term) = &cli.name {
        if !term.is_empty() {
            contains_all.push(term.to_ascii_lowercase().into_bytes().into_boxed_slice());
        }
    }
    for term in &cli.name_filter {
        if !term.is_empty() {
            contains_all.push(term.to_ascii_lowercase().into_bytes().into_boxed_slice());
        }
    }
    let prefix = cli
        .prefix
        .as_ref()
        .map(|value| value.to_ascii_lowercase().into_bytes().into_boxed_slice());
    let suffix = cli
        .suffix
        .as_ref()
        .map(|value| value.to_ascii_lowercase().into_bytes().into_boxed_slice());

    let filter =
        if extensions.is_some() || !contains_all.is_empty() || prefix.is_some() || suffix.is_some()
        {
            FileFilter::Composite {
                extensions,
                contains_all,
                prefix,
                suffix,
            }
        } else {
            FileFilter::All
        };

    // Resolve to absolute path
    let path = std::fs::canonicalize(&path_arg).unwrap_or_else(|_| PathBuf::from(&path_arg));
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

    if let Some(needle) = &cli.text {
        let query = query_content_live(
            &path,
            cli.depth,
            scan_options,
            filter.clone(),
            needle,
            cli.list,
            cli.segments,
            cli.limit,
            cli.max_size,
            cli.binary,
            cli.workers,
            cli.stats,
        )?;

        if !cli.list {
            println!("{}", query.matches);
        }

        if cli.stats && !cli.quiet {
            eprintln!(
                "Found {} matching files in {:.2?} ({} candidates)",
                query.matches, query.duration, query.candidates
            );
            let gib = query.bytes_read as f64 / (1024.0 * 1024.0 * 1024.0);
            eprintln!(
                "scan: {:.2?} | dispatch: {:.2?} | join: {:.2?}",
                query.scan_duration, query.dispatch_duration, query.worker_join_duration
            );
            eprintln!(
                "files seen/opened: {}/{} | read: {:.2} GiB ({} calls) | skipped too_large/binary: {}/{} | io errors open/read: {}/{}",
                query.files_seen,
                query.files_opened,
                gib,
                query.read_calls,
                query.skipped_too_large,
                query.skipped_binary,
                query.open_errors,
                query.read_errors
            );
        }
        return Ok(());
    }

    let handle = scan(
        &path_str,
        cli.list,
        cli.depth,
        filter,
        scan_options,
        None,
        cli.stats,
    );

    let mut total: usize = 0;
    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());
    let mut clamped_seen = if cli.list && cli.segments.is_some() {
        Some(HashSet::<String>::new())
    } else {
        None
    };

    for batch in &handle.receiver {
        match batch {
            ResultBatch::Count(n) => {
                total += n;
            }
            ResultBatch::Paths(paths) => {
                if let Some(seen) = clamped_seen.as_mut() {
                    for p in paths {
                        total += 1;
                        let clamped =
                            clamp_list_path_segments(&p, &path, cli.segments.unwrap_or(0));
                        if seen.insert(clamped.clone()) {
                            let _ = writeln!(writer, "{}", clamped);
                        }
                    }
                } else {
                    total += paths.len();
                    for p in paths {
                        let _ = writeln!(writer, "{}", p);
                    }
                }
            }
        }
    }
    let _ = writer.flush();

    let stats = handle.wait_for_completion();

    if !cli.list {
        println!("{}", total);
    }

    if cli.stats && !cli.quiet {
        eprintln!("Found {} files in {:.2?}", total, stats.duration);
        eprintln!("--- Statistics ---");
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
        eprintln!("fd budget misses:      {}", stats.fd_budget_misses);
        eprintln!("local stack pushes:    {}", stats.local_stack_pushes);
        eprintln!("global queue spills:   {}", stats.global_queue_spills);
        eprintln!("cancel-skipped dirs:   {}", stats.cancel_skipped_dirs);
    }
    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {}", err);
        std::process::exit(1);
    }
}
