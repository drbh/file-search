mod content;
mod scan;

use clap::Parser;
use content::query_content_live;
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

    /// Stop after returning this many matches
    #[arg(long)]
    max_results: Option<usize>,

    /// Search literal text via live filesystem scan (no persistent index)
    #[arg(long)]
    content_search: Option<String>,

    /// Max file size for content search (bytes)
    #[arg(long, default_value_t = 8 * 1024 * 1024)]
    content_max_file_size: u64,

    /// Include binary files in content search
    #[arg(long)]
    content_include_binary: bool,

    /// Number of worker threads for live content search (0 = auto)
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

    if let Some(needle) = &cli.content_search {
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
