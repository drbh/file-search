mod scan;

use clap::Parser;
use scan::{scan, FileFilter, ResultBatch};
use std::io::{self, BufWriter, Write};

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

    /// Quiet mode - only output file paths or count
    #[arg(short, long)]
    quiet: bool,
}

fn main() {
    let cli = Cli::parse();

    // Build filter from CLI options
    let filter = if let Some(exts) = &cli.ext {
        let normalized: Vec<String> = exts.iter().map(|e| e.to_ascii_lowercase()).collect();
        FileFilter::Extensions(normalized)
    } else if let Some(pattern) = &cli.contains {
        FileFilter::Contains(pattern.clone())
    } else if let Some(prefix) = &cli.prefix {
        FileFilter::Prefix(prefix.clone())
    } else if let Some(suffix) = &cli.suffix {
        FileFilter::Suffix(suffix.clone())
    } else {
        FileFilter::All
    };

    // Resolve to absolute path
    let path = std::fs::canonicalize(&cli.path)
        .unwrap_or_else(|_| std::path::PathBuf::from(&cli.path));
    let path_str = path.to_string_lossy().to_string();

    if !cli.quiet {
        eprintln!("Scanning: {}", path_str);
    }

    let handle = scan(&path_str, cli.list, cli.depth, filter);

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
        eprintln!("openat calls:          {}", stats.openat_calls);
        eprintln!("close calls:           {}", stats.close_calls);
        eprintln!("openat time:           {:.2} ms", stats.openat_ms);
        eprintln!("getattr time:          {:.2} ms", stats.getattr_ms);
    }
}
