# file-search

fast search tool for osx. search files and file contents easily.

__my osx search tool, since mdfind and find/fd don't do everything I want__

## Build

```bash
cargo install --git https://github.com/drbh/file-search.git
f --help
```

## One Mental Model

`f [PATH] [NAME] [filters]`

- `PATH` sets root.
- `NAME` and filename flags filter paths (AND).
- `-t/--text` filters file contents only after filename filters pass.

## Common Commands

```bash
# count
f ~/Projects

# find all pngs under the home dir 
f ~ -le png

# find all mp3 in home dir and list them
f ~ -le mp3

# find all markdown files in home dir and list them
f ~ README -e md -l

# find all markdown files in home dir that contain "kernel" and list them
f ~/Projects README -e md -t "kernel" -l

# clamp printed paths to N segments below root
f ~/Projects README -e md -l -g 2

# find all projects with "TODO" in markdown files including hidden folders
f ~/Projects todo -e md -l -g 1 -a

# find files modified in the last 24 hours
f ~/Projects -e rs -l -r 24h

# find files older than 30 days
f ~/Projects -e log -l -o 30d

# find files between 1 day and 7 days old
f ~/Projects -e rs -l -o 1d -r 7d

# find count of files with "main" in the name in the file-search dir
f ~/Projects/file-search main
```

## Useful Flags

- Scope: `-d/--depth`
- Filename: positional `NAME`, `-n/--name` (repeatable), `-e/--ext`, `-p/--prefix`, `-s/--suffix`
- Time: `-r/--max-age`, `-o/--min-age` (units: `ms/s/m/h/d/w`)
- Content: `-t/--text`, `-w/--workers`, `-m/--max-size`, `-b/--binary`, `-k/--limit`
- Output: `-l/--list`, `-g/--segments`, `-v/--stats`, `-q/--quiet`
- Ignore: `-a/--hidden`, `-P/--no-default-prunes`, `-x/--exclude-dir`, `-I/--ignore-config`

## Ignore File

Default: `<scan-root>/.file-search-ignore`

```text
node_modules
dir:.git
path:/Users/drbh/Downloads
```

## Output Contract

- `stdout`: results only (count or list)
- `stderr`: warnings/stats

## Comparison to other tools

ran on my home directory with 4,592,063 files that weights roughly 646GB at the time of writing.

I ran `f` with `-P/--no-default-prunes` to disable pruning of common directories like `node_modules` and `target` since `fd` and `find` don't prune by default. I also ran `f` with `-a/--hidden` to include hidden files. I ran `fd` with `-u/--unrestricted` to include hidden files and ignore `.gitignore` rules. I ran `find` without any pruning or ignoring flags since it doesn't have any.

### `f` ~9.5s

```bash
hyperfine 'f ~/ -e mp4 -l -P -a | wc -l'
Benchmark 1: f ~/ -e mp4 -l -P -a | wc -l
  Time (mean ± σ):      9.534 s ±  0.110 s    [User: 0.616 s, System: 30.792 s]
  Range (min … max):    9.394 s …  9.664 s    10 runs
```

### `fd` ~22.8s

```bash
hyperfine 'fd . ~/ -e mp4 -u | wc -l'
Benchmark 1: fd . ~/ -e mp4 -u | wc -l
  Time (mean ± σ):     22.837 s ±  0.741 s    [User: 4.813 s, System: 251.892 s]
  Range (min … max):   21.104 s … 23.780 s    10 runs
```

### `find` ~102.1s

```bash
hyperfine 'find ~/ -type f -iname "*.mp4" -print | wc -l'
Benchmark 1: find ~/ -type f -iname "*.mp4" -print | wc -l
  Time (mean ± σ):     102.108 s ±  1.332 s    [User: 2.469 s, System: 46.627 s]
  Range (min … max):   100.347 s … 104.297 s    10 runs
```


### `f` with defaults ~3.3s

note that if we run `f` the way I normally would without `-P/--no-default-prunes` and with `-a/--hidden`, it runs in ~3.3s since it prunes a lot of irrelevant directories by default.

```bash
hyperfine 'f ~/ -e mp4 -l | wc -l'
Benchmark 1: f ~/ -e mp4 -l | wc -l
  Time (mean ± σ):      3.341 s ±  0.095 s    [User: 0.251 s, System: 12.162 s]
  Range (min … max):    3.254 s …  3.584 s    10 runs
```

> [!TIP] TLDR;
> `f` is `~2.4x` faster than `fd` and `~10.7x` faster than `find` in this case. and more like `~6.8x` faster than `fd` and `~30.5x` faster than `find` when using the default pruning rules.

## Differences from `fd` and `find`:

### 1. File name searching

`f` intentionally does not support glob/regex filename matching
 
personally I either know the a substring of a file name or the extension. I rarely find myself searching for all files with only numbers in the name, or all files that start with a capital letter and end with a number. If I do need to do something like that, I can just use `fd` (great tool).

`f` only uses fast filename primitives (`name`, `ext`, `prefix`, `suffix`, `contains`) for speed.

> [!TIP] TLDR;
> `f` is optimized for simple matching that avoid complex pattern matching that adds complexity and slows down the search.

### 2. Content searching

`f` is optimized for searching file contents with a text search primitive (`-t/--text`) that is separate from filename searching. This allows `f` to first filter files by name or extension, and then only search the contents of the remaining files which is much faster than searching contents of all files.

so its highly recommend to add an extension filter when using the text search primitive to avoid searching contents of irrelevant files.

```bash
hyperfine 'f ~/ -e md -l -t "github.com/drbh/file-search"'
Benchmark 1: f ~/ -e md -l -t "github.com/drbh/file-search"
  Time (mean ± σ):      3.803 s ±  0.257 s    [User: 0.290 s, System: 13.577 s]
  Range (min … max):    3.352 s …  4.177 s    10 runs
```

and to be honest in most cases I have a rough idea of the folder that I want to search in and the file extension and its super fast in that case

```bash
hyperfine 'f ~/Projects -e md -l -t "github.com/drbh/file-search"'
Benchmark 1: f ~/Projects -e md -l -t "github.com/drbh/file-search"
  Time (mean ± σ):     230.9 ms ±  30.7 ms    [User: 40.4 ms, System: 1122.0 ms]
  Range (min … max):   217.3 ms … 317.4 ms    10 runs
```

kinda similar comparison is running `rg` in the `~/Projects` directory with `-g "*.md"` to filter by markdown files first before searching contents. The main difference is that `rg` returns the specific instances and `f` returns the file paths that contain the search term.

```bash
hyperfine 'rg  -g "*.md" --text "github.com/drbh/file-search"'
Benchmark 1: rg  -g "*.md" --text "github.com/drbh/file-search"
  Time (mean ± σ):     477.6 ms ±  13.0 ms    [User: 307.1 ms, System: 5257.0 ms]
  Range (min … max):   458.3 ms … 502.9 ms    10 runs
```

> [!TIP] TLDR;
> `f` also seaches file contents and is `~2.0x` faster than `rg` in many cases and can search out of the current directory.