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
