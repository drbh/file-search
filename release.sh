#!/bin/bash
set -e
VERSION=$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)"/\1/')
cargo build --release

echo "Ready to release v$VERSION"
read -p "Push to GitHub? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
  gh release create "v$VERSION" ./target/release/f --title "v$VERSION" --notes "Release v$VERSION"
else
  echo "Aborted"
fi