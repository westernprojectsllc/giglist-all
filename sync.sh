#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
SRC_DIR="$(cd "$ROOT/.." && pwd)"

sync_region() {
  local src="$1" dest="$2"
  rm -rf "$dest"
  mkdir -p "$dest"
  rsync -a \
    --include='*.html' --include='*.json' --include='*.svg' \
    --include='*.xml' --include='*.txt' \
    --exclude='*' \
    "$SRC_DIR/$src/" "$dest/"
}

sync_region mn-shows "$ROOT/mn"
sync_region tn-shows "$ROOT/tn"
echo "Synced mn/ and tn/ from $SRC_DIR"
