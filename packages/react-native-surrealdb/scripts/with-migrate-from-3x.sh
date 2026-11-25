#!/usr/bin/env bash
# Runs a command (usually `pnpm run ubrn:ios` / `ubrn:android`) with the
# `migrate-from-3x` Cargo feature added to the default feature set.
#
# Why a wrapper: `ubrn build` has no `--features` flag, and the 3.x reader adds
# roughly 8 MiB to the native library, so it is off by default. Cargo.toml is
# restored when the command exits, even on failure or Ctrl-C.
#
# Usage: bash scripts/with-migrate-from-3x.sh pnpm run ubrn:android
set -euo pipefail

if (( $# == 0 )); then
  echo "Usage: $0 <command> [args...]" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFEST="$SCRIPT_DIR/../../../crates/surrealdb-rn-core/Cargo.toml"
BACKUP="$(mktemp)"
cp "$MANIFEST" "$BACKUP"
trap 'cp "$BACKUP" "$MANIFEST"; rm -f "$BACKUP"' EXIT

DEFAULT_LINE='default = ["engine-mem", "engine-surrealkv", "protocol-ws"]'
if ! grep -Fxq "$DEFAULT_LINE" "$MANIFEST"; then
  echo "Unexpected default feature line in $MANIFEST; update this script." >&2
  exit 1
fi
NEW_LINE='default = ["engine-mem", "engine-surrealkv", "protocol-ws", "migrate-from-3x"]'
python3 - "$MANIFEST" "$DEFAULT_LINE" "$NEW_LINE" <<'PY'
import sys
path, old, new = sys.argv[1:]
text = open(path).read()
open(path, "w").write(text.replace(old, new, 1))
PY

"$@"
