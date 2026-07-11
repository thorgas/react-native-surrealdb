#!/bin/sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)

fail() {
  printf 'verify-core: %s\n' "$*" >&2
  exit 1
}

command -v cargo >/dev/null 2>&1 || fail "required command 'cargo' was not found in PATH"

cd "$REPO_ROOT"

printf 'verify-core: checking Rust formatting\n'
cargo fmt --all --check

printf 'verify-core: running Clippy with warnings denied\n'
cargo clippy --workspace --all-targets -- -D warnings

printf 'verify-core: running workspace tests\n'
cargo test --workspace

printf 'verify-core: checking mobile targets\n'
"$SCRIPT_DIR/check-mobile.sh"

printf 'verify-core: all checks passed\n'
