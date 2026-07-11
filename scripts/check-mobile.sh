#!/bin/sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
PACKAGE=surrealdb-rn-core
IOS_TARGET=aarch64-apple-ios-sim
ANDROID_TARGET=aarch64-linux-android

fail() {
  printf 'check-mobile: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "required command '$1' was not found in PATH"
}

require_target() {
  target=$1
  if ! rustup target list --installed | grep -qx "$target"; then
    fail "Rust target '$target' is not installed; run: rustup target add $target"
  fi
}

newest_ndk() {
  ndk_root=$HOME/Library/Android/sdk/ndk
  [ -d "$ndk_root" ] || return 1

  # NDK directory names are dotted numeric versions. Version sort is preferred;
  # lexical sorting remains a useful fallback on minimal POSIX environments.
  if sort -V </dev/null >/dev/null 2>&1; then
    printf '%s\n' "$ndk_root"/* 2>/dev/null | sort -V | tail -n 1
  else
    printf '%s\n' "$ndk_root"/* 2>/dev/null | sort | tail -n 1
  fi
}

require_command cargo
require_command rustup
require_command grep

cd "$REPO_ROOT"

require_target "$IOS_TARGET"
printf 'check-mobile: checking %s for iOS arm64 simulator\n' "$PACKAGE"
cargo check -p "$PACKAGE" --target "$IOS_TARGET"

require_target "$ANDROID_TARGET"

if [ -n "${ANDROID_NDK_HOME:-}" ]; then
  NDK_HOME=$ANDROID_NDK_HOME
else
  NDK_HOME=$(newest_ndk) || fail \
    "Android NDK not found; set ANDROID_NDK_HOME or install an NDK under ~/Library/Android/sdk/ndk"
fi

[ -d "$NDK_HOME" ] || fail "Android NDK directory does not exist: $NDK_HOME"

TOOLCHAIN_BIN=
for candidate in "$NDK_HOME"/toolchains/llvm/prebuilt/*/bin; do
  if [ -x "$candidate/aarch64-linux-android24-clang" ] && [ -x "$candidate/llvm-ar" ]; then
    TOOLCHAIN_BIN=$candidate
    break
  fi
done

[ -n "$TOOLCHAIN_BIN" ] || fail \
  "NDK at '$NDK_HOME' does not contain API 24 aarch64 clang and llvm-ar"

export CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="$TOOLCHAIN_BIN/aarch64-linux-android24-clang"
export CC_aarch64_linux_android="$TOOLCHAIN_BIN/aarch64-linux-android24-clang"
export AR_aarch64_linux_android="$TOOLCHAIN_BIN/llvm-ar"

printf 'check-mobile: checking %s for Android arm64 with NDK %s\n' "$PACKAGE" "$NDK_HOME"
cargo check -p "$PACKAGE" --target "$ANDROID_TARGET"

printf 'check-mobile: all mobile target checks passed\n'
