#!/usr/bin/env bash

set -euo pipefail

PACKAGE_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
NDK_ROOT=${ANDROID_NDK_HOME:-${ANDROID_NDK_ROOT:-${ANDROID_HOME:-}/ndk/27.1.12297006}}

if [[ -z "$NDK_ROOT" || ! -d "$NDK_ROOT/toolchains/llvm/prebuilt" ]]; then
  echo "Android NDK not found. Set ANDROID_NDK_HOME or ANDROID_HOME." >&2
  exit 1
fi

STRIP_TOOL=""
for candidate in "$NDK_ROOT"/toolchains/llvm/prebuilt/*/bin/llvm-strip; do
  if [[ -x "$candidate" ]]; then
    STRIP_TOOL=$candidate
    break
  fi
done

if [[ -z "$STRIP_TOOL" ]]; then
  echo "llvm-strip not found below $NDK_ROOT" >&2
  exit 1
fi

READELF_TOOL="$(dirname "$STRIP_TOOL")/llvm-readelf"
if [[ ! -x "$READELF_TOOL" ]]; then
  echo "llvm-readelf not found next to $STRIP_TOOL" >&2
  exit 1
fi

for abi in arm64-v8a x86_64; do
  library="$PACKAGE_ROOT/android/src/main/jniLibs/$abi/libsurrealdb_rn_core.so"
  if [[ ! -f "$library" ]]; then
    echo "Missing Android release library: $library" >&2
    exit 1
  fi
  "$STRIP_TOOL" --strip-unneeded "$library"
  if "$READELF_TOOL" --sections "$library" | grep '\.debug_' >/dev/null; then
    echo "Debug sections remain in $library" >&2
    exit 1
  fi
done

echo "Prepared stripped Android release libraries."
