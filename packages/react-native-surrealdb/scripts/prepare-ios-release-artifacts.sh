#!/usr/bin/env bash

set -euo pipefail

PACKAGE_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
LINK_TOOL=$(xcrun --find ld)
MODE=${1:-prepare}
if [[ "$MODE" != prepare && "$MODE" != --verify-only ]]; then
  echo "Use prepare or --verify-only" >&2
  exit 1
fi
PROBE_DIR=$(mktemp -d "${TMPDIR:-/tmp}/surrealdb-ios-archive-check.XXXXXX")
trap 'rm -rf "$PROBE_DIR"' EXIT

for library in \
  "$PACKAGE_ROOT/SurrealDbRnFramework.xcframework/ios-arm64/libsurrealdb_rn_core.a" \
  "$PACKAGE_ROOT/SurrealDbRnFramework.xcframework/ios-arm64-simulator/libsurrealdb_rn_core.a"; do
  if [[ ! -f "$library" ]]; then
    echo "Missing iOS release library: $library" >&2
    exit 1
  fi
  # Keep the original release archive. Xcode 27 strip corrupts AWS-LC
  # assembly unwind relocations even with debug-only -S. The consumer's
  # final linker removes unused code after resolving those relocations.
  sdk=iphoneos
  platform=ios
  if [[ "$library" == *ios-arm64-simulator* ]]; then
    sdk=iphonesimulator
    platform=ios-simulator
  fi
  # rustc embeds LLVM bitcode (__LLVM,__bitcode) in every object. When that
  # bitcode is newer than Xcode's LLVM, Apple's ld silently skips the object,
  # so all FFI symbols look undefined in device builds. Drop the section with
  # the LLVM that produced it; the machine code is untouched.
  if [[ "$MODE" == prepare ]]; then
    objcopy="$(rustc --print sysroot)/lib/rustlib/$(rustc -vV | sed -n 's/^host: //p')/bin/llvm-objcopy"
    if [[ ! -x "$objcopy" ]]; then
      echo "Missing $objcopy; run: rustup component add llvm-tools" >&2
      exit 1
    fi
    "$objcopy" --remove-section=__LLVM,__bitcode --remove-section=__LLVM,__cmdline \
      "$library" "$PROBE_DIR/stripped.a"
    mv "$PROBE_DIR/stripped.a" "$library"
    xcrun ranlib "$library"
  fi
  # Capture first: with pipefail, `grep -q` closing the pipe fails nm.
  symbols=$(xcrun nm -gU "$library" 2>&1 || true)
  if grep -q 'Unknown attribute kind' <<<"$symbols"; then
    echo "$library still contains bitcode Xcode's linker cannot read" >&2
    exit 1
  fi
  if ! grep -q '_ffi_surrealdb_rn_core_rust_future_cancel_f32$' <<<"$symbols"; then
    echo "$library does not export the UniFFI symbols" >&2
    exit 1
  fi
  sdk_version=$(xcrun --sdk "$sdk" --show-sdk-version)
  "$LINK_TOOL" -r -arch arm64 -platform_version "$platform" 15.1 "$sdk_version" \
    -all_load "$library" -o "$PROBE_DIR/$platform.o"
done

echo "Removed embedded bitcode and verified both iOS release archives link."
