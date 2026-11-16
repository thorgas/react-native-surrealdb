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
  sdk_version=$(xcrun --sdk "$sdk" --show-sdk-version)
  "$LINK_TOOL" -r -arch arm64 -platform_version "$platform" 15.1 "$sdk_version" \
    -all_load "$library" -o "$PROBE_DIR/$platform.o"
done

echo "Verified both original iOS release archives without rewriting relocations."
