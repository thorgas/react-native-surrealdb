#!/usr/bin/env bash

set -euo pipefail

PACKAGE_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
STRIP_TOOL=$(xcrun --sdk iphoneos --find strip)

for library in \
  "$PACKAGE_ROOT/SurrealDbRnFramework.xcframework/ios-arm64/libsurrealdb_rn_core.a" \
  "$PACKAGE_ROOT/SurrealDbRnFramework.xcframework/ios-arm64_x86_64-simulator/libsurrealdb_rn_core.a"; do
  if [[ ! -f "$library" ]]; then
    echo "Missing iOS release library: $library" >&2
    exit 1
  fi
  "$STRIP_TOOL" -S -x "$library"
done

echo "Prepared stripped iOS release libraries."
