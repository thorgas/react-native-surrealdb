#!/usr/bin/env bash
set -euo pipefail

HARNESS_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RESULTS_DIR="$HARNESS_DIR/size-results/android"
GRADLE="$HARNESS_DIR/android/gradlew"
ARCH_PROPERTY="-PreactNativeArchitectures=arm64-v8a"

mkdir -p "$RESULTS_DIR"

"$GRADLE" -p "$HARNESS_DIR/android" --no-daemon --no-build-cache clean "$ARCH_PROPERTY"
"$GRADLE" -p "$HARNESS_DIR/android" --no-daemon --no-build-cache app:assembleRelease "$ARCH_PROPERTY"
cp "$HARNESS_DIR/android/app/build/outputs/apk/release/app-release.apk" "$RESULTS_DIR/candidate.apk"

if [[ "$(unzip -Z1 "$RESULTS_DIR/candidate.apk" | grep -c "libreact-native-surrealdb.so")" -eq 0 ]]; then
  echo "Candidate is missing libreact-native-surrealdb.so" >&2
  exit 1
fi

node "$HARNESS_DIR/scripts/compare-release-size.mjs" \
  --platform=android \
  --candidate="$RESULTS_DIR/candidate.apk" \
  --output="$RESULTS_DIR/report.json"
