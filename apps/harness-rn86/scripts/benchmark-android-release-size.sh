#!/usr/bin/env bash
set -euo pipefail

# Builds both sides from the same checkout, then compares APK bytes. Running the
# two builds independently avoids Gradle/RNTA autolinking cache contamination.
HARNESS_DIR="$(cd "$(dirname "$0")/.." && pwd)"

bash "$HARNESS_DIR/scripts/measure-android-baseline.sh"

# RNX Kit caches the resolved react-native.config.js outside android/. Remove
# only that generated cache so the candidate resolves normal autolinking.
rm -f "$HARNESS_DIR/node_modules/.cache/rnx-kit/config.json" \
  "$HARNESS_DIR/node_modules/.cache/rnx-kit/config.sha256"

BASELINE_APK="$HARNESS_DIR/size-results/android/baseline.apk" \
  bash "$HARNESS_DIR/scripts/measure-android-release-size.sh"
