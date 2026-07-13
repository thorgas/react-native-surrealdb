#!/usr/bin/env bash
set -euo pipefail

HARNESS_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PLATFORM="${1:-android}"
PROFILE="${2:-smoke}"
RESULTS_DIR="$HARNESS_DIR/performance-results/$PLATFORM/$PROFILE"
LOG_PATH="$RESULTS_DIR/harness.log"
REPORT_PATH="$RESULTS_DIR/report.json"
JEST_RESULT_PATH="$RESULTS_DIR/jest-results.json"
DEVICE_LOG_PATH="$RESULTS_DIR/device.log"
DEVICE_LOG_PID=""

cleanup_device_log() {
  if [[ -n "$DEVICE_LOG_PID" ]]; then
    kill "$DEVICE_LOG_PID" >/dev/null 2>&1 || true
    wait "$DEVICE_LOG_PID" 2>/dev/null || true
    DEVICE_LOG_PID=""
  fi
}
trap cleanup_device_log EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ "$PLATFORM" != "android" && "$PLATFORM" != "ios" ]]; then
  echo "Usage: $0 <android|ios> <smoke|canonical|upstream>" >&2
  exit 2
fi
if [[ "$PROFILE" != "smoke" && "$PROFILE" != "canonical" && "$PROFILE" != "upstream" ]]; then
  echo "Usage: $0 <android|ios> <smoke|canonical|upstream>" >&2
  exit 2
fi
mkdir -p "$RESULTS_DIR"
rm -f "$REPORT_PATH" "$DEVICE_LOG_PATH"

if [[ "$PLATFORM" == "android" ]]; then
  source "$HARNESS_DIR/scripts/android-env.sh"
  "$HARNESS_DIR/android/gradlew" \
    -p "$HARNESS_DIR/android" \
    --no-daemon \
    app:assembleDebug \
    -PreactNativeArchitectures=arm64-v8a
  export HARNESS_APP_PATH="$HARNESS_DIR/android/app/build/outputs/apk/debug/app-debug.apk"

  # The benchmark APK contains the embedded database and is intentionally large.
  # Remove the previous installation before Harness installs the fresh build so
  # Android does not require enough free space for both APKs during replacement.
  ADB="$ANDROID_HOME/platform-tools/adb"
  if "$ADB" get-state >/dev/null 2>&1; then
    "$ADB" uninstall com.surrealdbharness >/dev/null 2>&1 || true
    "$ADB" logcat -c
  fi

  # Harness shuts down emulators that it started. Capture logcat concurrently
  # so the benchmark JSON is preserved before that teardown happens.
  (
    "$ADB" wait-for-device
    exec "$ADB" logcat -v raw
  ) > "$DEVICE_LOG_PATH" 2>&1 &
  DEVICE_LOG_PID=$!
else
  # The Apple runner may also shut down a simulator that it booted. Wait for a
  # booted simulator, then stream only benchmark chunks while the test runs.
  (
    until xcrun simctl list devices booted | grep -q '(Booted)'; do
      sleep 1
    done
    exec xcrun simctl spawn booted log stream \
      --style compact \
      --predicate 'eventMessage CONTAINS "SURREALDB_BENCHMARK_RESULT_CHUNK="'
  ) > "$DEVICE_LOG_PATH" 2>&1 &
  DEVICE_LOG_PID=$!
fi

if [[ "$PROFILE" == "upstream" ]]; then
  TEST_PATH="__benchmarks__/surreal-crud.upstream.performance.harness.ts"
  TEST_TIMEOUT=600000
elif [[ "$PROFILE" == "canonical" ]]; then
  TEST_PATH="__benchmarks__/surreal-crud.canonical.performance.harness.ts"
  TEST_TIMEOUT=300000
else
  TEST_PATH="__benchmarks__/surreal-crud.performance.harness.ts"
  TEST_TIMEOUT=120000
fi

set +e
pnpm exec react-native-harness \
  --config jest.performance.config.mjs \
  --harnessRunner "$PLATFORM" \
  --testTimeout "$TEST_TIMEOUT" \
  --runTestsByPath "$TEST_PATH" \
  --json \
  --outputFile "$JEST_RESULT_PATH" \
  2>&1 | tee "$LOG_PATH"
HARNESS_STATUS=${PIPESTATUS[0]}
set -e

cleanup_device_log

if [[ "$HARNESS_STATUS" -ne 0 ]]; then
  exit "$HARNESS_STATUS"
fi

node "$HARNESS_DIR/scripts/extract-performance-report.mjs" \
  --input="$DEVICE_LOG_PATH" \
  --output="$REPORT_PATH"

if [[ -n "${SURREALDB_PERFORMANCE_BASELINE:-}" ]]; then
  node "$HARNESS_DIR/scripts/compare-performance.mjs" \
    --baseline="$SURREALDB_PERFORMANCE_BASELINE" \
    --candidate="$REPORT_PATH" \
    --output="$RESULTS_DIR/comparison.json"
else
  echo "No SURREALDB_PERFORMANCE_BASELINE supplied; report recorded without a regression gate."
fi
