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
REPORT_RECEIVER_PID=""

cleanup_background_processes() {
  if [[ -n "$DEVICE_LOG_PID" ]]; then
    kill "$DEVICE_LOG_PID" >/dev/null 2>&1 || true
    wait "$DEVICE_LOG_PID" 2>/dev/null || true
    DEVICE_LOG_PID=""
  fi
  if [[ -n "$REPORT_RECEIVER_PID" ]]; then
    kill "$REPORT_RECEIVER_PID" >/dev/null 2>&1 || true
    wait "$REPORT_RECEIVER_PID" 2>/dev/null || true
    REPORT_RECEIVER_PID=""
  fi
}
trap cleanup_background_processes EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ "$PLATFORM" != "android" && "$PLATFORM" != "ios" ]]; then
  echo "Usage: $0 <android|ios> <smoke|canonical|upstream|sqlite>" >&2
  exit 2
fi
if [[ "$PROFILE" != "smoke" &&
  "$PROFILE" != "canonical" &&
  "$PROFILE" != "upstream" &&
  "$PROFILE" != "sqlite" ]]; then
  echo "Usage: $0 <android|ios> <smoke|canonical|upstream|sqlite>" >&2
  exit 2
fi
mkdir -p "$RESULTS_DIR"
rm -f "$REPORT_PATH" "$DEVICE_LOG_PATH"

node "$HARNESS_DIR/scripts/receive-performance-report.mjs" \
  --output="$REPORT_PATH" \
  > "$RESULTS_DIR/report-receiver.log" 2>&1 &
REPORT_RECEIVER_PID=$!
for _ in {1..50}; do
  if curl --fail --silent "http://127.0.0.1:18082/health" >/dev/null; then
    break
  fi
  sleep 0.1
done
if ! kill -0 "$REPORT_RECEIVER_PID" >/dev/null 2>&1; then
  echo "Benchmark report receiver exited during startup" >&2
  cat "$RESULTS_DIR/report-receiver.log" >&2
  exit 1
fi
if ! curl --fail --silent "http://127.0.0.1:18082/health" >/dev/null; then
  echo "Benchmark report receiver failed to start" >&2
  exit 1
fi

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
    "$ADB" logcat -c >/dev/null 2>&1 || true
  fi

  # Harness shuts down emulators that it started. Capture logcat concurrently
  # so the benchmark JSON is preserved before that teardown happens.
  (
    "$ADB" wait-for-device
    exec "$ADB" logcat -v raw
  ) > "$DEVICE_LOG_PATH" 2>&1 &
  DEVICE_LOG_PID=$!
else
  bash "$HARNESS_DIR/scripts/prepare-ios.sh"

  # The Apple runner may also shut down a simulator that it booted. Wait for a
  # specific simulator, then stream only benchmark chunks while the test runs.
  # Using the generic `booted` alias can attach to an unrelated simulator that
  # was already running before Harness boots its configured device.
  IOS_SIMULATOR_NAME="${SURREALDB_IOS_SIMULATOR_NAME:-iPhone 17 Pro}"
  IOS_DEVICE_UDID="$(
    xcrun simctl list devices available |
      awk -F '[()]' -v name="$IOS_SIMULATOR_NAME" '
        {
          candidate = $1
          gsub(/^[[:space:]]+|[[:space:]]+$/, "", candidate)
          if (candidate == name) {
            print $2
            exit
          }
        }
      '
  )"
  if [[ -z "$IOS_DEVICE_UDID" ]]; then
    echo "Could not find available iOS simulator: $IOS_SIMULATOR_NAME" >&2
    exit 1
  fi
  (
    xcrun simctl bootstatus "$IOS_DEVICE_UDID" -b
    exec xcrun simctl spawn "$IOS_DEVICE_UDID" log stream \
      --style compact \
      --predicate 'eventMessage CONTAINS "SURREALDB_BENCHMARK_RESULT_CHUNK="'
  ) > "$DEVICE_LOG_PATH" 2>&1 &
  DEVICE_LOG_PID=$!
fi

if [[ "$PROFILE" == "sqlite" ]]; then
  TEST_PATH="__benchmarks__/sqlite-bench.performance.harness.ts"
  TEST_TIMEOUT=600000
elif [[ "$PROFILE" == "upstream" ]]; then
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

cleanup_background_processes

if [[ "$HARNESS_STATUS" -ne 0 ]]; then
  exit "$HARNESS_STATUS"
fi

if [[ -s "$REPORT_PATH" ]]; then
  echo "Benchmark report received at $REPORT_PATH"
else
  node "$HARNESS_DIR/scripts/extract-performance-report.mjs" \
    --input="$DEVICE_LOG_PATH" \
    --output="$REPORT_PATH"
fi

if [[ -n "${SURREALDB_PERFORMANCE_BASELINE:-}" ]]; then
  node "$HARNESS_DIR/scripts/compare-performance.mjs" \
    --baseline="$SURREALDB_PERFORMANCE_BASELINE" \
    --candidate="$REPORT_PATH" \
    --output="$RESULTS_DIR/comparison.json"
else
  echo "No SURREALDB_PERFORMANCE_BASELINE supplied; report recorded without a regression gate."
fi
