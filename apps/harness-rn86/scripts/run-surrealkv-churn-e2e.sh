#!/usr/bin/env bash
set -euo pipefail

HARNESS_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PLATFORM="${1:-}"
RESULTS_DIR="$HARNESS_DIR/performance-results/$PLATFORM/surrealkv-churn"
SAMPLES_PATH="$RESULTS_DIR/rss-samples.tsv"
REPORT_PATH="$RESULTS_DIR/rss-report.json"
SAMPLER_PID=""

cleanup() {
  if [[ -n "$SAMPLER_PID" ]]; then
    kill "$SAMPLER_PID" >/dev/null 2>&1 || true
    wait "$SAMPLER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ "$PLATFORM" != "android" && "$PLATFORM" != "ios" ]]; then
  echo "Usage: $0 <android|ios>" >&2
  exit 2
fi

mkdir -p "$RESULTS_DIR"
rm -f "$SAMPLES_PATH" "$REPORT_PATH"

sample_android() {
  source "$HARNESS_DIR/scripts/android-env.sh"
  local adb="$ANDROID_HOME/platform-tools/adb"
  while true; do
    local pid rss
    pid="$($adb shell pidof com.surrealdbharness.rn86 2>/dev/null | tr -d '\r' | awk '{print $1}' || true)"
    if [[ -n "$pid" ]]; then
      rss="$($adb shell cat "/proc/$pid/status" 2>/dev/null | awk '/^VmRSS:/ {print $2}' || true)"
      if [[ "$rss" =~ ^[0-9]+$ ]]; then
        printf '%s\t%s\n' "$(date +%s000)" "$rss" >> "$SAMPLES_PATH"
      fi
    fi
    sleep 0.1
  done
}

sample_ios() {
  local simulator_name="${SURREALDB_IOS_SIMULATOR:-iPhone 17 Pro}"
  local udid
  udid="$(
    xcrun simctl list devices available |
      awk -F '[()]' -v name="$simulator_name" '
        {
          candidate = $1
          gsub(/^[[:space:]]+|[[:space:]]+$/, "", candidate)
          if (candidate == name) { print $2; exit }
        }
      '
  )"
  if [[ -z "$udid" ]]; then
    echo "Could not find available iOS simulator: $simulator_name" >&2
    return 1
  fi
  while true; do
    local pid rss
    pid="$(
      xcrun simctl spawn "$udid" launchctl list 2>/dev/null |
        awk '$3 ~ /org\.reactjs\.native\.example\.SurrealDbHarnessRN86/ {print $1; exit}' || true
    )"
    rss=""
    if [[ "$pid" =~ ^[0-9]+$ ]]; then
      rss="$(ps -o rss= -p "$pid" 2>/dev/null | awk '{print $1}' || true)"
    fi
    if [[ "$rss" =~ ^[0-9]+$ ]]; then
      printf '%s\t%s\n' "$(date +%s000)" "$rss" >> "$SAMPLES_PATH"
    fi
    sleep 0.1
  done
}

if [[ "$PLATFORM" == "android" ]]; then
  pnpm exec react-native run-android \
    --appId com.surrealdbharness.rn86 \
    --no-packager
  sample_android &
else
  pnpm run prepare:ios
  pnpm exec react-native run-ios \
    --scheme SurrealDbHarness \
    --simulator "${SURREALDB_IOS_SIMULATOR:-iPhone 17 Pro}" \
    --no-packager
  sample_ios &
fi
SAMPLER_PID=$!

pnpm exec react-native-harness \
  --harnessRunner "$PLATFORM" \
  --testTimeout 120000 \
  --testPathPatterns surrealkv-churn

cleanup
SAMPLER_PID=""
node "$HARNESS_DIR/scripts/summarize-rss.mjs" "$SAMPLES_PATH" "$REPORT_PATH"
echo "PASS SurrealKV churn on $PLATFORM; RSS report: $REPORT_PATH"
