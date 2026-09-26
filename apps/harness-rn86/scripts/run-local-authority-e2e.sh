#!/usr/bin/env bash
set -euo pipefail

HARNESS_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME_ROOT="$(cd -- "$HARNESS_DIR/../.." && pwd)"
DEFAULT_DEV_REPO="$(cd -- "$RUNTIME_ROOT/../.." && pwd)/surrealdb-sync-engine.dev"
DEV_REPO="${SYNC_ENGINE_DEV_REPO:-$DEFAULT_DEV_REPO}"
SECRETS_FILE="$DEV_REPO/.local-dev/env"
TOKEN_MODULE="$RUNTIME_ROOT/apps/harness-shared/e2e/local-authority-token.generated.js"
PLATFORM="${1:-}"
RESULTS_DIR="$HARNESS_DIR/performance-results/$PLATFORM/local-authority"
REPORT_PATH="$RESULTS_DIR/reports.jsonl"
REPORT_RECEIVER_PID=""

cleanup() {
  rm -f -- "$TOKEN_MODULE"
  if [[ -n "$REPORT_RECEIVER_PID" ]]; then
    kill "$REPORT_RECEIVER_PID" >/dev/null 2>&1 || true
    wait "$REPORT_RECEIVER_PID" 2>/dev/null || true
  fi
}

if [[ "$PLATFORM" != "android" && "$PLATFORM" != "ios" ]]; then
  echo "Usage: $0 <android|ios>" >&2
  exit 2
fi
if [[ ! -f "$SECRETS_FILE" ]]; then
  echo "Local sync secrets are unavailable; start the stack in $DEV_REPO first." >&2
  exit 1
fi

if ENV_MODE="$(stat -f '%Lp' "$SECRETS_FILE" 2>/dev/null)"; then
  :
else
  ENV_MODE="$(stat -c '%a' "$SECRETS_FILE")"
fi
if [[ "$ENV_MODE" != "600" ]]; then
  echo "Local sync secrets must have mode 600: $SECRETS_FILE" >&2
  exit 1
fi

SYNC_DEV_TOKEN="$(sed -n 's/^SYNC_DEV_TOKEN=//p' "$SECRETS_FILE")"
if [[ ! "$SYNC_DEV_TOKEN" =~ ^[0-9a-f]{64}$ ]]; then
  echo "SYNC_DEV_TOKEN must be exactly 64 lowercase hexadecimal characters." >&2
  exit 1
fi
if ! curl --fail --silent --show-error \
  "http://127.0.0.1:18091/healthz" >/dev/null; then
  echo "Local sync gateway is not healthy at http://127.0.0.1:18091." >&2
  exit 1
fi

RUN_SUFFIX="r$(od -An -N 12 -tx1 /dev/urandom | tr -d '[:space:]')"
umask 077
trap cleanup EXIT
printf '"use strict";\nmodule.exports = { runSuffix: "%s", syncDevToken: "%s" };\n' \
  "$RUN_SUFFIX" "$SYNC_DEV_TOKEN" >"$TOKEN_MODULE"
chmod 600 "$TOKEN_MODULE"

mkdir -p "$RESULTS_DIR"
node "$HARNESS_DIR/scripts/receive-performance-report.mjs" \
  --output="$REPORT_PATH" --append >"$RESULTS_DIR/report-receiver.log" 2>&1 &
REPORT_RECEIVER_PID=$!
for _ in {1..50}; do
  if curl --fail --silent "http://127.0.0.1:18082/health" >/dev/null; then
    break
  fi
  sleep 0.1
done
if ! kill -0 "$REPORT_RECEIVER_PID" >/dev/null 2>&1 ||
  ! curl --fail --silent "http://127.0.0.1:18082/health" >/dev/null; then
  echo "Local authority latency report receiver failed to start." >&2
  exit 1
fi

cd "$HARNESS_DIR"
./node_modules/.bin/react-native-harness \
  --config jest.local-authority.config.mjs \
  --harnessRunner "$PLATFORM" \
  --runTestsByPath e2e/local-authority.harness.ts
echo "Local authority timing samples: $REPORT_PATH"
