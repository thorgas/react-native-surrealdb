#!/usr/bin/env bash
set -euo pipefail

HARNESS_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME_ROOT="$(cd -- "$HARNESS_DIR/../.." && pwd)"
DEFAULT_DEV_REPO="$(cd -- "$RUNTIME_ROOT/../.." && pwd)/surrealdb-sync-engine.dev"
DEV_REPO="${SYNC_ENGINE_DEV_REPO:-$DEFAULT_DEV_REPO}"
SECRETS_FILE="$DEV_REPO/.local-dev/env"
TOKEN_MODULE="$RUNTIME_ROOT/apps/harness-shared/e2e/local-authority-token.generated.js"
PLATFORM="${1:-}"

cleanup() {
  rm -f -- "$TOKEN_MODULE"
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

cd "$HARNESS_DIR"
./node_modules/.bin/react-native-harness \
  --config jest.local-authority.config.mjs \
  --harnessRunner "$PLATFORM" \
  --runTestsByPath e2e/local-authority.harness.ts
