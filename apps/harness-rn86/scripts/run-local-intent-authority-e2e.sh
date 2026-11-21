#!/usr/bin/env bash
set -euo pipefail

HARNESS_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="$(cd -- "$HARNESS_DIR/../.." && pwd)"
CONFIG_MODULE="$ROOT/apps/harness-shared/e2e/local-intent-authority.generated.js"
PLATFORM="${1:-}"

if [[ "$PLATFORM" != "android" && "$PLATFORM" != "ios" ]]; then
  echo "Usage: $0 <android|ios>" >&2
  exit 2
fi
if [[ -z "${SDK_INTENT_AUTHORITY_BINARY:-}" || ! -x "$SDK_INTENT_AUTHORITY_BINARY" ]]; then
  echo "SDK_INTENT_AUTHORITY_BINARY must name a prepared executable with source-v2 support." >&2
  exit 2
fi
for name in SDK_INTENT_DB_ADDRESS SDK_INTENT_DB_USER SDK_INTENT_DB_PASS; do
  if [[ -z "${!name:-}" ]]; then
    echo "$name is required for a task-owned authority database." >&2
    exit 2
  fi
done
if [[ -e "$CONFIG_MODULE" ]]; then
  echo "A prior local intent harness config exists; inspect it before another run." >&2
  exit 2
fi

umask 077
TASK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/sdk-intent-authority.XXXXXXXX")"
AUTHORITY_PID=""
cleanup() {
  rm -f -- "$CONFIG_MODULE"
  if [[ -n "$AUTHORITY_PID" ]]; then
    kill "$AUTHORITY_PID" >/dev/null 2>&1 || true
    wait "$AUTHORITY_PID" 2>/dev/null || true
  fi
  rm -rf -- "$TASK_DIR"
}
trap cleanup EXIT

RUN_SUFFIX="$(od -An -N 12 -tx1 /dev/urandom | tr -d '[:space:]')"
PARTITION_ID="sdk-intent-$RUN_SUFFIX"
TOKEN="$(od -An -N 32 -tx1 /dev/urandom | tr -d '[:space:]')"
TOKEN_HASH="$(printf '%s' "$TOKEN" | shasum -a 256 | awk '{print $1}')"
PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
MANIFEST="$TASK_DIR/authority.json"
printf '{"version":1,"authorizationRevision":1,"workspaces":[{"id":"workspace","partitionId":"%s","scopeIdentity":"all","subscriptionRevision":1,"active":true}],"principals":[{"id":"intent-harness"}],"memberships":[{"principalId":"intent-harness","workspaceId":"workspace","active":true,"revision":1}],"credentials":[{"id":"intent-harness","tokenHash":"%s","principalId":"intent-harness","workspaceId":"workspace","expiresAt":null,"revokedAt":null}]}\n' \
  "$PARTITION_ID" "$TOKEN_HASH" >"$MANIFEST"
printf '1 %s\n' "$(shasum -a 256 "$MANIFEST" | awk '{print $1}')" >"$MANIFEST.revision"
chmod 600 "$MANIFEST" "$MANIFEST.revision"

if [[ "$PLATFORM" == "android" ]]; then
  DEVICE_HOST="10.0.2.2"
else
  DEVICE_HOST="127.0.0.1"
fi
printf '"use strict";\nmodule.exports = { authorityUrl: "http://%s:%s", token: "%s", partitionId: "%s", runSuffix: "%s" };\n' \
  "$DEVICE_HOST" "$PORT" "$TOKEN" "$PARTITION_ID" "$RUN_SUFFIX" >"$CONFIG_MODULE"
chmod 600 "$CONFIG_MODULE"

SURREALDB_ADAPTER_ADDRESS="$SDK_INTENT_DB_ADDRESS" \
SURREALDB_ADAPTER_USER="$SDK_INTENT_DB_USER" \
SURREALDB_ADAPTER_PASS="$SDK_INTENT_DB_PASS" \
SURREALDB_ADAPTER_NAMESPACE="sdk_intent_$RUN_SUFFIX" \
SURREALDB_ADAPTER_DATABASE="e2e" \
SURREALDB_AUTHORITY_BIND="127.0.0.1:$PORT" \
SYNC_DEV_AUTHORITY_MANIFEST="$MANIFEST" \
  "$SDK_INTENT_AUTHORITY_BINARY" serve >"$TASK_DIR/authority.log" 2>&1 &
AUTHORITY_PID=$!

authority_ready() {
  # A fresh authority has no scanner health row until its first authenticated
  # pull. Its health route returns 503 in that state, then the pull runs a scan.
  local status
  status="$(curl --max-time 2 --silent --output /dev/null --write-out '%{http_code}' "http://127.0.0.1:$PORT/healthz")" || return 1
  [[ "$status" == "200" || "$status" == "503" ]]
}

for _ in {1..100}; do
  if authority_ready; then
    break
  fi
  if ! kill -0 "$AUTHORITY_PID" 2>/dev/null; then
    echo "Task-owned authority exited before health check; inspect source and database configuration." >&2
    exit 1
  fi
  sleep 0.1
done
if ! authority_ready; then
  echo "Task-owned authority did not become healthy." >&2
  exit 1
fi

cd "$HARNESS_DIR"
./node_modules/.bin/react-native-harness \
  --config jest.local-intent-authority.config.mjs \
  --harnessRunner "$PLATFORM" \
  --runTestsByPath e2e/local-intent-authority.harness.ts
