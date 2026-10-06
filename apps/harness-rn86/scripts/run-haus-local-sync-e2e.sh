#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/$(basename -- "${BASH_SOURCE[0]}")"
HARNESS_DIR="$(cd -- "$(dirname -- "$SCRIPT_PATH")/.." && pwd)"
RUNTIME_ROOT="$(cd -- "$HARNESS_DIR/../.." && pwd)"
WORKSPACE_ROOT="$(cd -- "$RUNTIME_ROOT/.." && pwd)"
HAUS_ROOT="$WORKSPACE_ROOT/hauswirtschaft-local-sync"
FIXTURE_SCRIPT="$HAUS_ROOT/scripts/local-household-sync-fixture.ts"
GENERATED_MODULE="$RUNTIME_ROOT/apps/harness-shared/e2e/haus-sync-e2e.generated.js"
RESULTS_DIR="$HARNESS_DIR/performance-results/${1:-}/haus-local-sync"
PLATFORM="${1:-}"
FIXTURE_PATH=""
TEMP_ROOT=""
METRO_PID=""

start_metro() {
  local phase="$1"
  local log_path="$RESULTS_DIR/metro-$phase.log"
  if curl --fail --silent "http://127.0.0.1:8081/status" >/dev/null; then
    echo "A Metro server is already using port 8081; stop it before this E2E to avoid stale test modules." >&2
    exit 1
  fi
  echo "Starting a fresh RN 0.86 Metro server for $phase phase with cache reset..."
  (
    cd "$HARNESS_DIR"
    exec ./node_modules/.bin/react-native start --reset-cache --host 0.0.0.0 --port 8081
  ) >"$log_path" 2>&1 &
  METRO_PID=$!
  for _ in {1..120}; do
    if curl --fail --silent "http://127.0.0.1:8081/status" | rg -q 'packager-status:running'; then
      return
    fi
    if ! kill -0 "$METRO_PID" >/dev/null 2>&1; then
      echo "Metro exited before becoming ready; log: $log_path" >&2
      tail -n 120 "$log_path" >&2 || true
      exit 1
    fi
    sleep 1
  done
  echo "Metro did not become ready; log: $log_path" >&2
  tail -n 120 "$log_path" >&2 || true
  exit 1
}

stop_metro() {
  if [[ -n "$METRO_PID" ]]; then
    kill "$METRO_PID" >/dev/null 2>&1 || true
    wait "$METRO_PID" 2>/dev/null || true
    METRO_PID=""
  fi
}

cleanup() {
  rm -f -- "$GENERATED_MODULE"
  stop_metro
  if [[ -n "$FIXTURE_PATH" && -f "$FIXTURE_PATH" ]]; then
    (
      cd "$HAUS_ROOT"
      SURREALDB_ENDPOINT="ws://127.0.0.1:18881" \
        HAUS_SYNC_ISOLATED_E2E=1 DEPLOYMENT_ENV=local \
        "${BUN_BIN:-bun}" "$FIXTURE_SCRIPT" cleanup "$FIXTURE_PATH"
    ) || echo "Fixture cleanup failed; remove the disposable fixture with the same cleanup command." >&2
  fi
  if [[ -n "$TEMP_ROOT" && -d "$TEMP_ROOT" ]]; then
    rmdir "$TEMP_ROOT" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

usage() {
  echo "Usage: $0 <android|ios>" >&2
}

if [[ "$PLATFORM" != "android" && "$PLATFORM" != "ios" ]]; then
  usage
  exit 2
fi
PINNED_NODE="$(tr -d '[:space:]' <"$RUNTIME_ROOT/.node-version")"
if [[ "$(node --version)" != "v$PINNED_NODE" ]]; then
  if [[ "${HAUS_RN86_NODE_PINNED:-}" == "1" ]]; then
    echo "fnm did not activate required Node v$PINNED_NODE (current: $(node --version))." >&2
    exit 1
  fi
  FNM_BIN="${FNM_BIN:-$(command -v fnm || true)}"
  if [[ -n "$FNM_BIN" ]] && "$FNM_BIN" exec --using="$PINNED_NODE" node --version >/dev/null 2>&1; then
    exec "$FNM_BIN" exec --using="$PINNED_NODE" env \
      HAUS_RN86_NODE_PINNED=1 "$SCRIPT_PATH" "$PLATFORM"
  fi
  echo "This harness requires Node v$PINNED_NODE (rn-runtime/.node-version); current is $(node --version)." >&2
  echo "Install it with fnm, then rerun this script." >&2
  exit 1
fi
if [[ ! -f "$FIXTURE_SCRIPT" ]]; then
  echo "Haus local fixture CLI is missing: $FIXTURE_SCRIPT" >&2
  exit 1
fi
if ! command -v "${BUN_BIN:-bun}" >/dev/null 2>&1; then
  echo "Bun is required to create and revoke disposable local household fixtures." >&2
  exit 1
fi
if [[ ! -x "$HARNESS_DIR/node_modules/.bin/react-native-harness" ]]; then
  echo "Install rn-runtime dependencies and build/install the RN 0.86 harness first." >&2
  exit 1
fi
if ! curl --fail --silent --show-error \
  "http://127.0.0.1:18092/healthz" >/dev/null; then
  echo "Haus local sync authority is not healthy at http://127.0.0.1:18092." >&2
  exit 1
fi

mkdir -p "$RESULTS_DIR"
rm -f -- "$RESULTS_DIR/active-test.log" "$RESULTS_DIR/revoked-test.log"
TEMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/haus-local-sync-e2e.XXXXXX")"
FIXTURE_PATH="$TEMP_ROOT/fixture.json"
umask 077
(
  cd "$HAUS_ROOT"
  SURREALDB_ENDPOINT="ws://127.0.0.1:18881" \
    HAUS_SYNC_ISOLATED_E2E=1 DEPLOYMENT_ENV=local \
    "${BUN_BIN:-bun}" "$FIXTURE_SCRIPT" create "$FIXTURE_PATH"
)
if [[ "$(stat -f '%Lp' "$FIXTURE_PATH" 2>/dev/null || stat -c '%a' "$FIXTURE_PATH")" != "600" ]]; then
  echo "Fixture credentials must have mode 600: $FIXTURE_PATH" >&2
  exit 1
fi

# Check the fixture token against the exact local authority before Metro/native
# transport. The token and response body remain inside Node and are never logged.
node --input-type=module - "$FIXTURE_PATH" <<'NODE'
import { readFileSync } from 'node:fs';
const fixture = JSON.parse(readFileSync(process.argv[2], 'utf8'));
if (fixture.marker !== 'haus-local-sync-fixture-v1' || typeof fixture.ownerToken !== 'string') {
  throw new Error('Local household fixture is missing its owner token.');
}
const response = await fetch('http://127.0.0.1:18092/v1/sync/pull', {
  method: 'POST',
  headers: {
    Accept: 'application/vnd.surrealdb-sync+cbor',
    Authorization: `Bearer ${fixture.ownerToken}`,
    'Content-Type': 'application/vnd.surrealdb-sync+cbor',
  },
  body: new Uint8Array([0]),
});
console.info(`[Haus Sync E2E] host owner auth preflight: HTTP ${response.status}`);
if (response.status !== 400) {
  throw new Error(`Expected malformed-CBOR auth preflight HTTP 400, got ${response.status}.`);
}
NODE

write_test_module() {
  local phase="$1"
  node --input-type=module - "$FIXTURE_PATH" "$GENERATED_MODULE" "$phase" <<'NODE'
import { chmodSync, readFileSync, writeFileSync } from 'node:fs';
const [source, destination, phase] = process.argv.slice(2);
const fixture = JSON.parse(readFileSync(source, 'utf8'));
if (
  fixture.marker !== 'haus-local-sync-fixture-v1' ||
  !/^[0-9a-f-]{36}$/.test(fixture.runSuffix) ||
  !/^[0-9a-f-]{36}$/.test(fixture.recipeId) ||
  fixture.partitionId !== `household:${fixture.householdId}` ||
  fixture.requestedScope !== 'recipes-v1' ||
  fixture.subscriptionRevision !== 1 ||
  typeof fixture.ownerToken !== 'string' || fixture.ownerToken.length < 16 ||
  typeof fixture.memberToken !== 'string' || fixture.memberToken.length < 16 ||
  !['active', 'revoked'].includes(phase)
) {
  throw new Error('Local household fixture has an invalid schema or mismatched scope.');
}
fixture.phase = phase;
writeFileSync(destination, `module.exports = ${JSON.stringify(fixture)};\n`, { mode: 0o600 });
chmodSync(destination, 0o600);
NODE
}

probe_partition_head() {
  (
    cd "$HAUS_ROOT"
    SURREALDB_ENDPOINT="ws://127.0.0.1:18881" \
      HAUS_SYNC_ISOLATED_E2E=1 DEPLOYMENT_ENV=local \
      HAUS_SYNC_FIXTURE_PATH="$FIXTURE_PATH" "${BUN_BIN:-bun}" -e '
        import { readFileSync } from "node:fs";
        import { Surreal } from "surrealdb";
        const fixture = JSON.parse(readFileSync(process.env.HAUS_SYNC_FIXTURE_PATH, "utf8"));
        const db = new Surreal();
        try {
          await db.connect(process.env.SURREALDB_ENDPOINT, {
            namespace: "hauswirtschaft",
            database: "comparison",
            authentication: { username: "root", password: "root" },
          });
          const [heads] = await db.query(
            "SELECT VALUE head FROM sync_partition WHERE partition_id = $partition LIMIT 1",
            { partition: fixture.partitionId },
          );
          const [sequences] = await db.query(
            "SELECT VALUE sequence FROM sync_log WHERE partition_id = $partition ORDER BY sequence",
            { partition: fixture.partitionId },
          );
          const rows = sequences ?? [];
          console.info(`[Haus Sync E2E] trusted admin partition head=${String(heads?.[0] ?? "missing")} sync_log_count=${rows.length} max_sequence=${String(rows.at(-1) ?? "none")}`);
        } catch {
          console.info("[Haus Sync E2E] trusted admin partition-head probe failed");
          process.exitCode = 1;
        } finally {
          await db.close().catch(() => {});
        }
      '
  )
}

run_native_phase() {
  local phase="$1"
  local result_log="$RESULTS_DIR/$phase-test.log"
  local test_status=0
  echo "Running native $phase phase; redacted result log: $result_log"
  if (
    cd "$HARNESS_DIR"
    ./node_modules/.bin/react-native-harness \
      --config scripts/jest.haus-local-sync.config.mjs \
      --harnessRunner "$PLATFORM" \
      --runTestsByPath e2e/haus-local-sync.harness.ts
  ) 2>&1 | node --input-type=module -e '
    import { readFileSync } from "node:fs";
    const fixture = JSON.parse(readFileSync(process.argv[1], "utf8"));
    const secrets = [fixture.ownerToken, fixture.memberToken].filter(Boolean);
    let output = "";
    for await (const chunk of process.stdin) output += chunk.toString();
    for (const secret of secrets) output = output.replaceAll(secret, "[REDACTED]");
    process.stdout.write(output);
  ' "$FIXTURE_PATH" | tee "$result_log"; then
    test_status=0
  else
    test_status=$?
  fi
  if [[ "$phase" == "active" ]]; then
    probe_partition_head 2>&1 | tee -a "$result_log" || true
  fi
  return "$test_status"
}

IOS_SIMULATOR_NAME="${SURREALDB_IOS_SIMULATOR_NAME:-iPhone 17 Pro (26.1)}"
IOS_SIMULATOR_UDID="${SURREALDB_IOS_SIMULATOR:-}"
SKIP_IOS_BUILD_INSTALL="${HAUS_SYNC_E2E_SKIP_IOS_BUILD_INSTALL:-0}"
write_test_module active
if [[ "$PLATFORM" == "ios" ]]; then
  if [[ "$SKIP_IOS_BUILD_INSTALL" != "1" ]]; then
    echo "Preparing the RN 0.86 iOS harness with Node v$PINNED_NODE..."
    (
      cd "$HARNESS_DIR"
      pnpm run prepare:ios
    )
  else
    echo "Skipping iOS prepare/build/install; using the already installed harness."
  fi
  start_metro active
  if [[ "$SKIP_IOS_BUILD_INSTALL" != "1" ]]; then
    echo "Building and installing the iOS harness on $IOS_SIMULATOR_NAME${IOS_SIMULATOR_UDID:+ ($IOS_SIMULATOR_UDID)}..."
    (
      cd "$HARNESS_DIR"
      RUN_IOS_ARGS=(
        pnpm exec react-native run-ios
        --scheme SurrealDbHarness
        --simulator "$IOS_SIMULATOR_NAME"
      )
      if [[ -n "$IOS_SIMULATOR_UDID" ]]; then
        RUN_IOS_ARGS+=(--udid "$IOS_SIMULATOR_UDID")
      fi
      RUN_IOS_ARGS+=(--no-packager)
      "${RUN_IOS_ARGS[@]}"
    )
  fi
fi

run_native_phase active
if [[ "$PLATFORM" == "ios" ]]; then
  stop_metro
fi
(
  cd "$HAUS_ROOT"
  SURREALDB_ENDPOINT="ws://127.0.0.1:18881" \
    HAUS_SYNC_ISOLATED_E2E=1 DEPLOYMENT_ENV=local \
    "${BUN_BIN:-bun}" "$FIXTURE_SCRIPT" revoke "$FIXTURE_PATH"
)
write_test_module revoked
if [[ "$PLATFORM" == "ios" ]]; then
  start_metro revoked
fi
run_native_phase revoked

echo "PASS Hauswirtschaft create/pull, simultaneous conflict, and post-revocation denial ($PLATFORM)."
echo "Redacted phase logs: $RESULTS_DIR/active-test.log and $RESULTS_DIR/revoked-test.log"
echo "No UI screenshot is produced: this non-UI harness returns the simulator to its home screen."
