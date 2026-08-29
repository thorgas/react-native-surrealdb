#!/usr/bin/env bash
set -euo pipefail

HARNESS_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
REPOSITORY_ROOT="$(cd -- "$HARNESS_DIR/../.." && pwd)"
PACKAGE_ROOT="$REPOSITORY_ROOT/packages/react-native-surrealdb"
SEED_REVISION="e0c200b"
PLATFORM="${1:-}"
PNPM="${PNPM_BIN:-$(command -v pnpm || true)}"
TEMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/surrealdb-rn-migration.XXXXXX")"
SEED_WORKTREE="$TEMP_ROOT/seed-3.2.1"
CURRENT_ARTIFACT_INSTALLED=1
IOS_BUNDLE_ID="org.reactjs.native.example.SurrealDbHarnessRN86"
ANDROID_APP_ID="com.surrealdbharness.rn86"

usage() {
  echo "Usage: $0 <android|ios>" >&2
}

build_native() {
  local repository="$1"
  local platform="$2"
  (
    cd "$repository"
    "$PNPM" --filter react-native-surrealdb run "ubrn:$platform"
  )
}

replace_artifact() {
  local source_root="$1"
  case "$PLATFORM" in
    ios)
      rm -rf -- "$PACKAGE_ROOT/SurrealDbRnFramework.xcframework"
      cp -R "$source_root/packages/react-native-surrealdb/SurrealDbRnFramework.xcframework" \
        "$PACKAGE_ROOT/SurrealDbRnFramework.xcframework"
      ;;
    android)
      rm -rf -- "$PACKAGE_ROOT/android/src/main/jniLibs"
      cp -R "$source_root/packages/react-native-surrealdb/android/src/main/jniLibs" \
        "$PACKAGE_ROOT/android/src/main/jniLibs"
      ;;
  esac
}

install_host() {
  case "$PLATFORM" in
    ios)
      (
        cd "$HARNESS_DIR"
        "$PNPM" run prepare:ios
        "$PNPM" exec react-native run-ios \
          --scheme SurrealDbHarness \
          --simulator "${SURREALDB_IOS_SIMULATOR:-iPhone 17 Pro}" \
          --no-packager
      )
      ;;
    android)
      (
        cd "$HARNESS_DIR"
        "$PNPM" exec react-native run-android \
          --appId com.surrealdbharness.rn86 \
          --no-packager
      )
      ;;
  esac
}

run_phase() {
  local pattern="$1"
  (
    cd "$HARNESS_DIR"
    "$PNPM" exec react-native-harness \
      --harnessRunner "$PLATFORM" \
      --testPathPatterns "$pattern"
  )
}

clear_previous_app_data() {
  case "$PLATFORM" in
    ios)
      local simulator="${SURREALDB_IOS_SIMULATOR:-iPhone 17 Pro}"
      xcrun simctl boot "$simulator" >/dev/null 2>&1 || true
      xcrun simctl bootstatus "$simulator" -b
      xcrun simctl uninstall "$simulator" "$IOS_BUNDLE_ID" >/dev/null 2>&1 || true
      ;;
    android)
      adb wait-for-device
      adb shell pm clear "$ANDROID_APP_ID" >/dev/null 2>&1 || true
      ;;
  esac
}

restore_current_artifact() {
  if (( CURRENT_ARTIFACT_INSTALLED == 0 )); then
    echo "Restoring the current SurrealDB 3.2.4 native artifact after an interrupted run..." >&2
    build_native "$REPOSITORY_ROOT" "$PLATFORM" || \
      echo "Failed to restore the current native artifact; rebuild ubrn:$PLATFORM manually." >&2
  fi
}

cleanup() {
  restore_current_artifact
  git -C "$REPOSITORY_ROOT" worktree remove --force "$SEED_WORKTREE" >/dev/null 2>&1 || true
  rmdir "$TEMP_ROOT" >/dev/null 2>&1 || true
}

trap cleanup EXIT

if [[ "$PLATFORM" != "android" && "$PLATFORM" != "ios" ]]; then
  usage
  exit 2
fi
if [[ -z "$PNPM" || ! -x "$PNPM" ]]; then
  echo "pnpm is unavailable; use the repository-pinned package manager." >&2
  exit 1
fi
NODE_MAJOR="$(node -p 'Number(process.versions.node.split(".")[0])')"
if (( NODE_MAJOR < 20 || NODE_MAJOR >= 23 )); then
  echo "Node $(node --version) is unsupported; run this command with Node 22.22.0 from .node-version." >&2
  exit 1
fi
if [[ ! -d "$REPOSITORY_ROOT/node_modules" ]]; then
  echo "Run pnpm install --frozen-lockfile from $REPOSITORY_ROOT first." >&2
  exit 1
fi
git -C "$REPOSITORY_ROOT" cat-file -e "$SEED_REVISION^{commit}"
if ! git -C "$REPOSITORY_ROOT" show \
  "$SEED_REVISION:crates/surrealdb-rn-core/Cargo.toml" | \
  grep -Fq 'surrealdb = { version = "=3.2.1"'; then
  echo "Pinned seed revision no longer proves SurrealDB 3.2.1." >&2
  exit 1
fi
if ! grep -Fq 'surrealdb = { version = "=3.2.4"' \
  "$REPOSITORY_ROOT/crates/surrealdb-rn-core/Cargo.toml"; then
  echo "Current runtime is not pinned to SurrealDB 3.2.4." >&2
  exit 1
fi

git -C "$REPOSITORY_ROOT" worktree add --detach "$SEED_WORKTREE" "$SEED_REVISION"
(
  cd "$SEED_WORKTREE"
  "$PNPM" install --frozen-lockfile
)

echo "Building the pinned SurrealDB 3.2.1 seed artifact for $PLATFORM..."
build_native "$SEED_WORKTREE" "$PLATFORM"
replace_artifact "$SEED_WORKTREE"
CURRENT_ARTIFACT_INSTALLED=0
clear_previous_app_data
install_host
run_phase surrealkv-seed

echo "Replacing the native runtime with SurrealDB 3.2.4 without clearing app data..."
build_native "$REPOSITORY_ROOT" "$PLATFORM"
CURRENT_ARTIFACT_INSTALLED=1
install_host
run_phase surrealkv-restart

echo "PASS SurrealDB 3.2.1 seed -> 3.2.4 reopen on $PLATFORM"
