#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/android-env.sh"

# Reproduces the committed stock RNTA baseline. The package remains installed in
# the workspace, but React Native CLI is instructed to disable only SurrealDB's
# Android autolinking. The complete Android host is moved aside and restored;
# RNTA and Gradle keep autolinking state in both android/app and android/build.
HARNESS_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RESULTS_DIR="$HARNESS_DIR/size-results/android"
ANDROID_DIR="$HARNESS_DIR/android"
BACKUP_DIR="$HARNESS_DIR/android.size-benchmark-backup"
ARCH_PROPERTY="-PreactNativeArchitectures=arm64-v8a"

mkdir -p "$RESULTS_DIR"

restore_candidate_host() {
  rm -rf "$ANDROID_DIR"
  if [[ -d "$BACKUP_DIR" ]]; then
    mv "$BACKUP_DIR" "$ANDROID_DIR"
  fi
}
trap restore_candidate_host EXIT

rm -rf "$BACKUP_DIR"
mv "$ANDROID_DIR" "$BACKUP_DIR"
cp -R "$BACKUP_DIR" "$ANDROID_DIR"
rm -rf "$ANDROID_DIR/app" "$ANDROID_DIR/build" "$ANDROID_DIR/.gradle"

SURREALDB_SIZE_BASELINE=1 "$ANDROID_DIR/gradlew" \
  -p "$HARNESS_DIR/android" \
  --no-daemon \
  --no-build-cache \
  app:assembleRelease \
  "$ARCH_PROPERTY"

cp "$ANDROID_DIR/app/build/outputs/apk/release/app-release.apk" "$RESULTS_DIR/baseline.apk"

for library in libreact-native-surrealdb.so libsurrealdb_rn_core.so; do
  if [[ "$(unzip -Z1 "$RESULTS_DIR/baseline.apk" | grep -c "$library")" -ne 0 ]]; then
    echo "Baseline unexpectedly contains $library" >&2
    exit 1
  fi
done

node -e '
  const { statSync, writeFileSync } = require("node:fs");
  const [apk, output, packageJson] = process.argv.slice(1);
  const pkg = require(packageJson);
  const report = {
    schemaVersion: 1,
    kind: "react-native-test-app-baseline",
    bytes: statSync(apk).size,
    measuredAt: new Date().toISOString(),
    configuration: {
      reactNativeTestApp: pkg.devDependencies["react-native-test-app"],
      reactNative: pkg.dependencies["react-native"],
      javascriptEngine: "Hermes",
      architecture: "arm64-v8a",
      buildType: "Release",
      newArchitecture: true,
      surrealDbAutolinking: false
    },
    command: "pnpm --filter SurrealDbHarness run size:android:baseline",
    source: "apps/harness/react-native.config.js with SURREALDB_SIZE_BASELINE=1"
  };
  writeFileSync(output, `${JSON.stringify(report, null, 2)}\n`);
  console.log(`Stock RNTA baseline: ${(report.bytes / 1024 / 1024).toFixed(2)} MiB`);
' "$RESULTS_DIR/baseline.apk" "$RESULTS_DIR/baseline-report.json" "$HARNESS_DIR/package.json"
