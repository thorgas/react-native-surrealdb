#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HARNESS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$HARNESS_DIR/../.." && pwd)"
FRAMEWORK_DIR="$REPO_ROOT/packages/react-native-surrealdb/SurrealDbRnFramework.xcframework"
DEVICE_LIBRARY="$FRAMEWORK_DIR/ios-arm64/libsurrealdb_rn_core.a"
SIMULATOR_LIBRARY="$FRAMEWORK_DIR/ios-arm64_x86_64-simulator/libsurrealdb_rn_core.a"
IOS_BUNDLE="$HARNESS_DIR/dist/main.ios.jsbundle"
IOS_ASSETS="$HARNESS_DIR/dist/assets"
PODS_FRAMEWORK_SCRIPT="$HARNESS_DIR/ios/Pods/Target Support Files/Surrealdb/Surrealdb-xcframeworks.sh"
POD_LOCK="$HARNESS_DIR/ios/Podfile.lock"

if [[ ! -f "$DEVICE_LIBRARY" || ! -f "$SIMULATOR_LIBRARY" ]]; then
  echo "Building the SurrealDB iOS XCFramework (first run can take several minutes)..."
  (
    cd "$REPO_ROOT"
    pnpm --filter react-native-surrealdb run ubrn:ios
  )
else
  echo "Using the existing SurrealDB iOS XCFramework."
fi

# React Native Test App exposes these paths as CocoaPods resources. They must
# exist before the first pod install even when Metro serves the app in debug.
if [[ ! -f "$IOS_BUNDLE" || ! -d "$IOS_ASSETS" ]]; then
  echo "Generating React Native Test App iOS bundle resources..."
  (
    cd "$HARNESS_DIR"
    pnpm run build:ios
  )
fi

# A pod install performed before the ignored XCFramework exists creates a Pods
# project that compiles the bridge but never links Rust. Reinstall in that case.
if [[ ! -f "$PODS_FRAMEWORK_SCRIPT" ]] ||
  ! grep -q "SurrealDbRnFramework.xcframework" "$PODS_FRAMEWORK_SCRIPT" ||
  [[ ! -f "$POD_LOCK" ]] ||
  ! grep -q "  - op-sqlite (" "$POD_LOCK"; then
  echo "Installing CocoaPods with the SurrealDB XCFramework available..."
  (
    cd "$HARNESS_DIR/ios"
    bundle exec pod install
  )
else
  echo "CocoaPods already references the SurrealDB iOS XCFramework."
fi
