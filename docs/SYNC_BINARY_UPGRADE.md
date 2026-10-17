# SDK binary journal upgrade fixture

This fixture proves task11 SDK binary to task14 SDK binary storage compatibility
on a synthetic app-private SurrealKV journal. It does not prove a Hauswirtschaft
application upgrade, query-enabled format3 downgrade support, or production rollout.

Before each new campaign, change the exported `syncBinaryUpgradeCampaign` in
`apps/harness-shared/sync-binary-upgrade-fixture.ts` to an explicit fresh unique
identifier, then keep it unchanged across both phases and both platforms. The
seed refuses an existing marker, nonempty read snapshot, pending work, or checkpoint.
Never delete, reset, or reuse an unknown journal. A successful seed intentionally
leaves handles open; the runner stops the application process afterward.

The permanent RN86 wrappers select `sync-binary-upgrade-seed` and
`sync-binary-upgrade-verify`. Seed installs a confirmed bigint row, tombstone,
one pending commit and one unresolved conflict. It persists a receipt containing
full read snapshot (including root revision, coverage and row metadata), canonical
pending identity/protocol JSON, conflicts, checkpoint and status. Verify opens the
same path, compares the entire receipt exactly and separately asserts pending1 and
conflict1; it performs no sync-state mutation or cleanup.

## Matching source and binaries

Use preserved task11 apps and exact archived task11 SDK source, never current
bindings against an old binary. Example from repository root:

```sh
mkdir -p /private/tmp/task14-task11-source
git archive e9d5b00 packages/react-native-surrealdb | tar -x -C /private/tmp/task14-task11-source
ln -s "$PWD/packages/react-native-surrealdb/node_modules" /private/tmp/task14-task11-source/packages/react-native-surrealdb/node_modules
```

Back up `apps/harness-rn86/metro.config.js` byte-for-byte. Add task-owned cache
watch folders when required by the isolated checkout, including the archive root.
The harness overrides custom Metro `resolveRequest`; **change the task-owned
`apps/harness-rn86/node_modules/react-native-surrealdb` symlink** to the archived
package for the old phase. Restore it to the current SDK package for the new phase.
Do not modify either SDK source. Record both source SHAs and binary SHA256 hashes.

Use pinned Node22.22.0 on PATH. From `apps/harness-rn86`, run exactly one platform
at a time so each runner owns and releases Metro8081:

```sh
export PATH='/Users/timhorgas/Library/Application Support/fnm/node-versions/v22.22.0/installation/bin':$PATH
SURREALDB_IOS_SIMULATOR_NAME='Hauswirtschaft Sync Staging 20261012' ./node_modules/.bin/react-native-harness --harnessRunner ios --testPathPatterns sync-binary-upgrade-seed
SURREALDB_ANDROID_EMULATOR_NAME=HausSyncStaging20261012 ANDROID_SERIAL=emulator-5554 ANDROID_HOME=/Users/timhorgas/Library/Android/sdk ./node_modules/.bin/react-native-harness --harnessRunner android --testPathPatterns sync-binary-upgrade-seed
```

Before each platform's seed, install the preserved old app with `xcrun simctl
install 42EF5D86-641B-4FD1-871A-4DF6DBCF4A96 OLD.app` or
`adb -s emulator-5554 install -r OLD.apk`. Verify the device identity first.
After a passing seed, confirm the runner stopped the old process; cold-stop it
explicitly if still running. Install the new matching app over the same container
using the same commands, switch the SDK alias to the new source, then run the
corresponding command with `sync-binary-upgrade-verify`. Never uninstall, wipe,
clear data, or use a default emulator selection. Supplementary launch receipts
can record old/new PIDs using targeted `simctl launch` or `adb shell pidof` around
replacement; distinguish these from the harness's own short-lived test PIDs.

Restore the original SDK symlink and Metro bytes, check `git diff --check`, and
confirm owned runners and listeners exited. Keep both generations of artifacts.

## Recorded local campaign

Campaign `task11-to-task14-20261017-01` passed seed1/1 and verify1/1 on both
staging devices. Old source e9d5b00, new source b50a90b. Logs are
`/private/tmp/task14-ios-upgrade-seed-final.log`,
`/private/tmp/task14-ios-upgrade-verify.log`,
`/private/tmp/task14-android-upgrade-seed.log`, and
`/private/tmp/task14-android-upgrade-verify.log`.
Supplementary replacement launch PIDs: iOS15672→15738; Android5555→5617.
The initial old iOS attempt correctly failed before test execution when the
harness resolver accidentally loaded current bindings; the SDK alias correction
was necessary. No journal data was cleared. App binaries and hashes are retained
in `/private/tmp/task14-preserved-task11` and the matching task14 build outputs.
