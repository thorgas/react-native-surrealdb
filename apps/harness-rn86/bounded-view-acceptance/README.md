# Bounded native view Android acceptance

Synthetic development-harness acceptance only. This does not establish product startup latency, iOS performance, backend freshness or release readiness. The configured physical device model is Google Pixel 6a. The runner refuses multiple authorized Android devices or a missing/mismatched explicit serial. Keep one device actor during a run.

Use the repository-pinned Node/pnpm and existing frozen dependencies. A matching arm64 native SDK ABI, generated bindings and RN 0.86 APK must already be built and reviewed. Install that exact APK with Mobile Dev `mobile_install_app` on the selected physical device. Its manifest MUST use only `com.surrealdbharness.boundedviews20261023`; never substitute the main, QA or existing RN86 harness package. Record APK/library/engine hashes and installed APK readback independently. The scripts do not install or uninstall packages.

From the runtime repository root, with the approved serial selected:

```sh
export ANDROID_HOME="$HOME/Library/Android/sdk"
export BOUNDED_DEVICE_SERIAL='<approved selected device serial>'
# PATH must already select the repository-pinned Node version.
python3 apps/harness-rn86/bounded-view-acceptance/scripts/run-device.py
BOUNDED_PHASE=locale python3 apps/harness-rn86/bounded-view-acceptance/scripts/run-device.py
```

Each invocation creates its evidence directory under `output/bounded-view-android-20261023/device-run-*`, including a fresh baseline, PID samples, HTTP collector reports, exact server-emitted Metro responses, Jest result and cleanup receipt. No prior evidence files are required. Existing host listeners on 19061/19062 or device reverse mappings on 8080/19061/19062 cause refusal. Other reverse mappings remain untouched. The runner records the exact prior `hide_error_dialogs` setting and restores it (including deleting an originally absent setting), removes only mappings it created that still match, and closes its collector/Metro process group. Review cleanup errors; a changed mapping is preserved and reported.

Before a measured replay, freeze hashes of these sources/configs/scripts, shared imported modules, SDK sources/generated bindings, lockfile, native libraries and APK. Do not edit or format them until post-run equality is verified. Harness generates `.harness/manifest.js`; preserve that separately. The primary test sends `ready`, waits 30 seconds, executes synthetic native cases and five counterbalanced warm bridge comparisons, sends `report`, then holds the process for 30 seconds before closing handles. Attach Mobile Dev CPU/RSS/display-FPS recording during the first hold, confirm recording before workload, finish during the last hold and render the result. Profile setup and fixture creation separately from warm timers. The locale phase is separate and quick; its first collator construction is measured independently.

Metro capture retains bootstrap and `modulesOnly=true` test responses. Validate status 200, finished responses, no failure receipt, caps (16 responses, 64 MiB each), hashes, expected URLs and matching PID/Jest success. Prewarm responses are marked separately. Multipart bootstrap bytes include progress messages: retain raw bytes and extract/hash the JavaScript MIME part separately. Inspect content-encoding before describing reconstructed evaluated text. These are server-emitted HTTP bytes, not independent device-side hashes. Capture failures do not intentionally interrupt unrelated HTTP responses; they invalidate acceptance evidence.

After tests, verify handles closed, process stayed alive through terminal reports, collector/Metro stopped, original setting restored and owned reverse mappings removed. Uninstall ONLY the new synthetic package through the approved device tooling and verify absence; leave all other packages/data intact. No backend or paid resources are used.

Focused host checks (no device action):

```sh
node --test apps/harness-rn86/bounded-view-acceptance/scripts/capture-metro.test.cjs
node --test apps/harness-rn86/bounded-view-acceptance/scripts/close-owned.test.cjs
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s apps/harness-rn86/bounded-view-acceptance/scripts -p '*_test.py'
pnpm --filter surrealdb-harness-rn86 typecheck
```

The follow-up response-overload and ownership fixes have host regression coverage; they were not used in the recorded device replay. Exact exercised old helper scripts are retained in the corresponding ignored evidence directory. Keep `output/**`, `.harness/**`, staged Android projects, dependency backups and build caches out of source commits.

Failure cleanup independently closes acquired sibling/sync/database handles in reverse lifetime order and reports all close failures. Host regressions cover partial acquisition and sibling-close failure. Runner shutdown waits for its own process group, escalates TERM to KILL within bounded deadlines and waits for owned ports to settle; it never derives a kill target from an occupied port. Port closure is a success gate. Device restore/readback failures are collected independently and the cleanup receipt is persisted even when another cleanup action fails. These follow-ups are host-tested, not device-measured.
