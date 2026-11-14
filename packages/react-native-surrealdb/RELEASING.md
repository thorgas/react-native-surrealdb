# Releasing

Releases use one immutable package tarball from candidate testing through npm
publication. Building a candidate and publishing it are deliberately separate
workflows.

## Release channels

| Package version | GitHub release              | npm tag  | Purpose                                                         |
| --------------- | --------------------------- | -------- | --------------------------------------------------------------- |
| `0.1.0-alpha.3` | Prerelease                  | `next`   | Fix clean iOS TurboModule registration for local sync testing |
| `0.1.0-alpha.2` | Prerelease                  | `next`   | Test the experimental sync client in consuming apps |
| `0.1.0-alpha.1` | Prerelease                  | `next`   | Embedded SurrealDB and native bindings before sync |
| `0.1.0`         | Draft until npm publication | `latest` | Public stable release                                           |

An npm prerelease is still publicly downloadable. You do not need to publish an
alpha to npm to test it: install the `.tgz` attached to its GitHub prerelease.

## One-time setup

1. Make the GitHub repository public. npm provenance requires a public source
   repository.
2. In the GitHub repository, create an `npm-production` environment and require
   maintainer approval for deployments to it.
3. Protect release tags so only maintainers can create tags matching the
   release naming convention.
4. Enable two-factor authentication on the npm maintainer account.

The first npm publication must bootstrap ownership because npm trusted
publishing can only be configured after the package exists:

1. Create a granular npm access token that can publish new public packages.
2. Store it as the GitHub Actions secret `NPM_TOKEN`; never commit it or an
   authenticated `.npmrc`.
3. Publish the first version with the **Publish tested package to npm** workflow.
4. On npmjs.com, open the package settings and configure a GitHub Actions trusted
   publisher with these exact values:
   - owner: `thorgas`
   - repository: `react-native-surrealdb`
   - workflow filename: `publish-npm.yml`
   - environment: `npm-production`
   - allowed action: `npm publish`
5. Delete the `NPM_TOKEN` repository secret and revoke the bootstrap token. All
   later publications authenticate through short-lived OIDC credentials.

Trusted publishing automatically produces provenance. The workflow also passes
`--provenance` so the token-authenticated bootstrap release receives provenance
when npm accepts the GitHub OIDC attestation.

## Prepare a version

### Private wrapper artifacts

`0.1.0-alpha.8.haus.20261114.1` rebuilds all four native slices from merged
main `a1a5723`, including bounded pending queue ABI and bigint CAS handling.
The Rust crate version remains unchanged under the private wrapper scheme.
Use Node 22.22.0, pnpm 11.5.0, the pinned Rust toolchain, Xcode and Android
NDK 27.1.12297006. Set `ANDROID_HOME` to the SDK directory and
`ANDROID_NDK_HOME` to that NDK directory, then run `pnpm install --frozen-lockfile`,
`pnpm --filter react-native-surrealdb release:artifacts`,
`pnpm --filter react-native-surrealdb release:check`, and
`pnpm --filter react-native-surrealdb release:pack /absolute/private/output`.
These commands build and pack locally; do not publish or create release tags.
Verify `enqueue_bounded` and `pending_window` exports in all four slices and
retain library SHA256 values and the tarball SHA512 integrity with the artifact.


`0.1.0-alpha.7.haus.20261023.1` is an immutable private wrapper artifact. Its
native libraries retain the Rust inputs from
`fb1164ebf98469a64e41f0422938cb2bb23a5111`, including the existing
`surrealdb-rn-core` version `0.1.0-alpha.4.haus.20261019.1`. The wrapper version
distinguishes the package's source, generated bindings and matched binaries;
it does not claim that the Rust crate was versioned or rebuilt. This packaging
policy applies only to this private artifact. Do not replace alpha6, publish
the wrapper to npm, or create a public SDK tag or branch for this delivery.

Stage unchanged inputs without modifying their canonical bytes. Strip only
independent Android library copies; preserve their raw input hashes and audit
the staged library hashes, 16 KiB ELF alignment, all 72 UniFFI checksums and
contract version 30 against the generated TypeScript. Keep the existing iOS
device/simulator archive hashes and Apple link evidence. Hash the final tarball
and every admitted regular file before the consuming app pins the archive.
Static ABI checks and Android synthetic acceptance remain separate from rebuilt
app acceptance, iOS execution, startup measurements and release delivery.

### Public source releases

Update the package and Rust crate versions together. For a prerelease, retain
`publishConfig.tag: "next"`; for a stable version, change it to `"latest"`.
Update the changelog and lockfiles, then commit the version change.

Before tagging, run the checks that do not regenerate native output:

```sh
pnpm install --frozen-lockfile
cargo test --workspace
pnpm test
pnpm lint
pnpm typecheck:react-native-matrix
```

The tag must exactly equal the package version without a `v` prefix:

```sh
git tag -s 0.1.0-alpha.2 -m "react-native-surrealdb 0.1.0-alpha.2"
git push origin 0.1.0-alpha.2
```

Tags containing a prerelease suffix create a public GitHub prerelease. Stable
tags create a draft GitHub release. The **Build release candidate** workflow
builds Android arm64-v8a/x86_64 and the arm64 iOS device/simulator XCFramework,
runs the package checks, and attaches these files:

- `react-native-surrealdb-<version>.tgz` — the immutable package candidate;
- `npm-pack.json` — npm's file-count and size report; and
- `SHA256SUMS` — the tarball checksum.

The iOS simulator artifact requires Apple Silicon. Intel Mac simulators cannot
load this alpha because its x86_64 iOS slice was removed to stay below npm's
effective upload-size boundary. Android x86_64 emulators remain supported.

The workflow fails if the tag and package version differ. It never edits or
commits generated files. After binding generation, it strips debug and local
symbols from the distributable native libraries while preserving their public
linker symbols. Release builds abort rather than unwind on an unexpected Rust
panic. The pack step has a 180,000,000-byte ceiling and fails before
release creation when a candidate grows beyond it.

The locally verified alpha.1 candidate is 143,388,326 bytes compressed and
451,087,977 bytes unpacked. Treat the ceiling as a regression guard rather than
a target; investigate unexpected growth before raising it.

The React Native compatibility workflow performs the same full-package size
check for pull requests and pushes to `main`. It also verifies that the
repository root remains private and that the tarball is named
`react-native-surrealdb`; never run `npm publish` from the repository root.

## Test the candidate

Download the tarball from the GitHub prerelease and keep it in a location that
is not committed to the consuming app. Install the exact file:

```sh
pnpm add /absolute/path/to/react-native-surrealdb-0.1.0-alpha.2.tgz
cd ios && pod install
```

Commit the consuming app's lockfile if that app normally commits dependency
locks. Run clean iOS and Android native builds and exercise at least:

- callback and manual transaction commit;
- callback rollback;
- an open transaction cancelled by database close;
- live-query open, delivery, cancellation, and close;
- embedded database restart and persistence when SurrealKV is used; and
- installation size and startup behavior on representative production devices.

The first unstripped static-library alpha packed to approximately 284.5 MB and
was rejected by npm with HTTP 413. Stripping reduced it to approximately
258.7 MB, and abort-on-panic reduced the same full-architecture package to
approximately 220.1 MB; both remained too large for npm's effective request
boundary. Alpha.1 therefore supports current 64-bit production/development
targets and enforces a conservative 180 MB ceiling. Review `npm-pack.json` and
treat any unexpected increase as a release blocker.

If testing fails, fix the problem and create a new version and tag. Never replace
a tag or reuse a package version.

## Publish the tested tarball

Run **Publish tested package to npm** from GitHub Actions and enter:

- `release_tag`: the exact tested tag;
- `dist_tag`: `next` for a prerelease or `latest` for a stable release; and
- `publish_github_release`: enabled for a stable release when npm publication
  should also make the draft GitHub release public.

The workflow waits for approval in `npm-production`, downloads the existing
GitHub release asset, verifies its checksum, name, version, repository, and
channel, then publishes that tarball without rebuilding it. A successful stable
publication can mark the draft GitHub release public and latest. Alpha GitHub
releases remain marked as prereleases.

Verify the registry after publication:

```sh
npm view react-native-surrealdb@0.1.0-alpha.2 \
  name version dist-tags dist.tarball dist.fileCount dist.unpackedSize
npm dist-tag ls react-native-surrealdb
```

Consumers install the alpha channel with:

```sh
pnpm add react-native-surrealdb@next
```

Do not publish from a workstation after adopting this process. Do not move
`latest` to an alpha version, replace release assets after testing, reuse a
published version, or delete and recreate release tags.

## Private alpha.9 receipt build (2026-11-14)

The local-only `0.1.0-alpha.9.haus.20261114.1` wrapper adds explicit v2 receipt
codecs and v2 local read methods. V1 wire/read entry points retain their prior
contracts. Rebuild both Apple slices and Android ABIs; generated bindings alone
cannot activate these methods. No npm publication or release tag is part of this task.

From the workspace root (Node 22, pnpm pinned by packageManager, Rust toolchain,
Xcode and Android NDK 27.1.12297006 installed):

```sh
pnpm install --frozen-lockfile
./scripts/verify-core.sh
export ANDROID_HOME="$HOME/Library/Android/sdk"
pnpm --filter react-native-surrealdb run release:artifacts
pnpm --filter react-native-surrealdb run release:check
pnpm --filter react-native-surrealdb run release:pack /Users/timhorgas/git/sdk-artifacts
```

The journal upgrades additively to format 4 only when receipt/frontier,
transmission or deferred-successor state requires it. It retains outbox, conflicts,
outcomes, checkpoint and subscription state; prior format-2/3 readers refuse
format 4 before loading or mutating it. Legacy acceptances remain epoch-unknown.
The native boundary preserves raw protocol u64 JSON until Rust decoding and emits
canonical decimal strings for local read counters, including values above 2^53.

Applications opt into `createExperimentalCanonicalCborSyncHttpCodecV2`,
`readSnapshotV2` / `readChangesV2`, `enqueueLatestBounded` and `markTransmitted`.
Persist `markTransmitted` before HTTP; after that boundary the request bytes and
fingerprint are immutable. Queued local edits remain immediately visible while
network work is outstanding. Single-record upserts coalesce only before
transmission; bounded deferred FIFO successors are rebased/admitted by the same
persisted pull/outcome transition when coverage permits. Unresolved conflicts
retain successor intent for explicit resolution. Consumers must keep local
admission independent of HTTP waits. The ordinary bounded-queue API remains
available with its existing behavior.
