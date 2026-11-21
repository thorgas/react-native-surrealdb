# iOS archive link failure: strip transformation

## Finding

**Proven — 100% for the observed linker failure.** The SDK `.2` iOS archives
fail because the current release-artifact step strips Mach-O object members in
a way that makes them un-linkable by Xcode 27.2's `ld`. The same native linker
accepts the preserved raw simulator members and rejects the corresponding
published members. Reapplying the configured strip operation to a raw member
reproduces the same failure. This falsifies a pure unsupported-linker or
Rust-triple explanation for these reported errors.

The failing step is
`packages/react-native-surrealdb/scripts/prepare-ios-release-artifacts.sh`,
which runs Xcode's `strip -S -x` on both device and simulator static archives.
The source archive should not be repaired in place: the raw build output is
preserved separately.

## Evidence and validation signal

The app-side reproduction is documented at
`/Users/timhorgas/git/top-down-todos/docs/evidence/2026-11-21-ios-link-sdk2/README.md`.
That report records the original Xcode build failure and archive hashes.

On the host used for this diagnosis:

- Xcode 27.2 (27B52), Apple clang 21.0.0, `ld-27106.1`.
- App link target: `arm64-apple-ios17.0-simulator`.
- SDK worktree: `feat/offline-intent-runtime`, HEAD
  `785bf9c37c674431575bd3b5cc5f79967fc45958`.
- Published simulator archive and SDK worktree archive are byte-identical:
  SHA-256 `09a4e7f555590a568c19bc71641014ae52ba2f954288c10e8d2faf1aa2323bea`.
- Preserved raw simulator archive SHA-256:
  `ca939674a4f92b91ab89a62d4a21fbd5c241e69d92f7d4eb66178383d7e1c36b`.
  The raw archive is 235,478,264 bytes; the packaged archive is 63,675,776
  bytes.
- Preserved raw device archive SHA-256:
  `66672cbfd44df867fb34a66d3980c9ab2c1172e8a1c4af0b1dbf06fe24fab5fb`.
  Its size is 235,561,864 bytes. The staged device archive SHA-256 is
  `0c2317c79244c3365cfb4fb50608073356f1379284e0913d2ad2bb4bf2f2c38b`.
- Raw and packaged members share the same archive member names. For example,
  raw `ccbabe9886131d80-p521_jscalarmul_alt.o` links with `ld -r`, while the
  packaged member fails with `address=0xFFFFFFFFFFFFEC48 not in any section`.
  The corresponding p384, p256 and Edwards25519 members show the same
  raw-success/packaged-failure result.
- Running current Xcode `strip -S -x` on an extracted raw p521 object causes
  the same link failure. `strip -S` alone and `strip -x` alone also fail this
  probe.
- Object inspection shows the raw member's `__eh_frame` relocations are lost
  after stripping; section order changes, and the stripped member records
  `LC_BUILD_VERSION` tool `ld 27106.1`. The device archive's staged p521 and
  p384 members also fail the matching `ld -r` probe, indicating the release
  script affects both archive variants.

Minimal reproducible probe for an extracted object, substituting the input
path and platform as appropriate:

```sh
xcrun ld -r -arch arm64 -platform_version ios-simulator 15.1 27.2 \
  extracted.o -o /tmp/probe.o
```

For device objects, use `ios` instead of `ios-simulator`. Use deployment
version `15.1` (the objects' `LC_BUILD_VERSION minos`) and the installed SDK
version. For each of p521, p384, p256, and Edwards25519, all four raw members
pass this probe for both device and simulator; the staged p521/p384 members
fail with the `address=... not in any section` diagnostic.

## Packaging follow-up

The 180,000,000-byte guard checks npm's compressed `size`, not
`unpackedSize`. An isolated `release:pack` measurement used the exact package file
list and substituted the preserved raw b0442 device and simulator archives:
`size=158,568,920`, `unpackedSize=523,633,111`. This fits the compressed
tarball guard. The release flow now retains iOS archives unstripped and runs
`scripts/verify-ios-release-artifacts.mjs` against both slices; that script
checks the ARM64 archive/index, expected regression members, Mach-O platform,
unwind sections/relocations, then invokes the installed linker with `ld -r`.
The check passes on the preserved raw b0442 device and simulator archives and
fails on the current staged `.2` archives because their `__eh_frame`
relocations are absent.

The raw b0442 libraries are not a publishable replacement for `.2`: SDK source
HEAD `785bf9c37c674431575bd3b5cc5f79967fc45958` differs from b0442 in the
production query-wire decoder (`wire.rs`, strict datetime and typed record-key
decoding). The only other core-file difference is a test in `lib.rs`. The raw
archives are suitable for isolating the strip failure and measuring package
size, but using them with the later source would omit that decoder behavior.
The next candidate must rebuild the matching current Rust source before
packing. No archive was changed, no Rust build was run, and no package was
published.
