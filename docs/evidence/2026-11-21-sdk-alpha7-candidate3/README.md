# Private alpha7 `.3` candidate evidence

This is a local, unpublished package candidate for
`react-native-surrealdb@0.1.0-alpha.7.tdt.20261121.3`. Source/packaging
metadata is committed at `cd082db1ec31bcb5527e69d8382c1acb5fe2c563` on
`feat/offline-intent-runtime`; the iOS archive-preservation fix is
`b33f88d2d6d8ef7eec2c3167cdb3c1e08a7fbd64`. Native source includes the
datetime and typed record-ID wire decoders (`d391933088da3207c6d75d32de2ec3aafbb32cda`)
and the app UUID string identity regression coverage
(`785bf9c37c674431575bd3b5cc5f79967fc45958`).

## Build and validation

- Rust `1.96.1`, LLVM `22.1.2`, Xcode `27.2` (build `27B52`), iOS SDK
  `27.2`, Android NDK `27.1.12297006`, Node `22.22.0`, pnpm `11.5.0`.
- Reused the existing four-target Cargo cache with offline mode and one build
  job. Device, simulator, Android arm64, and Android x86_64 artifacts were
  built sequentially. iOS archives were retained unstripped; Android staged
  shared libraries use the existing `llvm-strip --strip-unneeded` step.
- `release:verify:ios`: passed four regression-member link probes for device
  arm64 and four for simulator arm64 using the installed Xcode linker.
- Release checks: Bob module/type declaration build passed; TypeScript check
  passed; 162 package tests passed; five package-size/parser script tests
  passed; package/license/file verification passed.
- All four native slices expose the same 88 UniFFI checksum symbols. Generated
  TypeScript bindings were unchanged from the committed generated sources.
- `release:pack` produced a `162100718` byte compressed tarball and
  `523680213` byte unpacked package. The compressed size is below the
  `180000000` byte guard.
- No consumer app build/runtime acceptance, device acceptance, upload, npm
  publication, or public tag was performed.

## Artifact hashes

| Artifact | Bytes | SHA-256 |
| --- | ---: | --- |
| Raw iOS device archive | 235575824 | `4759d9c4277a64d5e1d7a2d3a41bcd4b4e81a12e3bb12ba64bc1c48e15e2101b` |
| Staged iOS device archive | 235575824 | `4759d9c4277a64d5e1d7a2d3a41bcd4b4e81a12e3bb12ba64bc1c48e15e2101b` |
| Raw iOS simulator archive | 235492224 | `29a39848d467722262ac9d5e99304315e822e31f172a76729f172646ffb375ff` |
| Staged iOS simulator archive | 235492224 | `29a39848d467722262ac9d5e99304315e822e31f172a76729f172646ffb375ff` |
| Raw Android arm64 library | 45322592 | `007d73f157acf0bceabe302b92e24b09f59c9ffd92e9b0f560e4133834d358db` |
| Staged Android arm64 library | 21848864 | `50376e1245bd095e6f3cfd2e9842a6754561680b0981d4d48743d7912d66f159` |
| Raw Android x86_64 library | 44712560 | `f5ce160e1cf969b0af215d56791e6f80fd6f55f9b424efa16bc9482f46add63b` |
| Staged Android x86_64 library | 28533656 | `28278b95acc5bd3195c86a3cebd8ffbeb0a9052488d40ed6b6a4df5304a62c73` |
| Candidate tarball | 162100718 | `4650b997c0459b2f67a67e1547c64f3108a3e346229c61f04d39d9ad3a230b77` |
| npm pack report | — | `c9304ae8b56ca2d3155d6867224f62a120e97981cc744dca97cb6ff8aca31b3d` |

The exact tarball and `npm-pack.json` are in the local-only directory
`/tmp/react-native-surrealdb-alpha7-20261121-3/`. The private candidate has
not been uploaded or published.

## Input and license hashes

- `Cargo.toml`: `d0c27df70c5566200ba5aba3fcb098bc202061f8d943675f01f2b05ab474352f`
- `Cargo.lock`: `f54d6f6e372d9a4f825d9c6f76833af6a8d35c9d517760fb543e1883549931c4`
- `rust-toolchain.toml`: `d378cc6bc4c46cf85f3bc26a191bcb13a6f9efab78f758b39029555f88dc6e79`
- `crates/surrealdb-rn-core/src/wire.rs`:
  `66266db0433584f23f2676d9e0beacd435186f4cb3936cda433a3a1ed59ee2d4`
- `crates/surrealdb-rn-core/src/lib.rs`:
  `10ad664e1a64253b913d4e5857896967b20addc395b4792db17a14a433bb092f`
- `packages/react-native-surrealdb/src/wire.ts`:
  `9f5fe11071273ee8d4f82c33faeea16dc95189a8bf96add15251c77acb346acf`
- Generated binding sources remained unchanged:
  - `surrealdb_rn_core-ffi.ts`: `05da9f39c7507b9b5efe766e37cbf8267b630f5de3592f177c696e19436b4676`
  - `surrealdb_rn_core.ts`: `2f487856c935d04e0079388dadf5c707df27b877024096435c8ac6a311013b06`
- Third-party licenses match pinned inputs:
  - SurrealDB 3.2.4: `98a94ac615f88370865016487b436fa404560910bd329794ed7502277a94b805`
  - SurrealDB protocol 0.10.2: `ace8dd034851d648d4104b7d20ab7bbf3d9666aa4e665e9fc5b865f4433aca13`

The `.2` package and its known-bad staged iOS archives were not modified.
