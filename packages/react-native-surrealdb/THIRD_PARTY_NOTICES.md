# Third-party notices

This package contains generated integration code and depends on third-party
software. Those projects remain under their own licenses; the package's MIT
license applies only to the original `react-native-surrealdb` code.

## UniFFI and the React Native generator

The TypeScript, C++, Objective-C++, Kotlin, Gradle, CMake, manifest, and podspec
files marked with a generated attribution banner were produced by
[`uniffi-bindgen-react-native`](https://github.com/jhugman/uniffi-bindgen-react-native)
version 0.31.0-3. The project is led by James Hugman and identifies Filament,
Mozilla, and LiveKit as collaborators or funders. It is licensed under the
[Mozilla Public License 2.0](https://github.com/jhugman/uniffi-bindgen-react-native/blob/main/LICENSE).

`uniffi-bindgen-react-native` builds on
[Mozilla UniFFI](https://github.com/mozilla/uniffi-rs), the original
multi-language Rust bindings generator. UniFFI is also licensed under the
[Mozilla Public License 2.0](https://github.com/mozilla/uniffi-rs/blob/main/LICENSE).

Thank you to James Hugman, the UniFFI maintainers and contributors, and the
Filament, Mozilla, and LiveKit teams for making this bridge possible.

## Runtime and native dependencies

The native libraries include `surrealdb-sync-client` and
`surrealdb-sync-protocol` from the private canonical
`thorgas/surrealdb-sync-engine.dev` repository, pinned by the SDK's
`crates/surrealdb-rn-core/Cargo.toml` and root `Cargo.lock` to commit
`5f104c16e0d367b686fba6905550b38ef57653f4`. Historical local copies of
these crates remain in the SDK source tree and are documented in
`crates/SYNC_RUNTIME_ORIGIN.md`; the native core uses the pinned Git crates.
Both crates declare `Apache-2.0` through the engine workspace, and the
canonical license states Copyright 2026 Tim Horgas.
The package retains the full Apache License 2.0 terms in
[`LICENSE-APACHE-2.0`](./LICENSE-APACHE-2.0). This attribution does not change the
MIT license of the original wrapper code or the UniFFI generator notices above.

The published package also includes or links software from the SurrealDB Rust
SDK, React Native, Hermes, and their transitive dependency trees. Refer to the
exact release's `Cargo.lock`, `package.json`, and bundled artifacts for the
versions in use, and to each dependency's distribution for its governing
license and notices.
