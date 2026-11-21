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

## Pinned SurrealDB registry crates

The native core's locked registry graph includes these SurrealDB crates. The
license information below comes from each exact crate's `Cargo.toml` and
`LICENSE` file, rather than from the SDK wrapper's MIT license. `Cargo.toml`
for the three 3.2.4 crates uses `license-file = "LICENSE"` and has no `license`
SPDX expression; `surrealdb-protocol` also declares `license = "BUSL-1.1"`.

| Crate | Version | Cargo license metadata | Retained license file |
| --- | --- | --- | --- |
| `surrealdb` | 3.2.4 | `license-file = "LICENSE"`; no SPDX expression | `LICENSE-SURREALDB-3.2.4` |
| `surrealdb-core` | 3.2.4 | `license-file = "LICENSE"`; no SPDX expression | `LICENSE-SURREALDB-3.2.4` |
| `surrealdb-types` | 3.2.4 | `license-file = "LICENSE"`; no SPDX expression | `LICENSE-SURREALDB-3.2.4` |
| `surrealdb-protocol` | 0.10.2 | `license = "BUSL-1.1"`; `license-file = "LICENSE"` | `LICENSE-SURREALDB-PROTOCOL-0.10.2` |

The 3.2.4 crate license files are byte-identical (SHA256
`98a94ac615f88370865016487b436fa404560910bd329794ed7502277a94b805`).
The separate 0.10.2 protocol license file has SHA256
`ace8dd034851d648d4104b7d20ab7bbf3d9666aa4e665e9fc5b865f4433aca13`.
Both retained files identify Business Source License 1.1 with their respective
licensed-work parameters. The exact registry package checksums and dependency
versions remain in the SDK's `Cargo.lock`.

The package also includes or links software from React Native, Hermes, and
their transitive dependency trees. Refer to the exact release's `Cargo.lock`,
`package.json`, and bundled artifacts for the versions in use, and to each
dependency's distribution for its governing license and notices.
