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

The published package also includes or links software from the SurrealDB Rust
SDK, React Native, Hermes, and their transitive dependency trees. Refer to the
exact release's `Cargo.lock`, `package.json`, and bundled artifacts for the
versions in use, and to each dependency's distribution for its governing
license and notices.
