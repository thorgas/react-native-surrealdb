# Contributing

Thank you for helping improve `react-native-surrealdb`. This is an early-alpha
native library, so small, well-tested changes are easier to review and safer to
ship than broad rewrites.

## Before starting

- Search the existing issues and pull requests.
- Use the bug, improvement, or feature issue form for substantial work.
- Discuss API changes, new native dependencies, persistence guarantees, and
  synchronization designs before implementing them.
- Never include credentials, private databases, production data, or unredacted
  device logs in an issue or pull request.

## Development setup

Install Node.js 20 or newer, pnpm 11 or newer, and the Rust toolchain pinned in
`rust-toolchain.toml`:

```sh
pnpm install --frozen-lockfile
cargo test --workspace
pnpm test
pnpm lint
pnpm typecheck:react-native-matrix
```

Native validation additionally requires Xcode and CocoaPods for iOS, or the
Android SDK, NDK 27, Java, and `cargo-ndk` for Android. See the
[development documentation](./README.md#development) and the
[compatibility-host guide](./apps/harness-rn86/README.md) for platform commands.

## Generated and native files

Do not edit generated bindings, native host projects, prebuilt frameworks, or
packaged native libraries by hand. They are regenerated and overwritten by the
documented build tools. Change the Rust or TypeScript source and generator
configuration instead, then use the appropriate generation command when a
maintainer has requested refreshed outputs.

The five React Native Test App hosts are intentionally static. Shared app,
integration-test, and benchmark code belongs in `apps/harness-shared`; host
configuration belongs in each host's `app.json`.

## Tests and evidence

Add the narrowest regression test that proves a behavioral change. Native API,
lifecycle, persistence, or packaging changes should be exercised in a real
React Native runtime on every affected platform. Performance claims require a
reproducible Release build, pinned device and toolchain details, raw reports,
and the methodology described in [PERFORMANCE.md](./PERFORMANCE.md).

This package does not currently provide automatic local/remote replication.
Changes must not describe it as a synchronization engine without tested
authorization, conflict, tombstone, retry, migration, and crash-recovery
semantics.

## Pull requests

- Keep the diff focused and explain user-visible behavior and tradeoffs.
- Include the commands and platforms used for validation.
- Call out generated output, native binary-size changes, API compatibility, and
  persistence or migration implications.
- Update documentation and third-party notices when behavior, provenance, or
  dependencies change.
- Use a concise Conventional Commit-style title such as
  `fix: preserve live query cancellation`.

By contributing, you agree that your contribution is licensed under the
repository's [MIT License](./LICENSE).
