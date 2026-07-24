# Releasing

The package is public and unscoped. Alpha releases use the npm `next` dist-tag,
configured in `publishConfig`, so they do not replace a future stable `latest`
release.

## Prepare artifacts

Install the Rust targets once:

```sh
rustup target add \
  aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios \
  aarch64-linux-android armv7-linux-androideabi \
  x86_64-linux-android i686-linux-android
```

Then, from the repository root:

```sh
pnpm install --frozen-lockfile
pnpm --filter react-native-surrealdb run release:artifacts
pnpm --filter react-native-surrealdb run release:check
pnpm --filter react-native-surrealdb pack
```

Install the resulting tarball in a clean React Native test app and run both
platforms before publishing it. The consumer smoke test must cover callback and
manual transaction commit, callback rollback, an open transaction cancelled by
database close, and a live query opened and closed through the documented
pull-based handle. The first static-library alpha matrix packed to
approximately 308 MB compressed and installed to roughly 1.1 GB. This is the
historical upper reference, not an acceptable long-term target. Android now
ships the Rust core as a stripped-at-app-build shared library; record both the
new tarball and extracted sizes before publishing. Treat an unexpected increase
as a release blocker.

## Publish

Publishing intentionally remains a manual final step until npm ownership and
trusted publishing are configured. Use an npm account with two-factor
authentication. From this directory, after confirming the version and clean
Git state:

```sh
pnpm publish --provenance --no-git-checks
```

Do not commit an npm token or a user-level `.npmrc`.
