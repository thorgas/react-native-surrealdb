# Releasing

Releases use one immutable package tarball from candidate testing through npm
publication. Building a candidate and publishing it are deliberately separate
workflows.

## Release channels

| Package version | GitHub release              | npm tag  | Purpose                                                         |
| --------------- | --------------------------- | -------- | --------------------------------------------------------------- |
| `0.1.0-alpha.1` | Prerelease                  | `next`   | Install in production apps and validate before a stable release |
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
git tag -s 0.1.0-alpha.1 -m "react-native-surrealdb 0.1.0-alpha.1"
git push origin 0.1.0-alpha.1
```

Tags containing a prerelease suffix create a public GitHub prerelease. Stable
tags create a draft GitHub release. The **Build release candidate** workflow
builds all four Android ABIs and the iOS device/simulator XCFramework, runs the
package checks, and attaches these files:

- `react-native-surrealdb-<version>.tgz` — the immutable package candidate;
- `npm-pack.json` — npm's file-count and size report; and
- `SHA256SUMS` — the tarball checksum.

The workflow fails if the tag and package version differ. It never edits or
commits generated files.

## Test the candidate

Download the tarball from the GitHub prerelease and keep it in a location that
is not committed to the consuming app. Install the exact file:

```sh
pnpm add /absolute/path/to/react-native-surrealdb-0.1.0-alpha.1.tgz
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

The first static-library alpha packed to approximately 308 MB compressed and
installed to roughly 1.1 GB. This is a historical upper reference, not an
acceptable long-term target. Review `npm-pack.json` and treat an unexpected
increase as a release blocker.

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
npm view react-native-surrealdb@0.1.0-alpha.1 \
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
