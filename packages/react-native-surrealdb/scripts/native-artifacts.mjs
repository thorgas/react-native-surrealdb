#!/usr/bin/env node
// Native binaries are not shipped inside the npm tarball. The release workflow
// archives them, attaches the archives to the GitHub release and pins their
// SHA-256 in native-artifacts.json. The podspec (pod install) and the Gradle
// build call `fetch` to download, verify and unpack them.
//
//   node scripts/native-artifacts.mjs archive <output-dir>
//   node scripts/native-artifacts.mjs fetch <ios|android>
//
// Environment for `fetch`:
//   SURREALDB_NATIVE_ARCHIVE_DIR  read archives from this directory instead of
//                                 downloading (offline builds, CI, tests)
//   SURREALDB_NATIVE_BASE_URL     download from this mirror instead of the
//                                 GitHub release (archive file names unchanged)
// Both still require the pinned SHA-256 and size to match.

import { createHash, randomUUID } from "node:crypto";
import {
  lstat,
  mkdir,
  readFile,
  readdir,
  rename,
  rm,
  writeFile,
} from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { realpathSync } from "node:fs";
import { dirname, join, resolve, sep } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

export const MANIFEST_NAME = "native-artifacts.json";
const DEFAULT_REPOSITORY = "https://github.com/thorgas/react-native-surrealdb";

// Where each platform's files live in the package, and which top-level
// entries its archive may contain.
export const PLATFORMS = {
  ios: {
    installRoot: ".",
    entries: ["SurrealDbRnFramework.xcframework"],
    probe: "SurrealDbRnFramework.xcframework/Info.plist",
  },
  android: {
    installRoot: "android/src/main/jniLibs",
    entries: ["arm64-v8a", "x86_64"],
    probe: "arm64-v8a/libsurrealdb_rn_core.so",
  },
};

const defaultPackageRoot = fileURLToPath(new URL("..", import.meta.url));

function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

function runTar(args, options = {}) {
  const result = spawnSync("tar", args, {
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
    env: { ...process.env, COPYFILE_DISABLE: "1" },
    ...options,
  });
  if (result.status !== 0) {
    throw new Error(`tar ${args[0]} failed: ${result.stderr || result.error}`);
  }
  return result.stdout;
}

async function exists(path) {
  try {
    await lstat(path);
    return true;
  } catch {
    return false;
  }
}

export function archiveName(platform, version) {
  return `react-native-surrealdb-native-${platform}-${version}.tar.gz`;
}

/** Rejects entry names that could escape the install root. */
export function validateEntryNames(names, allowedRoots) {
  if (names.length === 0) throw new Error("archive is empty");
  for (const raw of names) {
    const name = raw.replace(/^\.\//, "");
    if (name === "" || name === ".") continue;
    if (name.startsWith("/") || /^[A-Za-z]:/.test(name) || name.includes("\\")) {
      throw new Error(`archive entry has an absolute or non-POSIX path: ${raw}`);
    }
    const segments = name.split("/").filter((segment) => segment !== "");
    if (segments.includes("..")) {
      throw new Error(`archive entry escapes the install root: ${raw}`);
    }
    if (!allowedRoots.includes(segments[0])) {
      throw new Error(`archive entry outside the expected roots: ${raw}`);
    }
  }
}

/** Fails on anything that is not a regular file or directory. */
async function assertPlainTree(path) {
  const info = await lstat(path);
  if (info.isSymbolicLink()) throw new Error(`archive contains a symlink: ${path}`);
  if (info.isDirectory()) {
    for (const child of await readdir(path)) {
      await assertPlainTree(join(path, child));
    }
  } else if (!info.isFile()) {
    throw new Error(`archive contains a special file: ${path}`);
  }
}

export async function archive(outputDirectory, packageRoot = defaultPackageRoot) {
  const packageJson = JSON.parse(
    await readFile(join(packageRoot, "package.json"), "utf8"),
  );
  const version = packageJson.version;
  await mkdir(outputDirectory, { recursive: true });
  const manifest = {
    version,
    baseUrl: `${DEFAULT_REPOSITORY}/releases/download/${version}`,
    platforms: {},
  };
  for (const [platform, spec] of Object.entries(PLATFORMS)) {
    const root = join(packageRoot, spec.installRoot);
    if (!(await exists(join(root, spec.probe)))) {
      throw new Error(`missing ${spec.probe} below ${root}; build ${platform} first`);
    }
    const file = archiveName(platform, version);
    const target = join(outputDirectory, file);
    runTar(["-czf", target, "-C", root, ...spec.entries]);
    const bytes = await readFile(target);
    manifest.platforms[platform] = {
      file,
      sha256: sha256(bytes),
      bytes: bytes.length,
    };
    console.log(`${file}: ${bytes.length} bytes, sha256 ${manifest.platforms[platform].sha256}`);
  }
  await writeFile(
    join(packageRoot, MANIFEST_NAME),
    `${JSON.stringify(manifest, null, 2)}\n`,
  );
  return manifest;
}

async function readArchive(entry, manifest, env) {
  if (env.SURREALDB_NATIVE_ARCHIVE_DIR) {
    return readFile(join(env.SURREALDB_NATIVE_ARCHIVE_DIR, entry.file));
  }
  const base = (env.SURREALDB_NATIVE_BASE_URL || manifest.baseUrl).replace(/\/+$/, "");
  const url = `${base}/${entry.file}`;
  if (!url.startsWith("https://")) {
    throw new Error(`refusing non-HTTPS native artifact URL ${url}`);
  }
  const response = await fetch(url, { redirect: "follow" });
  if (!response.ok) {
    throw new Error(`download failed: ${response.status} ${response.statusText} for ${url}`);
  }
  return Buffer.from(await response.arrayBuffer());
}

export async function fetchPlatform(
  platform,
  { packageRoot = defaultPackageRoot, env = process.env, log = console.log } = {},
) {
  const spec = PLATFORMS[platform];
  if (!spec) throw new Error(`unknown platform ${platform}; use ios or android`);
  const installRoot = join(packageRoot, spec.installRoot);
  const markerPath = join(packageRoot, ".native", `${platform}.sha256`);
  // Present while binaries are being replaced, so an interrupted install is
  // never mistaken for a local build on the next run.
  const installingPath = join(packageRoot, ".native", `${platform}.installing`);
  const manifestPath = join(packageRoot, MANIFEST_NAME);
  const hasManifest = await exists(manifestPath);
  const present = await exists(join(installRoot, spec.probe));

  if (!hasManifest) {
    if (present) return "local"; // workspace build from `pnpm run ubrn:<platform>`
    throw new Error(
      `No ${MANIFEST_NAME} and no prebuilt ${platform} binaries. In the source ` +
        `repository, run: pnpm --filter react-native-surrealdb run ubrn:${platform}`,
    );
  }

  const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
  const entry = manifest.platforms?.[platform];
  if (!entry || !/^[0-9a-f]{64}$/.test(entry.sha256) || !Number.isSafeInteger(entry.bytes)) {
    throw new Error(`${MANIFEST_NAME} has no valid ${platform} entry`);
  }
  if (entry.file !== archiveName(platform, manifest.version)) {
    throw new Error(`${MANIFEST_NAME} names an unexpected ${platform} archive: ${entry.file}`);
  }

  const interrupted = await exists(installingPath);
  if (present && !interrupted) {
    const marker = (await exists(markerPath)) ? (await readFile(markerPath, "utf8")).trim() : null;
    if (marker === entry.sha256) return "cached";
    if (marker === null) return "local"; // binaries were built or placed by hand
  }

  log(`react-native-surrealdb: fetching ${entry.file}`);
  const bytes = await readArchive(entry, manifest, env);
  if (bytes.length !== entry.bytes) {
    throw new Error(`${entry.file}: expected ${entry.bytes} bytes, got ${bytes.length}`);
  }
  const digest = sha256(bytes);
  if (digest !== entry.sha256) {
    throw new Error(`${entry.file}: SHA-256 mismatch (expected ${entry.sha256}, got ${digest})`);
  }

  const staging = join(packageRoot, ".native", `staging-${randomUUID()}`);
  await mkdir(staging, { recursive: true });
  try {
    const archivePath = join(staging, entry.file);
    await writeFile(archivePath, bytes);
    const names = runTar(["-tzf", archivePath]).split("\n").filter(Boolean);
    validateEntryNames(names, spec.entries);
    const extracted = join(staging, "out");
    await mkdir(extracted);
    runTar(["-xzf", archivePath, "-C", extracted]);
    for (const name of await readdir(extracted)) {
      if (!spec.entries.includes(name)) throw new Error(`unexpected archive root ${name}`);
      await assertPlainTree(join(extracted, name));
    }
    if (!(await exists(join(extracted, spec.probe)))) {
      throw new Error(`${entry.file} does not contain ${spec.probe}`);
    }
    await mkdir(installRoot, { recursive: true });
    await rm(markerPath, { force: true });
    await writeFile(installingPath, `${entry.sha256}\n`);
    for (const name of spec.entries) {
      const destination = resolve(installRoot, name);
      if (!destination.startsWith(resolve(installRoot) + sep)) {
        throw new Error(`refusing to install outside ${installRoot}`);
      }
      await rm(destination, { recursive: true, force: true });
      if (await exists(join(extracted, name))) {
        await rename(join(extracted, name), destination);
      }
    }
    await mkdir(dirname(markerPath), { recursive: true });
    await writeFile(markerPath, `${entry.sha256}\n`);
    await rm(installingPath, { force: true });
  } finally {
    await rm(staging, { recursive: true, force: true });
  }
  log(`react-native-surrealdb: installed ${platform} binaries (${entry.sha256.slice(0, 12)})`);
  return "fetched";
}

async function main([command, argument]) {
  if (command === "archive" && argument) {
    await archive(resolve(argument));
  } else if (command === "fetch" && argument) {
    await fetchPlatform(argument);
  } else {
    console.error("Usage: native-artifacts.mjs archive <output-dir> | fetch <ios|android>");
    process.exit(2);
  }
}

// Compare real paths: CocoaPods and Gradle reach this file through the
// node_modules symlink, while import.meta.url is already resolved.
function isEntryPoint() {
  if (!process.argv[1]) return false;
  try {
    return realpathSync(process.argv[1]) === realpathSync(fileURLToPath(import.meta.url));
  } catch {
    return false;
  }
}

if (isEntryPoint()) {
  main(process.argv.slice(2)).catch((error) => {
    console.error(`react-native-surrealdb: ${error.message}`);
    process.exit(1);
  });
}
