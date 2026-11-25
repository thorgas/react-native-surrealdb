import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import {
  mkdir,
  mkdtemp,
  readFile,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

// @ts-expect-error -- plain ESM build script without type declarations
import * as nativeArtifacts from "../scripts/native-artifacts.mjs";

const { archive, archiveName, fetchPlatform, validateEntryNames } =
  nativeArtifacts as {
    archive: (out: string, root: string) => Promise<Manifest>;
    archiveName: (platform: string, version: string) => string;
    fetchPlatform: (
      platform: string,
      options: { packageRoot: string; env: Record<string, string>; log: () => void },
    ) => Promise<string>;
    validateEntryNames: (names: string[], roots: string[]) => void;
  };

type Manifest = {
  version: string;
  baseUrl: string;
  platforms: Record<string, { file: string; sha256: string; bytes: number }>;
};

const VERSION = "9.9.9-test";
let work: string;
let source: string; // a package root holding built binaries (release job)
let consumer: string; // a package root as installed from npm (no binaries)
let archives: string;

async function makeSourcePackage(root: string) {
  await writeFile(join(root, "package.json"), JSON.stringify({ version: VERSION }));
  const framework = join(root, "SurrealDbRnFramework.xcframework");
  await mkdir(join(framework, "ios-arm64"), { recursive: true });
  await writeFile(join(framework, "Info.plist"), "<plist/>");
  await writeFile(join(framework, "ios-arm64", "libsurrealdb_rn_core.a"), "ios");
  for (const abi of ["arm64-v8a", "x86_64"]) {
    const dir = join(root, "android/src/main/jniLibs", abi);
    await mkdir(dir, { recursive: true });
    await writeFile(join(dir, "libsurrealdb_rn_core.so"), `so-${abi}`);
  }
}

function tarGz(target: string, cwd: string, entries: string[]) {
  const result = spawnSync("tar", ["-czf", target, "-C", cwd, ...entries], {
    env: { ...process.env, COPYFILE_DISABLE: "1" },
  });
  expect(result.status).toBe(0);
}

/** Replaces the android archive with `bytes` and re-pins the manifest to it. */
async function pinAndroidArchive(bytes: Buffer) {
  const manifest: Manifest = JSON.parse(
    await readFile(join(consumer, "native-artifacts.json"), "utf8"),
  );
  const file = archiveName("android", VERSION);
  await writeFile(join(archives, file), bytes);
  manifest.platforms.android = {
    file,
    sha256: createHash("sha256").update(bytes).digest("hex"),
    bytes: bytes.length,
  };
  await writeFile(join(consumer, "native-artifacts.json"), JSON.stringify(manifest));
}

const fetchAndroid = () =>
  fetchPlatform("android", {
    packageRoot: consumer,
    env: { SURREALDB_NATIVE_ARCHIVE_DIR: archives },
    log: () => {},
  });

beforeEach(async () => {
  work = await mkdtemp(join(tmpdir(), "rnsdb-native-"));
  source = join(work, "source");
  consumer = join(work, "consumer");
  archives = join(work, "archives");
  await mkdir(source);
  await mkdir(consumer);
  await makeSourcePackage(source);
  await archive(archives, source);
  await writeFile(join(consumer, "package.json"), JSON.stringify({ version: VERSION }));
  await writeFile(
    join(consumer, "native-artifacts.json"),
    await readFile(join(source, "native-artifacts.json")),
  );
});

afterEach(async () => {
  await rm(work, { recursive: true, force: true });
});

describe("native artifacts", () => {
  it("installs verified binaries into a package without them, then reuses them", async () => {
    expect(await fetchAndroid()).toBe("fetched");
    expect(
      await readFile(
        join(consumer, "android/src/main/jniLibs/x86_64/libsurrealdb_rn_core.so"),
        "utf8",
      ),
    ).toBe("so-x86_64");
    expect(await fetchAndroid()).toBe("cached");

    const ios = await fetchPlatform("ios", {
      packageRoot: consumer,
      env: { SURREALDB_NATIVE_ARCHIVE_DIR: archives },
      log: () => {},
    });
    expect(ios).toBe("fetched");
    expect(
      await readFile(join(consumer, "SurrealDbRnFramework.xcframework/Info.plist"), "utf8"),
    ).toBe("<plist/>");
  });

  it("leaves a workspace build alone when there is no manifest", async () => {
    // Binaries built in place (no marker) are kept even with a manifest...
    expect(
      await fetchPlatform("android", { packageRoot: source, env: {}, log: () => {} }),
    ).toBe("local");
    // ...and without one.
    await rm(join(source, "native-artifacts.json"));
    expect(
      await fetchPlatform("android", { packageRoot: source, env: {}, log: () => {} }),
    ).toBe("local");
  });

  it("rejects an archive whose bytes do not match the pinned SHA-256", async () => {
    const file = join(archives, archiveName("android", VERSION));
    const bytes = await readFile(file);
    bytes[bytes.length - 1] ^= 0xff;
    await writeFile(file, bytes);
    await expect(fetchAndroid()).rejects.toThrow(/SHA-256 mismatch/);
  });

  it("rejects a truncated archive by size", async () => {
    const file = join(archives, archiveName("android", VERSION));
    await writeFile(file, (await readFile(file)).subarray(0, 10));
    await expect(fetchAndroid()).rejects.toThrow(/expected \d+ bytes/);
  });

  it("rejects a manifest that points at another file name", async () => {
    const manifest: Manifest = JSON.parse(
      await readFile(join(consumer, "native-artifacts.json"), "utf8"),
    );
    manifest.platforms.android.file = "../../evil.tar.gz";
    await writeFile(join(consumer, "native-artifacts.json"), JSON.stringify(manifest));
    await expect(fetchAndroid()).rejects.toThrow(/unexpected android archive/);
  });

  it("refuses plain-HTTP mirrors", async () => {
    await expect(
      fetchPlatform("android", {
        packageRoot: consumer,
        env: { SURREALDB_NATIVE_BASE_URL: "http://mirror.invalid" },
        log: () => {},
      }),
    ).rejects.toThrow(/non-HTTPS/);
  });

  it("rejects a correctly pinned archive that contains a symlink", async () => {
    const evil = join(work, "evil");
    await mkdir(join(evil, "arm64-v8a"), { recursive: true });
    await writeFile(join(evil, "arm64-v8a", "libsurrealdb_rn_core.so"), "x");
    await symlink("/etc/passwd", join(evil, "arm64-v8a", "link"));
    const target = join(work, "evil.tar.gz");
    tarGz(target, evil, ["arm64-v8a"]);
    await pinAndroidArchive(await readFile(target));
    await expect(fetchAndroid()).rejects.toThrow(/symlink/);
  });

  it("rejects a correctly pinned archive with entries outside the expected roots", async () => {
    const evil = join(work, "evil");
    await mkdir(join(evil, "arm64-v8a"), { recursive: true });
    await mkdir(join(evil, "other"), { recursive: true });
    await writeFile(join(evil, "arm64-v8a", "libsurrealdb_rn_core.so"), "x");
    await writeFile(join(evil, "other", "payload"), "x");
    const target = join(work, "evil.tar.gz");
    tarGz(target, evil, ["arm64-v8a", "other"]);
    await pinAndroidArchive(await readFile(target));
    await expect(fetchAndroid()).rejects.toThrow(/outside the expected roots/);
  });

  it("validates entry names against traversal and absolute paths", () => {
    const roots = ["arm64-v8a"];
    expect(() => validateEntryNames(["arm64-v8a/../../x"], roots)).toThrow(/escapes/);
    expect(() => validateEntryNames(["/etc/passwd"], roots)).toThrow(/absolute/);
    expect(() => validateEntryNames(["C:/x"], roots)).toThrow(/absolute/);
    expect(() => validateEntryNames(["arm64-v8a\\..\\x"], roots)).toThrow(/non-POSIX/);
    expect(() => validateEntryNames([], roots)).toThrow(/empty/);
    expect(() => validateEntryNames(["./arm64-v8a/lib.so"], roots)).not.toThrow();
  });

  it("re-fetches after an interrupted install instead of trusting partial files", async () => {
    expect(await fetchAndroid()).toBe("fetched");
    await rm(join(consumer, "android/src/main/jniLibs/x86_64"), { recursive: true });
    await writeFile(join(consumer, ".native/android.installing"), "x");
    expect(await fetchAndroid()).toBe("fetched");
    expect(
      await readFile(
        join(consumer, "android/src/main/jniLibs/x86_64/libsurrealdb_rn_core.so"),
        "utf8",
      ),
    ).toBe("so-x86_64");
  });
});
