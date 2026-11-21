import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import process from "node:process";
import { fileURLToPath } from "node:url";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const frameworkRoot = join(packageRoot, "SurrealDbRnFramework.xcframework");
const regressionMembers = [
  "p521_jscalarmul_alt.o",
  "p384_montjscalarmul_alt.o",
  "p256_montjscalarmul_alt.o",
  "edwards25519_scalarmuldouble_alt.o",
];

function parseArguments(argv) {
  const paths = {
    device: join(frameworkRoot, "ios-arm64", "libsurrealdb_rn_core.a"),
    simulator: join(
      frameworkRoot,
      "ios-arm64-simulator",
      "libsurrealdb_rn_core.a",
    ),
  };
  for (let index = 0; index < argv.length; index += 1) {
    const option = argv[index];
    if (option !== "--device" && option !== "--simulator") {
      throw new Error(`unexpected argument: ${option}`);
    }
    const value = argv[index + 1];
    if (!value || value.startsWith("--")) {
      throw new Error(`${option} requires an archive path`);
    }
    paths[option.slice(2)] = resolve(value);
    index += 1;
  }
  return paths;
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    encoding: "utf8",
    maxBuffer: 8 * 1024 * 1024,
    ...options,
  });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    const detail =
      result.stderr?.trim() || result.stdout?.trim() || "no output";
    throw new Error(`${basename(command)} ${args.join(" ")} failed: ${detail}`);
  }
  return result.stdout;
}

function positiveRelocationCount(output, section) {
  const escaped = section.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const match = output.match(
    new RegExp(`Relocation information \\(${escaped}\\) (\\d+) entries`),
  );
  if (!match || Number(match[1]) < 1) {
    throw new Error(`missing relocations for ${section}`);
  }
}

async function validateArchive(
  { label, platform, linkPlatform, sdk, path },
  temporaryRoot,
) {
  const archs = run("xcrun", ["lipo", "-archs", path]).trim().split(/\s+/);
  if (archs.length !== 1 || archs[0] !== "arm64") {
    throw new Error(`${label}: expected a single arm64 slice, found ${archs}`);
  }

  const members = run("/usr/bin/ar", ["-t", path])
    .split(/\r?\n/)
    .filter(Boolean);
  if (!members[0]?.startsWith("__.SYMDEF")) {
    throw new Error(`${label}: archive symbol index is missing`);
  }

  const sdkVersion = run("xcrun", ["--sdk", sdk, "--show-sdk-version"]).trim();
  let linkedMembers = 0;
  for (const suffix of regressionMembers) {
    const matchingMembers = members.filter((member) => member.endsWith(suffix));
    if (matchingMembers.length === 0) {
      throw new Error(`${label}: missing regression member *${suffix}`);
    }

    for (const member of matchingMembers) {
      const object = join(temporaryRoot, `${label}-${linkedMembers}.o`);
      const linkedObject = `${object}.linked`;
      const extracted = spawnSync("/usr/bin/ar", ["-p", path, member], {
        encoding: null,
        maxBuffer: 8 * 1024 * 1024,
      });
      if (extracted.error) throw extracted.error;
      if (extracted.status !== 0 || !extracted.stdout?.length) {
        throw new Error(`${label}: failed to extract ${member}`);
      }
      await writeFile(object, extracted.stdout);

      const inspection = run("xcrun", [
        "llvm-objdump",
        "--macho",
        "--private-headers",
        "--section-headers",
        "--reloc",
        object,
      ]);
      if (!inspection.includes("ARM64")) {
        throw new Error(`${label}: ${member} is not an ARM64 Mach-O object`);
      }
      if (!inspection.includes(`platform ${platform}`)) {
        throw new Error(`${label}: ${member} has the wrong Mach-O platform`);
      }
      const minimumVersion = inspection.match(/minos\s+(\d+\.\d+)/)?.[1];
      if (!minimumVersion) {
        throw new Error(
          `${label}: ${member} is missing LC_BUILD_VERSION minos`,
        );
      }
      positiveRelocationCount(inspection, "__TEXT,__eh_frame");
      positiveRelocationCount(inspection, "__LD,__compact_unwind");

      run("xcrun", [
        "ld",
        "-r",
        "-arch",
        "arm64",
        "-platform_version",
        linkPlatform,
        minimumVersion,
        sdkVersion,
        object,
        "-o",
        linkedObject,
      ]);
      linkedMembers += 1;
    }
  }
  console.log(
    `${label}: ${linkedMembers} regression members linked (${platform}, arm64)`,
  );
}

const paths = parseArguments(process.argv.slice(2));
const temporaryRoot = await mkdtemp(join(tmpdir(), "surrealdb-ios-linkcheck-"));
try {
  await validateArchive(
    {
      label: "iOS device",
      platform: "ios",
      linkPlatform: "ios",
      sdk: "iphoneos",
      path: paths.device,
    },
    temporaryRoot,
  );
  await validateArchive(
    {
      label: "iOS simulator",
      platform: "iossimulator",
      linkPlatform: "ios-simulator",
      sdk: "iphonesimulator",
      path: paths.simulator,
    },
    temporaryRoot,
  );
} finally {
  await rm(temporaryRoot, { recursive: true, force: true });
}
