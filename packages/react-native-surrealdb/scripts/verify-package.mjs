import { access, readFile, stat } from "node:fs/promises";
import { join } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));
const failures = [];

async function exists(path) {
  try {
    await access(join(root, path));
    return true;
  } catch {
    return false;
  }
}

async function requireFile(path) {
  if (!(await exists(path))) failures.push(`missing ${path}`);
}

const packageJson = JSON.parse(
  await readFile(join(root, "package.json"), "utf8"),
);

if (packageJson.private === true) failures.push("package must not be private");
if (packageJson.name !== "react-native-surrealdb")
  failures.push("unexpected package name");
if (!packageJson.version.includes("-"))
  failures.push("alpha package version must be a prerelease");
if (packageJson.license !== "MIT") failures.push("package license must be MIT");
if (packageJson.publishConfig?.access !== "public")
  failures.push("publishConfig.access must be public");
if (packageJson.publishConfig?.tag !== "next")
  failures.push("prereleases must use the next dist-tag");

for (const path of [
  "README.md",
  "LICENSE",
  "THIRD_PARTY_NOTICES.md",
  "CHANGELOG.md",
  "Surrealdb.podspec",
  "lib/module/index.js",
  "lib/typescript/index.d.ts",
  "src/generated/surrealdb_rn_core.ts",
  "cpp/generated/surrealdb_rn_core.hpp",
  "SurrealDbRnFramework.xcframework/Info.plist",
]) {
  await requireFile(path);
}

for (const abi of ["arm64-v8a", "armeabi-v7a", "x86_64", "x86"]) {
  await requireFile(`android/src/main/jniLibs/${abi}/libsurrealdb_rn_core.so`);
}

const plistPath = join(root, "SurrealDbRnFramework.xcframework/Info.plist");
if (await exists("SurrealDbRnFramework.xcframework/Info.plist")) {
  const plist = await readFile(plistPath, "utf8");
  const entries = [...plist.matchAll(/<dict>([\s\S]*?)<\/dict>/g)].map(
    ([, entry]) => entry,
  );
  const simulator = entries.find((entry) =>
    /<key>SupportedPlatformVariant<\/key>\s*<string>simulator<\/string>/.test(
      entry,
    ),
  );
  const hasDeviceArm64 = /<string>ios-arm64<\/string>/.test(plist);
  const hasSimulatorArm64 = simulator?.includes("<string>arm64</string>");
  const hasSimulatorX64 = simulator?.includes("<string>x86_64</string>");
  if (!hasDeviceArm64) failures.push("XCFramework is missing iOS device arm64");
  if (!hasSimulatorArm64)
    failures.push("XCFramework is missing iOS simulator arm64");
  if (!hasSimulatorX64)
    failures.push("XCFramework is missing iOS simulator x86_64");
}

for (const path of [".npmrc"]) {
  if (await exists(path)) failures.push(`release tree contains ${path}`);
}

for (const path of [
  "SurrealDbRnFramework.xcframework/ios-arm64/libsurrealdb_rn_core.a",
  "SurrealDbRnFramework.xcframework/ios-arm64_x86_64-simulator/libsurrealdb_rn_core.a",
]) {
  if (await exists(path)) {
    const info = await stat(join(root, path));
    if (info.size === 0) failures.push(`${path} is empty`);
  }
}

if (failures.length > 0) {
  console.error("Package verification failed:");
  for (const failure of failures) console.error(`- ${failure}`);
  process.exit(1);
}

console.log(
  `Package ${packageJson.name}@${packageJson.version} is ready to pack.`,
);
