import { mkdir, writeFile } from "node:fs/promises";
import { isAbsolute, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import process from "node:process";
import { fileURLToPath } from "node:url";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const requestedOutput = process.argv.find(
  (argument, index) => index > 1 && argument !== "--",
);
const outputDirectory = requestedOutput
  ? isAbsolute(requestedOutput)
    ? requestedOutput
    : resolve(process.cwd(), requestedOutput)
  : join(packageRoot, "release");
// SurrealDB 4.0.1 grew the packed tarball from ~161 MB to ~261 MB: its core
// pulls in more crates, and iOS static archives keep all of them until the
// app links. See NATIVE_SIZE.md.
const maximumPackedBytes = Number(
  process.env.MAXIMUM_NPM_TARBALL_BYTES ?? 300_000_000,
);

if (!Number.isSafeInteger(maximumPackedBytes) || maximumPackedBytes <= 0) {
  throw new Error("MAXIMUM_NPM_TARBALL_BYTES must be a positive integer");
}

await mkdir(outputDirectory, { recursive: true });

const packed = spawnSync(
  "npm",
  [
    "pack",
    ".",
    "--ignore-scripts",
    "--json",
    "--pack-destination",
    outputDirectory,
  ],
  {
    cwd: packageRoot,
    encoding: "utf8",
    maxBuffer: 10 * 1024 * 1024,
  },
);

if (packed.status !== 0) {
  process.stderr.write(packed.stderr);
  process.exit(packed.status ?? 1);
}

const parsedReport = JSON.parse(packed.stdout);
const report = Array.isArray(parsedReport)
  ? parsedReport
  : Object.values(parsedReport);
if (report.length !== 1)
  throw new Error("npm pack returned an unexpected report");

const [artifact] = report;
if (artifact.name !== "react-native-surrealdb") {
  throw new Error(`refusing to pack unexpected package ${artifact.name}`);
}
await writeFile(
  join(outputDirectory, "npm-pack.json"),
  `${JSON.stringify(report, null, 2)}\n`,
);

console.log(
  `${artifact.name}@${artifact.version}: ${artifact.size} bytes packed, ${artifact.unpackedSize} bytes unpacked`,
);

if (artifact.size > maximumPackedBytes) {
  throw new Error(
    `npm tarball is ${artifact.size} bytes; release limit is ${maximumPackedBytes} bytes`,
  );
}
