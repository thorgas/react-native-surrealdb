import { readFile, readdir, stat, writeFile } from 'node:fs/promises';
import { execFile } from 'node:child_process';
import { basename, dirname, join, resolve } from 'node:path';
import process from 'node:process';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);

const args = Object.fromEntries(
  process.argv.slice(2).map(argument => {
    const [key, ...value] = argument.replace(/^--/, '').split('=');
    return [key, value.join('=')];
  }),
);

const platform = args.platform;
const baselinePath = args.baseline && resolve(args.baseline);
const candidatePath = args.candidate && resolve(args.candidate);
const outputPath = resolve(args.output ?? 'size-results/report.json');

if (!platform || !candidatePath) {
  console.error(
    'Usage: node compare-release-size.mjs --platform=android --candidate=<path> [--output=<path>]',
  );
  process.exit(2);
}

async function recursiveSize(path) {
  const info = await stat(path);
  if (info.isFile()) return info.size;

  let total = 0;
  for (const entry of await readdir(path)) {
    total += await recursiveSize(join(path, entry));
  }
  return total;
}

const root = new URL('..', import.meta.url);
const budget = JSON.parse(await readFile(new URL('size-budget.json', root)));
const baselineBytes = baselinePath
  ? await recursiveSize(baselinePath)
  : budget[platform]?.baselineBytes;
let baselineRun;
if (baselinePath) {
  try {
    baselineRun = JSON.parse(
      await readFile(
        join(dirname(baselinePath), 'baseline-report.json'),
        'utf8',
      ),
    );
  } catch {
    baselineRun = undefined;
  }
}
const candidateBytes = await recursiveSize(candidatePath);
const deltaBytes = candidateBytes - baselineBytes;
const maxDeltaBytes = budget[platform]?.maxDeltaBytes;
const nativeLibraries =
  platform === 'android' ? await androidNativeLibraries(candidatePath) : [];
const surrealNativeBytes = nativeLibraries
  .filter(library => library.name.includes('surreal'))
  .reduce((total, library) => total + library.bytes, 0);
const maxSurrealNativeBytes = budget[platform]?.maxSurrealNativeBytes;

if (
  !Number.isSafeInteger(baselineBytes) ||
  !Number.isSafeInteger(maxDeltaBytes)
) {
  throw new Error(`No size budget configured for ${platform}`);
}

const report = {
  schemaVersion: 1,
  platform,
  baseline: {
    name: baselinePath
      ? basename(baselinePath)
      : budget[platform].baselineDescription,
    bytes: baselineBytes,
    referenceBytes: budget[platform].baselineBytes,
    measuredAt: baselineRun?.measuredAt ?? budget[platform].baselineMeasuredAt,
    measuredOn:
      baselineRun?.configuration ?? budget[platform].baselineMeasuredOn,
    reproductionCommand:
      baselineRun?.command ?? budget[platform].baselineCommand,
    source: baselineRun?.source ?? budget[platform].baselineSource,
  },
  candidate: {
    name: basename(candidatePath),
    bytes: candidateBytes,
    nativeLibraries,
    surrealNativeBytes,
  },
  deltaBytes,
  maxDeltaBytes,
  maxSurrealNativeBytes,
  optimizedReference: budget[platform]?.optimizedCandidateBytes
    ? {
        candidateBytes: budget[platform].optimizedCandidateBytes,
        deltaBytes: budget[platform].optimizedDeltaBytes,
        surrealNativeBytes: budget[platform].optimizedSurrealNativeBytes,
        measuredAt: budget[platform].optimizedMeasuredAt,
        source: budget[platform].optimizedSource,
      }
    : undefined,
  comparisonCommand: budget[platform].comparisonCommand,
  passed:
    deltaBytes <= maxDeltaBytes &&
    (!Number.isSafeInteger(maxSurrealNativeBytes) ||
      surrealNativeBytes <= maxSurrealNativeBytes),
};

await writeFile(outputPath, `${JSON.stringify(report, null, 2)}\n`);

const mib = bytes => `${(bytes / 1024 / 1024).toFixed(2)} MiB`;
console.log(`${platform} release size comparison`);
console.log(`  baseline:  ${mib(baselineBytes)}`);
console.log(`  candidate: ${mib(candidateBytes)}`);
console.log(`  delta:     ${mib(deltaBytes)}`);
console.log(`  budget:    ${mib(maxDeltaBytes)}`);
console.log(`  SurrealDB native libraries: ${mib(surrealNativeBytes)}`);
for (const library of nativeLibraries.filter(library =>
  library.name.includes('surreal'),
)) {
  console.log(`    ${library.name}: ${mib(library.bytes)}`);
}

if (!report.passed) {
  if (deltaBytes > maxDeltaBytes) {
    console.error(
      `APK size regression: ${mib(deltaBytes - maxDeltaBytes)} over budget.`,
    );
  }
  if (
    Number.isSafeInteger(maxSurrealNativeBytes) &&
    surrealNativeBytes > maxSurrealNativeBytes
  ) {
    console.error(
      `Native size regression: ${mib(surrealNativeBytes - maxSurrealNativeBytes)} over budget.`,
    );
  }
  process.exit(1);
}

async function androidNativeLibraries(apkPath) {
  const { stdout } = await execFileAsync('unzip', ['-l', apkPath]);
  return stdout
    .split(/\r?\n/)
    .map(line => line.trim().match(/^(\d+)\s+\S+\s+\S+\s+(lib\/[^/]+\/[^/]+\.so)$/))
    .filter(Boolean)
    .map(match => ({
      path: match[2],
      name: basename(match[2]),
      bytes: Number(match[1]),
    }))
    .sort((left, right) => left.path.localeCompare(right.path));
}
