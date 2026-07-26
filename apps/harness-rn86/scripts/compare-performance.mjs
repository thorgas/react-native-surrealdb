import { readFile, writeFile } from 'node:fs/promises';
import process from 'node:process';

const args = Object.fromEntries(
  process.argv.slice(2).map(argument => {
    const [key, ...value] = argument.replace(/^--/, '').split('=');
    return [key, value.join('=')];
  }),
);
if (!args.baseline || !args.candidate) {
  console.error(
    'Usage: node compare-performance.mjs --baseline=<report> --candidate=<report> [--output=<report>]',
  );
  process.exit(2);
}

const baseline = JSON.parse(await readFile(args.baseline, 'utf8'));
const candidate = JSON.parse(await readFile(args.candidate, 'utf8'));
assertCompatible(baseline, candidate);

const relativeLimit = Number(args.relativeLimit ?? 0.15);
const absoluteFloorMs = Number(args.absoluteFloorMs ?? 0.1);
const baselineNames = baseline.metrics.map(metric => metric.name).sort();
const candidateNames = candidate.metrics.map(metric => metric.name).sort();
if (JSON.stringify(baselineNames) !== JSON.stringify(candidateNames)) {
  throw new Error('Baseline and candidate metric names differ');
}
const baselineMetrics = new Map(
  baseline.metrics.map(metric => [metric.name, metric]),
);
const comparisons = candidate.metrics.map(metric => {
  const previous = baselineMetrics.get(metric.name);
  if (!previous) throw new Error(`Baseline is missing metric ${metric.name}`);
  const deltaMs = metric.summary.medianMs - previous.summary.medianMs;
  const deltaRatio = deltaMs / previous.summary.medianMs;
  const regressed = deltaMs > absoluteFloorMs && deltaRatio > relativeLimit;
  return {
    name: metric.name,
    baselineMedianMs: previous.summary.medianMs,
    candidateMedianMs: metric.summary.medianMs,
    deltaMs,
    deltaPercent: deltaRatio * 100,
    regressed,
  };
});

const result = {
  schemaVersion: 1,
  relativeLimit,
  absoluteFloorMs,
  comparisons,
  passed: comparisons.every(comparison => !comparison.regressed),
};
if (args.output)
  await writeFile(args.output, `${JSON.stringify(result, null, 2)}\n`);

for (const comparison of comparisons) {
  const status = comparison.regressed ? 'REGRESSION' : 'ok';
  console.log(
    `${status.padEnd(10)} ${comparison.name.padEnd(
      38,
    )} ${comparison.baselineMedianMs.toFixed(
      3,
    )} -> ${comparison.candidateMedianMs.toFixed(
      3,
    )} ms (${comparison.deltaPercent.toFixed(1)}%)`,
  );
}
if (!result.passed) process.exit(1);

function assertCompatible(left, right) {
  if (left.schemaVersion !== right.schemaVersion) {
    throw new Error(
      `Incompatible benchmark schema: ${left.schemaVersion} != ${right.schemaVersion}`,
    );
  }
  const fields = [
    'platform',
    'device',
    'os',
    'reactNative',
    'surrealDb',
    'profile',
    'records',
    'samples',
    'warmups',
    'batchIterations',
    'batchSizes',
    'writeRatios',
    'engine',
    'buildType',
    'fullyMaterialized',
    'clients',
  ];
  for (const field of fields) {
    if (
      JSON.stringify(left.configuration[field]) !==
      JSON.stringify(right.configuration[field])
    ) {
      throw new Error(
        `Incompatible benchmark configuration for ${field}: ${left.configuration[field]} != ${right.configuration[field]}`,
      );
    }
  }
}
