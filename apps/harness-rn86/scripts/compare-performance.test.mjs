import assert from 'node:assert/strict';
import { mkdtemp, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const script = fileURLToPath(
  new URL('compare-performance.mjs', import.meta.url),
);

test('accepts compatible results inside both regression thresholds', async () => {
  const result = await compare(report(1), report(1.05));
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /ok\s+bridge\.return-one/);
});

test('fails when both the relative and absolute threshold are exceeded', async () => {
  const result = await compare(report(1), report(1.3));
  assert.equal(result.status, 1);
  assert.match(result.stdout, /REGRESSION\s+bridge\.return-one/);
});

test('rejects results from incompatible devices', async () => {
  const candidate = report(1);
  candidate.configuration.device = 'different-device';
  const result = await compare(report(1), candidate);
  assert.notEqual(result.status, 0);
  assert.match(
    result.stderr,
    /Incompatible benchmark configuration for device/,
  );
});

test('rejects missing candidate metrics', async () => {
  const candidate = report(1);
  candidate.metrics = [];
  const result = await compare(report(1), candidate);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /metric names differ/);
});

test('rejects sqlite-bench results with different cooldowns', async () => {
  const baseline = report(1);
  const candidate = report(1);
  baseline.configuration.cooldownMs = 2_500;
  candidate.configuration.cooldownMs = 0;
  const result = await compare(baseline, candidate);
  assert.notEqual(result.status, 0);
  assert.match(
    result.stderr,
    /Incompatible benchmark configuration for cooldownMs/,
  );
});

async function compare(baseline, candidate) {
  const directory = await mkdtemp(join(tmpdir(), 'surreal-benchmark-'));
  const baselinePath = join(directory, 'baseline.json');
  const candidatePath = join(directory, 'candidate.json');
  await writeFile(baselinePath, JSON.stringify(baseline));
  await writeFile(candidatePath, JSON.stringify(candidate));
  return spawnSync(
    process.execPath,
    [script, `--baseline=${baselinePath}`, `--candidate=${candidatePath}`],
    { encoding: 'utf8' },
  );
}

function report(medianMs) {
  return {
    schemaVersion: 2,
    configuration: {
      platform: 'android',
      device: 'Pixel_9',
      os: '36',
      reactNative: '0.86.0',
      surrealDb: '3.2.4',
      profile: 'smoke',
      records: 200,
      samples: 7,
      warmups: 3,
      batchIterations: 2,
      batchSizes: [100, 1_000],
      writeRatios: [0.15, 0.5],
      engine: 'memory',
      buildType: 'Debug harness',
      fullyMaterialized: true,
      clients: 1,
    },
    metrics: [
      {
        name: 'bridge.return-one',
        summary: { medianMs },
      },
    ],
  };
}
