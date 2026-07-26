import { readFileSync, writeFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';

export const RESULT_CHUNK_MARKER = 'SURREALDB_BENCHMARK_RESULT_CHUNK=';

export function extractPerformanceReport(text) {
  const runs = new Map();
  const pattern = new RegExp(
    `${RESULT_CHUNK_MARKER}(\\d+):(\\d+)\\/(\\d+):([^\\r\\n]*)`,
    'g',
  );

  for (const match of text.matchAll(pattern)) {
    const runId = Number(match[1]);
    const index = Number(match[2]);
    const total = Number(match[3]);
    const run = runs.get(runId) ?? { total, chunks: new Map() };
    if (run.total !== total) {
      throw new Error(`Conflicting chunk totals for run ${runId}`);
    }
    if (index < 1 || index > total) {
      throw new Error(`Invalid benchmark result chunk ${index}/${total}`);
    }
    run.chunks.set(index, match[4]);
    runs.set(runId, run);
  }

  if (runs.size === 0) {
    throw new Error(`Device log did not contain ${RESULT_CHUNK_MARKER}`);
  }
  const latestRunId = Math.max(...runs.keys());
  const latest = runs.get(latestRunId);
  const missing = Array.from(
    { length: latest.total },
    (_, index) => index + 1,
  ).filter(index => !latest.chunks.has(index));
  if (missing.length > 0) {
    throw new Error(`Missing benchmark result chunks: ${missing.join(', ')}`);
  }

  const encoded = Array.from({ length: latest.total }, (_, index) =>
    latest.chunks.get(index + 1),
  ).join('');
  const report = JSON.parse(encoded);
  if (
    ![1, 2, 3, 4].includes(report.schemaVersion) ||
    !Array.isArray(report.metrics)
  ) {
    throw new Error('Benchmark report has an unsupported schema');
  }
  return report;
}

function argument(name) {
  const prefix = `--${name}=`;
  return process.argv
    .find(value => value.startsWith(prefix))
    ?.slice(prefix.length);
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const input = argument('input');
  const output = argument('output');
  if (!input || !output) {
    throw new Error(
      'Usage: extract-performance-report.mjs --input=<log> --output=<json>',
    );
  }
  const report = extractPerformanceReport(readFileSync(input, 'utf8'));
  writeFileSync(output, `${JSON.stringify(report, null, 2)}\n`);
  console.log(`Wrote ${report.metrics.length} benchmark metrics to ${output}`);
}
