import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';

export function summarizeReports(contents) {
  const samples = new Map();
  for (const [index, line] of contents.split('\n').entries()) {
    if (!line.trim()) continue;
    let report;
    try {
      report = JSON.parse(line);
    } catch {
      throw new Error(`Invalid JSON on report line ${index + 1}`);
    }
    if (
      report.schemaVersion !== 4 ||
      !['concurrent-write', 'periodic-without-explicit-pull'].includes(
        report.profile,
      ) ||
      !['ios', 'android'].includes(report.platform) ||
      !Array.isArray(report.metrics)
    ) {
      throw new Error(`Invalid report shape on line ${index + 1}`);
    }
    for (const metric of report.metrics) {
      if (
        typeof metric.name !== 'string' ||
        !Number.isFinite(metric.durationMs) ||
        metric.durationMs < 0
      ) {
        throw new Error(`Invalid timing on report line ${index + 1}`);
      }
      const key = `${report.platform}/${report.profile}/${metric.name}`;
      const values = samples.get(key) ?? [];
      values.push(metric.durationMs);
      samples.set(key, values);
    }
  }
  if (samples.size === 0) throw new Error('No local authority timing samples');
  return [...samples.entries()].map(([name, values]) => {
    values.sort((a, b) => a - b);
    const middle = Math.floor(values.length / 2);
    return {
      name,
      count: values.length,
      minMs: values[0],
      medianMs:
        values.length % 2 === 0
          ? (values[middle - 1] + values[middle]) / 2
          : values[middle],
      maxMs: values.at(-1),
    };
  });
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const input = process.argv[2];
  if (!input) {
    throw new Error(
      'Usage: node scripts/summarize-local-authority-reports.mjs <reports.jsonl>',
    );
  }
  console.table(summarizeReports(readFileSync(input, 'utf8')));
}
