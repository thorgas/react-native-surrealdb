#!/usr/bin/env node

import { readFile, writeFile } from 'node:fs/promises';

export function summarizeRssSamples(input) {
  const samples = input
    .split(/\r?\n/u)
    .filter(Boolean)
    .map(line => {
      const [timestampMs, rssKiB] = line.split('\t').map(Number);
      if (
        !Number.isFinite(timestampMs) ||
        !Number.isFinite(rssKiB) ||
        rssKiB <= 0
      ) {
        throw new Error(`Invalid RSS sample: ${line}`);
      }
      return { timestampMs, rssKiB };
    });

  if (samples.length < 5) {
    throw new Error(
      `Expected at least 5 RSS samples, received ${samples.length}`,
    );
  }

  const sorted = samples.map(sample => sample.rssKiB).sort((a, b) => a - b);
  const percentile = value => sorted[Math.ceil((sorted.length - 1) * value)];

  return {
    schemaVersion: 1,
    unit: 'KiB',
    sampleCount: samples.length,
    samples,
    summary: {
      min: sorted[0],
      median: percentile(0.5),
      p95: percentile(0.95),
      max: sorted.at(-1),
    },
    regressionGate: null,
  };
}

if (process.argv[1] === new URL(import.meta.url).pathname) {
  const [inputPath, outputPath] = process.argv.slice(2);
  if (!inputPath || !outputPath) {
    throw new Error('Usage: summarize-rss.mjs <samples.tsv> <report.json>');
  }
  const report = summarizeRssSamples(await readFile(inputPath, 'utf8'));
  await writeFile(outputPath, `${JSON.stringify(report, null, 2)}\n`);
  console.log(JSON.stringify(report.summary));
}
