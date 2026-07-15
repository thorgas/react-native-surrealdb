import assert from 'node:assert/strict';
import test from 'node:test';

import {
  extractPerformanceReport,
  RESULT_CHUNK_MARKER,
} from './extract-performance-report.mjs';

function chunked(report, chunkLength = 20, runId = 1_720_000_000_000) {
  const json = JSON.stringify(report);
  const chunks = [];
  for (let offset = 0; offset < json.length; offset += chunkLength) {
    chunks.push(json.slice(offset, offset + chunkLength));
  }
  return chunks
    .map(
      (chunk, index) =>
        `07-13 W ReactNativeJS: ${RESULT_CHUNK_MARKER}${runId}:${index + 1}/${
          chunks.length
        }:${chunk}`,
    )
    .join('\n');
}

test('reconstructs a report from ordered device-log chunks', () => {
  const report = { schemaVersion: 1, metrics: [{ name: 'bridge.return-one' }] };
  assert.deepEqual(extractPerformanceReport(chunked(report)), report);
});

test('reconstructs out-of-order chunks', () => {
  const report = { schemaVersion: 1, metrics: [{ name: 'crud.read-one' }] };
  const lines = chunked(report, 15).split('\n').reverse().join('\n');
  assert.deepEqual(extractPerformanceReport(lines), report);
});

test('selects the latest report when device logs contain earlier runs', () => {
  const earlier = { schemaVersion: 1, metrics: [{ name: 'earlier' }] };
  const latest = { schemaVersion: 1, metrics: [{ name: 'latest' }] };
  const log = `${chunked(latest, 15, 1_720_000_000_001)}\n${chunked(
    earlier,
    15,
    1_720_000_000_000,
  )}`;
  assert.deepEqual(extractPerformanceReport(log), latest);
});

test('rejects missing chunks', () => {
  const report = { schemaVersion: 1, metrics: [{ name: 'scan.count-all' }] };
  const lines = chunked(report, 10).split('\n');
  lines.splice(1, 1);
  assert.throws(
    () => extractPerformanceReport(lines.join('\n')),
    /Missing benchmark result chunks/,
  );
});
