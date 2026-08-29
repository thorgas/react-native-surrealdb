import assert from 'node:assert/strict';
import test from 'node:test';

import { summarizeRssSamples } from './summarize-rss.mjs';

test('summarizes raw RSS samples without inventing a regression threshold', () => {
  const report = summarizeRssSamples(
    '1\t100\n2\t200\n3\t150\n4\t300\n5\t250\n',
  );

  assert.deepEqual(report.summary, {
    min: 100,
    median: 200,
    p95: 300,
    max: 300,
  });
  assert.equal(report.regressionGate, null);
});

test('rejects too few samples', () => {
  assert.throws(() => summarizeRssSamples('1\t100\n'), /at least 5/);
});
