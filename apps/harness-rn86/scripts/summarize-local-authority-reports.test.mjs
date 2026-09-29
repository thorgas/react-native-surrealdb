import assert from 'node:assert/strict';
import test from 'node:test';

import { summarizeReports } from './summarize-local-authority-reports.mjs';

const report = (durationMs, profile = 'concurrent-write') =>
  JSON.stringify({
    schemaVersion: 4,
    profile,
    platform: 'ios',
    metrics: [{ name: 'pushStartToOtherReadMs', durationMs }],
  });

test('summarizes repeatable local timing samples', () => {
  assert.deepEqual(
    summarizeReports([report(30), report(10), report(20), ''].join('\n')),
    [
      {
        name: 'ios/concurrent-write/pushStartToOtherReadMs',
        count: 3,
        minMs: 10,
        medianMs: 20,
        maxMs: 30,
      },
    ],
  );
  assert.equal(
    summarizeReports([report(10), report(20)].join('\n'))[0].medianMs,
    15,
  );
});

test('rejects malformed and negative timing reports', () => {
  assert.throws(() => summarizeReports('{'), /Invalid JSON/);
  assert.throws(() => summarizeReports(report(-1)), /Invalid timing/);
  assert.throws(
    () => summarizeReports(''),
    /No local authority timing samples/,
  );
});
