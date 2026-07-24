import { describe, expect, test } from 'react-native-harness';
import { Platform } from 'react-native';

import { emitBenchmarkReport } from '../benchmarks/report-output';
import {
  runSQLiteBenchBenchmark,
  SQLITE_BENCH_COOLDOWN_MS,
  SQLITE_BENCH_ITERATIONS,
  SQLITE_BENCH_SOURCE,
} from '../benchmarks/sqlite-bench';

// Adapted from:
// https://github.com/ospfranco/sqlite-bench/blob/4c022c9a38294b66af2cd79fae64f0e91f25353b/src/app/index.tsx
describe('SurrealDB ospfranco/sqlite-bench adaptation', () => {
  test('runs the 1,000-row async insert, transaction, and select workloads', async () => {
    const report = await runSQLiteBenchBenchmark({
      platform: Platform.OS === 'ios' ? 'ios' : 'android',
      device:
        Platform.OS === 'android'
          ? Platform.constants.Model
          : 'iPhone 17 Pro simulator',
      os: String(Platform.Version),
      reactNative: '0.86.0',
      surrealDb: '3.2.1',
    });

    expect(report.source.url).toBe('https://github.com/ospfranco/sqlite-bench');
    expect(report.source.revision).toBe(SQLITE_BENCH_SOURCE.revision);
    expect(report.configuration.records).toBe(SQLITE_BENCH_ITERATIONS);
    expect(report.configuration.cooldownMs).toBe(SQLITE_BENCH_COOLDOWN_MS);
    expect(report.configuration.syncApiAvailable).toBe(false);
    expect(report.metrics.map(metric => metric.name)).toEqual([
      'sqlite-bench.async-insert-1k',
      'sqlite-bench.transaction-insert-1k',
      'sqlite-bench.select-and-read-1k-times-1k',
    ]);
    expect(
      report.metrics.every(
        metric =>
          metric.samplesMs.length === 1 &&
          metric.summary.medianMs > 0 &&
          metric.summary.operationsPerSecond > 0,
      ),
    ).toBe(true);
    expect(BigInt(report.checksum)).toBeGreaterThan(0n);
    emitBenchmarkReport(report);
  });
});
