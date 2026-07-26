import { describe, expect, test } from 'react-native-harness';
import { Platform } from 'react-native';

import {
  runSQLiteBenchBenchmark,
  SQLITE_BENCH_SOURCE,
} from '../benchmarks/sqlite-bench';

describe('ospfranco/sqlite-bench adaptation', () => {
  test('runs a reduced functional profile through the native database', async () => {
    const report = await runSQLiteBenchBenchmark({
      platform: Platform.OS === 'ios' ? 'ios' : 'android',
      device:
        Platform.OS === 'android'
          ? Platform.constants.Model
          : 'iPhone simulator',
      os: String(Platform.Version),
      reactNative: '0.86.0',
      surrealDb: '3.2.1',
      iterations: 5,
      cooldownMs: 0,
      collectAttribution: true,
    });

    expect(report.source.url).toBe(SQLITE_BENCH_SOURCE.url);
    expect(report.configuration.records).toBe(5);
    expect(report.metrics).toHaveLength(3);
    expect(report.metrics.every(metric => metric.samplesMs[0] > 0)).toBe(true);
    if (!report.nativeBoundary || !report.transactionBatchSweep) {
      throw new Error('Attribution diagnostics are missing');
    }
    expect(report.nativeBoundary.iterations).toBe(5);
    expect(report.nativeBoundary.averageMs).toBeGreaterThan(0);
    expect(
      report.transactionBatchSweep.map(sample => sample.batchSize),
    ).toEqual([1, 5]);
    expect(
      report.metrics.every(
        metric =>
          metric.attribution?.callsProfiled === 5 &&
          metric.attribution.engineMs > 0 &&
          metric.attribution.engineSharePercent > 0,
      ),
    ).toBe(true);
    expect(report.checksum).toBe('850');
  });
});
