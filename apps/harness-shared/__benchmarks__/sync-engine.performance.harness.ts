import { describe, expect, test } from 'react-native-harness';
import { Platform } from 'react-native';

import { emitBenchmarkReport } from '../benchmarks/report-output';
import { runSyncEngineBenchmark } from '../benchmarks/sync-engine';

describe('local durable sync performance', () => {
  test('measures persistent enqueue, outbox reads, and reopen recovery', async () => {
    const report = await runSyncEngineBenchmark({
      platform: Platform.OS === 'ios' ? 'ios' : 'android',
      device:
        Platform.OS === 'android'
          ? Platform.constants.Model
          : 'iPhone 17 Pro simulator',
      os: String(Platform.Version),
      reactNative: '0.86.0',
      surrealDb: '3.2.4',
    });

    expect(report.configuration.warmups).toBe(3);
    expect(report.configuration.samples).toBe(10);
    expect(report.configuration.engine).toBe(
      'persistent-surrealkv-vs-file-sqlite',
    );
    expect(report.checksums.surrealDb).toBe(report.checksums.opSQLite);
    expect(report.metrics).toHaveLength(8);
    expect(
      report.metrics.every(
        metric =>
          metric.samplesMs.length >= 5 &&
          metric.summary.medianMs > 0 &&
          metric.summary.operationsPerSecond > 0,
      ),
    ).toBe(true);
    expect(
      report.comparisons.every(comparison =>
        comparison.caveat.includes('lower bound'),
      ),
    ).toBe(true);

    await emitBenchmarkReport(report);
  });
});
