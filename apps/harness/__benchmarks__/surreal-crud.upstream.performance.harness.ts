import { Platform } from 'react-native';
import { describe, expect, test } from 'react-native-harness';

import { emitBenchmarkReport } from '../benchmarks/report-output';
import {
  runSurrealCrudBenchmark,
  UPSTREAM_BENCHMARK_OPTIONS,
} from '../benchmarks/surreal-crud';

describe('SurrealDB upstream-coverage mobile performance', () => {
  test('runs the full default crud-bench matrix with the 5,000-row offset', async () => {
    const report = await runSurrealCrudBenchmark({
      ...UPSTREAM_BENCHMARK_OPTIONS,
      platform: Platform.OS === 'ios' ? 'ios' : 'android',
      device:
        Platform.OS === 'android'
          ? Platform.constants.Model
          : 'iPhone 17 Pro simulator',
      os: String(Platform.Version),
      reactNative: '0.86.0',
      surrealDb: '3.2.1',
    });

    expect(report.configuration.records).toBe(10_000);
    expect(report.metrics.length).toBe(141);
    expect(
      report.metrics.every(metric =>
        [1, 10, 50].includes(metric.samplesMs.length),
      ),
    ).toBe(true);
    emitBenchmarkReport(report);
  });
});
