import { describe, expect, test } from 'react-native-harness';
import { Platform } from 'react-native';

import {
  runSurrealCrudBenchmark,
  SMOKE_BENCHMARK_OPTIONS,
} from '../benchmarks/surreal-crud';
import { emitBenchmarkReport } from '../benchmarks/report-output';

describe('SurrealDB mobile performance', () => {
  test('runs the crud-bench-derived smoke profile', async () => {
    const report = await runSurrealCrudBenchmark({
      ...SMOKE_BENCHMARK_OPTIONS,
      platform: Platform.OS === 'ios' ? 'ios' : 'android',
      device:
        Platform.OS === 'android'
          ? Platform.constants.Model
          : 'iPhone 17 Pro simulator',
      os: String(Platform.Version),
      reactNative: '0.86.0',
      surrealDb: '3.2.1',
    });

    expect(report.metrics.length).toBeGreaterThanOrEqual(10);
    expect(report.metrics.every(metric => metric.samplesMs.length === 7)).toBe(
      true,
    );
    emitBenchmarkReport(report);
  });
});
