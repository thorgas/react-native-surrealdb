import { describe, expect, test } from 'react-native-harness';
import { Platform } from 'react-native';

import {
  CANONICAL_BENCHMARK_OPTIONS,
  runSurrealCrudBenchmark,
} from '../benchmarks/surreal-crud';
import { emitBenchmarkReport } from '../benchmarks/report-output';

describe('SurrealDB canonical mobile performance', () => {
  test('runs the 2,000-record crud-bench-derived profile', async () => {
    const report = await runSurrealCrudBenchmark({
      ...CANONICAL_BENCHMARK_OPTIONS,
      platform: Platform.OS === 'ios' ? 'ios' : 'android',
      device:
        Platform.OS === 'android'
          ? Platform.constants.Model
          : 'iPhone 17 Pro simulator',
      os: String(Platform.Version),
      reactNative: '0.86.0',
      surrealDb: '3.2.1',
    });

    expect(report.configuration.records).toBe(2_000);
    expect(report.metrics.length).toBeGreaterThanOrEqual(10);
    emitBenchmarkReport(report);
  });
});
