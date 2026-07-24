import { describe, expect, test } from 'react-native-harness';
import { Platform } from 'react-native';

import {
  CANONICAL_BENCHMARK_OPTIONS,
  runSurrealCrudBenchmark,
} from '../benchmarks/surreal-crud';
import { emitBenchmarkReport } from '../benchmarks/report-output';

// Upstream workload specification:
// https://github.com/surrealdb/crud-bench/blob/18eb1fc8d8edcfd3d6ba8328149789ffa7866659/config/bench.toml#L54-L544
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
    expect(report.metrics.length).toBe(141);
    expect(
      report.metrics.every(metric =>
        [1, 3, 20].includes(metric.samplesMs.length),
      ),
    ).toBe(true);
    await emitBenchmarkReport(report);
  });
});
