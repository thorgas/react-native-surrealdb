import { describe, expect, test } from 'react-native-harness';
import { Platform } from 'react-native';

import {
  runSurrealCrudBenchmark,
  SMOKE_BENCHMARK_OPTIONS,
} from '../benchmarks/surreal-crud';
import { emitBenchmarkReport } from '../benchmarks/report-output';

// Upstream workload specification:
// https://github.com/surrealdb/crud-bench/blob/18eb1fc8d8edcfd3d6ba8328149789ffa7866659/config/bench.toml#L54-L544
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

    expect(report.schemaVersion).toBe(2);
    expect(report.metrics.length).toBe(141);
    expect(
      report.metrics.every(metric =>
        [1, 2, 7].includes(metric.samplesMs.length),
      ),
    ).toBe(true);
    expect(report.metrics.map(metric => metric.name)).toEqual(
      expect.arrayContaining([
        'scan.where_field_integer_eq.full.no-index.write-15',
        'scan.where_field_integer_eq.full.indexed.write-50',
        'scan.where_field_fulltext_multi_and.full.indexed',
        'batch.read-1000',
      ]),
    );
    await emitBenchmarkReport(report);
  });
});
