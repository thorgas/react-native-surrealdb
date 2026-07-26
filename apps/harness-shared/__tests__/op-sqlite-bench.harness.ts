import { describe, expect, test } from 'react-native-harness';
import { Platform } from 'react-native';

import {
  OP_SQLITE_VERSION,
  runOpSQLiteBenchBenchmark,
} from '../benchmarks/op-sqlite-bench';

describe('op-sqlite sqlite-bench workload', () => {
  test('runs a reduced functional profile through the native database', async () => {
    const report = await runOpSQLiteBenchBenchmark({
      platform: Platform.OS === 'ios' ? 'ios' : 'android',
      device:
        Platform.OS === 'android'
          ? Platform.constants.Model
          : 'iPhone simulator',
      os: String(Platform.Version),
      reactNative: '0.86.0',
      iterations: 5,
      cooldownMs: 0,
    });

    expect(report.configuration.opSQLite).toBe(OP_SQLITE_VERSION);
    expect(report.configuration.engine).toBe('memory');
    expect(report.configuration.records).toBe(5);
    expect(report.metrics).toHaveLength(5);
    expect(report.metrics.every(metric => metric.samplesMs[0] > 0)).toBe(true);
    expect(report.checksum).toBe('850');
  });
});
