import { describe, expect, test } from 'react-native-harness';
import { Platform } from 'react-native';

import { emitBenchmarkReport } from '../benchmarks/report-output';
import {
  OP_SQLITE_VERSION,
  runOpSQLiteBenchBenchmark,
} from '../benchmarks/op-sqlite-bench';
import {
  runSQLiteBenchBenchmark,
  SQLITE_BENCH_COOLDOWN_MS,
  SQLITE_BENCH_ITERATIONS,
  SQLITE_BENCH_SOURCE,
} from '../benchmarks/sqlite-bench';
import { summarize } from '../benchmarks/statistics';

// Adapted from:
// https://github.com/ospfranco/sqlite-bench/blob/4c022c9a38294b66af2cd79fae64f0e91f25353b/src/app/index.tsx
describe('SurrealDB and op-sqlite paired sqlite-bench', () => {
  test('runs both libraries on the same device and memory-backed engines', async () => {
    const environment = {
      platform: Platform.OS === 'ios' ? 'ios' : 'android',
      device:
        Platform.OS === 'android'
          ? Platform.constants.Model
          : 'iPhone 17 Pro simulator',
      os: String(Platform.Version),
      reactNative: '0.86.0',
    } as const;
    const firstOrderSurrealReport = await runSQLiteBenchBenchmark({
      ...environment,
      surrealDb: '3.2.1',
    });
    const firstOrderOpSQLiteReport =
      await runOpSQLiteBenchBenchmark(environment);
    const secondOrderOpSQLiteReport =
      await runOpSQLiteBenchBenchmark(environment);
    const secondOrderSurrealReport = await runSQLiteBenchBenchmark({
      ...environment,
      surrealDb: '3.2.1',
    });

    expect(firstOrderSurrealReport.source.url).toBe(
      'https://github.com/ospfranco/sqlite-bench',
    );
    expect(firstOrderSurrealReport.source.revision).toBe(
      SQLITE_BENCH_SOURCE.revision,
    );
    expect(firstOrderSurrealReport.configuration.records).toBe(
      SQLITE_BENCH_ITERATIONS,
    );
    expect(firstOrderSurrealReport.configuration.cooldownMs).toBe(
      SQLITE_BENCH_COOLDOWN_MS,
    );
    expect(firstOrderSurrealReport.configuration.syncApiAvailable).toBe(false);
    expect(firstOrderSurrealReport.metrics.map(metric => metric.name)).toEqual([
      'sqlite-bench.async-insert-1k',
      'sqlite-bench.transaction-insert-1k',
      'sqlite-bench.select-and-read-1k-times-1k',
    ]);
    expect(firstOrderOpSQLiteReport.configuration.opSQLite).toBe(
      OP_SQLITE_VERSION,
    );
    expect(firstOrderOpSQLiteReport.configuration.engine).toBe(
      firstOrderSurrealReport.configuration.engine,
    );
    expect(firstOrderOpSQLiteReport.metrics).toHaveLength(5);
    expect(
      [
        ...firstOrderSurrealReport.metrics,
        ...secondOrderSurrealReport.metrics,
        ...firstOrderOpSQLiteReport.metrics,
        ...secondOrderOpSQLiteReport.metrics,
      ].every(
        metric =>
          metric.samplesMs.length === 1 &&
          metric.summary.medianMs > 0 &&
          metric.summary.operationsPerSecond > 0,
      ),
    ).toBe(true);
    expect(
      new Set([
        firstOrderSurrealReport.checksum,
        secondOrderSurrealReport.checksum,
        firstOrderOpSQLiteReport.checksum,
        secondOrderOpSQLiteReport.checksum,
      ]).size,
    ).toBe(1);

    const surrealMetrics = combineMetricSamples(
      firstOrderSurrealReport.metrics,
      secondOrderSurrealReport.metrics,
    );
    const opSQLiteMetrics = combineMetricSamples(
      firstOrderOpSQLiteReport.metrics,
      secondOrderOpSQLiteReport.metrics,
    );

    await emitBenchmarkReport({
      schemaVersion: 2,
      measuredAt: new Date().toISOString(),
      source: {
        upstream: SQLITE_BENCH_SOURCE,
        comparison:
          'Same Harness binary, device, OS, debug build, memory-backed engine, record count, cooldown, and fully materialized comparable workloads. Two samples use opposite execution orders to balance first-run and thermal effects.',
        caveat:
          'op-sqlite uses 1,000 JavaScript transaction calls; SurrealDB submits one 1,000-statement transaction because it has no JavaScript transaction handle.',
      },
      configuration: {
        profile: 'sqlite-bench-paired',
        records: SQLITE_BENCH_ITERATIONS,
        iterations: SQLITE_BENCH_ITERATIONS,
        samples: 2,
        cooldownMs: SQLITE_BENCH_COOLDOWN_MS,
        ...environment,
        surrealDb: firstOrderSurrealReport.configuration.surrealDb,
        opSQLite: OP_SQLITE_VERSION,
        engine: 'memory',
        buildType: 'Debug harness',
        fullyMaterialized: true,
        clients: 1,
        syncApiAvailable: 'op-sqlite-only',
        executionOrders: [
          ['surrealdb', 'op-sqlite'],
          ['op-sqlite', 'surrealdb'],
        ],
      },
      checksums: {
        surrealDb: firstOrderSurrealReport.checksum,
        opSQLite: firstOrderOpSQLiteReport.checksum,
      },
      metrics: [...surrealMetrics, ...opSQLiteMetrics],
    });
  });
});

type SingleSampleMetric = {
  name: string;
  operationsPerSample: number;
  samplesMs: [number];
  summary: ReturnType<typeof summarize>;
};

function combineMetricSamples<Metric extends SingleSampleMetric>(
  first: readonly Metric[],
  second: readonly Metric[],
) {
  expect(second.map(metric => metric.name)).toEqual(
    first.map(metric => metric.name),
  );

  return first.map((metric, index) => {
    const samplesMs = [
      metric.samplesMs[0],
      second[index]!.samplesMs[0],
    ] as const;
    return {
      ...metric,
      samplesMs,
      summary: summarize(samplesMs, metric.operationsPerSample),
    };
  });
}
