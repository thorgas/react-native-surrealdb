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
import { buildSQLiteBenchComparisons } from '../benchmarks/sqlite-comparison';
import { summarize } from '../benchmarks/statistics';

const environment = {
  platform: Platform.OS === 'ios' ? 'ios' : 'android',
  device:
    Platform.OS === 'android'
      ? Platform.constants.Model
      : 'iPhone 17 Pro simulator',
  os: String(Platform.Version),
  reactNative: '0.86.0',
} as const;

type SurrealReport = Awaited<ReturnType<typeof runSQLiteBenchBenchmark>>;
type OpSQLiteReport = Awaited<ReturnType<typeof runOpSQLiteBenchBenchmark>>;

let pairedContext:
  | {
      firstOrderSurrealReport: SurrealReport;
      secondOrderSurrealReport: SurrealReport;
      firstOrderOpSQLiteReport: OpSQLiteReport;
      secondOrderOpSQLiteReport: OpSQLiteReport;
    }
  | undefined;
let firstOrderContext:
  | {
      firstOrderSurrealReport: SurrealReport;
      firstOrderOpSQLiteReport: OpSQLiteReport;
    }
  | undefined;
let firstAttributionReport: SurrealReport | undefined;

// Adapted from:
// https://github.com/ospfranco/sqlite-bench/blob/4c022c9a38294b66af2cd79fae64f0e91f25353b/src/app/index.tsx
describe('SurrealDB and op-sqlite paired sqlite-bench', () => {
  test('measures SurrealDB then op-sqlite without diagnostics', async () => {
    const firstOrderSurrealReport = await runSQLiteBenchBenchmark({
      ...environment,
      surrealDb: '3.2.1',
    });
    const firstOrderOpSQLiteReport = await runOpSQLiteBenchBenchmark(
      environment,
    );
    firstOrderContext = {
      firstOrderSurrealReport,
      firstOrderOpSQLiteReport,
    };
  });

  test('measures the reverse order and validates the paired comparison', async () => {
    if (!firstOrderContext) {
      throw new Error('First comparison order did not complete');
    }
    const { firstOrderSurrealReport, firstOrderOpSQLiteReport } =
      firstOrderContext;
    const secondOrderOpSQLiteReport = await runOpSQLiteBenchBenchmark(
      environment,
    );
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
    const comparisons = buildSQLiteBenchComparisons(
      surrealMetrics,
      opSQLiteMetrics,
    );

    expect(comparisons.map(comparison => comparison.workload)).toEqual([
      'async-insert-1k',
      'transaction-insert-1k',
      'select-and-read-1k-times-1k',
    ]);
    expect(
      comparisons.every(
        comparison =>
          comparison.factor >= 1 &&
          comparison.statement.includes(' for ') &&
          comparison.basis === 'median duration; lower is faster',
      ),
    ).toBe(true);

    pairedContext = {
      firstOrderSurrealReport,
      secondOrderSurrealReport,
      firstOrderOpSQLiteReport,
      secondOrderOpSQLiteReport,
    };
  });

  test('collects the first dedicated SurrealDB attribution sample', async () => {
    firstAttributionReport = await runSQLiteBenchBenchmark({
      ...environment,
      surrealDb: '3.2.1',
      collectAttribution: true,
    });
  });

  test('collects the second attribution sample and emits the combined report', async () => {
    if (!pairedContext) {
      throw new Error('Paired comparison did not complete');
    }
    if (!firstAttributionReport) {
      throw new Error('First attribution sample did not complete');
    }
    const {
      firstOrderSurrealReport,
      secondOrderSurrealReport,
      firstOrderOpSQLiteReport,
      secondOrderOpSQLiteReport,
    } = pairedContext;
    const secondAttributionReport = await runSQLiteBenchBenchmark({
      ...environment,
      surrealDb: '3.2.1',
      collectAttribution: true,
    });
    const comparisonSurrealMetrics = combineMetricSamples(
      firstOrderSurrealReport.metrics,
      secondOrderSurrealReport.metrics,
    );
    const attributionMetrics = combineMetricSamples(
      firstAttributionReport.metrics,
      secondAttributionReport.metrics,
    );
    const surrealMetrics = comparisonSurrealMetrics.map((metric, index) => ({
      ...metric,
      attribution: attributionMetrics[index]!.attribution,
    }));
    const opSQLiteMetrics = combineMetricSamples(
      firstOrderOpSQLiteReport.metrics,
      secondOrderOpSQLiteReport.metrics,
    );
    const comparisons = buildSQLiteBenchComparisons(
      surrealMetrics,
      opSQLiteMetrics,
    );

    await emitBenchmarkReport({
      schemaVersion: 4,
      measuredAt: new Date().toISOString(),
      source: {
        upstream: SQLITE_BENCH_SOURCE,
        comparison:
          'Same Harness binary, device, OS, debug build, memory-backed engine, record count, cooldown, fully materialized comparable workloads, and 1,000 awaited JavaScript calls through each library transaction handle. Two samples use opposite execution orders to balance first-run and thermal effects.',
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
      attributionDiagnostics: {
        interpretation:
          'engineMs times the embedded SurrealDB SDK query future. packagePathMs covers JavaScript variable encoding, Rust variable decoding, Rust result encoding, generated binding/scheduling residual, and JavaScript result decoding. unattributedMs includes loop work, transaction lifecycle, query construction, and checksum/property reads.',
        nativeBoundarySamples: [
          requireNativeBoundary(firstAttributionReport),
          requireNativeBoundary(secondAttributionReport),
        ],
        transactionBatchSweepSamples: [
          requireBatchSweep(firstAttributionReport),
          requireBatchSweep(secondAttributionReport),
        ],
      },
      comparisons,
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

type AttributionSummary = {
  engineMs: number;
  packagePathMs: number;
  unattributedMs: number;
  engineSharePercent: number;
  packagePathSharePercent: number;
  measuredQueryMs: number;
  engineShareOfMeasuredQueryPercent: number;
  packagePathShareOfMeasuredQueryPercent: number;
};

function combineMetricSamples<
  Metric extends SingleSampleMetric & { attribution?: AttributionSummary },
>(first: readonly Metric[], second: readonly Metric[]) {
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
      ...(metric.attribution && second[index]!.attribution
        ? {
            attribution: {
              basis: 'two opposite-order samples',
              samples: [metric.attribution, second[index]!.attribution],
              medians: medianAttribution(
                metric.attribution,
                second[index]!.attribution,
              ),
            },
          }
        : {}),
    };
  });
}

function medianAttribution(
  first: AttributionSummary,
  second: AttributionSummary,
) {
  const engineMs = medianOfTwo(first.engineMs, second.engineMs);
  const packagePathMs = medianOfTwo(first.packagePathMs, second.packagePathMs);
  const dominantMeasuredLayer =
    engineMs > packagePathMs
      ? 'engine-sdk'
      : packagePathMs > engineMs
      ? 'package-path'
      : 'tie';
  return {
    engineMs,
    packagePathMs,
    unattributedMs: medianOfTwo(first.unattributedMs, second.unattributedMs),
    engineSharePercent: medianOfTwo(
      first.engineSharePercent,
      second.engineSharePercent,
    ),
    packagePathSharePercent: medianOfTwo(
      first.packagePathSharePercent,
      second.packagePathSharePercent,
    ),
    measuredQueryMs: medianOfTwo(first.measuredQueryMs, second.measuredQueryMs),
    engineShareOfMeasuredQueryPercent: medianOfTwo(
      first.engineShareOfMeasuredQueryPercent,
      second.engineShareOfMeasuredQueryPercent,
    ),
    packagePathShareOfMeasuredQueryPercent: medianOfTwo(
      first.packagePathShareOfMeasuredQueryPercent,
      second.packagePathShareOfMeasuredQueryPercent,
    ),
    dominantMeasuredLayer,
    statement:
      dominantMeasuredLayer === 'tie'
        ? 'Embedded SDK execution and the measured package path have equal median time.'
        : `${
            dominantMeasuredLayer === 'engine-sdk'
              ? 'Embedded SDK execution'
              : 'The measured package path'
          } dominates median individually profiled query time.`,
  };
}

function medianOfTwo(first: number, second: number) {
  return (first + second) / 2;
}

function requireNativeBoundary(
  report: Awaited<ReturnType<typeof runSQLiteBenchBenchmark>>,
) {
  if (!report.nativeBoundary) {
    throw new Error('Attribution report is missing native boundary timings');
  }
  return report.nativeBoundary;
}

function requireBatchSweep(
  report: Awaited<ReturnType<typeof runSQLiteBenchBenchmark>>,
) {
  if (!report.transactionBatchSweep) {
    throw new Error('Attribution report is missing transaction batch sweep');
  }
  return report.transactionBatchSweep;
}
