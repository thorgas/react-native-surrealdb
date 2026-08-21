import type { OpSQLiteBenchMetric } from './op-sqlite-bench';
import type { SQLiteBenchMetric } from './sqlite-bench';

export type SQLiteBenchComparison = {
  workload:
    | 'async-insert-1k'
    | 'transaction-insert-1k'
    | 'select-and-read-1k-times-1k';
  description: string;
  basis: 'median duration; lower is faster';
  surrealDbMetric: SQLiteBenchMetric['name'];
  opSQLiteMetric: OpSQLiteBenchMetric['name'];
  surrealDbMedianMs: number;
  opSQLiteMedianMs: number;
  fasterLibrary: 'surrealdb' | 'op-sqlite' | 'tie';
  factor: number;
  statement: string;
};

type CombinedMetric = {
  name: string;
  summary: {
    medianMs: number;
  };
};

const COMPARABLE_WORKLOADS = [
  {
    workload: 'async-insert-1k',
    description: '1,000 awaited async inserts',
    surrealDbMetric: 'sqlite-bench.async-insert-1k',
    opSQLiteMetric: 'op-sqlite.async-insert-1k',
  },
  {
    workload: 'transaction-insert-1k',
    description:
      '1,000 awaited inserts through one transaction handle followed by one commit',
    surrealDbMetric: 'sqlite-bench.transaction-insert-1k',
    opSQLiteMetric: 'op-sqlite.transaction-insert-1k',
  },
  {
    workload: 'select-and-read-1k-times-1k',
    description:
      '1,000 fully materialized selects of 1,000 rows with every property read',
    surrealDbMetric: 'sqlite-bench.select-and-read-1k-times-1k',
    opSQLiteMetric: 'op-sqlite.select-and-read-1k-times-1k',
  },
] as const;

export function buildSQLiteBenchComparisons(
  surrealMetrics: readonly CombinedMetric[],
  opSQLiteMetrics: readonly CombinedMetric[],
): SQLiteBenchComparison[] {
  return COMPARABLE_WORKLOADS.map(workload => {
    const surrealDbMedianMs = findMedian(
      surrealMetrics,
      workload.surrealDbMetric,
    );
    const opSQLiteMedianMs = findMedian(
      opSQLiteMetrics,
      workload.opSQLiteMetric,
    );
    const fasterLibrary =
      surrealDbMedianMs < opSQLiteMedianMs
        ? 'surrealdb'
        : opSQLiteMedianMs < surrealDbMedianMs
        ? 'op-sqlite'
        : 'tie';
    const factor =
      fasterLibrary === 'tie'
        ? 1
        : Math.max(surrealDbMedianMs, opSQLiteMedianMs) /
          Math.min(surrealDbMedianMs, opSQLiteMedianMs);

    return {
      ...workload,
      basis: 'median duration; lower is faster',
      surrealDbMedianMs,
      opSQLiteMedianMs,
      fasterLibrary,
      factor,
      statement:
        fasterLibrary === 'tie'
          ? `SurrealDB and op-sqlite have the same median duration for ${workload.description}.`
          : `${displayName(fasterLibrary)} is ${formatFactor(
              factor,
            )}× faster than ${displayName(
              fasterLibrary === 'surrealdb' ? 'op-sqlite' : 'surrealdb',
            )} for ${workload.description}.`,
    };
  });
}

function findMedian(
  metrics: readonly CombinedMetric[],
  metricName: string,
): number {
  const metric = metrics.find(candidate => candidate.name === metricName);
  if (!metric) {
    throw new Error(`Missing comparable sqlite-bench metric: ${metricName}`);
  }
  if (
    !Number.isFinite(metric.summary.medianMs) ||
    metric.summary.medianMs <= 0
  ) {
    throw new Error(`Invalid median for sqlite-bench metric: ${metricName}`);
  }
  return metric.summary.medianMs;
}

function displayName(library: 'surrealdb' | 'op-sqlite'): string {
  return library === 'surrealdb' ? 'SurrealDB' : 'op-sqlite';
}

function formatFactor(factor: number): string {
  return factor >= 10 ? factor.toFixed(1) : factor.toFixed(2);
}
