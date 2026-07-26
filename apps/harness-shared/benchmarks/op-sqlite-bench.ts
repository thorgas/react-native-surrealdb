import { open } from '@op-engineering/op-sqlite';

import {
  SQLITE_BENCH_COOLDOWN_MS,
  SQLITE_BENCH_ITERATIONS,
  SQLITE_BENCH_SOURCE,
} from './sqlite-bench';
import { summarize, type DistributionSummary } from './statistics';

export const OP_SQLITE_VERSION = '17.1.1';

export const OP_SQLITE_BENCH_ADAPTATION = {
  upstream: SQLITE_BENCH_SOURCE,
  library: '@op-engineering/op-sqlite',
  version: OP_SQLITE_VERSION,
  databaseMode:
    'Uses op-sqlite in-memory mode instead of the upstream named database file so both libraries use memory-backed engines.',
  comparableCases:
    'The async insert, transaction insert, and fully materialized select cases match the SurrealDB adaptation.',
  additionalCases:
    'The synchronous insert and HostObject select cases are retained as op-sqlite-only upstream workloads.',
} as const;

export type OpSQLiteBenchProgress = {
  completed: number;
  total: 5;
  metric: string;
  stage: 'setup' | 'cooldown' | 'measure' | 'complete';
};

export type OpSQLiteBenchMetric = {
  name:
    | 'op-sqlite.sync-insert-1k'
    | 'op-sqlite.async-insert-1k'
    | 'op-sqlite.transaction-insert-1k'
    | 'op-sqlite.select-hostobjects-1k-times-1k'
    | 'op-sqlite.select-and-read-1k-times-1k';
  category: 'op-sqlite-bench';
  upstreamCase: string;
  variant: string;
  operationsPerSample: number;
  samplesMs: [number];
  summary: DistributionSummary;
};

export type OpSQLiteBenchReport = {
  schemaVersion: 2;
  measuredAt: string;
  source: typeof OP_SQLITE_BENCH_ADAPTATION;
  configuration: {
    profile: 'op-sqlite-bench';
    records: number;
    iterations: number;
    cooldownMs: number;
    platform: 'android' | 'ios';
    device: string;
    os: string;
    reactNative: string;
    opSQLite: typeof OP_SQLITE_VERSION;
    engine: 'memory';
    buildType: 'Debug harness';
    fullyMaterialized: true;
    clients: 1;
    syncApiAvailable: true;
  };
  checksum: string;
  metrics: OpSQLiteBenchMetric[];
};

export type OpSQLiteBenchOptions = {
  platform: 'android' | 'ios';
  device: string;
  os: string;
  reactNative: string;
  iterations?: number;
  cooldownMs?: number;
  signal?: AbortSignal;
  onProgress?: (progress: OpSQLiteBenchProgress) => void;
};

export async function runOpSQLiteBenchBenchmark(
  options: OpSQLiteBenchOptions,
): Promise<OpSQLiteBenchReport> {
  const iterations = options.iterations ?? SQLITE_BENCH_ITERATIONS;
  const cooldownMs = options.cooldownMs ?? SQLITE_BENCH_COOLDOWN_MS;
  validateOptions(iterations, cooldownMs);
  const { signal, onProgress } = options;
  assertActive(signal);
  onProgress?.({
    completed: 0,
    total: 5,
    metric: 'op-sqlite database setup',
    stage: 'setup',
  });

  const database = open({
    name: 'op_sqlite_bench.db',
    location: ':memory:',
  });
  const handleAbort = () => database.interrupt();
  signal?.addEventListener('abort', handleAbort);

  try {
    database.executeSync('DROP TABLE IF EXISTS bench');
    database.executeSync(
      'CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, value REAL)',
    );

    await coolDown(cooldownMs, signal, onProgress, 0, 'sync insert 1k');
    const syncInsertMs = measureSync(() => {
      for (let index = 0; index < iterations; index += 1) {
        assertActive(signal);
        database.executeSync('INSERT INTO bench VALUES (?,?,?)', [
          index,
          `n${index}`,
          index * 1.5,
        ]);
      }
    });
    onProgress?.({
      completed: 1,
      total: 5,
      metric: 'sync insert 1k',
      stage: 'measure',
    });

    database.executeSync('DELETE FROM bench');
    await coolDown(cooldownMs, signal, onProgress, 1, 'async insert 1k');
    const asyncInsertMs = await measure(async () => {
      for (let index = 0; index < iterations; index += 1) {
        assertActive(signal);
        await database.execute('INSERT INTO bench VALUES (?,?,?)', [
          index,
          `n${index}`,
          index * 1.5,
        ]);
      }
    });
    onProgress?.({
      completed: 2,
      total: 5,
      metric: 'async insert 1k',
      stage: 'measure',
    });

    database.executeSync('DELETE FROM bench');
    await coolDown(cooldownMs, signal, onProgress, 2, 'transaction insert 1k');
    const transactionInsertMs = await measure(() =>
      database.transaction(async transaction => {
        for (let index = 0; index < iterations; index += 1) {
          assertActive(signal);
          await transaction.execute('INSERT INTO bench VALUES (?,?,?)', [
            index,
            `n${index}`,
            index * 1.5,
          ]);
        }
      }),
    );
    onProgress?.({
      completed: 3,
      total: 5,
      metric: 'transaction insert 1k',
      stage: 'measure',
    });

    await coolDown(
      cooldownMs,
      signal,
      onProgress,
      3,
      'select 1k × 1k HostObjects',
    );
    const selectHostObjectsMs = await measure(async () => {
      for (let iteration = 0; iteration < iterations; iteration += 1) {
        assertActive(signal);
        await database.executeWithHostObjects(
          'SELECT id, name, value FROM bench',
        );
      }
    });
    onProgress?.({
      completed: 4,
      total: 5,
      metric: 'select 1k × 1k HostObjects',
      stage: 'measure',
    });

    await coolDown(
      cooldownMs,
      signal,
      onProgress,
      4,
      'select 1k × 1k + read props',
    );
    let checksum = 0;
    const selectAndReadMs = await measure(async () => {
      for (let iteration = 0; iteration < iterations; iteration += 1) {
        assertActive(signal);
        const result = await database.execute(
          'SELECT id, name, value FROM bench',
        );
        if (result.rows.length !== iterations) {
          throw new Error(
            `op-sqlite select returned ${result.rows.length} rows; expected ${iterations}`,
          );
        }
        for (const [index, value] of result.rows.entries()) {
          if (!isOpSQLiteBenchRow(value)) {
            throw new Error(
              `op-sqlite select returned an invalid row at index ${index}`,
            );
          }
          checksum +=
            value.id + value.name.length + Math.round(value.value * 10);
        }
      }
    });
    onProgress?.({
      completed: 5,
      total: 5,
      metric: 'select 1k × 1k + read props',
      stage: 'complete',
    });

    return {
      schemaVersion: 2,
      measuredAt: new Date().toISOString(),
      source: OP_SQLITE_BENCH_ADAPTATION,
      configuration: {
        profile: 'op-sqlite-bench',
        records: iterations,
        iterations,
        cooldownMs,
        platform: options.platform,
        device: options.device,
        os: options.os,
        reactNative: options.reactNative,
        opSQLite: OP_SQLITE_VERSION,
        engine: 'memory',
        buildType: 'Debug harness',
        fullyMaterialized: true,
        clients: 1,
        syncApiAvailable: true,
      },
      checksum: String(checksum),
      metrics: [
        metric(
          'op-sqlite.sync-insert-1k',
          'sync insert 1k',
          '1,000 synchronous parameterized INSERT calls',
          iterations,
          syncInsertMs,
        ),
        metric(
          'op-sqlite.async-insert-1k',
          'async insert 1k',
          '1,000 awaited parameterized INSERT calls',
          iterations,
          asyncInsertMs,
        ),
        metric(
          'op-sqlite.transaction-insert-1k',
          'tx insert 1k',
          '1,000 awaited parameterized INSERT calls in one transaction',
          iterations,
          transactionInsertMs,
        ),
        metric(
          'op-sqlite.select-hostobjects-1k-times-1k',
          'select 1k×1k HostObjects',
          '1,000 SELECT queries returning 1,000 rows as HostObjects',
          iterations,
          selectHostObjectsMs,
        ),
        metric(
          'op-sqlite.select-and-read-1k-times-1k',
          'select 1k×1k',
          '1,000 SELECT queries returning 1,000 rows; read id, name, value',
          iterations,
          selectAndReadMs,
        ),
      ],
    };
  } finally {
    signal?.removeEventListener('abort', handleAbort);
    database.executeSync('DROP TABLE IF EXISTS bench');
    database.close();
  }
}

function metric(
  name: OpSQLiteBenchMetric['name'],
  upstreamCase: string,
  variant: string,
  operationsPerSample: number,
  durationMs: number,
): OpSQLiteBenchMetric {
  return {
    name,
    category: 'op-sqlite-bench',
    upstreamCase,
    variant,
    operationsPerSample,
    samplesMs: [durationMs],
    summary: summarize([durationMs], operationsPerSample),
  };
}

async function coolDown(
  cooldownMs: number,
  signal: AbortSignal | undefined,
  onProgress: OpSQLiteBenchOptions['onProgress'],
  completed: number,
  metricName: string,
) {
  assertActive(signal);
  onProgress?.({
    completed,
    total: 5,
    metric: `cooldown before ${metricName}`,
    stage: 'cooldown',
  });
  let allocation: number[] | null = new Array(500_000).fill(0);
  if (allocation.length !== 500_000) {
    throw new Error('op-sqlite cooldown allocation failed');
  }
  allocation = null;
  await delay(cooldownMs, signal);
}

function delay(ms: number, signal?: AbortSignal): Promise<void> {
  if (ms === 0) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(resolve, ms);
    signal?.addEventListener(
      'abort',
      () => {
        clearTimeout(timeout);
        reject(new Error('op-sqlite benchmark cancelled'));
      },
      { once: true },
    );
  });
}

function measureSync(run: () => void): number {
  const startedAt = performance.now();
  run();
  return performance.now() - startedAt;
}

async function measure(run: () => Promise<unknown>): Promise<number> {
  const startedAt = performance.now();
  await run();
  return performance.now() - startedAt;
}

type OpSQLiteBenchRow = {
  id: number;
  name: string;
  value: number;
};

function isOpSQLiteBenchRow(value: unknown): value is OpSQLiteBenchRow {
  if (!value || typeof value !== 'object') return false;
  const row = value as Partial<OpSQLiteBenchRow>;
  return (
    typeof row.id === 'number' &&
    typeof row.name === 'string' &&
    typeof row.value === 'number'
  );
}

function validateOptions(iterations: number, cooldownMs: number) {
  if (!Number.isSafeInteger(iterations) || iterations < 1) {
    throw new Error('op-sqlite iterations must be a positive integer');
  }
  if (!Number.isFinite(cooldownMs) || cooldownMs < 0) {
    throw new Error('op-sqlite cooldown must be non-negative');
  }
}

function assertActive(signal?: AbortSignal) {
  if (signal?.aborted) throw new Error('op-sqlite benchmark cancelled');
}
