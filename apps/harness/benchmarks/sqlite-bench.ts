import {
  benchmarkNativeBoundary,
  connect,
  type QueryProfile,
  type SurrealClient,
} from 'react-native-surrealdb';

import { summarize, type DistributionSummary } from './statistics';

export const SQLITE_BENCH_SOURCE = {
  name: 'ospfranco/sqlite-bench',
  url: 'https://github.com/ospfranco/sqlite-bench',
  revision: '4c022c9a38294b66af2cd79fae64f0e91f25353b',
  license: 'MIT',
  workloadPath: 'src/app/index.tsx',
  resultImage:
    'https://pbs.twimg.com/media/HLwBJGpWcAAV_Fl?format=jpg&name=4096x4096',
  researchedAt: '2026-07-24',
  adaptation:
    'Preserves the upstream 1,000 individual async inserts, 1,000 awaited inserts through one JavaScript transaction handle, and 1,000 full-table selects with every property read. It uses the same in-memory engine as the existing mobile regression suite.',
  exclusions:
    'The upstream synchronous insert and HostObject/HybridObject variants have no equivalent in the asynchronous SurrealDB client API.',
} as const;

export const SQLITE_BENCH_ITERATIONS = 1_000;
export const SQLITE_BENCH_COOLDOWN_MS = 2_500;

export const SQLITE_BENCH_PUBLISHED_RESULTS = {
  note: 'Values transcribed from the result image supplied by the benchmark author. Device, OS, and build configuration are not stated in the image, so these are context rather than a regression baseline.',
  libraries: [
    {
      name: 'op-sqlite',
      asyncInsertMs: 1_457.3,
      transactionInsertMs: 152.8,
      selectAndReadMs: 534.2,
    },
    {
      name: 'nitro-sqlite',
      asyncInsertMs: 1_448.6,
      transactionInsertMs: 124.1,
      selectAndReadMs: 1_843.5,
    },
    {
      name: 'expo-sqlite',
      asyncInsertMs: 2_046.3,
      transactionInsertMs: 551.3,
      selectAndReadMs: 3_344.3,
    },
  ],
} as const;

export type SQLiteBenchProgress = {
  completed: number;
  total: 3;
  metric: string;
  stage: 'setup' | 'cooldown' | 'measure' | 'complete';
};

export type SQLiteBenchMetric = {
  name:
    | 'sqlite-bench.async-insert-1k'
    | 'sqlite-bench.transaction-insert-1k'
    | 'sqlite-bench.select-and-read-1k-times-1k';
  category: 'sqlite-bench';
  upstreamCase: string;
  variant: string;
  operationsPerSample: number;
  samplesMs: [number];
  summary: DistributionSummary;
  attribution?: SQLiteBenchAttribution;
};

export type SQLiteBenchAttribution = {
  callsProfiled: number;
  totalWorkloadMs: number;
  engineMs: number;
  packagePathMs: number;
  unattributedMs: number;
  engineSharePercent: number;
  packagePathSharePercent: number;
  measuredQueryMs: number;
  engineShareOfMeasuredQueryPercent: number;
  packagePathShareOfMeasuredQueryPercent: number;
  dominantMeasuredLayer: 'engine-sdk' | 'package-path' | 'tie';
  statement: string;
  stages: {
    inputEncodeMs: number;
    nativeInputDecodeMs: number;
    nativeOutputEncodeMs: number;
    bindingAndSchedulingMs: number;
    outputDecodeMs: number;
  };
};

export type SQLiteBenchBatchSweep = {
  batchSize: number;
  nativeCalls: number;
  attribution: SQLiteBenchAttribution;
};

export type SQLiteBenchReport = {
  schemaVersion: 2;
  measuredAt: string;
  source: typeof SQLITE_BENCH_SOURCE;
  publishedReference: typeof SQLITE_BENCH_PUBLISHED_RESULTS;
  configuration: {
    profile: 'sqlite-bench';
    records: number;
    iterations: number;
    cooldownMs: number;
    platform: 'android' | 'ios';
    device: string;
    os: string;
    reactNative: string;
    surrealDb: string;
    engine: 'memory';
    buildType: 'Debug harness';
    fullyMaterialized: true;
    clients: 1;
    syncApiAvailable: false;
  };
  nativeBoundary?: Awaited<ReturnType<typeof benchmarkNativeBoundary>>;
  transactionBatchSweep?: SQLiteBenchBatchSweep[];
  checksum: string;
  metrics: SQLiteBenchMetric[];
};

export type SQLiteBenchOptions = {
  platform: 'android' | 'ios';
  device: string;
  os: string;
  reactNative: string;
  surrealDb: string;
  iterations?: number;
  cooldownMs?: number;
  collectAttribution?: boolean;
  signal?: AbortSignal;
  onProgress?: (progress: SQLiteBenchProgress) => void;
};

export async function runSQLiteBenchBenchmark(
  options: SQLiteBenchOptions,
): Promise<SQLiteBenchReport> {
  const iterations = options.iterations ?? SQLITE_BENCH_ITERATIONS;
  const cooldownMs = options.cooldownMs ?? SQLITE_BENCH_COOLDOWN_MS;
  const collectAttribution = options.collectAttribution ?? false;
  validateOptions(iterations, cooldownMs);
  const { signal, onProgress } = options;
  assertActive(signal);
  onProgress?.({
    completed: 0,
    total: 3,
    metric: 'database setup',
    stage: 'setup',
  });
  const nativeBoundary = collectAttribution
    ? await benchmarkNativeBoundary(iterations)
    : undefined;

  const database = await connect(
    {
      endpoint: 'memory',
      namespace: 'react_native_sqlite_bench',
      database: 'react_native_sqlite_bench',
    },
    signal ? { signal } : undefined,
  );

  try {
    await query(
      database,
      'REMOVE TABLE IF EXISTS sqlite_bench; DEFINE TABLE sqlite_bench SCHEMALESS',
      signal,
    );

    await coolDown(cooldownMs, signal, onProgress, 0, 'async insert 1k');
    const asyncInsertProfiles: QueryProfile[] = [];
    const asyncInsertMs = await measure(async () => {
      for (let index = 0; index < iterations; index += 1) {
        assertActive(signal);
        if (collectAttribution) {
          const profiled = await database.queryProfiled(
            createStatement(index),
            undefined,
            signal ? { signal } : undefined,
          );
          asyncInsertProfiles.push(profiled.profile);
        } else {
          await query(database, createStatement(index), signal);
        }
      }
    });
    onProgress?.({
      completed: 1,
      total: 3,
      metric: 'async insert 1k',
      stage: 'measure',
    });

    await query(database, 'DELETE sqlite_bench RETURN NONE', signal);
    await coolDown(cooldownMs, signal, onProgress, 1, 'transaction insert 1k');
    const transactionInsertProfiles: QueryProfile[] = [];
    const transactionInsertMs = await measure(() =>
      database.transaction(
        async transaction => {
          for (let index = 0; index < iterations; index += 1) {
            assertActive(signal);
            if (collectAttribution) {
              const profiled = await transaction.queryProfiled(
                createStatement(index),
              );
              transactionInsertProfiles.push(profiled.profile);
            } else {
              await transaction.query(createStatement(index));
            }
          }
        },
        signal ? { signal } : undefined,
      ),
    );
    onProgress?.({
      completed: 2,
      total: 3,
      metric: 'transaction insert 1k',
      stage: 'measure',
    });

    await coolDown(
      cooldownMs,
      signal,
      onProgress,
      2,
      'select 1k × 1k + read props',
    );
    let checksum = 0n;
    const selectProfiles: QueryProfile[] = [];
    const selectAndReadMs = await measure(async () => {
      for (let iteration = 0; iteration < iterations; iteration += 1) {
        assertActive(signal);
        const results = collectAttribution
          ? await database
              .queryProfiled<unknown>(
                'SELECT sequence, name, value FROM sqlite_bench',
                undefined,
                signal ? { signal } : undefined,
              )
              .then(profiled => {
                selectProfiles.push(profiled.profile);
                return profiled.results;
              })
          : await query(
              database,
              'SELECT sequence, name, value FROM sqlite_bench',
              signal,
            );
        const rows = results.at(-1)?.value;
        if (!Array.isArray(rows) || rows.length !== iterations) {
          throw new Error(
            `sqlite-bench select returned ${
              Array.isArray(rows) ? rows.length : 'a non-array value'
            }; expected ${iterations}`,
          );
        }
        for (const [index, value] of rows.entries()) {
          if (!isSQLiteBenchRow(value)) {
            throw new Error(
              `sqlite-bench select returned an invalid row at index ${index}`,
            );
          }
          checksum +=
            value.sequence +
            BigInt(value.name.length) +
            BigInt(Math.round(value.value * 10));
        }
      }
    });
    const transactionBatchSweep = collectAttribution
      ? await runTransactionBatchSweep(database, iterations, signal)
      : undefined;
    onProgress?.({
      completed: 3,
      total: 3,
      metric: 'select 1k × 1k + read props',
      stage: 'complete',
    });

    const configuration = {
      profile: 'sqlite-bench' as const,
      records: iterations,
      iterations,
      cooldownMs,
      platform: options.platform,
      device: options.device,
      os: options.os,
      reactNative: options.reactNative,
      surrealDb: options.surrealDb,
      engine: 'memory' as const,
      buildType: 'Debug harness' as const,
      fullyMaterialized: true as const,
      clients: 1 as const,
      syncApiAvailable: false as const,
    };
    return {
      schemaVersion: 2,
      measuredAt: new Date().toISOString(),
      source: SQLITE_BENCH_SOURCE,
      publishedReference: SQLITE_BENCH_PUBLISHED_RESULTS,
      configuration,
      ...(nativeBoundary ? { nativeBoundary } : {}),
      ...(transactionBatchSweep ? { transactionBatchSweep } : {}),
      checksum: checksum.toString(),
      metrics: [
        metric(
          'sqlite-bench.async-insert-1k',
          'async insert 1k',
          '1,000 awaited CREATE queries / 1,000 bridge calls',
          iterations,
          asyncInsertMs,
          asyncInsertProfiles,
        ),
        metric(
          'sqlite-bench.transaction-insert-1k',
          'tx insert 1k',
          '1,000 awaited CREATE calls through one transaction handle',
          iterations,
          transactionInsertMs,
          transactionInsertProfiles,
        ),
        metric(
          'sqlite-bench.select-and-read-1k-times-1k',
          'select 1k×1k',
          '1,000 SELECT queries returning 1,000 rows; read sequence, name, value',
          iterations,
          selectAndReadMs,
          selectProfiles,
        ),
      ],
    };
  } finally {
    await database.close();
  }
}

function metric(
  name: SQLiteBenchMetric['name'],
  upstreamCase: string,
  variant: string,
  operationsPerSample: number,
  durationMs: number,
  profiles: readonly QueryProfile[],
): SQLiteBenchMetric {
  return {
    name,
    category: 'sqlite-bench',
    upstreamCase,
    variant,
    operationsPerSample,
    samplesMs: [durationMs],
    summary: summarize([durationMs], operationsPerSample),
    ...(profiles.length > 0
      ? { attribution: buildAttribution(durationMs, profiles) }
      : {}),
  };
}

function buildAttribution(
  totalWorkloadMs: number,
  profiles: readonly QueryProfile[],
): SQLiteBenchAttribution {
  const stages = {
    inputEncodeMs: sum(profiles, profile => profile.inputEncodeMs),
    nativeInputDecodeMs: sum(profiles, profile => profile.nativeInputDecodeMs),
    nativeOutputEncodeMs: sum(
      profiles,
      profile => profile.nativeOutputEncodeMs,
    ),
    bindingAndSchedulingMs: sum(
      profiles,
      profile => profile.bindingAndSchedulingMs,
    ),
    outputDecodeMs: sum(profiles, profile => profile.outputDecodeMs),
  };
  const engineMs = sum(profiles, profile => profile.engineMs);
  const packagePathMs = Object.values(stages).reduce(
    (total, value) => total + value,
    0,
  );
  const unattributedMs = Math.max(
    0,
    totalWorkloadMs - engineMs - packagePathMs,
  );
  const measuredQueryMs = engineMs + packagePathMs;
  const dominantMeasuredLayer =
    engineMs > packagePathMs
      ? 'engine-sdk'
      : packagePathMs > engineMs
      ? 'package-path'
      : 'tie';
  return {
    callsProfiled: profiles.length,
    totalWorkloadMs,
    engineMs,
    packagePathMs,
    unattributedMs,
    engineSharePercent: (engineMs / totalWorkloadMs) * 100,
    packagePathSharePercent: (packagePathMs / totalWorkloadMs) * 100,
    measuredQueryMs,
    engineShareOfMeasuredQueryPercent: (engineMs / measuredQueryMs) * 100,
    packagePathShareOfMeasuredQueryPercent:
      (packagePathMs / measuredQueryMs) * 100,
    dominantMeasuredLayer,
    statement:
      dominantMeasuredLayer === 'tie'
        ? 'Embedded SDK execution and the measured package path took equal time.'
        : `${
            dominantMeasuredLayer === 'engine-sdk'
              ? 'Embedded SDK execution'
              : 'The measured package path'
          } dominated individually profiled query time.`,
    stages,
  };
}

async function runTransactionBatchSweep(
  database: SurrealClient,
  iterations: number,
  signal?: AbortSignal,
): Promise<SQLiteBenchBatchSweep[]> {
  const batchSizes = [...new Set([1, 10, 100, iterations])]
    .filter(batchSize => batchSize <= iterations)
    .sort((left, right) => left - right);
  const sweep: SQLiteBenchBatchSweep[] = [];

  for (const batchSize of batchSizes) {
    await query(database, 'DELETE sqlite_bench RETURN NONE', signal);
    const profiles: QueryProfile[] = [];
    const totalWorkloadMs = await measure(() =>
      database.transaction(
        async transaction => {
          for (let start = 0; start < iterations; start += batchSize) {
            assertActive(signal);
            const statements = Array.from(
              { length: Math.min(batchSize, iterations - start) },
              (_, offset) => createStatement(start + offset),
            ).join(';\n');
            const profiled = await transaction.queryProfiled(statements);
            profiles.push(profiled.profile);
          }
        },
        signal ? { signal } : undefined,
      ),
    );
    sweep.push({
      batchSize,
      nativeCalls: profiles.length,
      attribution: buildAttribution(totalWorkloadMs, profiles),
    });
  }
  return sweep;
}

function sum(
  profiles: readonly QueryProfile[],
  select: (profile: QueryProfile) => number,
): number {
  return profiles.reduce((total, profile) => total + select(profile), 0);
}

function createStatement(index: number): string {
  return `CREATE sqlite_bench:row_${index} CONTENT {
    sequence: ${index},
    name: 'n${index}',
    value: ${(index * 1.5).toFixed(1)}
  } RETURN NONE`;
}

async function coolDown(
  cooldownMs: number,
  signal: AbortSignal | undefined,
  onProgress: SQLiteBenchOptions['onProgress'],
  completed: number,
  metricName: string,
) {
  assertActive(signal);
  onProgress?.({
    completed,
    total: 3,
    metric: `cooldown before ${metricName}`,
    stage: 'cooldown',
  });
  let allocation: number[] | null = new Array(500_000).fill(0);
  if (allocation.length !== 500_000) {
    throw new Error('sqlite-bench cooldown allocation failed');
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
        reject(new Error('sqlite-bench cancelled'));
      },
      { once: true },
    );
  });
}

async function measure(run: () => Promise<unknown>): Promise<number> {
  const startedAt = performance.now();
  await run();
  return performance.now() - startedAt;
}

type SQLiteBenchRow = {
  sequence: bigint;
  name: string;
  value: number;
};

function isSQLiteBenchRow(value: unknown): value is SQLiteBenchRow {
  if (!value || typeof value !== 'object') return false;
  const row = value as Partial<SQLiteBenchRow>;
  return (
    typeof row.sequence === 'bigint' &&
    typeof row.name === 'string' &&
    typeof row.value === 'number'
  );
}

async function query(
  database: SurrealClient,
  surql: string,
  signal?: AbortSignal,
) {
  assertActive(signal);
  return database.query<unknown>(
    surql,
    undefined,
    signal ? { signal } : undefined,
  );
}

function validateOptions(iterations: number, cooldownMs: number) {
  if (!Number.isSafeInteger(iterations) || iterations < 1) {
    throw new Error('sqlite-bench iterations must be a positive integer');
  }
  if (!Number.isFinite(cooldownMs) || cooldownMs < 0) {
    throw new Error('sqlite-bench cooldown must be non-negative');
  }
}

function assertActive(signal?: AbortSignal) {
  if (signal?.aborted) throw new Error('sqlite-bench cancelled');
}
