import { open, type DB } from '@op-engineering/op-sqlite';
import { NativeModules, Platform } from 'react-native';
import {
  connect,
  type ExperimentalSyncClient,
  type SurrealClient,
} from 'react-native-surrealdb';

import { OP_SQLITE_VERSION } from './op-sqlite-bench';
import { summarize, type DistributionSummary } from './statistics';

const DEFAULT_WARMUPS = 3;
const DEFAULT_SAMPLES = 10;
const BATCH_SIZE = 10;
const PENDING_COMMITS = 25;

const syncOptions = {
  partitionId: 'benchmark-partition',
  clientId: 'benchmark-client',
  requestedScope: 'all',
  subscriptionRevision: 1n,
} as const;

type StorageConstants = {
  ANDROID_FILES_PATH?: unknown;
  IOS_LIBRARY_PATH?: unknown;
};

export type SyncEngineBenchmarkMetric = {
  name:
    | 'sync-local.surrealkv-enqueue-single'
    | 'sync-local.sqlite-enqueue-single-lower-bound'
    | 'sync-local.surrealkv-enqueue-batch-10'
    | 'sync-local.sqlite-enqueue-batch-10-lower-bound'
    | 'sync-local.surrealkv-materialize-pending'
    | 'sync-local.sqlite-materialize-outbox-lower-bound'
    | 'sync-local.surrealkv-reopen'
    | 'sync-local.sqlite-reopen-lower-bound';
  category: 'sync-local';
  layer: 'durable-enqueue' | 'durable-read' | 'durable-reopen';
  implementation: 'surrealkv-sync-runtime' | 'op-sqlite-lower-bound';
  operationsPerSample: number;
  samplesMs: number[];
  summary: DistributionSummary;
};

export type SyncEngineBenchmarkReport = {
  schemaVersion: 4;
  measuredAt: string;
  source: {
    purpose: string;
    comparisonBoundary: string;
    excluded: string;
  };
  configuration: {
    profile: 'sync-local-durable';
    records: number;
    iterations: number;
    samples: number;
    warmups: number;
    pendingCommits: number;
    batchSizes: readonly [1, 10];
    cooldownMs: 0;
    platform: 'android' | 'ios';
    device: string;
    os: string;
    reactNative: string;
    surrealDb: string;
    opSQLite: typeof OP_SQLITE_VERSION;
    sqliteJournalMode: 'WAL';
    sqliteSynchronous: 'FULL';
    storageResetPolicy: 'logical-tables-reset-physical-stores-reused';
    engine: 'persistent-surrealkv-vs-file-sqlite';
    buildType: 'Debug harness';
    fullyMaterialized: true;
    clients: 1;
    syncApiAvailable: true;
  };
  checksums: {
    surrealDb: string;
    opSQLite: string;
  };
  comparisons: Array<{
    workload: 'single-record-commit' | 'ten-record-atomic-commit';
    basis: 'median duration; lower is faster';
    caveat: string;
    surrealDbMedianMs: number;
    opSQLiteMedianMs: number;
    ratio: number;
  }>;
  metrics: SyncEngineBenchmarkMetric[];
};

export type SyncEngineBenchmarkOptions = {
  platform: 'android' | 'ios';
  device: string;
  os: string;
  reactNative: string;
  surrealDb: string;
  warmups?: number;
  samples?: number;
};

export async function runSyncEngineBenchmark(
  options: SyncEngineBenchmarkOptions,
): Promise<SyncEngineBenchmarkReport> {
  const warmups = options.warmups ?? DEFAULT_WARMUPS;
  const samples = options.samples ?? DEFAULT_SAMPLES;
  validateCount('warmups', warmups, true);
  validateCount('samples', samples, false);

  const storageRoot = appPrivateStorageRoot();
  const surrealConfig = {
    endpoint: `surrealkv://${storageRoot}/surrealdb-sync-local-benchmark`,
    namespace: 'sync-local-benchmark',
    database: 'sync-local-benchmark',
  } as const;
  const sqliteConfig = {
    name: 'sync-local-benchmark.sqlite',
    location: storageRoot,
  } as const;

  let surreal: SurrealClient | undefined;
  let sync: ExperimentalSyncClient | undefined;
  let sqlite: DB | undefined;

  try {
    surreal = await connect(surrealConfig);
    await resetSurreal(surreal);
    sqlite = open(sqliteConfig);
    configureSQLite(sqlite);
    resetSQLite(sqlite);

    const single = await measurePairedEnqueue({
      surreal,
      sqlite,
      batchSize: 1,
      prefix: 'single',
      warmups,
      samples,
    });
    const batch = await measurePairedEnqueue({
      surreal,
      sqlite,
      batchSize: BATCH_SIZE,
      prefix: 'batch',
      warmups,
      samples,
    });
    await resetSurreal(surreal);
    resetSQLite(sqlite);
    sync = await surreal.openExperimentalSync(syncOptions);
    for (let index = 0; index < PENDING_COMMITS; index += 1) {
      const suffix = `pending-${index}`;
      await enqueueSurreal(sync, suffix, 1);
      await enqueueSQLite(sqlite, suffix, 1);
    }

    const surrealPendingSamples = await measureSamples(samples, async () => {
      const pending = await sync!.pending();
      assertEqual(pending.length, PENDING_COMMITS, 'SurrealDB pending commits');
      normalizeSurrealPending(pending);
    });
    const sqlitePendingSamples = await measureSamples(samples, async () => {
      const result = await sqlite!.execute(
        'SELECT commit_id, payload FROM sync_outbox ORDER BY commit_id',
      );
      assertEqual(result.rows.length, PENDING_COMMITS, 'SQLite outbox rows');
      normalizeSQLitePending(result.rows);
    });

    const surrealStatus = await sync.status();
    assertEqual(
      surrealStatus.pendingCount,
      PENDING_COMMITS,
      'SurrealDB status pending count',
    );
    const surrealChecksum = await semanticSurrealChecksum(sync, surreal);
    const sqliteChecksum = await semanticSQLiteChecksum(sqlite);
    if (surrealChecksum !== sqliteChecksum) {
      throw new Error(
        `Materialized benchmark state differs: ${surrealChecksum} != ${sqliteChecksum}`,
      );
    }

    await sync.close();
    sync = undefined;
    await surreal.close();
    surreal = undefined;
    sqlite.close();
    sqlite = undefined;

    const surrealReopenSamples: number[] = [];
    const sqliteReopenSamples: number[] = [];
    for (let index = 0; index < samples; index += 1) {
      if (index % 2 === 0) {
        surrealReopenSamples.push(
          await measureSurrealReopen(surrealConfig, PENDING_COMMITS),
        );
        sqliteReopenSamples.push(
          await measureSQLiteReopen(sqliteConfig, PENDING_COMMITS),
        );
      } else {
        sqliteReopenSamples.push(
          await measureSQLiteReopen(sqliteConfig, PENDING_COMMITS),
        );
        surrealReopenSamples.push(
          await measureSurrealReopen(surrealConfig, PENDING_COMMITS),
        );
      }
    }

    const metrics: SyncEngineBenchmarkMetric[] = [
      metric(
        'sync-local.surrealkv-enqueue-single',
        'durable-enqueue',
        'surrealkv-sync-runtime',
        1,
        single.surreal,
      ),
      metric(
        'sync-local.sqlite-enqueue-single-lower-bound',
        'durable-enqueue',
        'op-sqlite-lower-bound',
        1,
        single.sqlite,
      ),
      metric(
        'sync-local.surrealkv-enqueue-batch-10',
        'durable-enqueue',
        'surrealkv-sync-runtime',
        BATCH_SIZE,
        batch.surreal,
      ),
      metric(
        'sync-local.sqlite-enqueue-batch-10-lower-bound',
        'durable-enqueue',
        'op-sqlite-lower-bound',
        BATCH_SIZE,
        batch.sqlite,
      ),
      metric(
        'sync-local.surrealkv-materialize-pending',
        'durable-read',
        'surrealkv-sync-runtime',
        PENDING_COMMITS,
        surrealPendingSamples,
      ),
      metric(
        'sync-local.sqlite-materialize-outbox-lower-bound',
        'durable-read',
        'op-sqlite-lower-bound',
        PENDING_COMMITS,
        sqlitePendingSamples,
      ),
      metric(
        'sync-local.surrealkv-reopen',
        'durable-reopen',
        'surrealkv-sync-runtime',
        1,
        surrealReopenSamples,
      ),
      metric(
        'sync-local.sqlite-reopen-lower-bound',
        'durable-reopen',
        'op-sqlite-lower-bound',
        1,
        sqliteReopenSamples,
      ),
    ];
    const caveat =
      'The SQLite transaction writes the same logical application rows plus a minimal durable outbox, but omits sync protocol validation, canonical fingerprints, optimistic-state reconstruction, and conflict bookkeeping. It is a lower bound, not a substitute sync engine.';

    return {
      schemaVersion: 4,
      measuredAt: new Date().toISOString(),
      source: {
        purpose:
          'Measure the local persistence cost of the experimental sync runtime before adding network and authority latency.',
        comparisonBoundary:
          'Same RN86 Debug Harness process, device, persistent app-private storage, identical empty logical state before every enqueue sample, sample order balancing, record values, and transaction batch sizes.',
        excluded:
          'HTTP transport, server execution, authorization, changefeed ingestion, conflict resolution, and release-build claims.',
      },
      configuration: {
        profile: 'sync-local-durable',
        records: PENDING_COMMITS,
        iterations: samples,
        samples,
        warmups,
        pendingCommits: PENDING_COMMITS,
        batchSizes: [1, 10],
        cooldownMs: 0,
        platform: options.platform,
        device: options.device,
        os: options.os,
        reactNative: options.reactNative,
        surrealDb: options.surrealDb,
        opSQLite: OP_SQLITE_VERSION,
        sqliteJournalMode: 'WAL',
        sqliteSynchronous: 'FULL',
        storageResetPolicy: 'logical-tables-reset-physical-stores-reused',
        engine: 'persistent-surrealkv-vs-file-sqlite',
        buildType: 'Debug harness',
        fullyMaterialized: true,
        clients: 1,
        syncApiAvailable: true,
      },
      checksums: {
        surrealDb: surrealChecksum,
        opSQLite: sqliteChecksum,
      },
      comparisons: [
        comparison(
          'single-record-commit',
          single.surreal,
          single.sqlite,
          caveat,
        ),
        comparison(
          'ten-record-atomic-commit',
          batch.surreal,
          batch.sqlite,
          caveat,
        ),
      ],
      metrics,
    };
  } finally {
    if (sync && !sync.isClosed) await sync.close();
    if (surreal && !surreal.isClosed) {
      await resetSurreal(surreal);
      await surreal.close();
    } else {
      const cleanup = await connect(surrealConfig);
      await resetSurreal(cleanup);
      await cleanup.close();
    }
    if (sqlite) {
      resetSQLite(sqlite);
      sqlite.close();
    } else {
      const cleanup = open(sqliteConfig);
      configureSQLite(cleanup);
      resetSQLite(cleanup);
      cleanup.close();
    }
  }
}

async function measurePairedEnqueue(options: {
  surreal: SurrealClient;
  sqlite: DB;
  batchSize: number;
  prefix: string;
  warmups: number;
  samples: number;
}): Promise<{ surreal: number[]; sqlite: number[] }> {
  const surreal: number[] = [];
  const sqlite: number[] = [];
  const total = options.warmups + options.samples;
  for (let index = 0; index < total; index += 1) {
    const record = index >= options.warmups;
    const suffix = `${options.prefix}-${index}`;
    if (index % 2 === 0) {
      const surrealMs = await measureFreshSurrealEnqueue(
        options.surreal,
        suffix,
        options.batchSize,
      );
      const sqliteMs = await measureFreshSQLiteEnqueue(
        options.sqlite,
        suffix,
        options.batchSize,
      );
      if (record) {
        surreal.push(surrealMs);
        sqlite.push(sqliteMs);
      }
    } else {
      const sqliteMs = await measureFreshSQLiteEnqueue(
        options.sqlite,
        suffix,
        options.batchSize,
      );
      const surrealMs = await measureFreshSurrealEnqueue(
        options.surreal,
        suffix,
        options.batchSize,
      );
      if (record) {
        sqlite.push(sqliteMs);
        surreal.push(surrealMs);
      }
    }
  }
  return { surreal, sqlite };
}

async function measureFreshSurrealEnqueue(
  database: SurrealClient,
  suffix: string,
  batchSize: number,
): Promise<number> {
  await resetSurreal(database);
  const sync = await database.openExperimentalSync(syncOptions);
  try {
    return await measure(() => enqueueSurreal(sync, suffix, batchSize));
  } finally {
    await sync.close();
  }
}

async function measureFreshSQLiteEnqueue(
  database: DB,
  suffix: string,
  batchSize: number,
): Promise<number> {
  resetSQLite(database);
  return measure(() => enqueueSQLite(database, suffix, batchSize));
}

async function enqueueSurreal(
  sync: ExperimentalSyncClient,
  suffix: string,
  batchSize: number,
) {
  const operations = Array.from({ length: batchSize }, (_, index) => ({
    kind: 'upsert',
    record_id: `sync_bench_record:${suffix}-${index}`,
    base_version: 'absent',
    value: benchmarkValue(suffix, index),
    reference: null,
  }));
  await sync.enqueue({
    identity: {
      clientCommitId: `commit-${suffix}`,
      fingerprint: 'computed-by-native',
    },
    operations,
  });
}

async function enqueueSQLite(database: DB, suffix: string, batchSize: number) {
  const operations = Array.from({ length: batchSize }, (_, index) => ({
    recordId: `sync_bench_record:${suffix}-${index}`,
    value: benchmarkValue(suffix, index),
  }));
  await database.transaction(async transaction => {
    for (const operation of operations) {
      await transaction.execute(
        'INSERT INTO sync_record (id, payload) VALUES (?, ?)',
        [operation.recordId, JSON.stringify(operation.value)],
      );
    }
    await transaction.execute(
      'INSERT INTO sync_outbox (commit_id, payload) VALUES (?, ?)',
      [`commit-${suffix}`, JSON.stringify(operations)],
    );
  });
}

function benchmarkValue(suffix: string, index: number) {
  return {
    title: `Benchmark record ${suffix}-${index}`,
    completed: index % 2 === 0,
    priority: index,
    quantity: index + 0.25,
    tags: ['local', 'durable', `batch-${index}`],
  };
}

type BenchmarkValue = ReturnType<typeof benchmarkValue>;

async function semanticSurrealChecksum(
  sync: ExperimentalSyncClient,
  database: SurrealClient,
): Promise<string> {
  const pending = normalizeSurrealPending(await sync.pending());
  const results = await database.query<BenchmarkValue[]>(
    'SELECT VALUE { title: title, completed: completed, priority: priority, quantity: quantity, tags: tags } FROM sync_bench_record ORDER BY title',
  );
  const values = results.at(-1)?.value;
  if (!Array.isArray(values)) {
    throw new Error('SurrealDB benchmark records were not materialized');
  }
  const records = values.map(normalizeBenchmarkValue).sort();
  assertEqual(records.length, PENDING_COMMITS, 'SurrealDB records');
  return semanticChecksum(records, pending);
}

async function semanticSQLiteChecksum(database: DB): Promise<string> {
  const pendingRows = await database.execute(
    'SELECT commit_id, payload FROM sync_outbox ORDER BY commit_id',
  );
  const pending = normalizeSQLitePending(pendingRows.rows);
  const recordRows = await database.execute(
    'SELECT id, payload FROM sync_record ORDER BY id',
  );
  const records = recordRows.rows
    .map(row => {
      if (typeof row.id !== 'string' || typeof row.payload !== 'string') {
        throw new Error('SQLite benchmark record materialization is invalid');
      }
      return normalizeBenchmarkValue(JSON.parse(row.payload));
    })
    .sort();
  assertEqual(records.length, PENDING_COMMITS, 'SQLite records');
  return semanticChecksum(records, pending);
}

function normalizeSurrealPending(pending: readonly unknown[]): string[] {
  return pending
    .flatMap(value => {
      if (!isRecord(value)) {
        throw new Error('SurrealDB pending commit is invalid');
      }
      const identity = value.identity;
      const operations = value.operations;
      if (!isRecord(identity) || typeof identity.clientCommitId !== 'string') {
        throw new Error('SurrealDB pending identity is invalid');
      }
      const commitId = identity.clientCommitId;
      if (!Array.isArray(operations)) {
        throw new Error('SurrealDB pending operations are invalid');
      }
      return operations.map(operation => {
        if (!isRecord(operation) || typeof operation.record_id !== 'string') {
          throw new Error('SurrealDB pending operation is invalid');
        }
        return semanticOperation(
          commitId,
          operation.record_id,
          operation.value,
        );
      });
    })
    .sort();
}

function normalizeSQLitePending(
  rows: Awaited<ReturnType<DB['execute']>>['rows'],
): string[] {
  return rows
    .flatMap(row => {
      if (
        typeof row.commit_id !== 'string' ||
        typeof row.payload !== 'string'
      ) {
        throw new Error('SQLite outbox materialization returned invalid data');
      }
      const operations: unknown = JSON.parse(row.payload);
      if (!Array.isArray(operations)) {
        throw new Error('SQLite outbox operations are invalid');
      }
      return operations.map(operation => {
        if (!isRecord(operation) || typeof operation.recordId !== 'string') {
          throw new Error('SQLite outbox operation is invalid');
        }
        return semanticOperation(
          row.commit_id as string,
          operation.recordId,
          operation.value,
        );
      });
    })
    .sort();
}

function semanticOperation(
  commitId: string,
  recordId: string,
  value: unknown,
): string {
  return `${commitId}|${recordId}|${normalizeBenchmarkValue(value)}`;
}

function normalizeBenchmarkValue(value: unknown): string {
  if (!isRecord(value)) {
    throw new Error('Benchmark value is not an object');
  }
  const { title, completed, priority, quantity, tags } = value;
  if (
    typeof title !== 'string' ||
    typeof completed !== 'boolean' ||
    (typeof priority !== 'number' && typeof priority !== 'bigint') ||
    typeof quantity !== 'number' ||
    !Array.isArray(tags) ||
    !tags.every(tag => typeof tag === 'string')
  ) {
    throw new Error(
      `Benchmark value is invalid (keys=${Object.keys(value).join(
        ',',
      )}; title=${typeof title}, completed=${typeof completed}, priority=${typeof priority}, quantity=${typeof quantity}, tags=${
        Array.isArray(tags)
          ? tags.map(tag => typeof tag).join(',')
          : typeof tags
      })`,
    );
  }
  return `${title}|${completed}|${priority}|${quantity}|${tags.join(',')}`;
}

function semanticChecksum(records: string[], pending: string[]): string {
  let hash = 0;
  const materialized = [
    ...records.map(value => `record:${value}`),
    ...pending.map(value => `pending:${value}`),
  ].sort();
  for (const value of materialized.join('\n')) {
    hash = (hash * 31 + value.charCodeAt(0)) % 4_294_967_291;
  }
  return `${records.length}:${pending.length}:hash32:${hash
    .toString(16)
    .padStart(8, '0')}`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

async function measureSurrealReopen(
  config: Parameters<typeof connect>[0],
  expectedCommits: number,
): Promise<number> {
  const startedAt = performance.now();
  const database = await connect(config);
  const sync = await database.openExperimentalSync(syncOptions);
  const status = await sync.status();
  assertEqual(
    status.pendingCount,
    expectedCommits,
    'SurrealDB reopened pending',
  );
  await sync.close();
  await database.close();
  return performance.now() - startedAt;
}

async function measureSQLiteReopen(
  config: Parameters<typeof open>[0],
  expectedCommits: number,
): Promise<number> {
  const startedAt = performance.now();
  const database = open(config);
  const count = scalarCount(
    await database.execute('SELECT COUNT(*) AS count FROM sync_outbox'),
  );
  assertEqual(count, expectedCommits, 'SQLite reopened outbox');
  database.close();
  return performance.now() - startedAt;
}

async function measureSamples(
  count: number,
  run: () => Promise<void>,
): Promise<number[]> {
  const samples: number[] = [];
  for (let index = 0; index < count; index += 1) {
    samples.push(await measure(run));
  }
  return samples;
}

async function measure(run: () => Promise<unknown>): Promise<number> {
  const startedAt = performance.now();
  await run();
  return performance.now() - startedAt;
}

function metric(
  name: SyncEngineBenchmarkMetric['name'],
  layer: SyncEngineBenchmarkMetric['layer'],
  implementation: SyncEngineBenchmarkMetric['implementation'],
  operationsPerSample: number,
  samplesMs: number[],
): SyncEngineBenchmarkMetric {
  return {
    name,
    category: 'sync-local',
    layer,
    implementation,
    operationsPerSample,
    samplesMs,
    summary: summarize(samplesMs, operationsPerSample),
  };
}

function comparison(
  workload: SyncEngineBenchmarkReport['comparisons'][number]['workload'],
  surrealSamples: number[],
  sqliteSamples: number[],
  caveat: string,
): SyncEngineBenchmarkReport['comparisons'][number] {
  const surrealDbMedianMs = summarize(surrealSamples, 1).medianMs;
  const opSQLiteMedianMs = summarize(sqliteSamples, 1).medianMs;
  return {
    workload,
    basis: 'median duration; lower is faster',
    caveat,
    surrealDbMedianMs,
    opSQLiteMedianMs,
    ratio:
      Math.max(surrealDbMedianMs, opSQLiteMedianMs) /
      Math.min(surrealDbMedianMs, opSQLiteMedianMs),
  };
}

async function resetSurreal(database: SurrealClient) {
  await database.query(
    'REMOVE TABLE IF EXISTS _sync_client_state; REMOVE TABLE IF EXISTS sync_bench_record;',
  );
}

function resetSQLite(database: DB) {
  database.executeSync('DROP TABLE IF EXISTS sync_record');
  database.executeSync('DROP TABLE IF EXISTS sync_outbox');
  database.executeSync(
    'CREATE TABLE sync_record (id TEXT PRIMARY KEY, payload TEXT NOT NULL)',
  );
  database.executeSync(
    'CREATE TABLE sync_outbox (commit_id TEXT PRIMARY KEY, payload TEXT NOT NULL)',
  );
}

function configureSQLite(database: DB) {
  database.executeSync('PRAGMA journal_mode = WAL');
  database.executeSync('PRAGMA synchronous = FULL');
}

function scalarCount(result: Awaited<ReturnType<DB['execute']>>): number {
  const count = result.rows[0]?.count;
  if (typeof count !== 'number') {
    throw new Error('SQLite count query returned invalid data');
  }
  return count;
}

function appPrivateStorageRoot(): string {
  const module = NativeModules.OPSQLite as
    | (StorageConstants & { getConstants?: () => StorageConstants })
    | undefined;
  const constants = module?.getConstants?.() ?? module;
  const root =
    Platform.OS === 'android'
      ? constants?.ANDROID_FILES_PATH
      : constants?.IOS_LIBRARY_PATH;
  if (typeof root !== 'string' || root.length === 0) {
    throw new Error('app-private storage root is unavailable');
  }
  return root.replace(/\/+$/, '');
}

function assertEqual(actual: number, expected: number, label: string) {
  if (actual !== expected) {
    throw new Error(`${label}: expected ${expected}, received ${actual}`);
  }
}

function validateCount(name: string, value: number, allowZero: boolean) {
  if (!Number.isSafeInteger(value) || (allowZero ? value < 0 : value < 1)) {
    throw new Error(
      `${name} must be ${allowZero ? 'non-negative' : 'positive'}`,
    );
  }
}
