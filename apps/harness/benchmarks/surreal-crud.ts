import { connect, type SurrealClient } from 'react-native-surrealdb';

import { summarize, type DistributionSummary } from './statistics';
import {
  buildScanQuery,
  indexBuildQuery,
  indexDropQuery,
  mutableIndexField,
  resolveScans,
  scanMetricStem,
  type ResolvedScan,
} from './upstream-workloads';

export const CRUD_BENCH_SOURCE = {
  name: 'surrealdb/crud-bench mobile adaptation',
  url: 'https://github.com/surrealdb/crud-bench',
  revision: '18eb1fc8d8edcfd3d6ba8328149789ffa7866659',
  config: 'config/bench.toml',
  researchedAt: '2026-07-13',
  upstreamCoverage:
    'Single-record CRUD; count, ID, full, limit, offset, filter, order, heap, indexed, mixed read/write, and BM25 scans; index build/drop; batch CRUD at 100 and 1,000 records.',
  adaptation:
    'Runs the default crud-bench workload matrix sequentially through Hermes, JSI, UniFFI, and embedded SurrealDB. Mobile profiles reduce upstream iteration counts, use deterministic data, and keep concurrency at one client because the public package currently exposes one database handle.',
  exclusions:
    'The separate upstream vector.toml suite, server resource profiling, Docker lifecycle, multi-client concurrency, and cross-database comparisons are not part of this embedded mobile profile.',
} as const;

export type MobileBenchmarkProfile = 'smoke' | 'canonical' | 'upstream';

export type MobileBenchmarkOptions = {
  profile: MobileBenchmarkProfile;
  records: number;
  samples: number;
  warmups: number;
  batchIterations: number;
  batchSizes: readonly number[];
  writeRatios: readonly number[];
  platform: 'android' | 'ios';
  device: string;
  os: string;
  reactNative: string;
  surrealDb: string;
};

export type MobileBenchmarkProgress = {
  completed: number;
  total: number;
  metric: string;
  stage: 'setup' | 'measure' | 'complete';
};

export type MobileBenchmarkRunOptions = MobileBenchmarkOptions & {
  signal?: AbortSignal;
  onProgress?: (progress: MobileBenchmarkProgress) => void;
};

export type MobileBenchmarkCategory =
  | 'bridge'
  | 'crud'
  | 'scan'
  | 'index'
  | 'fulltext'
  | 'batch'
  | 'graph';

export type MobileBenchmarkMetric = {
  name: string;
  category: MobileBenchmarkCategory;
  upstreamCase: string;
  variant: string;
  operationsPerSample: number;
  samplesMs: number[];
  summary: DistributionSummary;
};

export type MobileBenchmarkReport = {
  schemaVersion: 2;
  measuredAt: string;
  source: typeof CRUD_BENCH_SOURCE;
  configuration: MobileBenchmarkOptions & {
    engine: 'memory';
    buildType: 'Debug harness';
    fullyMaterialized: true;
    clients: 1;
  };
  metrics: MobileBenchmarkMetric[];
};

type Iteration = {
  index: number;
  warmup: boolean;
};

type Workload = {
  name: string;
  category: MobileBenchmarkCategory;
  upstreamCase: string;
  variant: string;
  operationsPerSample?: number;
  samples?: number;
  warmups?: number;
  run: (iteration: Iteration) => Promise<unknown>;
};

type ProfileOptions = Omit<
  MobileBenchmarkOptions,
  'platform' | 'device' | 'os' | 'reactNative' | 'surrealDb'
>;

export const SMOKE_BENCHMARK_OPTIONS: ProfileOptions = {
  profile: 'smoke',
  records: 200,
  samples: 7,
  warmups: 3,
  batchIterations: 2,
  batchSizes: [100, 1_000],
  writeRatios: [0.15, 0.5],
};

export const CANONICAL_BENCHMARK_OPTIONS: ProfileOptions = {
  profile: 'canonical',
  records: 2_000,
  samples: 20,
  warmups: 5,
  batchIterations: 3,
  batchSizes: [100, 1_000],
  writeRatios: [0.15, 0.5],
};

/** Full default workload coverage with the upstream 5,000-row offset intact. */
export const UPSTREAM_BENCHMARK_OPTIONS: ProfileOptions = {
  profile: 'upstream',
  records: 10_000,
  samples: 50,
  warmups: 10,
  batchIterations: 10,
  batchSizes: [100, 1_000],
  writeRatios: [0.15, 0.5],
};

export async function runSurrealCrudBenchmark(
  options: MobileBenchmarkRunOptions,
): Promise<MobileBenchmarkReport> {
  validateOptions(options);
  const { signal, onProgress, ...configuration } = options;
  assertActive(signal);
  onProgress?.({
    completed: 0,
    total: 0,
    metric: 'database setup',
    stage: 'setup',
  });

  const database = await connect(
    {
      endpoint: 'memory',
      namespace: 'react_native_benchmark',
      database: 'react_native_benchmark',
    },
    callOptions(signal),
  );

  try {
    await setupDatabase(database, options, signal);
    const workloads = buildWorkloads(database, options, signal);
    const metrics: MobileBenchmarkMetric[] = [];
    onProgress?.({
      completed: 0,
      total: workloads.length,
      metric: workloads[0]?.name ?? 'complete',
      stage: 'measure',
    });

    for (const workload of workloads) {
      assertActive(signal);
      metrics.push(
        await measure(workload, options.warmups, options.samples, signal),
      );
      onProgress?.({
        completed: metrics.length,
        total: workloads.length,
        metric: workload.name,
        stage: metrics.length === workloads.length ? 'complete' : 'measure',
      });
    }

    return {
      schemaVersion: 2,
      measuredAt: new Date().toISOString(),
      source: CRUD_BENCH_SOURCE,
      configuration: {
        ...configuration,
        engine: 'memory',
        buildType: 'Debug harness',
        fullyMaterialized: true,
        clients: 1,
      },
      metrics,
    };
  } finally {
    await database.close();
  }
}

function buildWorkloads(
  database: SurrealClient,
  options: MobileBenchmarkOptions,
  signal?: AbortSignal,
): Workload[] {
  const workloads: Workload[] = [];
  const runQuery = (surql: string) => query(database, surql, signal);

  workloads.push({
    name: 'bridge.return-one',
    category: 'bridge',
    upstreamCase: 'transport extension',
    variant: 'RETURN 1',
    run: () => runQuery('RETURN 1'),
  });

  workloads.push(
    {
      name: 'crud.create-one',
      category: 'crud',
      upstreamCase: 'Create',
      variant: 'single record / individual transaction',
      run: iteration =>
        runQuery(
          `CREATE ${crudId(iteration)} CONTENT ${recordContent(
            iteration.index,
          )} RETURN NONE`,
        ),
    },
    {
      name: 'crud.read-one',
      category: 'crud',
      upstreamCase: 'Read',
      variant: 'single record / individual transaction',
      run: iteration => runQuery(`SELECT * FROM ONLY ${crudId(iteration)}`),
    },
    {
      name: 'crud.update-one',
      category: 'crud',
      upstreamCase: 'Update',
      variant: 'single record / individual transaction',
      run: iteration =>
        runQuery(
          `UPDATE ${crudId(
            iteration,
          )} SET active = !active, score = score + 1 RETURN NONE`,
        ),
    },
  );

  for (const scan of resolveScans(options.records)) {
    appendScanWorkloads(workloads, database, scan, options, signal);
  }

  workloads.push(
    {
      name: 'extension.graph.out-depth-one',
      category: 'graph',
      upstreamCase: 'React Native SurrealDB extension',
      variant: 'one-hop materialized graph traversal',
      run: () =>
        runQuery(
          'SELECT ->benchmark_edge->benchmark_record.* FROM ONLY benchmark_record:record_0',
        ),
    },
    {
      name: 'extension.graph.out-depth-two',
      category: 'graph',
      upstreamCase: 'React Native SurrealDB extension',
      variant: 'two-hop materialized graph traversal',
      run: () =>
        runQuery(
          'SELECT ->benchmark_edge->benchmark_record->benchmark_edge->benchmark_record.* FROM ONLY benchmark_record:record_0',
        ),
    },
    {
      name: 'crud.delete-one',
      category: 'crud',
      upstreamCase: 'Delete',
      variant: 'single record / individual transaction',
      run: iteration => runQuery(`DELETE ${crudId(iteration)} RETURN NONE`),
    },
  );

  for (const batchSize of options.batchSizes) {
    appendBatchWorkloads(workloads, database, batchSize, options, signal);
  }

  return workloads;
}

function appendScanWorkloads(
  workloads: Workload[],
  database: SurrealClient,
  scan: ResolvedScan,
  options: MobileBenchmarkOptions,
  signal?: AbortSignal,
) {
  const stem = scanMetricStem(scan);
  const scanCategory =
    scan.index?.type === 'fulltext' ? ('fulltext' as const) : ('scan' as const);
  const runScan = () => executeScan(database, scan, signal);

  if (scan.index?.type !== 'fulltext') {
    workloads.push({
      name: `${stem}.no-index`,
      category: scanCategory,
      upstreamCase: scan.id,
      variant: `${scan.projection} / heap`,
      run: runScan,
    });
    if (scan.mixedWrites && scan.index) {
      for (const ratio of options.writeRatios) {
        workloads.push(mixedScanWorkload(database, scan, ratio, false, signal));
      }
    }
  }

  if (!scan.index) return;

  workloads.push({
    name: `${stem}.index-build`,
    category: 'index',
    upstreamCase: scan.id,
    variant:
      scan.index.type === 'fulltext' ? 'BM25 index build' : 'index build',
    samples: 1,
    warmups: 0,
    run: () => query(database, indexBuildQuery(scan), signal),
  });
  workloads.push({
    name: `${stem}.indexed`,
    category: scanCategory,
    upstreamCase: scan.id,
    variant: `${scan.projection} / indexed`,
    run: runScan,
  });
  if (scan.mixedWrites) {
    for (const ratio of options.writeRatios) {
      workloads.push(mixedScanWorkload(database, scan, ratio, true, signal));
    }
  }
  workloads.push({
    name: `${stem}.index-drop`,
    category: 'index',
    upstreamCase: scan.id,
    variant:
      scan.index.type === 'fulltext' ? 'BM25 index removal' : 'index removal',
    samples: 1,
    warmups: 0,
    run: () => query(database, indexDropQuery(scan), signal),
  });
}

function mixedScanWorkload(
  database: SurrealClient,
  scan: ResolvedScan,
  ratio: number,
  indexed: boolean,
  signal?: AbortSignal,
): Workload {
  const percent = Math.round(ratio * 100);
  return {
    name: `${scanMetricStem(scan)}.${
      indexed ? 'indexed' : 'no-index'
    }.write-${percent}`,
    category: 'scan',
    upstreamCase: scan.id,
    variant: `${scan.projection} / ${
      indexed ? 'indexed' : 'heap'
    } / ${percent}% updates`,
    run: async iteration => {
      const result = await executeScan(database, scan, signal);
      if (sampleIncludesWrites(iteration.index, ratio)) {
        await swapIndexedValues(database, scan, iteration.index, signal);
      }
      return result;
    },
  };
}

function appendBatchWorkloads(
  workloads: Workload[],
  database: SurrealClient,
  batchSize: number,
  options: MobileBenchmarkOptions,
  signal?: AbortSignal,
) {
  const table = `benchmark_batch_${batchSize}`;
  const workload = (
    operation: 'create' | 'read' | 'update' | 'delete',
    run: (ids: string[], iteration: Iteration) => Promise<unknown>,
  ): Workload => ({
    name: `batch.${operation}-${batchSize}`,
    category: 'batch',
    upstreamCase: `batch_${operation}_${batchSize}`,
    variant: `${operation.toUpperCase()} ${batchSize} records`,
    operationsPerSample: batchSize,
    samples: options.batchIterations,
    warmups: 1,
    run: iteration => run(batchIds(table, batchSize, iteration), iteration),
  });

  workloads.push(
    workload('create', (ids, iteration) =>
      query(
        database,
        transaction(
          ids.map(
            (id, index) =>
              `CREATE ${id} CONTENT ${recordContent(
                iteration.index * batchSize + index,
              )} RETURN NONE`,
          ),
        ),
        signal,
      ),
    ),
    workload('read', async ids => {
      const result = await query(
        database,
        `SELECT * FROM [${ids.join(', ')}]`,
        signal,
      );
      assertArrayLength(result.at(-1)?.value, ids.length, 'batch read');
      return result;
    }),
    workload('update', ids =>
      query(
        database,
        `FOR $id IN [${ids.join(
          ', ',
        )}] { UPDATE $id SET active = !active RETURN NONE }`,
        signal,
      ),
    ),
    workload('delete', ids =>
      query(database, `DELETE [${ids.join(', ')}] RETURN NONE`, signal),
    ),
  );
}

async function measure(
  workload: Workload,
  defaultWarmups: number,
  defaultSamples: number,
  signal?: AbortSignal,
): Promise<MobileBenchmarkMetric> {
  const warmups = workload.warmups ?? defaultWarmups;
  const sampleCount = workload.samples ?? defaultSamples;
  for (let index = 0; index < warmups; index += 1) {
    assertActive(signal);
    await workload.run({ index, warmup: true });
  }

  const samplesMs: number[] = [];
  for (let index = 0; index < sampleCount; index += 1) {
    assertActive(signal);
    const startedAt = performance.now();
    await workload.run({ index, warmup: false });
    samplesMs.push(performance.now() - startedAt);
  }

  const operationsPerSample = workload.operationsPerSample ?? 1;
  return {
    name: workload.name,
    category: workload.category,
    upstreamCase: workload.upstreamCase,
    variant: workload.variant,
    operationsPerSample,
    samplesMs,
    summary: summarize(samplesMs, operationsPerSample),
  };
}

async function setupDatabase(
  database: SurrealClient,
  options: MobileBenchmarkOptions,
  signal?: AbortSignal,
) {
  const tables = options.batchSizes
    .map(size => `DEFINE TABLE benchmark_batch_${size} SCHEMALESS`)
    .join(';');
  await query(
    database,
    `REMOVE TABLE IF EXISTS benchmark_record; REMOVE TABLE IF EXISTS benchmark_crud; DEFINE TABLE benchmark_record SCHEMALESS; DEFINE TABLE benchmark_crud SCHEMALESS; ${tables}`,
    signal,
  );

  for (let offset = 0; offset < options.records; offset += 50) {
    assertActive(signal);
    const end = Math.min(offset + 50, options.records);
    const statements: string[] = [];
    for (let index = offset; index < end; index += 1) {
      statements.push(
        `CREATE benchmark_record:record_${index} CONTENT ${recordContent(
          index,
        )} RETURN NONE`,
      );
    }
    await query(database, transaction(statements), signal);
  }

  const edgeCount = Math.min(options.records - 1, 100);
  if (edgeCount > 0) {
    const edges = Array.from(
      { length: edgeCount },
      (_, index) =>
        `RELATE benchmark_record:record_${index}->benchmark_edge->benchmark_record:record_${
          index + 1
        } RETURN NONE`,
    );
    await query(database, transaction(edges), signal);
  }
}

async function executeScan(
  database: SurrealClient,
  scan: ResolvedScan,
  signal?: AbortSignal,
) {
  const result = await query(database, buildScanQuery(scan), signal);
  if (scan.expect !== undefined && scan.projection !== 'count') {
    assertArrayLength(result.at(-1)?.value, scan.expect, scan.id);
  }
  if (scan.projection === 'count') {
    assertArrayLength(result.at(-1)?.value, 1, `${scan.id} count`);
  }
  return result;
}

async function swapIndexedValues(
  database: SurrealClient,
  scan: ResolvedScan,
  sample: number,
  signal?: AbortSignal,
) {
  const field = mutableIndexField(scan);
  const a = sample % 97;
  const b = (sample * 31 + 53) % 97;
  if (a === b) return;
  await query(
    database,
    transaction([
      `LET $a = (SELECT * FROM ONLY benchmark_record:record_${a})`,
      `LET $b = (SELECT * FROM ONLY benchmark_record:record_${b})`,
      `UPDATE benchmark_record:record_${a} SET ${field} = $b.${field} RETURN NONE`,
      `UPDATE benchmark_record:record_${b} SET ${field} = $a.${field} RETURN NONE`,
    ]),
    signal,
  );
}

function sampleIncludesWrites(sample: number, ratio: number): boolean {
  if (ratio <= 0) return false;
  if (ratio >= 1) return true;
  const threshold = Math.round(ratio * 1_000);
  const wrapped = (sample * 2_654_435_761) % 2 ** 32;
  return wrapped % 1_000 < threshold;
}

function crudId(iteration: Iteration): string {
  return `benchmark_crud:${iteration.warmup ? 'warmup' : 'sample'}_${
    iteration.index
  }`;
}

function batchIds(
  table: string,
  batchSize: number,
  iteration: Iteration,
): string[] {
  const prefix = iteration.warmup ? 'warmup' : 'sample';
  return Array.from(
    { length: batchSize },
    (_, index) => `${table}:${prefix}_${iteration.index}_${index}`,
  );
}

function transaction(statements: readonly string[]): string {
  return `BEGIN TRANSACTION; ${statements.join('; ')}; COMMIT TRANSACTION`;
}

function recordContent(index: number): string {
  const statuses = ['draft', 'published', 'archived'] as const;
  const cities = ['London', 'Paris', 'Berlin', 'Tokyo', 'New York'] as const;
  const httpStatuses = [200, 201, 400, 404, 500] as const;
  const continents = ['Africa', 'Asia', 'Europe', 'Oceania'] as const;
  const second = String(index % 60).padStart(2, '0');
  const requestId = String(index).padStart(12, '0');
  return `{
    text: 'deterministic benchmark text ${index}',
    name: 'record ${index}',
    age: ${(index % 100) + 1},
    city: '${cities[index % cities.length]}',
    score: ${(index * 17) % 101}.${index % 10},
    number: ${(index % 5_000) + 1},
    integer: ${index},
    active: ${index % 2 === 0},
    created_at: d'2024-01-01T00:00:${second}Z',
    http_status: ${httpStatuses[index % httpStatuses.length]},
    request_id: '00000000-0000-4000-8000-${requestId}',
    status: '${statuses[index % statuses.length]}',
    description: 'hello world benchmark description data query index document database performance ${index}',
    tier: ${index % 4},
    words: 'hello world foo bar test search data query index document database performance',
    tags: ['${index % 2 === 0 ? 'alpha' : 'beta'}', 'theta', '${
    index % 3 === 0 ? 'omicron' : 'rho'
  }'],
    geography: {
      code: 'g${String(index % 100).padStart(2, '0')}',
      text: 'region ${index % 10}',
      location: '${continents[index % continents.length]}',
      description: 'hello world geography data query index document database performance'
    }
  }`;
}

async function query(
  database: SurrealClient,
  surql: string,
  signal?: AbortSignal,
) {
  assertActive(signal);
  return database.query<unknown>(surql, undefined, callOptions(signal));
}

function callOptions(signal?: AbortSignal) {
  return signal ? { signal } : undefined;
}

function assertArrayLength(
  value: unknown,
  expected: number,
  operation: string,
) {
  if (!Array.isArray(value)) {
    throw new Error(`${operation} returned a non-array result`);
  }
  if (value.length !== expected) {
    throw new Error(
      `${operation} returned ${value.length} rows; expected ${expected}`,
    );
  }
}

function assertActive(signal?: AbortSignal) {
  if (signal?.aborted) {
    throw new Error('Benchmark cancelled');
  }
}

function validateOptions(options: MobileBenchmarkOptions) {
  for (const [name, value] of Object.entries({
    records: options.records,
    samples: options.samples,
    warmups: options.warmups,
    batchIterations: options.batchIterations,
  })) {
    if (!Number.isSafeInteger(value) || value <= 0) {
      throw new Error(`${name} must be a positive integer`);
    }
  }
  if (options.records < 100) throw new Error('records must be at least 100');
  if (
    options.batchSizes.length === 0 ||
    options.batchSizes.some(size => !Number.isSafeInteger(size) || size <= 0)
  ) {
    throw new Error('batchSizes must contain positive integers');
  }
  if (
    options.writeRatios.length === 0 ||
    options.writeRatios.some(ratio => ratio <= 0 || ratio > 1)
  ) {
    throw new Error('writeRatios must be within (0, 1]');
  }
}
