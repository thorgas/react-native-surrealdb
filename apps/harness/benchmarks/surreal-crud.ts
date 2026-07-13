import { connect, type SurrealClient } from 'react-native-surrealdb';

import { summarize, type DistributionSummary } from './statistics';

export const CRUD_BENCH_SOURCE = {
  name: 'surrealdb/crud-bench mobile adaptation',
  url: 'https://github.com/surrealdb/crud-bench',
  revision: '18eb1fc8d8edcfd3d6ba8328149789ffa7866659',
  researchedAt: '2026-07-13',
  upstreamProfile:
    'Create, read, update, delete, count, limit, indexed-filter, order, and graph workloads',
  adaptation:
    'Runs deterministic, fully materialized queries through Hermes, JSI, UniFFI, and the embedded SurrealDB memory engine. Results are for mobile regression tracking and are not directly comparable with SurrealDB server benchmark numbers.',
} as const;

export type MobileBenchmarkOptions = {
  profile: 'smoke' | 'canonical';
  records: number;
  samples: number;
  warmups: number;
  batchSize: number;
  platform: 'android' | 'ios';
  device: string;
  os: string;
  reactNative: string;
  surrealDb: string;
};

export type MobileBenchmarkMetric = {
  name: string;
  category: 'bridge' | 'crud' | 'scan' | 'index' | 'graph';
  operationsPerSample: number;
  samplesMs: number[];
  summary: DistributionSummary;
};

export type MobileBenchmarkReport = {
  schemaVersion: 1;
  measuredAt: string;
  source: typeof CRUD_BENCH_SOURCE;
  configuration: MobileBenchmarkOptions & {
    engine: 'memory';
    buildType: 'Debug harness';
    fullyMaterialized: true;
  };
  metrics: MobileBenchmarkMetric[];
};

type Workload = {
  name: string;
  category: MobileBenchmarkMetric['category'];
  operationsPerSample?: number;
  run: () => Promise<unknown>;
};

export const SMOKE_BENCHMARK_OPTIONS: Omit<
  MobileBenchmarkOptions,
  'platform' | 'device' | 'os' | 'reactNative' | 'surrealDb'
> = {
  profile: 'smoke',
  records: 200,
  samples: 7,
  warmups: 3,
  batchSize: 25,
};

// SurrealDB's published 3.0 tables label scan workloads with 2,000 records,
// while crud-bench's checked-in batch profile includes batches of 100. This
// mobile profile keeps those shapes but reduces upstream iteration counts to
// avoid sustained thermal throttling on phones.
export const CANONICAL_BENCHMARK_OPTIONS: Omit<
  MobileBenchmarkOptions,
  'platform' | 'device' | 'os' | 'reactNative' | 'surrealDb'
> = {
  profile: 'canonical',
  records: 2_000,
  samples: 20,
  warmups: 5,
  batchSize: 100,
};

export async function runSurrealCrudBenchmark(
  options: MobileBenchmarkOptions,
): Promise<MobileBenchmarkReport> {
  validateOptions(options);
  const database = await connect({
    endpoint: 'memory',
    namespace: 'react_native_benchmark',
    database: 'react_native_benchmark',
  });

  try {
    await seedDatabase(
      database,
      options.records,
      options.samples + options.warmups,
    );
    const metrics: MobileBenchmarkMetric[] = [];
    let createId = 0;
    let deleteId = 0;

    const workloads: Workload[] = [
      {
        name: 'bridge.return-one',
        category: 'bridge',
        run: () => database.query('RETURN 1'),
      },
      {
        name: 'crud.create-one',
        category: 'crud',
        run: () =>
          database.query(
            `CREATE benchmark_create:record_${createId++} SET number = 21, active = true`,
          ),
      },
      {
        name: 'crud.read-one',
        category: 'crud',
        run: () =>
          database.query('SELECT * FROM ONLY benchmark_record:record_21'),
      },
      {
        name: 'crud.update-one',
        category: 'crud',
        run: () =>
          database.query(
            'UPDATE benchmark_record:record_21 SET active = !active RETURN AFTER',
          ),
      },
      {
        name: 'crud.delete-one',
        category: 'crud',
        run: () =>
          database.query(`DELETE benchmark_delete:record_${deleteId++}`),
      },
      {
        name: 'scan.count-all',
        category: 'scan',
        run: () =>
          database.query('RETURN count(SELECT VALUE id FROM benchmark_record)'),
      },
      {
        name: 'scan.limit-100-materialized',
        category: 'scan',
        run: () => database.query('SELECT * FROM benchmark_record LIMIT 100'),
      },
      {
        name: 'index.number-equality',
        category: 'index',
        run: () =>
          database.query('SELECT * FROM benchmark_record WHERE number = 21'),
      },
      {
        name: 'scan.order-by-score-limit-100',
        category: 'scan',
        run: () =>
          database.query(
            'SELECT * FROM benchmark_record ORDER BY score DESC LIMIT 100',
          ),
      },
      {
        name: 'graph.out-depth-one',
        category: 'graph',
        run: () =>
          database.query(
            'SELECT ->benchmark_edge->benchmark_record.* FROM ONLY benchmark_record:record_0',
          ),
      },
      {
        name: 'graph.out-depth-two',
        category: 'graph',
        run: () =>
          database.query(
            'SELECT ->benchmark_edge->benchmark_record->benchmark_edge->benchmark_record.* FROM ONLY benchmark_record:record_0',
          ),
      },
      {
        name: `crud.batch-create-${options.batchSize}`,
        category: 'crud',
        operationsPerSample: options.batchSize,
        run: () =>
          database.query(batchCreateQuery(options.batchSize, createId++)),
      },
    ];

    for (const workload of workloads) {
      metrics.push(await measure(workload, options.warmups, options.samples));
    }

    return {
      schemaVersion: 1,
      measuredAt: new Date().toISOString(),
      source: CRUD_BENCH_SOURCE,
      configuration: {
        ...options,
        engine: 'memory',
        buildType: 'Debug harness',
        fullyMaterialized: true,
      },
      metrics,
    };
  } finally {
    await database.close();
  }
}

async function measure(
  workload: Workload,
  warmups: number,
  sampleCount: number,
): Promise<MobileBenchmarkMetric> {
  for (let index = 0; index < warmups; index += 1) await workload.run();

  const samplesMs: number[] = [];
  for (let index = 0; index < sampleCount; index += 1) {
    const startedAt = performance.now();
    await workload.run();
    samplesMs.push(performance.now() - startedAt);
  }

  const operationsPerSample = workload.operationsPerSample ?? 1;
  return {
    name: workload.name,
    category: workload.category,
    operationsPerSample,
    samplesMs,
    summary: summarize(samplesMs, operationsPerSample),
  };
}

async function seedDatabase(
  database: SurrealClient,
  records: number,
  disposableRecords: number,
) {
  await database.query(
    'DEFINE TABLE benchmark_record SCHEMALESS; DEFINE INDEX benchmark_number ON benchmark_record FIELDS number;',
  );

  for (let offset = 0; offset < records; offset += 100) {
    const end = Math.min(offset + 100, records);
    const statements: string[] = ['BEGIN TRANSACTION'];
    for (let index = offset; index < end; index += 1) {
      statements.push(
        `CREATE benchmark_record:record_${index} CONTENT { number: ${index % 50}, score: ${(index * 17) % 101}, active: ${index % 2 === 0}, status: '${index % 3 === 0 ? 'published' : 'draft'}', text: 'deterministic record ${index}', tags: ['mobile', 'benchmark'] }`,
      );
    }
    statements.push('COMMIT TRANSACTION');
    await database.query(statements.join(';'));
  }

  const disposable = Array.from(
    { length: disposableRecords },
    (_, index) =>
      `CREATE benchmark_delete:record_${index} SET disposable = true`,
  );
  if (disposable.length > 0) await database.query(disposable.join(';'));

  const edgeStatements: string[] = [];
  const edgeCount = Math.min(records - 1, 100);
  for (let index = 0; index < edgeCount; index += 1) {
    edgeStatements.push(
      `RELATE benchmark_record:record_${index}->benchmark_edge->benchmark_record:record_${index + 1}`,
    );
  }
  if (edgeStatements.length > 0) await database.query(edgeStatements.join(';'));
}

function batchCreateQuery(batchSize: number, sample: number) {
  const statements = ['BEGIN TRANSACTION'];
  for (let index = 0; index < batchSize; index += 1) {
    statements.push(
      `CREATE benchmark_batch:sample_${sample}_${index} SET number = ${index}, active = true`,
    );
  }
  statements.push('COMMIT TRANSACTION');
  return statements.join(';');
}

function validateOptions(options: MobileBenchmarkOptions) {
  for (const [name, value] of Object.entries({
    records: options.records,
    samples: options.samples,
    warmups: options.warmups,
    batchSize: options.batchSize,
  })) {
    if (!Number.isSafeInteger(value) || value <= 0) {
      throw new Error(`${name} must be a positive integer`);
    }
  }
  if (options.records < 101) throw new Error('records must be at least 101');
}
