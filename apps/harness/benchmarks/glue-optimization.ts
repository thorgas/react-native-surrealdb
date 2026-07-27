import { open, type SQLBatchTuple } from '@op-engineering/op-sqlite';
import {
  connect,
  type QueryVariables,
  type QueryProfile,
  type QueryProfileOptions,
} from 'react-native-surrealdb';

import { summarize } from './statistics';

const RECORDS = 1_000;
const CODEC_ITERATIONS = 100;
const SHORT_COOLDOWN_MS = 500;

type CodecVariant = {
  name: 'baseline' | 'decoder-only' | 'serializer-only' | 'combined';
  decodeMode: NonNullable<QueryProfileOptions['decodeMode']>;
  nativeOutputEncoding: NonNullable<
    QueryProfileOptions['nativeOutputEncoding']
  >;
};

const CODEC_VARIANTS: readonly CodecVariant[] = [
  {
    name: 'baseline',
    decodeMode: 'copy',
    nativeOutputEncoding: 'tree',
  },
  {
    name: 'decoder-only',
    decodeMode: 'in-place',
    nativeOutputEncoding: 'tree',
  },
  {
    name: 'serializer-only',
    decodeMode: 'copy',
    nativeOutputEncoding: 'streaming',
  },
  {
    name: 'combined',
    decodeMode: 'in-place',
    nativeOutputEncoding: 'streaming',
  },
] as const;

export type GlueOptimizationReport = {
  configuration: {
    records: typeof RECORDS;
    codecIterations: typeof CODEC_ITERATIONS;
    engine: 'memory';
    buildType: 'Debug harness';
  };
  batch: {
    statementCount: typeof RECORDS;
    parameterized: true;
    oneAsyncBatchPayload: true;
    fullTransactionLifecycle: true;
    executionOrders: [['surrealdb', 'op-sqlite'], ['op-sqlite', 'surrealdb']];
    surrealDbSamplesMs: [number, number];
    opSQLiteSamplesMs: [number, number];
    surrealDbSummary: ReturnType<typeof summarize>;
    opSQLiteSummary: ReturnType<typeof summarize>;
    checksums: { surrealDb: string; opSQLite: string };
  };
  codec: {
    query: 'SELECT sequence, name, value FROM glue_bench';
    fullyMaterialized: true;
    executionOrders: [
      ['baseline', 'decoder-only', 'serializer-only', 'combined'],
      ['combined', 'serializer-only', 'decoder-only', 'baseline'],
    ];
    variants: Array<{
      name: CodecVariant['name'];
      decodeMode: CodecVariant['decodeMode'];
      nativeOutputEncoding: CodecVariant['nativeOutputEncoding'];
      samplesMs: [number, number];
      summary: ReturnType<typeof summarize>;
      medianNativeOutputEncodeMs: number;
      medianJsOutputDecodeMs: number;
      checksum: string;
    }>;
    isolatedSpeedups: {
      inPlaceDecoder: number;
      streamingSerializer: number;
      combined: number;
    };
  };
};

export async function runGlueOptimizationBenchmark(): Promise<GlueOptimizationReport> {
  const surreal = await connect({
    endpoint: 'memory',
    namespace: 'react_native_glue_bench',
    database: 'react_native_glue_bench',
  });
  const sqlite = open({
    name: 'op_sqlite_glue_bench.db',
    location: ':memory:',
  });

  try {
    await surreal.query(
      'REMOVE TABLE IF EXISTS glue_bench; DEFINE TABLE glue_bench SCHEMALESS',
    );
    sqlite.executeSync('DROP TABLE IF EXISTS glue_bench');
    sqlite.executeSync(
      'CREATE TABLE glue_bench (sequence INTEGER PRIMARY KEY, name TEXT, value REAL)',
    );

    const surrealBatchVariables = createSurrealBatchVariables();
    const sqliteBatchQueries = createSQLiteBatchQueries();
    const surrealDbSamplesMs: [number, number] = [0, 0];
    const opSQLiteSamplesMs: [number, number] = [0, 0];

    surrealDbSamplesMs[0] = await measureSurrealBatch(
      surreal,
      surrealBatchVariables,
    );
    await coolDown();
    opSQLiteSamplesMs[0] = await measureSQLiteBatch(sqlite, sqliteBatchQueries);
    await coolDown();
    opSQLiteSamplesMs[1] = await measureSQLiteBatch(sqlite, sqliteBatchQueries);
    await coolDown();
    surrealDbSamplesMs[1] = await measureSurrealBatch(
      surreal,
      surrealBatchVariables,
    );

    const surrealChecksum = await readSurrealChecksum(surreal);
    const sqliteChecksum = readSQLiteChecksum(sqlite);
    if (surrealChecksum !== sqliteChecksum) {
      throw new Error(
        `batch checksums differ: SurrealDB=${surrealChecksum}, op-sqlite=${sqliteChecksum}`,
      );
    }

    const codecSamples = new Map<
      CodecVariant['name'],
      Array<Awaited<ReturnType<typeof measureCodecVariant>>>
    >();
    for (const variant of CODEC_VARIANTS) {
      codecSamples.set(variant.name, []);
    }
    for (const order of [
      CODEC_VARIANTS,
      [...CODEC_VARIANTS].reverse(),
    ] as const) {
      for (const variant of order) {
        await coolDown();
        codecSamples
          .get(variant.name)!
          .push(await measureCodecVariant(surreal, variant));
      }
    }

    const variants = CODEC_VARIANTS.map(variant => {
      const samples = codecSamples.get(variant.name)!;
      const samplesMs = [samples[0]!.durationMs, samples[1]!.durationMs] as [
        number,
        number,
      ];
      if (samples[0]!.checksum !== samples[1]!.checksum) {
        throw new Error(`${variant.name} codec checksums differ`);
      }
      return {
        ...variant,
        samplesMs,
        summary: summarize(samplesMs, CODEC_ITERATIONS),
        medianNativeOutputEncodeMs: median([
          samples[0]!.nativeOutputEncodeMs,
          samples[1]!.nativeOutputEncodeMs,
        ]),
        medianJsOutputDecodeMs: median([
          samples[0]!.outputDecodeMs,
          samples[1]!.outputDecodeMs,
        ]),
        checksum: samples[0]!.checksum,
      };
    });
    const baseline = variants.find(variant => variant.name === 'baseline')!;
    const decoder = variants.find(variant => variant.name === 'decoder-only')!;
    const serializer = variants.find(
      variant => variant.name === 'serializer-only',
    )!;
    const combined = variants.find(variant => variant.name === 'combined')!;
    if (new Set(variants.map(variant => variant.checksum)).size !== 1) {
      throw new Error('codec variants produced different checksums');
    }

    return {
      configuration: {
        records: RECORDS,
        codecIterations: CODEC_ITERATIONS,
        engine: 'memory',
        buildType: 'Debug harness',
      },
      batch: {
        statementCount: RECORDS,
        parameterized: true,
        oneAsyncBatchPayload: true,
        fullTransactionLifecycle: true,
        executionOrders: [
          ['surrealdb', 'op-sqlite'],
          ['op-sqlite', 'surrealdb'],
        ],
        surrealDbSamplesMs,
        opSQLiteSamplesMs,
        surrealDbSummary: summarize(surrealDbSamplesMs, RECORDS),
        opSQLiteSummary: summarize(opSQLiteSamplesMs, RECORDS),
        checksums: {
          surrealDb: surrealChecksum,
          opSQLite: sqliteChecksum,
        },
      },
      codec: {
        query: 'SELECT sequence, name, value FROM glue_bench',
        fullyMaterialized: true,
        executionOrders: [
          ['baseline', 'decoder-only', 'serializer-only', 'combined'],
          ['combined', 'serializer-only', 'decoder-only', 'baseline'],
        ],
        variants,
        isolatedSpeedups: {
          inPlaceDecoder: baseline.summary.medianMs / decoder.summary.medianMs,
          streamingSerializer:
            baseline.summary.medianMs / serializer.summary.medianMs,
          combined: baseline.summary.medianMs / combined.summary.medianMs,
        },
      },
    };
  } finally {
    sqlite.close();
    await surreal.close();
  }
}

function createSurrealBatchVariables(): QueryVariables[] {
  return Array.from({ length: RECORDS }, (_, sequence) => ({
    sequence,
    name: `n${sequence}`,
    value: sequence * 1.5,
  }));
}

function createSQLiteBatchQueries(): SQLBatchTuple[] {
  const parameters = Array.from({ length: RECORDS }, (_, sequence) => [
    sequence,
    `n${sequence}`,
    sequence * 1.5,
  ]);
  return [['INSERT INTO glue_bench VALUES (?,?,?)', parameters]];
}

async function measureSurrealBatch(
  database: Awaited<ReturnType<typeof connect>>,
  variables: readonly QueryVariables[],
): Promise<number> {
  await database.query('DELETE glue_bench RETURN NONE');
  return measure(() =>
    database.transaction(transaction =>
      transaction.executeBatch(
        'CREATE glue_bench CONTENT { sequence: $sequence, name: $name, value: <float>$value } RETURN NONE',
        variables,
      ),
    ),
  );
}

async function measureSQLiteBatch(
  database: ReturnType<typeof open>,
  queries: SQLBatchTuple[],
): Promise<number> {
  database.executeSync('DELETE FROM glue_bench');
  return measure(() => database.executeBatch(queries));
}

async function measureCodecVariant(
  database: Awaited<ReturnType<typeof connect>>,
  variant: CodecVariant,
) {
  let checksum = 0n;
  const profiles: QueryProfile[] = [];
  const durationMs = await measure(async () => {
    for (let iteration = 0; iteration < CODEC_ITERATIONS; iteration += 1) {
      const profiled = await database.queryProfiled<unknown>(
        'SELECT sequence, name, value FROM glue_bench',
        undefined,
        variant,
      );
      profiles.push(profiled.profile);
      checksum += checksumRows(profiled.results.at(-1)?.value);
    }
  });
  return {
    durationMs,
    nativeOutputEncodeMs: sum(
      profiles,
      profile => profile.nativeOutputEncodeMs,
    ),
    outputDecodeMs: sum(profiles, profile => profile.outputDecodeMs),
    checksum: checksum.toString(),
  };
}

async function readSurrealChecksum(
  database: Awaited<ReturnType<typeof connect>>,
): Promise<string> {
  const results = await database.query<unknown>(
    'SELECT sequence, name, value FROM glue_bench',
  );
  return checksumRows(results.at(-1)?.value).toString();
}

function readSQLiteChecksum(database: ReturnType<typeof open>): string {
  const result = database.executeSync(
    'SELECT sequence, name, value FROM glue_bench',
  );
  return checksumRows(result.rows).toString();
}

function checksumRows(value: unknown): bigint {
  if (!Array.isArray(value) || value.length !== RECORDS) {
    throw new Error(
      `glue benchmark expected ${RECORDS} rows, received ${
        Array.isArray(value) ? value.length : 'a non-array value'
      }`,
    );
  }
  let checksum = 0n;
  for (const row of value) {
    if (!row || typeof row !== 'object') {
      throw new Error('glue benchmark received an invalid row');
    }
    const candidate = row as Record<string, unknown>;
    const sequence =
      typeof candidate.sequence === 'bigint'
        ? candidate.sequence
        : typeof candidate.sequence === 'number'
          ? BigInt(candidate.sequence)
          : undefined;
    if (
      sequence === undefined ||
      typeof candidate.name !== 'string' ||
      typeof candidate.value !== 'number'
    ) {
      throw new Error('glue benchmark received invalid row properties');
    }
    checksum +=
      sequence +
      BigInt(candidate.name.length) +
      BigInt(Math.round(candidate.value * 10));
  }
  return checksum;
}

async function measure(run: () => Promise<unknown>): Promise<number> {
  const startedAt = performance.now();
  await run();
  return performance.now() - startedAt;
}

async function coolDown() {
  await new Promise(resolve => setTimeout(resolve, SHORT_COOLDOWN_MS));
}

function sum(
  profiles: readonly QueryProfile[],
  select: (profile: QueryProfile) => number,
): number {
  return profiles.reduce((total, profile) => total + select(profile), 0);
}

function median(values: [number, number]): number {
  return (values[0] + values[1]) / 2;
}
