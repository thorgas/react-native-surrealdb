import { connect, type SurrealClient } from 'react-native-surrealdb';

export const STARTUP_LOAD_RECORD_COUNT = 10_000;
export const STARTUP_LOAD_BATCH_SIZE = 250;

export type StartupLoadStage = 'open' | 'seed' | 'load' | 'complete';

export type StartupLoadProgress = {
  stage: StartupLoadStage;
  completed: number;
  total: number;
};

export type StartupLoadReport = {
  schemaVersion: 1;
  measuredAt: string;
  configuration: {
    records: typeof STARTUP_LOAD_RECORD_COUNT;
    batchSize: typeof STARTUP_LOAD_BATCH_SIZE;
    engine: 'memory';
    fullyMaterialized: true;
  };
  rowsLoaded: number;
  checksum: string;
  timingsMs: {
    open: number;
    seed: number;
    queryAndDecode: number;
    materialize: number;
    ready: number;
  };
};

export type StartupRenderEntry = {
  sequence: string;
  label: string;
  bucket: string;
  active: boolean;
  score: string;
};

export type StartupLoadResult = {
  report: StartupLoadReport;
  entries: StartupRenderEntry[];
};

type StartupLoadOptions = {
  signal?: AbortSignal;
  onProgress?: (progress: StartupLoadProgress) => void;
};

type StartupLoadRow = {
  sequence: bigint;
  label: string;
  bucket: bigint;
  active: boolean;
  score: bigint;
};

export async function runStartupLoadBenchmark(
  options: StartupLoadOptions = {},
): Promise<StartupLoadResult> {
  const { signal, onProgress } = options;
  const startedAt = performance.now();
  assertActive(signal);
  onProgress?.({ stage: 'open', completed: 0, total: 1 });

  const openStartedAt = performance.now();
  const database = await connect(
    {
      endpoint: 'memory',
      namespace: 'react_native_startup',
      database: 'react_native_startup',
    },
    signal ? { signal } : undefined,
  );
  const openMs = performance.now() - openStartedAt;

  try {
    assertActive(signal);
    onProgress?.({
      stage: 'seed',
      completed: 0,
      total: STARTUP_LOAD_RECORD_COUNT,
    });

    const seedStartedAt = performance.now();
    await query(
      database,
      'REMOVE TABLE IF EXISTS startup_entry; DEFINE TABLE startup_entry SCHEMALESS',
      signal,
    );

    for (
      let offset = 0;
      offset < STARTUP_LOAD_RECORD_COUNT;
      offset += STARTUP_LOAD_BATCH_SIZE
    ) {
      assertActive(signal);
      const end = Math.min(
        offset + STARTUP_LOAD_BATCH_SIZE,
        STARTUP_LOAD_RECORD_COUNT,
      );
      const statements = Array.from(
        { length: end - offset },
        (_, batchIndex) => {
          const index = offset + batchIndex;
          return `CREATE startup_entry:entry_${index} CONTENT {
            sequence: ${index},
            label: 'startup entry ${index}',
            bucket: ${index % 100},
            active: ${index % 2 === 0},
            score: ${(index * 17) % 1_001}
          } RETURN NONE`;
        },
      );
      await query(database, transaction(statements), signal);
      onProgress?.({
        stage: 'seed',
        completed: end,
        total: STARTUP_LOAD_RECORD_COUNT,
      });
    }
    const seedMs = performance.now() - seedStartedAt;

    assertActive(signal);
    onProgress?.({
      stage: 'load',
      completed: 0,
      total: STARTUP_LOAD_RECORD_COUNT,
    });
    const queryStartedAt = performance.now();
    const result = await query(
      database,
      'SELECT sequence, label, bucket, active, score FROM startup_entry ORDER BY sequence',
      signal,
    );
    const queryAndDecodeMs = performance.now() - queryStartedAt;
    const rows = result.at(-1)?.value;
    if (!Array.isArray(rows)) {
      throw new Error('Startup load returned a non-array result');
    }
    if (rows.length !== STARTUP_LOAD_RECORD_COUNT) {
      throw new Error(
        `Startup load returned ${rows.length} rows; expected ${STARTUP_LOAD_RECORD_COUNT}`,
      );
    }

    const materializeStartedAt = performance.now();
    const { checksum, entries } = materializeRows(rows);
    const materializeMs = performance.now() - materializeStartedAt;
    const readyMs = performance.now() - startedAt;
    onProgress?.({
      stage: 'complete',
      completed: rows.length,
      total: STARTUP_LOAD_RECORD_COUNT,
    });

    return {
      report: {
        schemaVersion: 1,
        measuredAt: new Date().toISOString(),
        configuration: {
          records: STARTUP_LOAD_RECORD_COUNT,
          batchSize: STARTUP_LOAD_BATCH_SIZE,
          engine: 'memory',
          fullyMaterialized: true,
        },
        rowsLoaded: rows.length,
        checksum,
        timingsMs: {
          open: openMs,
          seed: seedMs,
          queryAndDecode: queryAndDecodeMs,
          materialize: materializeMs,
          ready: readyMs,
        },
      },
      entries,
    };
  } finally {
    await database.close();
  }
}

function materializeRows(rows: unknown[]): {
  checksum: string;
  entries: StartupRenderEntry[];
} {
  let checksum = 0n;
  const entries = rows.map((value, index) => {
    if (!isStartupLoadRow(value)) {
      throw new Error(`Startup load returned an invalid row at index ${index}`);
    }
    checksum +=
      value.sequence +
      BigInt(value.label.length) +
      value.bucket +
      value.score +
      (value.active ? 1n : 0n);
    return {
      sequence: value.sequence.toString(),
      label: value.label,
      bucket: value.bucket.toString(),
      active: value.active,
      score: value.score.toString(),
    };
  });
  return { checksum: checksum.toString(), entries };
}

function isStartupLoadRow(value: unknown): value is StartupLoadRow {
  if (!value || typeof value !== 'object') return false;
  const row = value as Partial<StartupLoadRow>;
  return (
    typeof row.sequence === 'bigint' &&
    typeof row.label === 'string' &&
    typeof row.bucket === 'bigint' &&
    typeof row.active === 'boolean' &&
    typeof row.score === 'bigint'
  );
}

function transaction(statements: readonly string[]): string {
  return `BEGIN TRANSACTION; ${statements.join('; ')}; COMMIT TRANSACTION`;
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

function assertActive(signal?: AbortSignal) {
  if (signal?.aborted) throw new Error('Startup load benchmark cancelled');
}
