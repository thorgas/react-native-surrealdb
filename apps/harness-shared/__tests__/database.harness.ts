import { afterEach, describe, expect, test } from 'react-native-harness';
import {
  ExperimentalSyncHttpAdapter,
  ExperimentalSyncScheduler,
  SurrealRecordId,
  connect,
  experimentalJsonSyncHttpCodec,
  type SurrealClient,
} from 'react-native-surrealdb';

describe('react-native-surrealdb native core', () => {
  let database: SurrealClient | undefined;

  afterEach(async () => {
    if (database && !database.isClosed) await database.close();
    database = undefined;
  });

  test('executes SurrealQL through Hermes and the embedded memory engine', async () => {
    database = await connect({
      endpoint: 'memory',
      namespace: 'harness',
      database: 'harness',
    });

    const results = await database.query(
      'RETURN $large; RETURN { id: $id, native: true };',
      {
        large: 9_007_199_254_740_993n,
        id: new SurrealRecordId('person:ada'),
      },
    );

    expect(results).toHaveLength(2);
    expect(results[0]?.value).toBe(9_007_199_254_740_993n);
    expect(results[1]?.value).toEqual({
      id: new SurrealRecordId('person:ada'),
      native: true,
    });

    await database.close();
    expect(database.isClosed).toBe(true);
  });

  test('persists experimental optimistic sync state across facade reopen', async () => {
    database = await connect({
      endpoint: 'memory',
      namespace: 'harness-sync',
      database: 'harness-sync',
    });
    const options = {
      partitionId: 'partition',
      clientId: 'client',
      requestedScope: 'all',
      subscriptionRevision: 1n,
    };
    const identity = {
      clientCommitId: 'commit-1',
      fingerprint: 'fingerprint-1',
    };
    const sync = await database.openExperimentalSync(options);

    const queued = await sync.enqueue({
      identity,
      operations: [
        {
          kind: 'upsert',
          record_id: 'person:ada',
          base_version: 'absent',
          value: { name: 'Ada' },
          reference: null,
        },
      ],
    });
    expect(queued.pendingCount).toBe(1);
    const [optimistic] = await database.query<string[]>(
      'SELECT VALUE name FROM person:ada',
    );
    expect(optimistic?.value).toEqual(['Ada']);

    await sync.close();
    const reopened = await database.openExperimentalSync(options);
    const pending = await reopened.pending();
    expect(pending).toHaveLength(1);
    const canonicalIdentity = (pending[0] as { identity: typeof identity })
      .identity;
    expect(canonicalIdentity.fingerprint).toMatch(/^sha256:[0-9a-f]{64}$/);
    expect((await reopened.status()).pendingCount).toBe(1);

    const conflicted = await reopened.recordPushResponse({
      schemaVersion: 'v1',
      partitionId: 'partition',
      clientId: 'client',
      outcome: {
        status: 'conflict',
        identity: canonicalIdentity,
        record_id: 'person:ada',
        authoritative: { state: 'absent' },
      },
    });
    expect(conflicted.pendingCount).toBe(0);
    expect(conflicted.conflictCount).toBe(1);
    expect(await reopened.conflicts()).toHaveLength(1);
    const [canonical] = await database.query<string[]>(
      'SELECT VALUE name FROM person:ada',
    );
    expect(canonical?.value).toEqual([]);
    await reopened.close();
  });

  test('runs application-owned HTTP sync against a mocked authority', async () => {
    database = await connect({
      endpoint: 'memory',
      namespace: 'harness-sync-http',
      database: 'harness-sync-http',
    });
    const options = {
      partitionId: 'partition-http',
      clientId: 'client-http',
      requestedScope: 'all',
      subscriptionRevision: 1n,
    };
    const identity = {
      clientCommitId: 'commit-http-1',
      fingerprint: 'fingerprint-http-1',
    };
    const sync = await database.openExperimentalSync(options);
    await sync.enqueue({
      identity,
      operations: [
        {
          kind: 'upsert',
          record_id: 'temp:http',
          base_version: 'absent',
          value: { name: 'HTTP proof' },
          reference: null,
        },
      ],
    });
    const [pending] = await sync.pending();
    const canonicalIdentity = (pending as { identity: typeof identity })
      .identity;
    expect(canonicalIdentity.fingerprint).toMatch(/^sha256:[0-9a-f]{64}$/);

    const requests: Array<{ url: string; authorization: string | null }> = [];
    const jsonResponse = (value: unknown): Response => {
      const body = JSON.stringify(value);
      return new Response(body, {
        headers: {
          'Content-Length': String(new TextEncoder().encode(body).byteLength),
          'Content-Type': 'application/json',
        },
        status: 200,
      });
    };
    const fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      requests.push({
        url,
        authorization: new Headers(init?.headers).get('Authorization'),
      });
      if (url.endsWith('/v1/sync/push')) {
        return jsonResponse({
          schemaVersion: 'v1',
          partitionId: options.partitionId,
          clientId: options.clientId,
          outcome: {
            status: 'accepted',
            identity: canonicalIdentity,
            sequence: 1,
            id_mappings: [
              {
                localId: 'temp:http',
                canonicalId: 'person:http',
              },
            ],
          },
        });
      }
      return jsonResponse({
        response: 'reset',
        reason: 'checkpoint_expired',
        checkpoint: {
          token: 'checkpoint-http-1',
          cursor: { epoch: 1, sequence: 1 },
          scope: {
            identity: 'all',
            authorizationRevision: 1,
            subscriptionRevision: 1,
          },
        },
        records: [
          {
            recordId: 'person:http',
            state: {
              state: 'present',
              value: { name: 'HTTP proof' },
              version: 1,
              reference: null,
            },
          },
        ],
      });
    }) as typeof globalThis.fetch;
    const adapter = new ExperimentalSyncHttpAdapter({
      sync,
      baseUrl: 'https://sync.invalid',
      ...options,
      accessToken: () => 'redacted-harness-token',
      codec: experimentalJsonSyncHttpCodec,
      fetch,
    });

    let onlineListener: ((online: boolean) => void) | undefined;
    let invalidationHint: (() => void) | undefined;
    const scheduler = new ExperimentalSyncScheduler({
      adapter,
      connectivity: {
        current: () => false,
        subscribe: listener => {
          onlineListener = listener;
          return () => {
            onlineListener = undefined;
          };
        },
      },
      invalidations: {
        start: onHint => {
          invalidationHint = onHint;
          return () => {
            invalidationHint = undefined;
          };
        },
      },
    });
    scheduler.start();
    expect(scheduler.status.state).toBe('offline');
    expect(requests).toHaveLength(0);
    onlineListener?.(true);
    await eventually(
      () => scheduler.status.state === 'idle',
      () => `scheduler remained ${JSON.stringify(scheduler.status)}`,
    );
    expect((await sync.status()).pendingCount).toBe(0);
    expect(await sync.checkpointToken()).toBe('checkpoint-http-1');
    const [mapped] = await database.query<string[]>(
      'SELECT VALUE name FROM person:http',
    );
    const [temporary] = await database.query<string[]>(
      'SELECT VALUE name FROM temp:http',
    );
    expect(mapped?.value).toEqual(['HTTP proof']);
    expect(temporary?.value).toEqual([]);
    expect(requests).toEqual([
      {
        url: 'https://sync.invalid/v1/sync/push',
        authorization: 'Bearer redacted-harness-token',
      },
      {
        url: 'https://sync.invalid/v1/sync/pull',
        authorization: 'Bearer redacted-harness-token',
      },
    ]);
    invalidationHint?.();
    await eventually(() => requests.length === 3);
    expect(requests[2]?.url).toBe('https://sync.invalid/v1/sync/pull');
    scheduler.stop();
    await sync.close();
  });

  test('streams live query notifications and closes deterministically', async () => {
    database = await connect({
      endpoint: 'memory',
      namespace: 'harness-live',
      database: 'harness-live',
    });
    await database.query('DEFINE TABLE event SCHEMALESS');
    const events = await database.live<{ id: SurrealRecordId; ready: boolean }>(
      'LIVE SELECT * FROM event',
    );

    await database.query('CREATE event:one SET ready = true');
    const notification = await events.next();

    expect(notification).toEqual({
      queryId: expect.any(String),
      action: 'create',
      value: {
        id: new SurrealRecordId('event:one'),
        ready: true,
      },
    });

    await events.close();
    expect(events.isClosed).toBe(true);
    expect(await events.next()).toBeUndefined();
  });

  test('commits and rolls back callback transactions', async () => {
    database = await connect({
      endpoint: 'memory',
      namespace: 'harness-transactions',
      database: 'harness-transactions',
    });

    await database.transaction(async transaction => {
      await transaction.query(
        'CREATE person:ada SET name = $name RETURN NONE',
        { name: 'Ada' },
      );
      await transaction.query(
        'CREATE person:lin SET name = $name RETURN NONE',
        { name: 'Lin' },
      );
    });

    await expect(
      database.transaction(async transaction => {
        await transaction.query(
          'CREATE person:grace SET name = $name RETURN NONE',
          { name: 'Grace' },
        );
        throw new Error('roll back');
      }),
    ).rejects.toThrow('roll back');

    const [result] = await database.query<string[]>(
      'SELECT VALUE name FROM person ORDER BY name',
    );
    expect(result?.value).toEqual(['Ada', 'Lin']);
  });

  test('executes parameter batches and preserves codec equivalence', async () => {
    database = await connect({
      endpoint: 'memory',
      namespace: 'harness-batch',
      database: 'harness-batch',
    });

    const executed = await database.transaction(transaction =>
      transaction.executeBatch(
        'CREATE item CONTENT { sequence: $sequence, name: $name } RETURN NONE',
        [
          { sequence: 1, name: 'one' },
          { sequence: 2, name: 'two' },
        ],
      ),
    );
    expect(executed).toBe(2);
    const batchResults = await database.transaction(transaction =>
      transaction.queryBatch([
        { surql: 'RETURN $value', variables: { value: 'first' } },
        { surql: 'RETURN $value', variables: { value: 'second' } },
      ]),
    );
    expect(batchResults.map(result => result.results[0]?.value)).toEqual([
      'first',
      'second',
    ]);

    const variants = await Promise.all([
      database.queryProfiled('SELECT sequence, name FROM item', undefined, {
        decodeMode: 'copy',
        nativeOutputEncoding: 'tree',
      }),
      database.queryProfiled('SELECT sequence, name FROM item', undefined, {
        decodeMode: 'in-place',
        nativeOutputEncoding: 'tree',
      }),
      database.queryProfiled('SELECT sequence, name FROM item', undefined, {
        decodeMode: 'copy',
        nativeOutputEncoding: 'streaming',
      }),
      database.queryProfiled('SELECT sequence, name FROM item', undefined, {
        decodeMode: 'in-place',
        nativeOutputEncoding: 'streaming',
      }),
    ]);

    expect(variants.map(variant => variant.results)).toEqual([
      variants[0]?.results,
      variants[0]?.results,
      variants[0]?.results,
      variants[0]?.results,
    ]);
  });
});

async function eventually(
  predicate: () => boolean,
  describeFailure: () => string = () => 'timed out waiting for harness state',
): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error(describeFailure());
    await new Promise(resolve => setTimeout(resolve, 10));
  }
}
