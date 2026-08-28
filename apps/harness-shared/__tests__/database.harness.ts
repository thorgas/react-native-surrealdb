import { afterEach, describe, expect, test } from 'react-native-harness';
import {
  SurrealRecordId,
  connect,
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
    expect((await reopened.pending())).toHaveLength(1);
    expect((await reopened.status()).pendingCount).toBe(1);

    const conflicted = await reopened.recordPushResponse({
      schemaVersion: 'v1',
      partitionId: 'partition',
      clientId: 'client',
      outcome: {
        status: 'conflict',
        identity,
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
