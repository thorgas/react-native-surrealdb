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
      }
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

  test('streams live query notifications and closes deterministically', async () => {
    database = await connect({
      endpoint: 'memory',
      namespace: 'harness-live',
      database: 'harness-live',
    });
    await database.query('DEFINE TABLE event SCHEMALESS');
    const events = await database.live<{ id: SurrealRecordId; ready: boolean }>(
      'LIVE SELECT * FROM event'
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
});
