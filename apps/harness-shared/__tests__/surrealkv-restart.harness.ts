import { describe, expect, test } from 'react-native-harness';
import { connect } from 'react-native-surrealdb';

import {
  syncRestartEndpoint,
  syncRestartIdentity,
  syncRestartOptions,
} from '../sync-restart-fixture';

describe('SurrealKV sync process restart recovery', () => {
  test('recovers durable outbox and optimistic state in a new process', async () => {
    const database = await connect({
      endpoint: syncRestartEndpoint,
      namespace: 'restart-e2e',
      database: 'restart-e2e',
    });
    const [probe] = await database.query<string[]>(
      'SELECT VALUE phase FROM restart_probe:marker',
    );
    expect(probe?.value).toEqual(['seeded']);

    const sync = await database.openExperimentalSync(syncRestartOptions);
    expect((await sync.status()).pendingCount).toBe(1);
    expect(await sync.pending()).toHaveLength(1);
    const [optimistic] = await database.query<string[]>(
      'SELECT VALUE name FROM person:restart',
    );
    expect(optimistic?.value).toEqual(['Restart proof']);

    const resolved = await sync.recordPushResponse({
      schemaVersion: 'v1',
      partitionId: syncRestartOptions.partitionId,
      clientId: syncRestartOptions.clientId,
      outcome: {
        status: 'conflict',
        identity: syncRestartIdentity,
        record_id: 'person:restart',
        authoritative: { state: 'absent' },
      },
    });
    expect(resolved.pendingCount).toBe(0);
    expect(resolved.conflictCount).toBe(1);

    await sync.close();
    await database.query(
      'REMOVE TABLE IF EXISTS _sync_client_state; REMOVE TABLE IF EXISTS restart_probe;',
    );
    await database.close();
  });
});
