import { describe, expect, test } from 'react-native-harness';
import { connect } from 'react-native-surrealdb';

import {
  syncRestartEndpoint,
  syncRestartIdentity,
  syncRestartOptions,
} from '../sync-restart-fixture';

describe('SurrealKV sync process restart seed', () => {
  test('commits optimistic sync state before forced process termination', async () => {
    const database = await connect({
      endpoint: syncRestartEndpoint,
      namespace: 'restart-e2e',
      database: 'restart-e2e',
    });
    await database.query(
      'REMOVE TABLE IF EXISTS _sync_client_state; REMOVE TABLE IF EXISTS restart_probe;',
    );
    await database.query(
      "CREATE restart_probe:marker SET phase = 'seeded' RETURN NONE",
    );

    const sync = await database.openExperimentalSync(syncRestartOptions);
    const status = await sync.enqueue({
      identity: syncRestartIdentity,
      operations: [
        {
          kind: 'upsert',
          record_id: 'person:restart',
          base_version: 'absent',
          value: { name: 'Restart proof' },
          reference: null,
        },
      ],
    });

    expect(status.pendingCount).toBe(1);
    const [optimistic] = await database.query<string[]>(
      'SELECT VALUE name FROM person:restart',
    );
    expect(optimistic?.value).toEqual(['Restart proof']);

    // Deliberately leave both native handles open. The Harness runner force-stops
    // the app before the verification file, exercising process-death recovery.
  });
});
