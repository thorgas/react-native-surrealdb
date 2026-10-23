import { acknowledgedClose, closeOwned, type OwnedClose } from '../close-owned';
import { describe, expect, test } from 'react-native-harness';
import { Platform } from 'react-native';
import {
  connect,
  encodeSurrealValue,
  type SyncJsonValue,
  type SyncViewRequest,
} from 'react-native-surrealdb';
const count: SyncViewRequest = {
  protocolVersion: 1,
  operation: 'count',
  selector: { kind: 'eq', path: ['category'], value: 'synthetic' },
};
const page: SyncViewRequest = {
  protocolVersion: 1,
  operation: 'page',
  selector: { kind: 'all' },
  projection: [['name'], ['counter'], ['bytes']],
  limit: 2,
};
const wait = (ms: number) =>
  new Promise<void>(resolve => setTimeout(resolve, ms));
async function notify(kind: 'ready' | 'report' | 'closed', value: unknown) {
  const response = await fetch(`http://127.0.0.1:19062/${kind}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(value),
  });
  if (!response.ok)
    throw new Error('Synthetic acceptance collector refused report');
}
function checkpoint(authorizationRevision = 1) {
  return {
    token: 'synthetic-view-checkpoint',
    cursor: { epoch: 1, sequence: 1 },
    scope: { identity: 'all', authorizationRevision, subscriptionRevision: 1 },
  };
}
function wireLength(value: unknown) {
  return encodeSurrealValue(value as SyncJsonValue).length;
}
describe('bounded native view Android acceptance', () => {
  test('executes the matching native ABI, bounded reads, cursor and owner barriers on Hermes', async () => {
    const hermes = (
      globalThis as unknown as {
        HermesInternal?: { getRuntimeProperties?: () => unknown };
      }
    ).HermesInternal;
    expect(hermes !== undefined).toBe(true);
    console.info(
      'BOUNDED_VIEW_READY=' +
        JSON.stringify({
          at: Date.now(),
          hermes: hermes?.getRuntimeProperties?.(),
          reactNative: Platform.constants.reactNativeVersion,
        }),
    );
    await notify('ready', {
      at: Date.now(),
      hermes: hermes?.getRuntimeProperties?.(),
      reactNative: Platform.constants.reactNativeVersion,
    });
    // Allows external CPU/RSS/FPS collection to attach before the synthetic workload.
    await wait(30000);
    const owned: OwnedClose[] = [];
    try {
      const database = await connect({
        endpoint: 'mem://',
        namespace: 'bounded-view-device-only',
        database: 'bounded-view-device-only',
      });
      owned.push({ name: 'database', close: () => database.close() });
      const options = {
        partitionId: 'synthetic-bounded-view',
        clientId: 'device-probe',
        requestedScope: 'all',
        subscriptionRevision: 1n,
      };
      const sync = await database.openExperimentalSync(options);
      owned.push({ name: 'sync', close: () => sync.close() });
      const sibling = await database.openExperimentalSync(options);
      owned.push({ name: 'sibling', close: () => sibling.close() });
      const unknown = await sync.readView(count);
      expect(unknown.kind).toBe('count');
      expect(unknown.metadata.coverage.kind).toBe('unknown');
      if (unknown.kind !== 'count') throw new Error('missing count');
      expect(unknown.observedCount).toBe('0');
      const records = Array.from({ length: 1000 }, (_, i) => ({
        recordId: `probe:${String(i).padStart(5, '0')}`,
        state: {
          state: 'present',
          value: {
            category: 'synthetic',
            name: `name ${i}`,
            counter: 9223372036854775807n,
            bytes: new Uint8Array([0, 1, 255]),
            large: 'x'.repeat(1024),
          },
          version: 1,
          reference: null,
        },
      }));
      await sync.applyPullResponse({
        response: 'reset',
        reason: 'epoch_changed',
        checkpoint: checkpoint(),
        records,
      });
      const total = await sync.readView(count);
      expect(total.kind).toBe('count');
      if (total.kind !== 'count') throw new Error('missing count');
      expect(total.observedCount).toBe('1000');
      expect(total.metadata.coverage.kind).toBe('authorized_reset');
      const first = await sync.readView(page);
      expect(first.kind).toBe('page');
      if (first.kind !== 'page') throw new Error('missing page');
      expect(first.rows.length).toBe(2);
      expect(first.nextCursor !== null).toBe(true);
      expect(first.rows[0]?.fields[1]?.value).toBe(9223372036854775807n);
      expect(first.rows[0]?.fields[2]?.value).toEqual(
        new Uint8Array([0, 1, 255]),
      );
      const second = await sync.readView({
        ...page,
        operation: 'page',
        cursor: first.nextCursor,
      });
      if (second.kind !== 'page') throw new Error('missing continuation');
      expect(second.rows[0]?.recordId).toBe('probe:00002');
      const tag = await sync.readView({
        protocolVersion: 1,
        operation: 'count',
        selector: {
          kind: 'eq',
          path: ['counter', 'value'],
          value: '9223372036854775807',
        },
      });
      if (tag.kind !== 'count') throw new Error('missing tag count');
      expect(tag.observedCount).toBe('0');
      await sync.enqueue({
        identity: {
          clientCommitId: 'device-pending',
          fingerprint: 'native-recomputed',
        },
        operations: [
          {
            kind: 'upsert',
            record_id: 'probe:00000',
            base_version: { exact: 1 },
            value: {
              category: 'synthetic',
              name: 'pending name',
              counter: 9223372036854775807n,
              bytes: new Uint8Array([0, 1, 255]),
              large: 'x'.repeat(1024),
            },
            reference: null,
          },
        ],
      });
      expect(
        (
          await sync.readView({
            ...page,
            operation: 'page',
            cursor: first.nextCursor,
          })
        ).kind,
      ).toBe('stale_cursor');
      const pending = await sync.readView(page);
      if (pending.kind !== 'page') throw new Error('missing pending page');
      expect(pending.rows[0]?.pending.length).toBe(1);
      const warmSnapshot = await sync.readSnapshot();
      const warmCount = await sync.readView(count);
      const warmPage = await sync.readView(page);
      const rounds: Array<{
        round: number;
        snapshotMs: number;
        countMs: number;
        pageMs: number;
      }> = [];
      for (let i = 0; i < 5; i++) {
        const result = { round: i, snapshotMs: 0, countMs: 0, pageMs: 0 };
        for (const name of i % 2 === 0
          ? ['snapshot', 'count', 'page']
          : ['page', 'count', 'snapshot']) {
          const start = performance.now();
          if (name === 'snapshot') {
            const value = await sync.readSnapshot();
            result.snapshotMs = performance.now() - start;
            expect(Object.keys(value.snapshot.rows).length).toBe(1000);
          }
          if (name === 'count') {
            const value = await sync.readView(count);
            result.countMs = performance.now() - start;
            if (value.kind !== 'count')
              throw new Error('missing measured count');
            expect(value.observedCount).toBe('1000');
          }
          if (name === 'page') {
            const value = await sync.readView(page);
            result.pageMs = performance.now() - start;
            if (value.kind !== 'page') throw new Error('missing measured page');
            expect(value.rows.length).toBe(2);
          }
        }
        rounds.push(result);
      }
      const disposable = await database.openExperimentalSync(options);
      const disposableOwner = acknowledgedClose('disposable', () =>
        disposable.close(),
      );
      owned.push(disposableOwner);
      await disposableOwner.close();
      await expect(disposable.readView(count)).rejects.toThrow('closed');
      await expect(
        sync.applyPullResponse({
          response: 'reset',
          reason: 'authorization_changed',
          checkpoint: checkpoint(2),
          records: [],
        }),
      ).rejects.toThrow();
      await expect(sync.readView(count)).rejects.toThrow(
        'new authorized journal',
      );
      await expect(sibling.readView(count)).rejects.toThrow(
        'new authorized journal',
      );
      const report = {
        at: Date.now(),
        synthetic: true,
        productStartupProved: false,
        fixtureRecords: 1000,
        unknownCoverageProved: true,
        losslessAtomsProved: true,
        staleCursorProved: true,
        closedRefusalProved: true,
        revokedAndSiblingRefusalProved: true,
        hermes: hermes?.getRuntimeProperties?.(),
        reactNative: Platform.constants.reactNativeVersion,
        rounds,
        losslessReencodedAsciiBytes: {
          snapshot: wireLength(warmSnapshot),
          count: wireLength(warmCount),
          page: wireLength(warmPage),
        },
        work: {
          count: warmCount.kind === 'count' ? warmCount.work : null,
          page: warmPage.kind === 'page' ? warmPage.work : null,
        },
      };
      console.info('BOUNDED_VIEW_REPORT=' + JSON.stringify(report));
      await notify('report', report);
      console.info('BOUNDED_VIEW_CASES_PASS=' + Date.now());
      await wait(30000);
    } finally {
      const cleanupErrors = await closeOwned(owned);
      console.info(
        'BOUNDED_VIEW_FIXTURE_CLEANUP=' +
          JSON.stringify({ at: Date.now(), cleanupErrors }),
      );
      await notify('closed', {
        at: Date.now(),
        handlesClosed: cleanupErrors.length === 0,
        databaseClosed:
          owned.some(resource => resource.name === 'database') &&
          !cleanupErrors.some(error => error.startsWith('database:')),
        cleanupErrors,
        synthetic: true,
      });
      if (cleanupErrors.length)
        throw new Error(`Fixture cleanup failed: ${cleanupErrors.join('; ')}`);
    }
  });
});
