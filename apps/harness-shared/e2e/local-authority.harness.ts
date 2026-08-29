import { describe, expect, test } from "react-native-harness";
import {
  ExperimentalSyncHttpAdapter,
  type ExperimentalSyncApplicationLifecycle,
  type ExperimentalSyncApplicationState,
  type ExperimentalSyncConnectivity,
  ExperimentalSyncLifecycleCoordinator,
  ExperimentalSyncScheduler,
  connect,
  createExperimentalCanonicalCborSyncHttpCodec,
  type ExperimentalSyncClient,
  type SurrealClient,
} from "react-native-surrealdb";

import { resolveLocalAuthorityUrl } from "../local-authority-url";

type GeneratedLocalAuthorityConfig = {
  runSuffix: string;
  syncDevToken: string;
};

declare const require: (
  path: "./local-authority-token.generated",
) => GeneratedLocalAuthorityConfig;

const {
  runSuffix,
  syncDevToken,
} = require("./local-authority-token.generated");

const partitionId = "partition";
const requestedScope = "all";
const subscriptionRevision = 1n;
const recordId = `local_authority_probe:${runSuffix}`;

function transport(sync: ExperimentalSyncClient, clientId: string) {
  return new ExperimentalSyncHttpAdapter({
    sync,
    baseUrl: resolveLocalAuthorityUrl(),
    allowInsecureLocalhost: true,
    partitionId,
    clientId,
    requestedScope,
    subscriptionRevision,
    accessToken: () => syncDevToken,
    codec: createExperimentalCanonicalCborSyncHttpCodec(),
  });
}

function dynamicTransport(
  sync: ExperimentalSyncClient,
  clientId: string,
  accessToken: () => string,
) {
  return new ExperimentalSyncHttpAdapter({
    sync,
    baseUrl: resolveLocalAuthorityUrl(),
    allowInsecureLocalhost: true,
    partitionId,
    clientId,
    requestedScope,
    subscriptionRevision,
    accessToken,
    codec: createExperimentalCanonicalCborSyncHttpCodec(),
  });
}

async function close(database: SurrealClient | undefined) {
  if (database && !database.isClosed) await database.close();
}

describe("live local authority", () => {
  test("converges two offline clients after an authoritative conflict", async () => {
    const clientA = `rn-a-${runSuffix}`;
    const clientB = `rn-b-${runSuffix}`;
    const optionsA = {
      partitionId,
      clientId: clientA,
      requestedScope,
      subscriptionRevision,
    };
    const optionsB = { ...optionsA, clientId: clientB };
    let databaseA: SurrealClient | undefined;
    let databaseB: SurrealClient | undefined;

    try {
      [databaseA, databaseB] = await Promise.all([
        connect({
          endpoint: "memory",
          namespace: `local-authority-a-${runSuffix}`,
          database: "e2e",
        }),
        connect({
          endpoint: "memory",
          namespace: `local-authority-b-${runSuffix}`,
          database: "e2e",
        }),
      ]);
      const syncA = await databaseA.openExperimentalSync(optionsA);
      let syncB = await databaseB.openExperimentalSync(optionsB);
      const transportA = transport(syncA, clientA);
      let transportB = transport(syncB, clientB);

      await Promise.all([transportA.pull(), transportB.pull()]);
      expect(await syncA.checkpointToken()).toEqual(expect.any(String));
      expect(await syncB.checkpointToken()).toEqual(expect.any(String));

      await Promise.all([
        syncA.enqueue({
          identity: {
            clientCommitId: `commit-a-${runSuffix}`,
            fingerprint: "computed-by-native",
          },
          operations: [
            {
              kind: "upsert",
              record_id: recordId,
              base_version: "absent",
              value: { winner: "client-a", servings: 2.5 },
              reference: null,
            },
          ],
        }),
        syncB.enqueue({
          identity: {
            clientCommitId: `commit-b-${runSuffix}`,
            fingerprint: "computed-by-native",
          },
          operations: [
            {
              kind: "upsert",
              record_id: recordId,
              base_version: "absent",
              value: { winner: "client-b", servings: 3.75 },
              reference: null,
            },
          ],
        }),
      ]);

      const accepted = await transportA.push();
      expect(accepted).toHaveLength(1);
      expect(accepted[0]).toMatchObject({
        pendingCount: 0,
        outcomeCount: 1,
        conflictCount: 0,
      });

      const conflicted = await transportB.push();
      expect(conflicted).toHaveLength(1);
      expect(conflicted[0]).toMatchObject({
        pendingCount: 0,
        outcomeCount: 1,
        conflictCount: 1,
      });

      await syncB.close();
      syncB = await databaseB.openExperimentalSync(optionsB);
      expect((await syncB.status()).conflictCount).toBe(1);
      expect(await syncB.conflicts()).toHaveLength(1);
      transportB = transport(syncB, clientB);

      const [pulledA, pulledB] = await Promise.all([
        transportA.pull(),
        transportB.pull(),
      ]);
      expect(typeof pulledA.cursorSequence).toBe("bigint");
      expect(pulledB.cursorSequence).toBe(pulledA.cursorSequence);

      const [recordsA, recordsB] = await Promise.all([
        databaseA.query<Array<{ winner: string; servings: number }>>(
          `SELECT VALUE { winner: winner, servings: servings } FROM ${recordId}`,
        ),
        databaseB.query<Array<{ winner: string; servings: number }>>(
          `SELECT VALUE { winner: winner, servings: servings } FROM ${recordId}`,
        ),
      ]);
      expect(recordsA[0]?.value).toEqual([
        { winner: "client-a", servings: 2.5 },
      ]);
      expect(recordsB[0]?.value).toEqual(recordsA[0]?.value);
      expect(await syncB.conflicts()).toHaveLength(1);

      await Promise.all([syncA.close(), syncB.close()]);
    } finally {
      await Promise.all([close(databaseA), close(databaseB)]);
    }
  });

  test("recovers durable work across connectivity, authentication, and lifecycle changes", async () => {
    const consumerId = `rn-lifecycle-${runSuffix}`;
    const producerId = `rn-producer-${runSuffix}`;
    const pendingRecord = `local_authority_pending:${runSuffix}`;
    const foregroundRecord = `local_authority_foreground:${runSuffix}`;
    const periodicRecord = `local_authority_periodic:${runSuffix}`;
    let consumerDatabase: SurrealClient | undefined;
    let producerDatabase: SurrealClient | undefined;
    let consumerSync: ExperimentalSyncClient | undefined;
    let producerSync: ExperimentalSyncClient | undefined;
    let online = false;
    let onlineListener: ((next: boolean) => void) | undefined;
    let applicationState: ExperimentalSyncApplicationState = "background";
    let lifecycleListener:
      ((next: ExperimentalSyncApplicationState) => void) | undefined;
    let token = "invalid-token-0123456789";

    const connectivity: ExperimentalSyncConnectivity = {
      current: () => online,
      subscribe: (listener) => {
        onlineListener = listener;
        return () => {
          onlineListener = undefined;
        };
      },
    };
    const lifecycle: ExperimentalSyncApplicationLifecycle = {
      current: () => applicationState,
      subscribe: (listener) => {
        lifecycleListener = listener;
        return () => {
          lifecycleListener = undefined;
        };
      },
    };
    const emitLifecycle = (next: ExperimentalSyncApplicationState) => {
      applicationState = next;
      lifecycleListener?.(next);
    };
    const emitConnectivity = (next: boolean) => {
      online = next;
      onlineListener?.(next);
    };

    try {
      [consumerDatabase, producerDatabase] = await Promise.all([
        connect({
          endpoint: "memory",
          namespace: `local-lifecycle-consumer-${runSuffix}`,
          database: "e2e",
        }),
        connect({
          endpoint: "memory",
          namespace: `local-lifecycle-producer-${runSuffix}`,
          database: "e2e",
        }),
      ]);
      const consumer = consumerDatabase;
      const producer = producerDatabase;
      consumerSync = await consumer.openExperimentalSync({
        partitionId,
        clientId: consumerId,
        requestedScope,
        subscriptionRevision,
      });
      producerSync = await producer.openExperimentalSync({
        partitionId,
        clientId: producerId,
        requestedScope,
        subscriptionRevision,
      });
      await consumerSync.enqueue({
        identity: {
          clientCommitId: `commit-pending-${runSuffix}`,
          fingerprint: "computed-by-native",
        },
        operations: [
          {
            kind: "upsert",
            record_id: pendingRecord,
            base_version: "absent",
            value: { phase: "durable-offline" },
            reference: null,
          },
        ],
      });

      const scheduler = new ExperimentalSyncScheduler({
        adapter: dynamicTransport(consumerSync, consumerId, () => token),
        connectivity,
        periodicPullMs: 250,
      });
      const coordinator = new ExperimentalSyncLifecycleCoordinator({
        scheduler,
        lifecycle,
        refreshAuthentication: async (_status, { signal }) => {
          if (signal.aborted) return false;
          token = syncDevToken;
          return true;
        },
      });
      coordinator.start();
      expect(scheduler.status.state).toBe("stopped");
      expect((await consumerSync.status()).pendingCount).toBe(1);

      emitLifecycle("active");
      expect(scheduler.status.state).toBe("offline");
      emitConnectivity(true);
      await eventuallyAsync(
        async () =>
          (await consumerSync?.status())?.pendingCount === 0 &&
          (await consumerSync?.checkpointToken()) != null,
        () => `consumer did not recover: ${JSON.stringify(scheduler.status)}`,
      );

      emitLifecycle("background");
      expect(scheduler.status.state).toBe("stopped");
      const producerTransport = transport(producerSync, producerId);
      await producerSync.enqueue({
        identity: {
          clientCommitId: `commit-foreground-${runSuffix}`,
          fingerprint: "computed-by-native",
        },
        operations: [
          {
            kind: "upsert",
            record_id: foregroundRecord,
            base_version: "absent",
            value: { phase: "foreground-catch-up" },
            reference: null,
          },
        ],
      });
      await producerTransport.push();
      emitLifecycle("active");
      await eventuallyAsync(async () => {
        const [result] = await consumer.query<string[]>(
          `SELECT VALUE phase FROM ${foregroundRecord}`,
        );
        return result?.value[0] === "foreground-catch-up";
      });

      await producerSync.enqueue({
        identity: {
          clientCommitId: `commit-periodic-${runSuffix}`,
          fingerprint: "computed-by-native",
        },
        operations: [
          {
            kind: "upsert",
            record_id: periodicRecord,
            base_version: "absent",
            value: { phase: "periodic-catch-up" },
            reference: null,
          },
        ],
      });
      await producerTransport.push();
      await eventuallyAsync(async () => {
        const [result] = await consumer.query<string[]>(
          `SELECT VALUE phase FROM ${periodicRecord}`,
        );
        return result?.value[0] === "periodic-catch-up";
      });

      coordinator.stop();
    } finally {
      await Promise.allSettled([consumerSync?.close(), producerSync?.close()]);
      await Promise.all([close(consumerDatabase), close(producerDatabase)]);
    }
  });
});

async function eventuallyAsync(
  predicate: () => Promise<boolean>,
  describeFailure: () => string = () =>
    "timed out waiting for local authority lifecycle state",
): Promise<void> {
  const deadline = Date.now() + 30_000;
  while (true) {
    try {
      if (await predicate()) return;
    } catch {
      // Embedded reads can briefly contend with the atomic pull install; retry the observation.
    }
    if (Date.now() >= deadline) throw new Error(describeFailure());
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
}
