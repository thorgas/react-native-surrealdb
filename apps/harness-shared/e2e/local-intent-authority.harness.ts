import { describe, expect, test } from "react-native-harness";
import {
  connect,
  createExperimentalCanonicalCborSyncHttpCodec,
  ExperimentalSyncHttpAdapter,
  type ExperimentalSyncClient,
  type SurrealClient,
} from "react-native-surrealdb";

import { surrealkvChurnEndpoint } from "../surrealkv-churn-fixture";

type AuthorityConfig = {
  authorityUrl: string;
  token: string;
  partitionId: string;
  runSuffix: string;
};

declare const require: (
  path: "./local-intent-authority.generated",
) => AuthorityConfig;

const {
  authorityUrl,
  token,
  partitionId,
  runSuffix,
} = require("./local-intent-authority.generated");
const storageRoot = surrealkvChurnEndpoint.slice(
  0,
  surrealkvChurnEndpoint.lastIndexOf("/"),
);
const requestedScope = "all";
const subscriptionRevision = 1n;

function transport(sync: ExperimentalSyncClient, clientId: string) {
  return new ExperimentalSyncHttpAdapter({
    sync,
    baseUrl: authorityUrl,
    allowInsecureLocalhost: true,
    partitionId,
    clientId,
    requestedScope,
    subscriptionRevision,
    accessToken: () => token,
    codec: createExperimentalCanonicalCborSyncHttpCodec(),
    pullSourceProof: true,
  });
}

function store(client: "a" | "b") {
  return {
    endpoint: `${storageRoot}/local-intent-authority-${client}-${runSuffix}`,
    namespace: `local-intent-${client}-${runSuffix}`,
    database: "e2e",
  };
}

function intent(id: string, recordId: string, value: string) {
  return {
    id,
    record_id: recordId,
    value: { state: "present", value: { phrase: value }, reference: null },
    observation: null,
  } as const;
}

async function close(
  sync: ExperimentalSyncClient | undefined,
  db: SurrealClient | undefined,
) {
  if (sync) await sync.close();
  if (db && !db.isClosed) await db.close();
}

describe("live source-proof intent authority", () => {
  test("recovers two durable offline clients and completes a causal pull", async () => {
    const clientA = `intent-a-${runSuffix}`;
    const clientB = `intent-b-${runSuffix}`;
    const optionsA = {
      partitionId,
      clientId: clientA,
      requestedScope,
      subscriptionRevision,
    };
    const optionsB = { ...optionsA, clientId: clientB };
    const hiddenRecord = `local_intent_hidden:${runSuffix}`;
    const syncedRecord = `local_intent_synced:${runSuffix}`;
    const hiddenCreate = intent(
      `hidden-create-${runSuffix}`,
      hiddenRecord,
      "temporary",
    );
    const syncedCreate = intent(
      `synced-create-${runSuffix}`,
      syncedRecord,
      "from-a",
    );
    let dbA: SurrealClient | undefined;
    let dbB: SurrealClient | undefined;
    let syncA: ExperimentalSyncClient | undefined;
    let syncB: ExperimentalSyncClient | undefined;

    try {
      dbA = await connect(store("a"));
      dbB = await connect(store("b"));
      syncA = await dbA.openExperimentalSync(optionsA);
      syncB = await dbB.openExperimentalSync(optionsB);
      const firstA = transport(syncA, clientA);
      const firstB = transport(syncB, clientB);
      await firstA.pull();
      await firstB.pull();

      // Network calls stop here. Both edits enter the native durable lane only.
      await syncA.enqueueIntent(
        hiddenCreate,
        await syncA.observeRecord(hiddenRecord),
      );
      await syncA.enqueueIntent(
        {
          id: `hidden-delete-${runSuffix}`,
          record_id: hiddenRecord,
          value: { state: "deleted" },
          observation: null,
        },
        await syncA.observeRecord(hiddenRecord),
      );
      await syncA.enqueueIntent(
        syncedCreate,
        await syncA.observeRecord(syncedRecord),
      );
      const beforeRestart = await syncA.readIntentLanes();
      expect(
        beforeRestart.lanes.find((lane) => lane.recordId === hiddenRecord)
          ?.pendingIntentIds,
      ).toEqual([hiddenCreate.id, `hidden-delete-${runSuffix}`]);
      expect(
        (await syncA.readIntentRecord(hiddenRecord)).record.row,
      ).toBeUndefined();

      await close(syncA, dbA);
      await close(syncB, dbB);
      syncA = undefined;
      syncB = undefined;
      dbA = await connect(store("a"));
      dbB = await connect(store("b"));
      syncA = await dbA.openExistingExperimentalSync(optionsA);
      syncB = await dbB.openExistingExperimentalSync(optionsB);
      const resumedA = transport(syncA, clientA);
      const resumedB = transport(syncB, clientB);
      const recovered = await syncA.readIntentLanes();
      expect(
        recovered.lanes.find((lane) => lane.recordId === hiddenRecord)
          ?.pendingIntentIds,
      ).toEqual([hiddenCreate.id, `hidden-delete-${runSuffix}`]);
      expect(
        recovered.lanes.find((lane) => lane.recordId === syncedRecord)
          ?.pendingIntentIds,
      ).toEqual([syncedCreate.id]);
      expect(
        (await syncA.readIntentRecord(hiddenRecord)).record.row,
      ).toBeUndefined();

      // The stable ID is a replay key. A changed payload with that ID must fail.
      await syncA.enqueueIntent(
        syncedCreate,
        await syncA.observeRecord(syncedRecord),
      );
      await expect(
        syncA.enqueueIntent(
          intent(syncedCreate.id, syncedRecord, "equivocated"),
          await syncA.observeRecord(syncedRecord),
        ),
      ).rejects.toThrow();
      expect(
        (await syncA.readIntentRecord(syncedRecord)).lane.pendingIntentIds,
      ).toEqual([syncedCreate.id]);

      await syncA.materializeNextIntentWithCommitId(
        syncedRecord,
        `wire-${runSuffix}`,
      );
      await expect(
        syncA.materializeNextIntentWithCommitId(
          syncedRecord,
          `wire-${runSuffix}`,
        ),
      ).rejects.toThrow();
      expect(
        (await syncA.readIntentRecord(syncedRecord)).lane.headIdentityJson,
      ).toEqual(expect.any(String));
      const pushed = await resumedA.push();
      expect(pushed).toHaveLength(1);
      expect(pushed[0]?.conflictCount).toBe(0);
      const pulledB = await resumedB.pull();
      expect(pulledB.cursorSequence).toEqual(expect.any(BigInt));
      const [visibleB] = await dbB.query<string[]>(
        `SELECT VALUE phrase FROM ${syncedRecord}`,
      );
      expect(visibleB?.value).toEqual(["from-a"]);
      await resumedA.pull();
      expect(
        (await syncA.readIntentRecord(syncedRecord)).lane.pendingIntentIds,
      ).toEqual([]);
      expect(
        (await syncA.readIntentLanes()).lanes.find(
          (lane) => lane.recordId === hiddenRecord,
        )?.pendingIntentIds,
      ).toEqual([hiddenCreate.id, `hidden-delete-${runSuffix}`]);
    } finally {
      await Promise.allSettled([close(syncA, dbA), close(syncB, dbB)]);
    }
  });
});
