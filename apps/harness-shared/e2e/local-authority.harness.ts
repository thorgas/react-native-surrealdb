import { describe, expect, test } from "react-native-harness";
import {
  ExperimentalSyncHttpAdapter,
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
  path: "./local-authority-token.generated"
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
    partitionId,
    clientId,
    requestedScope,
    subscriptionRevision,
    accessToken: () => syncDevToken,
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
              value: { winner: "client-a" },
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
              value: { winner: "client-b" },
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
        databaseA.query<string[]>(`SELECT VALUE winner FROM ${recordId}`),
        databaseB.query<string[]>(`SELECT VALUE winner FROM ${recordId}`),
      ]);
      expect(recordsA[0]?.value).toEqual(["client-a"]);
      expect(recordsB[0]?.value).toEqual(recordsA[0]?.value);
      expect(await syncB.conflicts()).toHaveLength(1);

      await Promise.all([syncA.close(), syncB.close()]);
    } finally {
      await Promise.all([close(databaseA), close(databaseB)]);
    }
  });
});
