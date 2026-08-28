import { describe, expect, test } from "react-native-harness";
import {
  connect,
  createExperimentalCanonicalCborSyncHttpCodec,
} from "react-native-surrealdb";

import {
  syncRestartEndpoint,
  syncRestartIdentity,
  syncRestartOptions,
} from "../sync-restart-fixture";

describe("SurrealKV sync process restart seed", () => {
  test("commits optimistic sync state before forced process termination", async () => {
    const database = await connect({
      endpoint: syncRestartEndpoint,
      namespace: "restart-e2e",
      database: "restart-e2e",
    });
    await database.query(
      "REMOVE TABLE IF EXISTS _sync_client_state; REMOVE TABLE IF EXISTS restart_probe;",
    );
    await database.query(
      "CREATE restart_probe:marker SET phase = 'seeded' RETURN NONE",
    );

    const sync = await database.openExperimentalSync(syncRestartOptions);
    const status = await sync.enqueue({
      identity: syncRestartIdentity,
      operations: [
        {
          kind: "upsert",
          record_id: "person:restart",
          base_version: "absent",
          value: { name: "Restart proof" },
          reference: null,
        },
      ],
    });

    expect(status.pendingCount).toBe(1);

    const codec = createExperimentalCanonicalCborSyncHttpCodec();
    const [pendingJson] = await sync.pendingProtocolJson();
    const pushBytes = await codec.encodePushRequest({
      partitionId: syncRestartOptions.partitionId,
      clientId: syncRestartOptions.clientId,
      commitJson: pendingJson!,
    });
    expect(new Uint8Array(pushBytes).slice(0, 2)).toEqual(
      Uint8Array.from([0x84, 0x70]),
    );

    const pullResponse = await codec.decodePullResponse(
      new Response(
        Uint8Array.from([
          0x84, 0x70, 0x73, 0x75, 0x72, 0x72, 0x65, 0x61, 0x6c, 0x64, 0x62,
          0x2d, 0x73, 0x79, 0x6e, 0x63, 0x2f, 0x31, 0x00, 0x03, 0x82, 0x00,
          0x80,
        ]).buffer,
      ),
    );
    expect(JSON.parse(pullResponse)).toEqual({ response: "batch", frames: [] });

    await sync.applyPullResponse({
      response: "reset",
      reason: "checkpoint_expired",
      checkpoint: {
        token: "checkpoint-before-process-restart",
        cursor: { epoch: 1, sequence: 0 },
        scope: {
          identity: syncRestartOptions.requestedScope,
          authorizationRevision: 1,
          subscriptionRevision: 1,
        },
      },
      records: [],
    });
    expect(await sync.checkpointToken()).toBe(
      "checkpoint-before-process-restart",
    );
    const [optimistic] = await database.query<string[]>(
      "SELECT VALUE name FROM person:restart",
    );
    expect(optimistic?.value).toEqual(["Restart proof"]);

    // Deliberately leave both native handles open. The Harness runner force-stops
    // the app before the verification file, exercising process-death recovery.
  });
});
