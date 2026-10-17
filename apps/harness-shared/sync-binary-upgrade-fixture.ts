import { expect } from "react-native-harness";
import { connect } from "react-native-surrealdb";
import { surrealkvChurnEndpoint } from "./surrealkv-churn-fixture";

// A dedicated synthetic campaign identity. Seed refuses reuse; never delete this journal.
export const syncBinaryUpgradeCampaign = "task11-to-task14-20261017-01";
const campaign = syncBinaryUpgradeCampaign;
const endpoint = `${surrealkvChurnEndpoint.slice(
  0,
  surrealkvChurnEndpoint.lastIndexOf("/")
)}/${campaign}`;
const options = {
  partitionId: campaign,
  clientId: "upgrade-client",
  requestedScope: "all",
  subscriptionRevision: 1n,
};
const json = (value: unknown) =>
  JSON.stringify(value, (_, item) =>
    typeof item === "bigint" ? { upgradeBigint: item.toString() } : item
  );

async function runPhase(phase: "seed" | "verify") {
  const database = await connect({
    endpoint,
    namespace: "binary-upgrade",
    database: "binary-upgrade",
  });
  await database.query("DEFINE TABLE IF NOT EXISTS upgrade_probe SCHEMALESS");
  const [marker] = await database.query<string[]>(
    "SELECT VALUE receipt FROM upgrade_probe:marker"
  );
  const sync = await database.openExperimentalSync(options);
  const receipt = async () =>
    json({
      snapshot: (await sync.readSnapshot()).snapshot,
      pending: await sync.pendingProtocolJson(),
      conflicts: await sync.conflicts(),
      checkpoint: await sync.checkpointToken(),
      status: await sync.status(),
    });
  if (phase === "seed") {
    expect(marker?.value).toEqual([]);
    expect((await sync.status()).pendingCount).toBe(0);
    expect(Object.keys((await sync.readSnapshot()).snapshot.rows)).toHaveLength(
      0
    );
    expect(await sync.checkpointToken()).toBeUndefined();
    await sync.applyPullResponse({
      response: "reset",
      reason: "checkpoint_expired",
      checkpoint: {
        token: "binary-upgrade-checkpoint",
        cursor: { epoch: 1, sequence: 0 },
        scope: {
          identity: "all",
          authorizationRevision: 1,
          subscriptionRevision: 1,
        },
      },
      records: [
        {
          recordId: "upgrade:confirmed",
          state: {
            state: "present",
            value: { name: "Confirmed", counter: 9007199254740993n },
            version: 7,
            reference: null,
          },
        },
        {
          recordId: "upgrade:deleted",
          state: { state: "tombstone", version: 8 },
        },
      ],
    });
    for (const id of ["pending", "conflict"]) {
      await sync.enqueue({
        identity: { clientCommitId: id, fingerprint: id },
        operations: [
          {
            kind: "upsert",
            record_id: `upgrade:${id}`,
            base_version: "absent",
            value: { name: id },
            reference: null,
          },
        ],
      });
    }
    const pending = await sync.pending<{
      identity: { clientCommitId: string; fingerprint: string };
    }>();
    const conflict = pending.find(
      (item) => item.identity.clientCommitId === "conflict"
    );
    if (!conflict) throw new Error("missing seeded conflict commit");
    await sync.recordPushResponse({
      schemaVersion: "v1",
      partitionId: campaign,
      clientId: options.clientId,
      outcome: {
        status: "conflict",
        identity: conflict.identity,
        record_id: "upgrade:conflict",
        authoritative: { state: "absent" },
      },
    });
    expect((await sync.status()).pendingCount).toBe(1);
    expect((await sync.status()).conflictCount).toBe(1);
    await database.query(
      "CREATE upgrade_probe:marker SET receipt = $receipt RETURN NONE",
      { receipt: await receipt() }
    );
    console.log("BINARY_UPGRADE_SEEDED", campaign, await receipt());
    // Deliberately leave handles open: the app is cold-stopped before replacement.
  } else {
    expect(marker?.value).toHaveLength(1);
    expect(await receipt()).toBe(marker?.value?.[0]);
    expect((await sync.status()).pendingCount).toBe(1);
    expect((await sync.status()).conflictCount).toBe(1);
    console.log("BINARY_UPGRADE_VERIFIED", campaign, await receipt());
    await sync.close();
    await database.close();
  }
}

export async function runBinaryUpgradePhase(phase: "seed" | "verify") {
  try {
    await runPhase(phase);
  } catch (error) {
    throw new Error(`binary upgrade ${phase}: ${String(error)} ${json(error)}`);
  }
}
