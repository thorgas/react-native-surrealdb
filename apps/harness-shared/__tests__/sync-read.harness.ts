import { describe, expect, test } from "react-native-harness";
import { connect } from "react-native-surrealdb";

describe("native sync read protocol v1", () => {
  test("captures complete commits and preserves pending state across handle reopen", async () => {
    const database = await connect({
      endpoint: "mem://",
      namespace: "read-contract",
      database: "read-contract",
    });
    const options = {
      partitionId: "read-contract",
      clientId: "read-client",
      requestedScope: "all",
      subscriptionRevision: 1n,
    };
    const sync = await database.openExperimentalSync(options);
    let closed = false;
    try {
      expect(sync.readCapabilities().protocolVersion).toBe(1);
      expect(await sync.requiresAuthorizedReset()).toBe(true);
      const initial = await sync.readSnapshot();
      await sync.enqueue({
        identity: {
          clientCommitId: "atomic-read",
          fingerprint: "atomic-read-v1",
        },
        operations: ["a", "b"].map((id) => ({
          kind: "upsert" as const,
          record_id: `read_probe:${id}`,
          base_version: "absent" as const,
          value: { name: id, large: 9007199254740993n },
          reference: null,
        })),
      });
      const changes = await sync.readChanges(initial.cursor, 8);
      expect(changes.kind).toBe("batches");
      if (changes.kind !== "batches") throw new Error("unexpected read reset");
      expect(changes.batches.length).toBe(1);
      expect(changes.batches[0]?.changes.length).toBe(2);
      const installed = await sync.readSnapshot();
      expect(installed.cursor.revision).toBe(changes.cursor.revision);
      expect(Object.keys(installed.snapshot.rows).sort()).toEqual([
        "read_probe:a",
        "read_probe:b",
      ]);
      const optimistic = installed.snapshot.rows["read_probe:a"]?.optimistic;
      expect(optimistic?.state).toBe("present");
      if (optimistic?.state !== "present")
        throw new Error("missing pending row");
      expect(optimistic.value).toEqual({ name: "a", large: 9007199254740993n });
      await sync.close();
      closed = true;
      const reopened = await database.openExperimentalSync(options);
      try {
        const recovered = await reopened.readSnapshot();
        expect(recovered.snapshot).toEqual(installed.snapshot);
        expect(await reopened.requiresAuthorizedReset()).toBe(true);
        expect((await reopened.readChanges(initial.cursor, 8)).kind).toBe(
          "reset_required"
        );
        expect((await reopened.status()).pendingCount).toBe(1);
      } finally {
        await reopened.close();
      }
    } finally {
      if (!closed) await sync.close();
      await database.close();
    }
  });
});
