import { describe, expect, test } from "react-native-harness";
import { connect, type WindowRequest } from "react-native-surrealdb";
import {
  encodeSubscriptionRequestJson,
  decodeSubscriptionResponse,
} from "../../../packages/react-native-surrealdb/src/native";
import { surrealkvChurnEndpoint } from "../surrealkv-churn-fixture";

// Canonical Rust subscription codec fixture, exported by
// sync_subscription::tests::subscription_codec_preserves_structural_i64_u64_and_payload_wire_tags.
const requestHex =
  "8378187375727265616c64622d737562736372697074696f6e2f316772657175657374a567636f6d6d616e64a6646b696e64657374617274656c696d6974026a67656e65726174696f6e016a706172616d6574657273a16573636f7265a2646b696e6463696e746576616c75653b7fffffffffffffff6c646566696e6974696f6e49646672616e6b65646e737562736372697074696f6e49646677696e646f7768636c69656e74496466636c69656e746b706172746974696f6e496469706172746974696f6e6e72657175657374656453636f706563616c6c74737562736372697074696f6e5265766973696f6e01";
const responseHex =
  "8378187375727265616c64622d737562736372697074696f6e2f3168726573706f6e7365ad6573636f7065a3686964656e7469747963616c6c74737562736372697074696f6e5265766973696f6e0175617574686f72697a6174696f6e5265766973696f6e0165746f6b656e6c726573756d652d746f6b656e676368616e67657382a263726f77a46576616c7565a2646e616d65616167636f756e7465721b002000000000000167736f72744b6579a2646b696e6463696e746576616c75653b7fffffffffffffff6776657273696f6e1bffffffffffffffff687265636f726449646770726f62653a61646b696e6466757073657274a263726f77a46576616c7565a1646e616d65616267736f72744b6579a2646b696e6463696e746576616c7565006776657273696f6e02687265636f726449646770726f62653a62646b696e646675707365727468636c69656e74496466636c69656e7468636f766572616765a5656c696d69740269646972656374696f6e69617363656e64696e67696f726465724b696e6463696e746a6f726465724669656c646573636f72656c646566696e6974696f6e49646672616e6b65646866726f6e74696572a26565706f6368016873657175656e63650068726573706f6e7365a2646b696e6468736e617073686f7466726561736f6e67696e697469616c6a67656e65726174696f6e016a6f726465726564496473826770726f62653a616770726f62653a626b706172746974696f6e496469706172746974696f6e6c766965775265766973696f6e016e737562736372697074696f6e49646677696e646f776f706c616e46696e6765727072696e7464706c616e";
const request: WindowRequest = {
  partitionId: "partition",
  clientId: "client",
  requestedScope: "all",
  subscriptionRevision: "1",
  command: {
    kind: "start",
    subscriptionId: "window",
    generation: "1",
    definitionId: "ranked",
    limit: 2,
    parameters: { score: { kind: "int", value: "-9223372036854775808" } },
  },
};
function bytes(hex: string): ArrayBuffer {
  return Uint8Array.from(hex.match(/../g) ?? [], (value) =>
    Number.parseInt(value, 16),
  ).buffer;
}
function hex(buffer: ArrayBuffer): string {
  return [...new Uint8Array(buffer)]
    .map((value) => value.toString(16).padStart(2, "0"))
    .join("");
}

describe("native authorized window protocol 1", () => {
  test("installs atomic windows and preserves local cancellation across SurrealKV database reopen", async () => {
    // A fresh app-private path per run; never clear or reuse user databases.
    const endpoint = `${surrealkvChurnEndpoint.slice(0, surrealkvChurnEndpoint.lastIndexOf("/"))}/sync-window-contract-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const databaseOptions = {
      endpoint,
      namespace: "window-contract",
      database: "window-contract",
    };
    const syncOptions = {
      partitionId: "partition",
      clientId: "client",
      requestedScope: "all",
      subscriptionRevision: 1n,
    };
    let database = await connect(databaseOptions);
    let sync = await database.openExperimentalSync(syncOptions);
    try {
      expect(sync.readCapabilities().predicateSubscriptions).toBe(false);
      const windows = sync.createWindowClient();
      expect(windows.capabilities().protocolVersion).toBe(1);
      expect(await sync.requiresAuthorizedReset()).toBe(true);
      const initial = await windows.snapshot("window");
      expect(initial.snapshot).toBe(null);
      expect(
        hex(await encodeSubscriptionRequestJson(JSON.stringify(request))),
      ).toBe(requestHex);
      const decoded = await decodeSubscriptionResponse(bytes(responseHex));
      const raw = JSON.parse(decoded);
      expect(raw.changes[0].row.version).toBe("18446744073709551615");
      expect(raw.changes[0].row.sortKey.value).toBe("-9223372036854775808");
      await windows.start(request);
      await sync.enqueue({
        identity: {
          clientCommitId: "outside-window",
          fingerprint: "outside-window-v1",
        },
        operations: [
          {
            kind: "upsert",
            record_id: "window_local:outside",
            base_version: "absent",
            value: { name: "pending" },
            reference: null,
          },
        ],
      });
      await windows.applyResponse(decoded);
      const installed = await windows.snapshot("window");
      expect(installed.snapshot?.rows.map((row) => row.recordId)).toEqual([
        "probe:a",
        "probe:b",
      ]);
      expect(installed.snapshot?.rows[0]?.value).toEqual({
        name: "a",
        counter: 9007199254740993n,
      });
      expect(installed.snapshot?.metadata.revision).toBe(
        installed.cursor.revision,
      );
      expect(installed.snapshot?.metadata.generation).toBe("1");
      expect(installed.snapshot?.pendingOutsideWindow).toBe(null);
      const changes = await windows.changes(initial.cursor, 8);
      expect(changes.kind).toBe("batches");
      if (changes.kind !== "batches")
        throw new Error("unexpected window reset");
      expect(changes.batches.length).toBe(3);
      expect(changes.batches[2]?.snapshot?.rows.length).toBe(2);
      expect(await sync.requiresAuthorizedReset()).toBe(true);
      expect((await windows.request("window"))?.command.kind).toBe("resume");
      await sync.close();
      await database.close();
      database = await connect(databaseOptions);
      sync = await database.openExperimentalSync(syncOptions);
      const reopened = sync.createWindowClient();
      expect((await reopened.snapshot("window")).snapshot).toEqual(
        installed.snapshot,
      );
      expect((await sync.status()).pendingCount).toBe(1);
      expect((await reopened.changes(initial.cursor, 8)).kind).toBe(
        "reset_required",
      );
      await reopened.cancel("window", 1n);
      await expect(reopened.applyResponse(decoded)).rejects.toThrow();
      const cancelled = await reopened.snapshot("window");
      expect(cancelled.snapshot).toBe(null);
      expect(cancelled.cancelledThrough).toBe("1");
      expect(cancelled.request?.command.kind).toBe("cancel");
      await sync.close();
      await database.close();
      database = await connect(databaseOptions);
      sync = await database.openExperimentalSync(syncOptions);
      const final = sync.createWindowClient();
      expect((await final.snapshot("window")).cancelledThrough).toBe("1");
      await expect(final.applyResponse(decoded)).rejects.toThrow();
      expect((await sync.status()).pendingCount).toBe(1);
    } finally {
      await sync.close();
      await database.close();
    }
  });
});
