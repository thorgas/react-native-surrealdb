import { describe, expect, it, vi } from "vitest";
import type { NativeSyncClientLike } from "../src/generated/surrealdb_rn_core";
import { ExperimentalSyncClient } from "../src/sync";
import { SyncReadScopeError, type SyncReadEvent } from "../src/sync-read";

const capabilities = {
  protocolVersion: 1,
  maxJournalBatches: 128,
  maxJournalBytes: 4194304,
  maxOutputBytes: 16777216,
  requiresMatchingNativeAbi: true,
  predicateSubscriptions: false,
};
const snapshot = {
  protocolVersion: 1,
  metadata: {
    revision: "9007199254740993",
    scope: {
      partitionId: "household",
      scopeIdentity: "member",
      subscriptionRevision: "1",
      authorizationRevision: null,
    },
    asOf: null,
    readiness: "local_available_awaiting_coverage",
    coverage: { kind: "unknown" },
  },
  rows: {},
};
const cursor = { processEpoch: "handle", revision: snapshot.metadata.revision };
function fixture() {
  const readMethods = {
    readCapabilitiesJson: vi.fn(() => JSON.stringify(capabilities)),
    readSnapshotJson: vi.fn(async () => JSON.stringify({ snapshot, cursor })),
    readChangesJson: vi.fn(async () =>
      JSON.stringify({ kind: "batches", batches: [], cursor }),
    ),
    requiresAuthorizedReset: vi.fn(async () => true),
    scopeTransitionDecision: vi.fn(async () => "needs_new_scope_journal"),
    applyPullResponse: vi.fn(async () => {
      throw Object.assign(new Error("new journal required"), {
        tag: "NeedsNewScopeJournal",
      });
    }),
    close: vi.fn(async () => undefined),
  };
  const native = readMethods as NativeSyncClientLike & typeof readMethods;
  return { native, client: new ExperimentalSyncClient(native) };
}
describe("native sync read facade", () => {
  it("requires the explicit protocol capability before calling read symbols", async () => {
    const old = new ExperimentalSyncClient({} as NativeSyncClientLike);
    await expect(old.readSnapshot()).rejects.toThrow(
      "matching rebuilt native binaries",
    );
    const { native, client } = fixture();
    native.readCapabilitiesJson.mockReturnValue(
      JSON.stringify({ ...capabilities, protocolVersion: 2 }),
    );
    await expect(client.readSnapshot()).rejects.toThrow(
      "Invalid sync read protocol",
    );
    expect(native.readSnapshotJson).not.toHaveBeenCalled();
  });
  it("preserves decimal revisions, forwards cancellation and checks batch limits", async () => {
    const { native, client } = fixture();
    const signal = new AbortController().signal;
    expect((await client.readSnapshot({ signal })).cursor).toEqual(cursor);
    await client.readChanges(cursor, 2, { signal });
    expect(native.readSnapshotJson).toHaveBeenCalledWith({ signal });
    expect(native.readChangesJson).toHaveBeenCalledWith(
      JSON.stringify(cursor),
      2,
      { signal },
    );
    await expect(client.readChanges(cursor, 129)).rejects.toThrow(
      "between 1 and 128",
    );
    expect(native.readChangesJson).toHaveBeenCalledOnce();
    expect((await client.readRecord("missing")).absence).toBe("unknown");
  });
  it("requests authorized bootstrap without clearing an old journal", async () => {
    const { native, client } = fixture();
    expect(await client.requiresAuthorizedReset()).toBe(true);
    expect(await client.scopeTransitionDecision("new-scope", 2n)).toBe(
      "needs_new_scope_journal",
    );
    expect(native.scopeTransitionDecision).toHaveBeenCalledWith(
      "new-scope",
      2n,
      undefined,
    );
    expect(native.applyPullResponse).not.toHaveBeenCalled();
  });
  it("withdraws reactive state and ignores an in-flight read after a scope rejection", async () => {
    const { native, client } = fixture();
    let resolve!: (value: string) => void;
    native.readSnapshotJson.mockImplementation(
      () =>
        new Promise<string>((done) => {
          resolve = done;
        }),
    );
    const store = client.createReadStore(),
      events: SyncReadEvent[] = [];
    store.watchCollection((event) => events.push(event));
    await expect(
      client.applyPullResponseProtocolJson("{}"),
    ).rejects.toBeInstanceOf(SyncReadScopeError);
    expect(events).toMatchObject([{ kind: "error", invalidated: true }]);
    resolve(JSON.stringify({ snapshot, cursor }));
    await Promise.resolve();
    await Promise.resolve();
    expect(store.getSnapshot()).toBeUndefined();
    expect(events).toHaveLength(1);
    expect(() => store.refresh()).toThrow(SyncReadScopeError);
    await client.close();
    expect(events.map((event) => event.kind)).toEqual(["error", "closed"]);
    expect(native.close).toHaveBeenCalledOnce();
  });
});
