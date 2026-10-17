import { URL } from "node:url";
import { readFileSync } from "node:fs";
import { describe, expect, it, vi } from "vitest";
import {
  decodeWindowFrame,
  decodeWindowChanges,
  validateWindowRequest,
} from "../src/sync-window-codec";
import {
  ExperimentalSyncWindowClient,
  type WindowFrame,
  type WindowRequest,
} from "../src/sync-window";
import { encodeSurrealValue } from "../src/wire";
function request(): WindowRequest {
  return {
    partitionId: "house",
    clientId: "phone",
    requestedScope: "member",
    subscriptionRevision: "0",
    command: {
      kind: "start",
      subscriptionId: "recent",
      generation: "9007199254740993",
      definitionId: "by_priority",
      parameters: { priority: { kind: "int", value: "-9223372036854775808" } },
      limit: 2,
    },
  };
}
function frame(): WindowFrame {
  return {
    request: request(),
    cancelledThrough: null,
    cursor: {
      processEpoch: "handle",
      subscriptionId: "recent",
      revision: "9007199254740995",
    },
    snapshot: {
      metadata: {
        revision: "9007199254740995",
        subscriptionId: "recent",
        generation: "9007199254740993",
        planFingerprint: "trusted",
        scope: {
          identity: "member",
          authorizationRevision: "2",
          subscriptionRevision: "0",
        },
        frontier: { epoch: "1", sequence: "9" },
        viewRevision: "1",
        coverage: {
          definitionId: "by_priority",
          limit: 2,
          orderField: "priority",
          orderKind: "int",
          direction: "ascending",
        },
      },
      rows: [
        {
          recordId: "item:a",
          version: "18446744073709551615",
          sortKey: { kind: "int", value: "-9223372036854775808" },
          value: JSON.parse(
            encodeSurrealValue({
              large: 9007199254740993n,
              kind: "int",
              value: "user value",
            }),
          ),
        },
      ],
      pendingOutsideWindow: ["item:pending"],
    },
  };
}
describe("ordered window boundary", () => {
  it("preserves query keys and counters while decoding only user payload tags", () => {
    const result = decodeWindowFrame(JSON.stringify(frame()), "recent");
    expect(result.snapshot?.rows[0]?.value).toEqual({
      large: 9007199254740993n,
      kind: "int",
      value: "user value",
    });
    expect(result.snapshot?.rows[0]?.sortKey).toEqual({
      kind: "int",
      value: "-9223372036854775808",
    });
    expect(result.snapshot?.rows[0]?.version).toBe("18446744073709551615");
  });
  it.each(["-9223372036854775809", "9223372036854775808", "-0", "01", "1.0"])(
    "rejects invalid query integers %s",
    (value) => {
      const next = request();
      if (next.command.kind === "start")
        next.command.parameters = { x: { kind: "int", value } };
      expect(() => validateWindowRequest(next)).toThrow();
    },
  );
  it("rejects cancellation barrier reuse and correlated snapshot inconsistencies", () => {
    const value = frame();
    value.cancelledThrough = "9007199254740993";
    expect(() => decodeWindowFrame(JSON.stringify(value), "recent")).toThrow();
    value.cancelledThrough = null;
    value.snapshot!.metadata.revision = "1";
    expect(() => decodeWindowFrame(JSON.stringify(value), "recent")).toThrow();
  });
  it("preflights all batches and rejects identity changes", () => {
    const initial = frame();
    const { cursor, ...replacement } = initial;
    const good = {
      ...replacement,
      beforeRevision: "9007199254740994",
      revision: cursor.revision,
    };
    const envelope = { kind: "batches", cursor, batches: [good] };
    expect(
      decodeWindowChanges(
        JSON.stringify(envelope),
        { ...cursor, revision: "9007199254740994" },
        32,
      ).kind,
    ).toBe("batches");
    envelope.batches.push({
      ...good,
      beforeRevision: cursor.revision,
      revision: "9007199254740996",
    });
    expect(() =>
      decodeWindowChanges(
        JSON.stringify(envelope),
        { ...cursor, revision: "9007199254740994" },
        32,
      ),
    ).toThrow();
    expect(() =>
      decodeWindowChanges(
        JSON.stringify({
          ...envelope,
          batches: [],
          cursor: { ...cursor, processEpoch: "other" },
        }),
        cursor,
        32,
      ),
    ).toThrow();
  });
  it("represents cancelled and awaiting coverage states without inventing readiness", () => {
    const value = frame();
    value.snapshot = null;
    value.request = null;
    value.cancelledThrough = "9007199254740993";
    expect(
      decodeWindowFrame(JSON.stringify(value), "recent").snapshot,
    ).toBeNull();
    value.cancelledThrough = null;
    value.request = request();
    expect(
      decodeWindowFrame(JSON.stringify(value), "recent").snapshot,
    ).toBeNull();
  });
  it("forwards once and rejects late completion after close", async () => {
    let finish!: () => void;
    const native = {
      subscriptionCapabilitiesJson: () =>
        JSON.stringify({
          protocolVersion: 1,
          maxSubscriptions: 32,
          maxWindowRows: 32,
          maxJournalBatches: 128,
          maxJournalBytes: 1048576,
          maxAggregateJournalBytes: 16777216,
          maxOutputBytes: 16777216,
          requiresMatchingNativeAbi: true,
        }),
      startSubscriptionJson: vi.fn(
        () =>
          new Promise<void>((resolve) => {
            finish = resolve;
          }),
      ),
    };
    const client = new ExperimentalSyncWindowClient(native);
    const operation = client.start(request());
    client.close();
    finish();
    await expect(operation).rejects.toThrow("closed");
    expect(native.startSubscriptionJson).toHaveBeenCalledTimes(1);
    await expect(client.start(request())).rejects.toThrow("closed");
  });
  it("rejects old native ABI before mutations", async () => {
    const start = vi.fn();
    const client = new ExperimentalSyncWindowClient({
      startSubscriptionJson: start,
    });
    await expect(client.start(request())).rejects.toThrow("matching rebuilt");
    expect(start).not.toHaveBeenCalled();
  });
});

describe("bounded subscription HTTP", () => {
  const media = "application/vnd.surrealdb-subscription.v1+cbor";
  async function setup(
    status = 200,
    headers: Record<string, string> = { "Content-Type": media },
  ) {
    const { experimentalCanonicalSubscriptionHttpTransport } =
      await import("../src/sync-http");
    const native = {
      encodeSubscriptionRequestJson: vi.fn(
        async () => new Uint8Array([1]).buffer,
      ),
      decodeSubscriptionResponse: vi.fn(async () => '{"decoded":true}'),
    };
    const fetcher = vi.fn<typeof fetch>(
      async () => new Response(new Uint8Array([2]), { status, headers }),
    );
    const transport = experimentalCanonicalSubscriptionHttpTransport({
      baseUrl: "http://127.0.0.1:18092",
      allowInsecureLocalhost: true,
      native,
      accessToken: () => "fixture",
      fetch: fetcher,
    });
    return { transport, native, fetcher };
  }
  it("uses the separate route and canonical codec exactly once", async () => {
    const { transport, native, fetcher } = await setup();
    expect(await transport.exchange(request())).toBe('{"decoded":true}');
    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(fetcher.mock.calls[0]?.[0]).toBe(
      "http://127.0.0.1:18092/v1/sync/subscriptions",
    );
    expect(native.decodeSubscriptionResponse).toHaveBeenCalledTimes(1);
  });
  it.each([401, 403, 409, 410, 413])(
    "does not retry status %s",
    async (status) => {
      const { transport, native, fetcher } = await setup(status);
      await expect(transport.exchange(request())).rejects.toMatchObject({
        kind: "http",
        status,
      });
      expect(fetcher).toHaveBeenCalledTimes(1);
      expect(native.decodeSubscriptionResponse).not.toHaveBeenCalled();
    },
  );
  it("rejects oversized and wrong media responses before decoding", async () => {
    const wrong = await setup(200, { "Content-Type": "application/json" });
    await expect(wrong.transport.exchange(request())).rejects.toMatchObject({
      kind: "content_type",
    });
    expect(wrong.native.decodeSubscriptionResponse).not.toHaveBeenCalled();
    const large = await setup(200, {
      "Content-Type": media,
      "Content-Length": "262145",
    });
    await expect(large.transport.exchange(request())).rejects.toMatchObject({
      kind: "body_limit",
    });
    expect(large.native.decodeSubscriptionResponse).not.toHaveBeenCalled();
  });
  it("aborts before encoding and bounds stalled token acquisition", async () => {
    const { transport, native } = await setup();
    const controller = new AbortController();
    controller.abort();
    await expect(
      transport.exchange(request(), { signal: controller.signal }),
    ).rejects.toMatchObject({ kind: "aborted" });
    expect(native.encodeSubscriptionRequestJson).not.toHaveBeenCalled();
    const { experimentalCanonicalSubscriptionHttpTransport } =
      await import("../src/sync-http");
    const fetcher = vi.fn();
    const timed = experimentalCanonicalSubscriptionHttpTransport({
      baseUrl: "https://example.test",
      native,
      accessToken: () => new Promise<string>(() => {}),
      fetch: fetcher,
      requestTimeoutMs: 5,
    });
    await expect(timed.exchange(request())).rejects.toMatchObject({
      kind: "timeout",
    });
    expect(fetcher).not.toHaveBeenCalled();
  });
});

describe("committed window observation", () => {
  it("withdraws on reentrant invalidation before later observers see old rows", async () => {
    const { ExperimentalSyncWindowStore } =
      await import("../src/sync-window-store");
    const source = {
      snapshot: vi.fn(async () => frame()),
      changes: vi.fn(() => new Promise<never>(() => {})),
    };
    const store = new ExperimentalSyncWindowStore(source, "recent");
    const seen: string[] = [];
    store.subscribe((event) => {
      if (event.kind === "frame") store.invalidateScope(new Error("revoked"));
    });
    store.subscribe((event) => seen.push(event.kind));
    await vi.waitFor(() => expect(seen).toEqual(["error"]));
    expect(store.getSnapshot()).toBeUndefined();
    expect(source.changes).not.toHaveBeenCalled();
    store.close();
  });
  it("isolates observer errors and discards late native reads after close", async () => {
    const { ExperimentalSyncWindowStore } =
      await import("../src/sync-window-store");
    let finish!: (value: import("../src/sync-window").WindowChanges) => void;
    const source = {
      snapshot: vi.fn(async () => frame()),
      changes: vi.fn(
        () =>
          new Promise<import("../src/sync-window").WindowChanges>((resolve) => {
            finish = resolve;
          }),
      ),
    };
    const store = new ExperimentalSyncWindowStore(source, "recent");
    const seen: string[] = [];
    store.subscribe(() => {
      throw new Error("observer failed");
    });
    store.subscribe((event) => seen.push(event.kind));
    await vi.waitFor(() => expect(source.changes).toHaveBeenCalledTimes(1));
    store.close();
    finish({ kind: "reset_required", ...frame() });
    await Promise.resolve();
    await Promise.resolve();
    expect(seen).toEqual(["frame", "closed"]);
    expect(store.getSnapshot()).toBeUndefined();
    expect(store.observerErrors).toBe(2);
  });
});

it("decodes the actual native SurrealKV fixture through the complete TypeScript boundary", () => {
  // Emitted by subscription_native_atomic_cancel_reopen_and_scope_barriers; UUIDs normalized only.
  const fixture = JSON.parse(
    readFileSync(
      new URL("./fixtures/sync-window-native.json", import.meta.url),
      "utf8",
    ),
  );
  const installed = decodeWindowFrame(
    JSON.stringify(fixture.installed),
    "window",
  );
  expect(installed.snapshot?.rows[0]?.value).toEqual({
    counter: 9007199254740993n,
    name: "a",
  });
  expect(installed.snapshot?.rows[0]?.version).toBe("18446744073709551615");
  expect(installed.snapshot?.pendingOutsideWindow).toBeNull();
  expect(installed.request?.command.kind).toBe("resume");
  const changes = decodeWindowChanges(
    JSON.stringify(fixture.changes),
    { ...fixture.changes.cursor, revision: "1" },
    32,
  );
  expect(changes.kind === "batches" && changes.batches.length).toBe(2);
  const cancelled = decodeWindowFrame(
    JSON.stringify(fixture.cancelled),
    "window",
  );
  expect(cancelled.cancelledThrough).toBe("1");
  expect(cancelled.request?.command.kind).toBe("cancel");
  expect(cancelled.snapshot).toBeNull();
});

it("propagates a scope barrier to every factory client including clients created afterward", async () => {
  const { ExperimentalSyncClient } = await import("../src/sync");
  const native = {
    subscriptionCapabilitiesJson: () =>
      JSON.stringify({
        protocolVersion: 1,
        maxSubscriptions: 32,
        maxWindowRows: 32,
        maxJournalBatches: 128,
        maxJournalBytes: 1048576,
        maxAggregateJournalBytes: 16777216,
        maxOutputBytes: 16777216,
        requiresMatchingNativeAbi: true,
      }),
    subscriptionSnapshotJson: vi.fn(async () => JSON.stringify(frame())),
    applySubscriptionResponseJson: vi.fn(async () => {
      throw { tag: "PendingQuarantineRequired" };
    }),
    cancelSubscription: vi.fn(async () => ({
      revision: 4n,
      pendingCount: 1,
      outcomeCount: 0,
      conflictCount: 0,
    })),
    close: vi.fn(async () => {}),
  };
  const sync = new ExperimentalSyncClient(
    native as unknown as import("../src/generated/surrealdb_rn_core").NativeSyncClientLike,
  );
  const first = sync.createWindowClient(),
    second = sync.createWindowClient();
  await expect(first.applyResponse("{}")).rejects.toMatchObject({
    reason: "pending_quarantine_required",
  });
  await expect(second.snapshot("recent")).rejects.toMatchObject({
    reason: "pending_quarantine_required",
  });
  await expect(
    sync.createWindowClient().snapshot("recent"),
  ).rejects.toMatchObject({ reason: "pending_quarantine_required" });
  await expect(sync.readSnapshot()).rejects.toMatchObject({
    reason: "pending_quarantine_required",
  });
  expect(native.subscriptionSnapshotJson).not.toHaveBeenCalled();
  await expect(second.cancel("recent", 1n)).resolves.toMatchObject({
    pendingCount: 1,
  });
  expect(native.cancelSubscription).toHaveBeenCalledTimes(1);
  await sync.close();
  await expect(second.cancel("recent", 2n)).rejects.toThrow("closed");
});

it("does not restart native reads when an immediate observer closes the store", async () => {
  const { ExperimentalSyncWindowStore } =
    await import("../src/sync-window-store");
  const source = {
    snapshot: vi.fn(async () => frame()),
    changes: vi.fn(() => new Promise<never>(() => {})),
  };
  const store = new ExperimentalSyncWindowStore(source, "recent");
  store.subscribe(() => {});
  await vi.waitFor(() => expect(source.changes).toHaveBeenCalledTimes(1));
  store.subscribe((event) => {
    if (event.kind === "frame") store.close();
  });
  await Promise.resolve();
  expect(source.snapshot).toHaveBeenCalledTimes(1);
  expect(store.getSnapshot()).toBeUndefined();
});
