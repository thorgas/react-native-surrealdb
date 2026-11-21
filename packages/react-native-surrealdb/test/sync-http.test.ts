import { createNativeReadMocks } from "./helpers/native-read-mocks";
import { describe, expect, it, vi } from "vitest";

import type {
  NativeSyncClientLike,
  NativeSyncStatus,
} from "../src/generated/surrealdb_rn_core";
import { NativeConflictPolicy } from "../src/generated/surrealdb_rn_core";
import {
  ExperimentalSyncHttpAdapter,
  ExperimentalSyncHttpError,
  experimentalCanonicalCborSyncHttpCodec,
  experimentalJsonSyncHttpCodec,
  type ExperimentalSyncHttpCodec,
  type ExperimentalSyncHttpLimits,
  type ExperimentalSyncHttpNativeBridge,
  type ExperimentalSyncHttpOptions,
} from "../src/sync-http";
import { ExperimentalSyncClient } from "../src/sync";

const status: NativeSyncStatus = {
  revision: 1n,
  pendingCount: 1,
  outcomeCount: 0,
  conflictCount: 0,
};

const commit = {
  identity: {
    clientCommitId: "commit-1",
    fingerprint: "fingerprint-1",
  },
  operations: [],
} as const;

function createSync(laneCapable = false) {
  const native = {
    ...(laneCapable ? {
      laneCapabilitiesJson: vi.fn(() => JSON.stringify({ schemaVersion: 2, storageFormat: 5, pullSourceProfile: "pull-source-v2" })),
      observeRecordJson: vi.fn(async () => "{}"),
      enqueueIntentJson: vi.fn(async () => status),
      materializeNextIntent: vi.fn(async () => status),
      laneConflictSnapshotJson: vi.fn(async () => undefined),
      laneResolutionObservationJson: vi.fn(async () => JSON.stringify({ phase: "unknown" })),
      laneResolutionReceiptJson: vi.fn(async () => JSON.stringify({ status: "unknown" })),
      acknowledgeLaneResolutionReceipt: vi.fn(async () => status),
      readIntentRecordJson: vi.fn(async (_recordId: string): Promise<string> => { throw new Error("Intent read fixture not configured"); }),
      readIntentLanesJson: vi.fn(async (): Promise<string> => { throw new Error("Intent lane fixture not configured"); }),
      prepareLaneResolutionJson: vi.fn(async (_snapshot: string, _choice: string) => ({ resolutionId: "stable", requestJson: '{"replacement_identity":{"sequence":18446744073709551615}}' })),
      projectLaneConflictSnapshotJson: vi.fn(() => JSON.stringify({
        recordId: "recipe:one", coveredIntentPrefixJson: '["intent-1"]',
        localJson: JSON.stringify({ title: "local" }), remoteJson: JSON.stringify({ title: "remote" }), observedJson: null,
      })),
      inspectLaneResolutionJson: vi.fn(async (_snapshot: string, _resolutionId?: string, _requestJson?: string) => JSON.stringify({ state: "absent", receiptJson: null })),
      materializeNextIntentWithCommitId: vi.fn(async () => status),
      resolveLaneConflictJson: vi.fn(async () => status),
    } : {}),
    ...createNativeReadMocks(),
    readCapabilitiesJson: vi.fn(() =>
      JSON.stringify({
        protocolVersion: 1,
        maxJournalBatches: 128,
        maxJournalBytes: 4194304,
        maxOutputBytes: 16777216,
        requiresMatchingNativeAbi: true,
        predicateSubscriptions: false,
      }),
    ),
    applyPullResponse: vi.fn(async () => status),
    checkpointToken: vi.fn(async () => "checkpoint-0"),
    close: vi.fn(async () => undefined),
    conflictsJson: vi.fn(async () => []),
    enqueue: vi.fn(async () => status),
    isClosed: vi.fn(() => false),
    pendingJson: vi.fn(async () => [JSON.stringify(commit)]),
    recordPushResponse: vi.fn(async () => status),
    recordPushResponseWithPolicy: vi.fn(async () => status),
    resolveConflictKeepLocal: vi.fn(async () => status),
    resolveConflictKeepServer: vi.fn(async () => status),
    resolveConflictMerge: vi.fn(async () => status),
    status: vi.fn(async () => status),
    diagnostics: vi.fn(() => ({
      schemaVersion: 1,
      spanPublishDrops: 0n,
      runtimeLockAcquisitions: 0n,
      runtimeLockWaitNs: 0n,
      runtimeLockHoldNs: 0n,
      storageAttempts: 0n,
      storageNs: 0n,
      storageErrors: 0n,
      stateEncodingAttempts: 0n,
      stateEncodingNs: 0n,
    })),
  };
  return {
    native,
    sync: new ExperimentalSyncClient(native as NativeSyncClientLike),
  };
}

function response(
  value: unknown,
  statusCode = 200,
  contentType = "application/json",
): Response {
  return new Response(JSON.stringify(value), {
    headers: { "Content-Type": contentType },
    status: statusCode,
  });
}

function pullResponse(checkpoint = "checkpoint-1") {
  return {
    response: "reset",
    reason: "checkpoint_expired",
    checkpoint: {
      token: checkpoint,
      cursor: { epoch: 1, sequence: 1 },
      scope: {
        identity: "all",
        authorizationRevision: 1,
        subscriptionRevision: 7,
      },
    },
    records: [],
  };
}

function createAdapter(
  fetch: typeof globalThis.fetch,
  codec: ExperimentalSyncHttpCodec = experimentalJsonSyncHttpCodec,
  limits?: Partial<ExperimentalSyncHttpLimits>,
  accessToken: ExperimentalSyncHttpOptions["accessToken"] = async () =>
    "redacted-test-token",
) {
  const { native, sync } = createSync();
  const adapter = new ExperimentalSyncHttpAdapter({
    sync,
    baseUrl: "https://sync.example.test/",
    partitionId: "partition-1",
    clientId: "client-1",
    requestedScope: "all",
    subscriptionRevision: 7n,
    accessToken,
    codec,
    fetch,
    limits,
  });
  return { adapter, native };
}

describe("ExperimentalSyncHttpAdapter", () => {
  const v1 = "application/vnd.surrealdb-sync.v1+cbor";
  const sourceV2 = "application/vnd.surrealdb-sync.pull-source-v2+cbor";
  const sourceCodec: ExperimentalSyncHttpCodec = {
    mediaType: v1,
    encodePushRequest: vi.fn(async () => new ArrayBuffer(0)),
    decodePushResponse: vi.fn(async () => "{}"),
    encodePullRequest: vi.fn(async () => new ArrayBuffer(1)),
    decodePullResponse: vi.fn(async () => "legacy"),
    decodePullResponseWithSource: vi.fn(async () => JSON.stringify(pullResponse())),
  };

  it("uses exact source-v2 route and headers while keeping the request v1", async () => {
    const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
      new Response(new Uint8Array(1), { headers: { "Content-Type": sourceV2 } }),
    );
    const { native, sync } = createSync(true);
    const adapter = new ExperimentalSyncHttpAdapter({
      sync, baseUrl: "https://sync.example.test", partitionId: "p", clientId: "c",
      requestedScope: "all", subscriptionRevision: 7n, accessToken: () => "token",
      codec: sourceCodec, fetch, pullSourceProof: true,
    });
    await adapter.pull();
    expect(fetch.mock.calls[0]?.[0]).toBe("https://sync.example.test/v1/sync/pull/source-v2");
    const headers = fetch.mock.calls[0]?.[1]?.headers as Record<string, string>;
    expect(headers.Accept).toBe(sourceV2);
    expect(headers["Content-Type"]).toBe(v1);
    expect(sourceCodec.decodePullResponseWithSource).toHaveBeenCalled();
    expect(sourceCodec.decodePullResponse).not.toHaveBeenCalled();
    expect(native.pendingJson).not.toHaveBeenCalled();
    await sync.resolveLaneConflict("resolution-1", "opaque-snapshot", { strategy: "keep_server" });
    expect(native.resolveLaneConflictJson).toHaveBeenCalledWith(
      "resolution-1", "opaque-snapshot", JSON.stringify({ strategy: "keep_server" }), undefined,
    );
    await expect(sync.laneResolutionReceipt("resolution-1"))
      .resolves.toBe(JSON.stringify({ status: "unknown" }));
    await sync.acknowledgeLaneResolutionReceipt("resolution-1");
    expect(native.acknowledgeLaneResolutionReceipt).toHaveBeenCalledWith("resolution-1", undefined);
  });

  it("decodes one native intent revision with opaque high-precision counters", async () => {
    const { native, sync } = createSync(true);
    new ExperimentalSyncHttpAdapter({
      sync, baseUrl: "https://sync.example.test", partitionId: "p", clientId: "c",
      requestedScope: "all", subscriptionRevision: 7n, accessToken: () => "token",
      codec: sourceCodec, pullSourceProof: true,
    });
    native.readIntentRecordJson?.mockResolvedValue(JSON.stringify({
      viewJson: JSON.stringify({
        snapshot: {
          protocolVersion: 1,
          metadata: {
            revision: "9007199254740993",
            scope: { partitionId: "p", scopeIdentity: "all", subscriptionRevision: "7", authorizationRevision: null },
            asOf: null, readiness: "local_available_awaiting_coverage", coverage: { kind: "unknown" },
          },
          rows: {},
        },
        cursor: { processEpoch: "test", revision: "9007199254740993" },
      }),
      lane: {
        pendingIntentIds: ["intent-1"], blockedSnapshotJson: "opaque",
        blockedReason: "conflict", headIdentityJson: null,
        resolutionRevision: "9007199254740995",
      },
    }));
    const result = await sync.readIntentRecord("recipe:one");
    expect(result.record.metadata.revision).toBe("9007199254740993");
    expect(result.lane.resolutionRevision).toBe("9007199254740995");
    expect(result.lane.pendingIntentIds).toEqual(["intent-1"]);
    expect(result.lane.blockedReason).toBe("conflict");
    expect(native.readIntentRecordJson).toHaveBeenCalledWith("recipe:one", undefined);
  });

  it("enumerates durable lanes without projecting their record rows or losing counters", async () => {
    const { native, sync } = createSync(true);
    new ExperimentalSyncHttpAdapter({
      sync, baseUrl: "https://sync.example.test", partitionId: "p", clientId: "c",
      requestedScope: "all", subscriptionRevision: 7n, accessToken: () => "token",
      codec: sourceCodec, pullSourceProof: true,
    });
    const encoded = JSON.stringify({
      revision: "9007199254740993",
      cursor: { processEpoch: "process:1", revision: "9007199254740993" },
      lanes: [
        { recordId: "note:created-then-deleted", pendingIntentIds: ["create", "delete"],
          headIdentityJson: null, blockedSnapshotJson: null, blockedReason: null,
          resolutionRevision: "9007199254740995" },
        { recordId: "note:blocked", pendingIntentIds: ["edit"],
          headIdentityJson: '{"clientCommitId":"opaque"}', blockedSnapshotJson: '{"version":18446744073709551615}',
          blockedReason: "conflict", resolutionRevision: "9007199254740996" },
      ],
    });
    native.readIntentLanesJson?.mockResolvedValue(encoded);
    const result = await sync.readIntentLanes();
    expect(result.revision).toBe("9007199254740993");
    expect(result.cursor).toEqual({ processEpoch: "process:1", revision: "9007199254740993" });
    expect(result.lanes.map((lane) => lane.recordId)).toEqual(["note:created-then-deleted", "note:blocked"]);
    expect(result.lanes[1]?.blockedSnapshotJson).toBe('{"version":18446744073709551615}');
    expect(native.readIntentLanesJson).toHaveBeenCalledWith(undefined);
    native.readIntentLanesJson?.mockResolvedValue(encoded.replace('"9007199254740993"', '9007199254740993'));
    await expect(sync.readIntentLanes()).rejects.toThrow("Invalid native intent lane snapshot");
  });

  it("prepares and replays opaque lane requests without parsing protocol counters", async () => {
    const { native, sync } = createSync(true);
    new ExperimentalSyncHttpAdapter({
      sync, baseUrl: "https://sync.example.test", partitionId: "p", clientId: "c",
      requestedScope: "all", subscriptionRevision: 7n, accessToken: () => "token",
      codec: sourceCodec, pullSourceProof: true,
    });
    const snapshot = '{"remote_state":{"version":18446744073709551615}}';
    const choice = { strategy: "merge" as const, value: { count: 9007199254740993n } };
    const prepared = await sync.prepareLaneResolution(snapshot, choice);
    expect(prepared.resolutionId).toBe("stable");
    expect(native.prepareLaneResolutionJson).toHaveBeenCalledWith(snapshot, expect.any(String), undefined);
    expect(native.prepareLaneResolutionJson?.mock.calls[0]?.[1]).toContain("9007199254740993");
    await sync.submitPreparedLaneResolution(prepared.resolutionId, snapshot, prepared.requestJson);
    expect(native.resolveLaneConflictJson).toHaveBeenCalledWith("stable", snapshot, prepared.requestJson, undefined);
    const receiptJson = '{"accepted":{"sequence":18446744073709551615}}';
    native.inspectLaneResolutionJson?.mockResolvedValue(JSON.stringify({ state: "awaiting_pull", receiptJson }));
    const inspected = await sync.inspectLaneResolution(snapshot, prepared);
    expect(inspected).toEqual({ state: "awaiting_pull", receiptJson });
    expect(native.inspectLaneResolutionJson).toHaveBeenCalledWith(snapshot, "stable", prepared.requestJson, undefined);
    expect(sync.projectLaneConflictSnapshot(snapshot)).toMatchObject({
      recordId: "recipe:one", coveredIntentPrefixJson: '["intent-1"]',
      local: { title: "local" }, remote: { title: "remote" }, observed: null,
    });
  });

  it("distinguishes supported absence from unsupported or invalid receipt projections", async () => {
    const { native, sync } = createSync(true);
    const snapshot = '{"remote_state":{"version":9007199254740993}}';
    const attempt = { resolutionId: "stable", requestJson: '{"strategy":"keep_server"}' };
    await expect(sync.inspectLaneResolution(snapshot, attempt)).rejects.toThrow(/matching native source-proof bindings/);
    expect(native.inspectLaneResolutionJson).not.toHaveBeenCalled();
    new ExperimentalSyncHttpAdapter({
      sync, baseUrl: "https://sync.example.test", partitionId: "p", clientId: "c",
      requestedScope: "all", subscriptionRevision: 7n, accessToken: () => "token",
      codec: sourceCodec, pullSourceProof: true,
    });
    await expect(sync.inspectLaneResolution(snapshot, attempt))
      .resolves.toEqual({ state: "absent", receiptJson: null });
    native.inspectLaneResolutionJson?.mockResolvedValueOnce(JSON.stringify({ state: "unknown", receiptJson: null }));
    await expect(sync.inspectLaneResolution(snapshot, attempt))
      .resolves.toEqual({ state: "unknown", receiptJson: null });
    native.inspectLaneResolutionJson?.mockResolvedValueOnce(JSON.stringify({ state: "absent", receiptJson: "{}" }));
    await expect(sync.inspectLaneResolution(snapshot, attempt)).rejects.toThrow(/Invalid native lane receipt projection/);
    native.inspectLaneResolutionJson?.mockRejectedValueOnce(new Error("native read failed"));
    await expect(sync.inspectLaneResolution(snapshot, attempt)).rejects.toThrow("native read failed");
    expect(native.resolveLaneConflictJson).not.toHaveBeenCalled();
  });

  it.each(["", v1, "application/vnd.surrealdb-sync.v2+cbor", "application/json"])(
    "rejects wrong source response profile %s without changing pending state", async (contentType) => {
      const fetch = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
        new Response(new Uint8Array(1), { headers: contentType ? { "Content-Type": contentType } : {} }),
      );
      const { native, sync } = createSync(true);
      const adapter = new ExperimentalSyncHttpAdapter({
        sync, baseUrl: "https://sync.example.test", partitionId: "p", clientId: "c",
        requestedScope: "all", subscriptionRevision: 7n, accessToken: () => "token",
        codec: sourceCodec, fetch, pullSourceProof: true,
      });
      await expect(adapter.pull()).rejects.toMatchObject({ kind: "content_type" });
      expect(native.applyPullResponse).not.toHaveBeenCalled();
      expect(native.pendingJson).not.toHaveBeenCalled();
    },
  );

  it("refuses source-proof opt-in without decoder and leaves legacy v1 as default", async () => {
    const { sync } = createSync();
    expect(() => new ExperimentalSyncHttpAdapter({
      sync, baseUrl: "https://sync.example.test", partitionId: "p", clientId: "c",
      requestedScope: "all", subscriptionRevision: 7n, accessToken: () => "token",
      codec: { ...sourceCodec, decodePullResponseWithSource: undefined }, pullSourceProof: true,
    })).toThrow(/source-v2 CBOR decoder/);
    expect(() => new ExperimentalSyncHttpAdapter({
      sync, baseUrl: "https://sync.example.test", partitionId: "p", clientId: "c",
      requestedScope: "all", subscriptionRevision: 7n, accessToken: () => "token",
      codec: { ...sourceCodec, mediaType: "application/vnd.surrealdb-sync.v2+cbor" },
      pullSourceProof: true,
    })).toThrow(/source-v2 CBOR decoder/);
    await expect(sync.enqueueIntent({ id: "x" }, "{}"))
      .rejects.toThrow(/source-proof pull capability/);
  });

  it("refuses old native artifacts and mismatched lane storage before queue mutation", () => {
    const old = createSync();
    const options = {
      baseUrl: "https://sync.example.test", partitionId: "p", clientId: "c",
      requestedScope: "all", subscriptionRevision: 7n, accessToken: () => "token",
      codec: sourceCodec, pullSourceProof: true,
    };
    expect(() => new ExperimentalSyncHttpAdapter({ ...options, sync: old.sync }))
      .toThrow(/matching rebuilt native source-proof bindings/);
    expect(old.native.enqueue).not.toHaveBeenCalled();
    const wrong = createSync(true);
    wrong.native.laneCapabilitiesJson?.mockReturnValue(JSON.stringify({
      schemaVersion: 2, storageFormat: 3, pullSourceProfile: "pull-source-v2",
    }));
    expect(() => new ExperimentalSyncHttpAdapter({ ...options, sync: wrong.sync }))
      .toThrow(/matching rebuilt native source-proof bindings/);
    expect(wrong.native.enqueue).not.toHaveBeenCalled();
    const withoutEnumeration = createSync(true);
    delete withoutEnumeration.native.readIntentLanesJson;
    expect(() => new ExperimentalSyncHttpAdapter({ ...options, sync: withoutEnumeration.sync }))
      .toThrow(/matching rebuilt native source-proof bindings/);
    expect(withoutEnumeration.native.enqueueIntentJson).not.toHaveBeenCalled();
  });
  it("bootstraps legacy read coverage without deleting or dispatching pending intent", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValue(response(pullResponse()));
    const { adapter, native } = createAdapter(fetch);
    await adapter.bootstrapReadCoverage();
    expect(native.checkpointToken).not.toHaveBeenCalled();
    expect(native.pendingJson).not.toHaveBeenCalled();
    expect(native.enqueue).not.toHaveBeenCalled();
    expect(native.close).not.toHaveBeenCalled();
    const body = JSON.parse(String(fetch.mock.calls[0]?.[1]?.body));
    expect(body).toMatchObject({
      checkpoint: null,
      requestedScope: "all",
      subscriptionRevision: 7,
    });
    expect(native.applyPullResponse).toHaveBeenCalledOnce();
  });

  it("keeps manual as the default and opts in only through explicit configuration", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValue(response({ outcome: { status: "conflict" } }));
    const { native, sync } = createSync();
    const adapter = new ExperimentalSyncHttpAdapter({
      sync,
      conflictPolicy: "preferServerV1",
      baseUrl: "https://sync.example.test",
      partitionId: "partition-1",
      clientId: "client-1",
      requestedScope: "all",
      subscriptionRevision: 7n,
      accessToken: () => "redacted-test-token",
      codec: experimentalJsonSyncHttpCodec,
      fetch,
    });
    await adapter.push();
    expect(native.recordPushResponse).not.toHaveBeenCalled();
    expect(native.recordPushResponseWithPolicy).toHaveBeenCalledWith(
      expect.any(String),
      NativeConflictPolicy.PreferServerV1,
      undefined,
    );
  });
  it("pushes pending commits then pulls from the durable checkpoint", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValueOnce(
        response({
          schemaVersion: "v1",
          partitionId: "partition-1",
          clientId: "client-1",
          outcome: { status: "accepted" },
        }),
      )
      .mockResolvedValueOnce(response(pullResponse()));
    const { adapter, native } = createAdapter(fetch);
    const signal = new AbortController().signal;

    await expect(adapter.syncOnce({ signal })).resolves.toEqual({
      push: [status],
      pull: status,
    });

    expect(fetch).toHaveBeenCalledTimes(2);
    const [pushUrl, pushInit] = fetch.mock.calls[0]!;
    expect(pushUrl).toBe("https://sync.example.test/v1/sync/push");
    expect(pushInit).toMatchObject({
      method: "POST",
      signal,
      headers: {
        Accept: "application/json",
        Authorization: "Bearer redacted-test-token",
        "Content-Type": "application/json",
      },
    });
    expect(JSON.parse(pushInit?.body as string)).toEqual({
      schemaVersion: "v1",
      partitionId: "partition-1",
      clientId: "client-1",
      commit,
    });
    const [pullUrl, pullInit] = fetch.mock.calls[1]!;
    expect(pullUrl).toBe("https://sync.example.test/v1/sync/pull");
    expect(JSON.parse(pullInit?.body as string)).toEqual({
      schemaVersion: "v1",
      partitionId: "partition-1",
      clientId: "client-1",
      checkpoint: "checkpoint-0",
      requestedScope: "all",
      subscriptionRevision: 7,
    });
    expect(native.recordPushResponse).toHaveBeenCalledOnce();
    expect(native.applyPullResponse).toHaveBeenCalledOnce();
    expect(native.checkpointToken).toHaveBeenCalledOnce();
  });

  it.each([400, 401, 403])(
    "does not apply an HTTP %s response to native state",
    async (statusCode) => {
      const fetch = vi
        .fn<typeof globalThis.fetch>()
        .mockResolvedValue(response({ error: "redacted" }, statusCode));
      const { adapter, native } = createAdapter(fetch);

      await expect(adapter.push()).rejects.toEqual(
        expect.objectContaining<Partial<ExperimentalSyncHttpError>>({
          name: "ExperimentalSyncHttpError",
          status: statusCode,
        }),
      );
      expect(native.recordPushResponse).not.toHaveBeenCalled();
    },
  );

  it("leaves a lost push response pending and permits an explicit retry", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockRejectedValueOnce(new TypeError("network lost"))
      .mockResolvedValueOnce(
        response({
          schemaVersion: "v1",
          partitionId: "partition-1",
          clientId: "client-1",
          outcome: { status: "accepted" },
        }),
      );
    const { adapter, native } = createAdapter(fetch);

    await expect(adapter.push()).rejects.toMatchObject({ kind: "network" });
    expect(native.recordPushResponse).not.toHaveBeenCalled();
    await expect(adapter.push()).resolves.toEqual([status]);
    expect(native.recordPushResponse).toHaveBeenCalledOnce();
  });

  it("uses an injected codec and serializes concurrent operations", async () => {
    let releaseFirst: (() => void) | undefined;
    const firstResponse = new Promise<Response>((resolve) => {
      releaseFirst = () =>
        resolve(
          response(
            { status: "accepted" },
            200,
            "application/vnd.surrealdb.sync.test",
          ),
        );
    });
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockReturnValueOnce(firstResponse)
      .mockResolvedValueOnce(
        response(pullResponse(), 200, "application/vnd.surrealdb.sync.test"),
      );
    const body = new ArrayBuffer(3);
    const encode = vi.fn(async () => body);
    const decode = vi.fn(async (value: ArrayBuffer) =>
      new TextDecoder().decode(value),
    );
    const codec: ExperimentalSyncHttpCodec = {
      mediaType: "application/vnd.surrealdb.sync.test",
      encodePushRequest: encode,
      decodePushResponse: decode,
      encodePullRequest: encode,
      decodePullResponse: decode,
    };
    const { adapter } = createAdapter(fetch, codec);

    const push = adapter.push();
    const pull = adapter.pull();
    await vi.waitFor(() => expect(fetch).toHaveBeenCalledTimes(1));
    releaseFirst?.();
    await expect(push).resolves.toEqual([status]);
    await expect(pull).resolves.toEqual(status);

    expect(fetch).toHaveBeenCalledTimes(2);
    expect(encode).toHaveBeenCalledTimes(2);
    expect(fetch.mock.calls[0]?.[1]?.body).toBe(body);
  });

  it("reuses the old native checkpoint when pull application fails", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockImplementation(async () => response(pullResponse("checkpoint-2")));
    const { adapter, native } = createAdapter(fetch);
    native.checkpointToken.mockResolvedValue("checkpoint-1");
    native.applyPullResponse
      .mockRejectedValueOnce(new Error("native persistence unavailable"))
      .mockResolvedValueOnce(status);

    await expect(adapter.pull()).rejects.toThrow("persistence unavailable");
    expect(native.applyPullResponse).toHaveBeenCalledOnce();

    await expect(adapter.pull()).resolves.toEqual(status);
    expect(native.applyPullResponse).toHaveBeenCalledTimes(2);
    expect(native.checkpointToken).toHaveBeenCalledTimes(2);
    for (const [, init] of fetch.mock.calls) {
      expect(JSON.parse(init?.body as string).checkpoint).toBe("checkpoint-1");
    }
  });

  it("forwards cancellation and rejects unsafe JSON integers", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValue(response(pullResponse()));
    const { adapter } = createAdapter(fetch);
    const controller = new AbortController();

    await adapter.pull({ signal: controller.signal });
    expect(fetch.mock.calls[0]?.[1]?.signal).not.toBe(controller.signal);
    await expect(
      experimentalJsonSyncHttpCodec.encodePullRequest({
        partitionId: "p",
        clientId: "c",
        requestedScope: "all",
        subscriptionRevision: 2n ** 63n,
      }),
    ).rejects.toThrow("safe range");
  });

  it("bounds request and response bodies before protocol decoding", async () => {
    const fetch = vi.fn<typeof globalThis.fetch>();
    const oversizedCodec: ExperimentalSyncHttpCodec = {
      ...experimentalJsonSyncHttpCodec,
      encodePushRequest: vi.fn(async () => new ArrayBuffer(5)),
    };
    const request = createAdapter(fetch, oversizedCodec, {
      maxRequestBytes: 4,
    }).adapter;
    await expect(request.push()).rejects.toMatchObject({ kind: "body_limit" });
    expect(fetch).not.toHaveBeenCalled();

    const declared = vi.fn<typeof globalThis.fetch>().mockResolvedValue(
      new Response("12345", {
        headers: {
          "Content-Length": "5",
          "Content-Type": "application/json",
        },
      }),
    );
    const declaredAdapter = createAdapter(declared, undefined, {
      maxResponseBytes: 4,
    }).adapter;
    await expect(declaredAdapter.pull()).rejects.toMatchObject({
      kind: "body_limit",
    });

    const streamed = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValue(response("12345"));
    const streamedAdapter = createAdapter(streamed, undefined, {
      maxResponseBytes: 4,
    }).adapter;
    await expect(streamedAdapter.pull()).rejects.toMatchObject({
      kind: "body_limit",
    });
  });

  it("rejects wrong content types without decoding the body", async () => {
    const fetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValue(response(pullResponse(), 200, "text/plain"));
    const { adapter, native } = createAdapter(fetch);

    await expect(adapter.pull()).rejects.toMatchObject({
      kind: "content_type",
    });
    expect(native.applyPullResponse).not.toHaveBeenCalled();
  });

  it("distinguishes timeout from caller cancellation", async () => {
    const abortingFetch = vi.fn<typeof globalThis.fetch>(
      (_url, init) =>
        new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () =>
            reject(new DOMException("aborted", "AbortError")),
          );
        }),
    );
    const timeoutAdapter = createAdapter(abortingFetch, undefined, {
      requestTimeoutMs: 1,
    }).adapter;
    await expect(timeoutAdapter.pull()).rejects.toMatchObject({
      kind: "timeout",
    });

    const controller = new AbortController();
    const cancelled = createAdapter(abortingFetch).adapter.pull({
      signal: controller.signal,
    });
    controller.abort();
    await expect(cancelled).rejects.toMatchObject({ kind: "aborted" });
  });

  it("bounds token acquisition and encoding before fetch", async () => {
    const tokenFetch = vi
      .fn<typeof globalThis.fetch>()
      .mockResolvedValue(response(pullResponse()));
    let tokenSignal: AbortSignal | undefined;
    const accessToken = vi
      .fn<ExperimentalSyncHttpOptions["accessToken"]>()
      .mockImplementationOnce((options) => {
        tokenSignal = options?.signal;
        return new Promise<string>(() => undefined);
      })
      .mockResolvedValue("redacted-test-token");
    const tokenAdapter = createAdapter(
      tokenFetch,
      undefined,
      { requestTimeoutMs: 1 },
      accessToken,
    ).adapter;
    await expect(tokenAdapter.pull()).rejects.toMatchObject({
      kind: "timeout",
    });
    expect(tokenSignal?.aborted).toBe(true);
    await expect(tokenAdapter.pull()).resolves.toEqual(status);
    expect(tokenFetch).toHaveBeenCalledOnce();

    const fetch = vi.fn<typeof globalThis.fetch>();
    const stalledCodec: ExperimentalSyncHttpCodec = {
      ...experimentalJsonSyncHttpCodec,
      encodePullRequest: async () => new Promise(() => undefined),
    };
    const encodeAdapter = createAdapter(fetch, stalledCodec, {
      requestTimeoutMs: 1,
    }).adapter;
    await expect(encodeAdapter.pull()).rejects.toMatchObject({
      kind: "timeout",
    });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("requires secure transport unless localhost is explicitly allowed", () => {
    const { sync } = createSync();
    const options = {
      sync,
      partitionId: "p",
      clientId: "c",
      requestedScope: "all",
      subscriptionRevision: 1n,
      accessToken: () => "redacted",
      codec: experimentalJsonSyncHttpCodec,
    };
    expect(
      () =>
        new ExperimentalSyncHttpAdapter({
          ...options,
          baseUrl: "http://example.test",
        }),
    ).toThrow("must use https");
    expect(
      () =>
        new ExperimentalSyncHttpAdapter({
          ...options,
          baseUrl: "http://127.0.0.1:8080",
          allowInsecureLocalhost: true,
        }),
    ).not.toThrow();
    expect(
      () =>
        new ExperimentalSyncHttpAdapter({
          ...options,
          baseUrl: "http://10.0.2.2:8080",
          allowInsecureLocalhost: true,
        }),
    ).not.toThrow();
  });

  it("wires canonical CBOR bytes and raw protocol JSON without coercion", async () => {
    const pushGolden = Uint8Array.from([0x84, 0x70, 0x73]).buffer;
    const pullGolden = Uint8Array.from([0x84, 0x70, 0x74]).buffer;
    const bridge: ExperimentalSyncHttpNativeBridge = {
      encodeSyncPushRequest: vi.fn(async () => pushGolden),
      decodeSyncPushResponse: vi.fn(async () => '{"status":"push"}'),
      encodeSyncPullRequest: vi.fn(async () => pullGolden),
      decodeSyncPullResponse: vi.fn(async () => '{"status":"pull"}'),
    };
    const codec = experimentalCanonicalCborSyncHttpCodec(bridge);

    const pushBody = await codec.encodePushRequest({
      partitionId: "p",
      clientId: "c",
      commitJson: '{"identity":{"clientCommitId":"i"}}',
    });
    expect(pushBody).toEqual(pushGolden);
    expect(await codec.decodePushResponse(pushGolden)).toBe(
      '{"status":"push"}',
    );

    const pullBody = await codec.encodePullRequest({
      partitionId: "p",
      clientId: "c",
      checkpoint: "opaque",
      requestedScope: "all",
      subscriptionRevision: 2n ** 63n,
    });
    expect(pullBody).toEqual(pullGolden);
    expect(await codec.decodePullResponse(pullGolden)).toBe(
      '{"status":"pull"}',
    );
    expect(bridge.encodeSyncPullRequest).toHaveBeenCalledWith(
      "p",
      "c",
      "opaque",
      "all",
      2n ** 63n,
      undefined,
    );
  });
});

it("joins measured transport metadata to server-generated request ordinals without payloads", async () => {
  const { ExperimentalSyncTelemetry } = await import("../src/sync-telemetry");
  const telemetry = new ExperimentalSyncTelemetry();
  const events: unknown[] = [];
  telemetry.subscribe((event) => {
    events.push(event);
  });
  const { sync } = createSync();
  const adapter = new ExperimentalSyncHttpAdapter({
    sync,
    telemetry,
    baseUrl: "https://sync.example.test",
    partitionId: "SECRET-partition",
    clientId: "SECRET-user",
    requestedScope: "all",
    subscriptionRevision: 1n,
    accessToken: () => "SECRET-token",
    codec: experimentalJsonSyncHttpCodec,
    fetch: vi.fn<typeof globalThis.fetch>().mockImplementation(
      async () =>
        new Response(
          JSON.stringify({
            outcome: {
              identity: { clientCommitId: "SECRET-commit" },
              status: "accepted",
            },
          }),
          {
            headers: {
              "Content-Type": "application/json",
              "x-sync-request-id": "sync-12",
            },
          },
        ),
    ),
  });
  await adapter.push();
  await telemetry.flush();
  expect(events).toContainEqual(
    expect.objectContaining({
      phase: "transport",
      boundary: "end",
      serverRequest: 12,
      responseBytes: expect.any(Number),
      bytes: expect.any(Number),
      parent: expect.any(Number),
    }),
  );
  expect(events).toContainEqual(
    expect.objectContaining({
      reason: "authority_accepted",
      commit: expect.any(Number),
    }),
  );
  expect(JSON.stringify(events)).not.toContain("SECRET");
  expect(telemetry.snapshot().lastPushSuccessAtMs).toEqual(expect.any(Number));
  await telemetry.close();
});
