import { beforeEach, describe, expect, it, vi } from "vitest";
import type {
  NativeSyncClientLike,
  NativeBootstrapSessionLike,
} from "../src/generated/surrealdb_rn_core";
const bridge = vi.hoisted(() => ({ capabilities: vi.fn(), bind: vi.fn() }));
vi.mock("../src/generated/surrealdb_rn_core", async (original) => ({
  ...(await original<object>()),
  bootstrapCapabilities: bridge.capabilities,
  bindBootstrapSession: bridge.bind,
}));
import {
  NativeBootstrapStageState as State,
  NativeBootstrapCompletion as Completion,
} from "../src/generated/surrealdb_rn_core";
import { ExperimentalSyncClient } from "../src/sync";
import {
  ExperimentalBootstrapError,
  type ExperimentalBootstrapStage,
} from "../src/sync-bootstrap";

const context = {
  authorityId: "authority",
  principalId: "owner",
  partitionId: "partition",
  clientId: "account.generation",
  journalGeneration: "generation",
  requestedScope: "household-state-v2",
  subscriptionRevision: 1n,
  authorizationRevision: 9007199254740993n,
};
const caps = {
  protocol: "surrealdb-sync-bootstrap/1",
  requiresMatchingNativeAbi: true,
  maxFrameBytes: 4194304n,
  maxPersistenceBytes: 67108864n,
  maxComponents: 16384,
  nativeKeyBytes: 512,
  completionDeadlineSeconds: 30,
};
function fixture() {
  const stage = {};
  const native = {
    begin: vi.fn<NativeBootstrapSessionLike["begin"]>(async () => stage),
    page: vi.fn(async () => {}),
    progress: vi.fn<NativeBootstrapSessionLike["progress"]>(async () => ({
      state: State.Missing,
    })),
    cancel: vi.fn(async () => {}),
    complete: vi.fn(async () => Completion.Applied),
  };
  bridge.bind.mockResolvedValue(native);
  const parent = { isClosed: vi.fn(() => false), close: vi.fn(async () => {}) };
  const client = new ExperimentalSyncClient(
    parent as unknown as NativeSyncClientLike,
  );
  return { stage, native, parent, client };
}
beforeEach(() => {
  vi.resetAllMocks();
  bridge.capabilities.mockReturnValue(caps);
});
describe("opaque bootstrap facade", () => {
  it("fails before storage on missing native ABI or incompatible capability", async () => {
    const { client } = fixture();
    bridge.capabilities.mockImplementationOnce(() => {
      throw new TypeError("missing native symbol, private details");
    });
    await expect(client.bindBootstrapSession(context)).rejects.toMatchObject({
      code: "UnsupportedAbi",
    });
    expect(bridge.bind).not.toHaveBeenCalled();
    bridge.capabilities.mockReturnValueOnce({
      ...caps,
      maxFrameBytes: 4194305n,
    });
    await expect(client.bindBootstrapSession(context)).rejects.toThrow(
      "matching rebuilt native binaries",
    );
    expect(bridge.bind).not.toHaveBeenCalled();
  });
  it("validates independent identity/u64 context before binding, and snapshots it", async () => {
    const { client } = fixture();
    await expect(
      client.bindBootstrapSession({
        ...context,
        authorizationRevision: 1 as unknown as bigint,
      }),
    ).rejects.toMatchObject({ code: "InvalidInput" });
    await expect(
      client.bindBootstrapSession({ ...context, clientId: "é".repeat(257) }),
    ).rejects.toMatchObject({ code: "InvalidInput" });
    expect(bridge.bind).not.toHaveBeenCalled();
    const independent = { ...context };
    await client.bindBootstrapSession(independent);
    independent.principalId = "changed";
    expect(bridge.bind.mock.calls[0]![1]).toEqual(context);
  });
  it("copies exact typed view slices before async dispatch, forwards signal and native bigint context", async () => {
    const { client, native, stage } = fixture();
    const signal = new AbortController().signal;
    const session = await client.bindBootstrapSession(context, { signal });
    const backing = new Uint8Array([99, 1, 2, 3, 88]);
    const handle = await session.begin(
      backing.subarray(1, 4),
      new DataView(backing.buffer, 2, 2),
      { signal },
    );
    backing.fill(0);
    expect(new Uint8Array(native.begin.mock.calls[0]![0]!)).toEqual(
      new Uint8Array([1, 2, 3]),
    );
    expect(new Uint8Array(native.begin.mock.calls[0]![1]!)).toEqual(
      new Uint8Array([2, 3]),
    );
    expect(native.begin.mock.calls[0]![2]).toEqual({ signal });
    await session.page(handle, new Uint16Array([1, 2]), { signal });
    expect(native.page).toHaveBeenCalledWith(stage, expect.any(ArrayBuffer), {
      signal,
    });
    expect(
      await session.complete(handle, new Uint8Array([4]), { signal }),
    ).toBe("applied");
    expect(native.complete).toHaveBeenCalledWith(
      stage,
      expect.any(ArrayBuffer),
      { signal },
    );
    expect(bridge.bind.mock.calls[0]![1].authorizationRevision).toBe(
      9007199254740993n,
    );
  });
  it("rejects oversized frames and foreign stage objects without crossing bridge", async () => {
    const { client, native } = fixture();
    const session = await client.bindBootstrapSession(context);
    const huge = new Uint8Array(4194305);
    await expect(session.begin(huge, new ArrayBuffer(0))).rejects.toMatchObject(
      { code: "InvalidInput" },
    );
    expect(native.begin).not.toHaveBeenCalled();
    const handle = await session.begin(new ArrayBuffer(0), new ArrayBuffer(0));
    await expect(session.page(handle, huge)).rejects.toMatchObject({
      code: "InvalidInput",
    });
    await expect(session.complete(handle, huge)).rejects.toMatchObject({
      code: "InvalidInput",
    });
    await expect(
      session.cancel({} as ExperimentalBootstrapStage),
    ).rejects.toMatchObject({ code: "StageMismatch" });
    const other = await client.bindBootstrapSession(context);
    await expect(other.cancel(handle)).rejects.toMatchObject({
      code: "StageMismatch",
    });
    expect(native.page).not.toHaveBeenCalled();
    expect(native.complete).not.toHaveBeenCalled();
    expect(native.cancel).not.toHaveBeenCalled();
  });
  it("keeps Missing explicit and validates all progress states without beginning", async () => {
    const { client, native, stage } = fixture();
    const session = await client.bindBootstrapSession(context);
    expect(await session.progress(new ArrayBuffer(0))).toEqual({
      kind: "missing",
    });
    for (const state of [State.Staging, State.Cancelled, State.Consumed]) {
      const value = {
        state,
        stage,
        receivedIndices: [0n],
        expectedPages: 1n,
        ...(state === State.Consumed
          ? { successfulRevision: 9007199254740993n }
          : {}),
      };
      native.progress.mockResolvedValueOnce(value);
      const progress = await session.progress(new ArrayBuffer(0));
      expect(progress.kind).toBe(
        state === State.Staging
          ? "staging"
          : state === State.Cancelled
            ? "cancelled"
            : "consumed",
      );
      if (progress.kind !== "missing") {
        await session.cancel(progress.stage);
        expect(progress.receivedIndices).toEqual([0n]);
      }
    }
    expect(native.begin).not.toHaveBeenCalled();
    native.progress.mockResolvedValueOnce({
      state: State.Staging,
      stage,
      receivedIndices: [0n, 0n],
      expectedPages: 1n,
    });
    await expect(session.progress(new ArrayBuffer(0))).rejects.toMatchObject({
      code: "CorruptStage",
    });
  });
  it("distinguishes historical identity and redacts typed and unknown errors", async () => {
    const { client, native } = fixture();
    const session = await client.bindBootstrapSession(context);
    const stage = await session.begin(new ArrayBuffer(0), new ArrayBuffer(0));
    native.complete.mockResolvedValueOnce(Completion.HistoricalSuccess);
    expect(await session.complete(stage, new ArrayBuffer(0))).toBe(
      "historical_success",
    );
    native.page.mockRejectedValueOnce({
      tag: "PendingQuarantineRequired",
      message: "private payload",
    });
    await expect(session.page(stage, new ArrayBuffer(0))).rejects.toMatchObject(
      { code: "PendingQuarantineRequired" },
    );
    native.complete.mockRejectedValueOnce(new Error("private receipt/token"));
    const error = await session
      .complete(stage, new ArrayBuffer(0))
      .catch((error) => error);
    expect(error).toBeInstanceOf(ExperimentalBootstrapError);
    expect(error.code).toBe("RecoveryRequired");
    expect(String(error)).not.toContain("private");
    expect(error.cause).toBeUndefined();
  });
  it("checks parent closure before calls and after delayed binding/completion", async () => {
    const { client, native } = fixture();
    const session = await client.bindBootstrapSession(context);
    const stage = await session.begin(new ArrayBuffer(0), new ArrayBuffer(0));
    let resolve!: (value: Completion) => void;
    native.complete.mockImplementationOnce(
      () =>
        new Promise<Completion>((done) => {
          resolve = done;
        }),
    );
    const pending = session.complete(stage, new ArrayBuffer(0));
    await client.close();
    resolve(Completion.Applied);
    await expect(pending).rejects.toMatchObject({ code: "Closed" });
    await expect(session.cancel(stage)).rejects.toMatchObject({
      code: "Closed",
    });
    expect(native.cancel).not.toHaveBeenCalled();
    const another = fixture();
    let bind!: (value: typeof native) => void;
    bridge.bind.mockImplementationOnce(
      () =>
        new Promise<typeof native>((done) => {
          bind = done;
        }),
    );
    const binding = another.client.bindBootstrapSession(context);
    await another.client.close();
    bind(native);
    await expect(binding).rejects.toMatchObject({ code: "Closed" });
  });
  it("does not cancel durable data on wrapper close; pre-abort does no work and in-flight abort is unknown", async () => {
    const { client, native } = fixture();
    const session = await client.bindBootstrapSession(context);
    const stage = await session.begin(new ArrayBuffer(0), new ArrayBuffer(0));
    const controller = new AbortController();
    controller.abort();
    await expect(
      session.page(stage, new ArrayBuffer(0), { signal: controller.signal }),
    ).rejects.toMatchObject({ code: "Aborted" });
    expect(native.page).not.toHaveBeenCalled();
    native.page.mockRejectedValueOnce(
      new DOMException("private", "AbortError"),
    );
    await expect(session.page(stage, new ArrayBuffer(0))).rejects.toMatchObject(
      { code: "RecoveryRequired" },
    );
    session.close();
    await expect(session.cancel(stage)).rejects.toMatchObject({
      code: "Closed",
    });
    expect(native.cancel).not.toHaveBeenCalled();
  });
  it("redacts throwing accessors and proxies without reading private error payloads", async () => {
    const { client, native } = fixture();
    const session = await client.bindBootstrapSession(context);
    const stage = await session.begin(new ArrayBuffer(0), new ArrayBuffer(0));
    const privateFailure = () => {
      throw new Error("private fixture payload");
    };
    const failures = [
      Object.defineProperty({}, "tag", { get: privateFailure }),
      new Proxy(
        {},
        {
          getPrototypeOf: privateFailure,
          getOwnPropertyDescriptor: privateFailure,
          has: privateFailure,
          get: privateFailure,
        },
      ),
    ];
    for (const failure of failures) {
      native.page.mockRejectedValueOnce(failure);
      const mutation = await session
        .page(stage, new ArrayBuffer(0))
        .catch((error) => error);
      expect(mutation).toBeInstanceOf(ExperimentalBootstrapError);
      expect(mutation.code).toBe("RecoveryRequired");
      expect(String(mutation)).not.toContain("private");
      expect(mutation.cause).toBeUndefined();
      native.progress.mockRejectedValueOnce(failure);
      const read = await session
        .progress(new ArrayBuffer(0))
        .catch((error) => error);
      expect(read).toBeInstanceOf(ExperimentalBootstrapError);
      expect(read.code).toBe("Storage");
      expect(String(read)).not.toContain("private");
      expect(read.cause).toBeUndefined();
    }
  });
  it("accepts safe own data tags and actual generated errors without invoking has/get traps", async () => {
    const { client, native } = fixture();
    const session = await client.bindBootstrapSession(context);
    const stage = await session.begin(new ArrayBuffer(0), new ArrayBuffer(0));
    const privateFailure = () => {
      throw new Error("private fixture payload");
    };
    const generated = await import("../src/generated/surrealdb_rn_core");
    for (const failure of [
      new Proxy(
        { tag: "Expired" },
        { has: privateFailure, get: privateFailure },
      ),
      generated.NativeBootstrapError.Expired.new(),
    ]) {
      native.page.mockRejectedValueOnce(failure);
      await expect(
        session.page(stage, new ArrayBuffer(0)),
      ).rejects.toMatchObject({ code: "Expired" });
    }
  });
});
