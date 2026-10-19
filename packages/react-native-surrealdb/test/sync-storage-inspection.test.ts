import { beforeEach, describe, expect, it, vi } from "vitest";
import type { SurrealDatabaseLike } from "../src/generated/surrealdb_rn_core";
const bridge = vi.hoisted(() => ({ inspectStorage: vi.fn(), available: true }));
vi.mock("../src/generated/surrealdb_rn_core", async (original) => ({
  ...(await original<object>()),
  get inspectStorage() {
    return bridge.available ? bridge.inspectStorage : undefined;
  },
}));
vi.mock(
  "../src/native",
  async () => await import("../src/generated/surrealdb_rn_core"),
);
import { SurrealClient } from "../src/client";
import {
  ExperimentalStorageInspectionError,
  type ExperimentalStorageInspectionOptions,
} from "../src/sync-storage-inspection";
const options = (): ExperimentalStorageInspectionOptions => ({
  namespace: "household-local",
  database: "household-local",
  expectedJournal: {
    partitionId: "household-state-v2:fixture",
    clientId: "account.generation-state-v2",
    requestedScope: "household-state-v2",
    subscriptionRevision: 1n,
  },
  applicationTables: ["owner_marker", "projection"],
});
const selected = () => ({
  tag: "Selected",
  inner: {
    native: {
      tag: "Present",
      inner: {
        revision: 9007199254740993n,
        pendingCount: 2,
        outcomeCount: 1,
        conflictCount: 1,
        barrier: 2,
      },
    },
    applicationTables: [
      { table: "owner_marker", schemaPresent: true, rowsPresent: true },
      { table: "projection", schemaPresent: false, rowsPresent: false },
    ],
  },
});
const database = () => new SurrealClient({} as SurrealDatabaseLike);
beforeEach(() => {
  vi.resetAllMocks();
  bridge.available = true;
});
describe("whole-storage observation facade", () => {
  it("fails closed when method-level native ABI is missing", async () => {
    bridge.available = false;
    await expect(
      database().inspectExperimentalStorage(options()),
    ).rejects.toMatchObject({ code: "UnsupportedAbi" });
    expect(bridge.inspectStorage).not.toHaveBeenCalled();
  });

  it("preserves pristine versus selected missing without opening or selecting", async () => {
    bridge.inspectStorage.mockResolvedValueOnce({ tag: "PristineUnselected" });
    const result = await database().inspectExperimentalStorage(options());
    expect(result).toEqual({ kind: "pristine_unselected" });
    expect(Object.isFrozen(result)).toBe(true);
    bridge.inspectStorage.mockResolvedValueOnce({
      ...selected(),
      inner: { ...selected().inner, native: { tag: "Missing" } },
    });
    expect(
      await database().inspectExperimentalStorage(options()),
    ).toMatchObject({ kind: "selected", native: { kind: "missing" } });
    expect(bridge.inspectStorage).toHaveBeenCalledTimes(2);
  });
  it("snapshots inputs before async dispatch and copies bounded outputs with lossless counts", async () => {
    const value = selected();
    let finish!: (value: unknown) => void;
    bridge.inspectStorage.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    );
    const input = options();
    const signal = new AbortController().signal;
    const pending = database().inspectExperimentalStorage(input, { signal });
    (input.expectedJournal as { clientId: string }).clientId = "successor";
    (input.applicationTables as string[])[0] = "changed";
    expect(bridge.inspectStorage.mock.calls[0]![1]).toEqual(options());
    expect(bridge.inspectStorage.mock.calls[0]![2]).toEqual({ signal });
    finish(value);
    const result = await pending;
    expect(result).toMatchObject({
      kind: "selected",
      native: {
        kind: "present",
        revision: 9007199254740993n,
        barrier: "pending_quarantine_required",
      },
    });
    value.inner.applicationTables[0]!.table = "changed";
    expect(result).toMatchObject({
      applicationTables: [{ table: "owner_marker" }, { table: "projection" }],
    });
    if (result.kind !== "selected") throw new Error("selected");
    expect(Object.isFrozen(result.native)).toBe(true);
    expect(Object.isFrozen(result.applicationTables)).toBe(true);
    expect(Object.isFrozen(result.applicationTables[0])).toBe(true);
  });
  it("rejects malformed identities, SQL table injection, duplicates and limits before bridge", async () => {
    const invalid = [
      { ...options(), namespace: "injected;DELETE" },
      { ...options(), applicationTables: ["x;DELETE"] },
      { ...options(), applicationTables: ["_sync_client_meta"] },
      { ...options(), applicationTables: ["x", "x"] },
      {
        ...options(),
        applicationTables: Array.from({ length: 17 }, (_, i) => `x${i}`),
      },
      {
        ...options(),
        expectedJournal: {
          ...options().expectedJournal,
          clientId: "é".repeat(257),
        },
      },
      {
        ...options(),
        expectedJournal: {
          ...options().expectedJournal,
          subscriptionRevision: 18446744073709551616n,
        },
      },
    ];
    for (const value of invalid)
      await expect(
        database().inspectExperimentalStorage(value),
      ).rejects.toMatchObject({ code: "InvalidOptions" });
    expect(bridge.inspectStorage).not.toHaveBeenCalled();
  });
  it("never evaluates input accessors and redacts throwing or revoked proxies", async () => {
    const getter = vi.fn(() => {
      throw new Error("private token");
    });
    const input = Object.defineProperty(options(), "namespace", {
      get: getter,
    });
    const proxy = new Proxy(
      {},
      {
        getOwnPropertyDescriptor() {
          throw new Error("private database");
        },
      },
    );
    const revoked = Proxy.revocable({}, {});
    revoked.revoke();
    for (const value of [input, proxy, revoked.proxy]) {
      const error = await database()
        .inspectExperimentalStorage(
          value as ExperimentalStorageInspectionOptions,
        )
        .catch((error) => error);
      expect(error).toBeInstanceOf(ExperimentalStorageInspectionError);
      expect(error.code).toBe("InvalidOptions");
      expect(String(error)).not.toMatch(/private|token|database/);
    }
    expect(getter).not.toHaveBeenCalled();
    expect(bridge.inspectStorage).not.toHaveBeenCalled();
  });
  it("rejects malformed or foreign output and never exposes native extra fields", async () => {
    const bad = [
      { tag: "Unknown" },
      {
        ...selected(),
        inner: {
          ...selected().inner,
          native: {
            tag: "Present",
            inner: { ...selected().inner.native.inner, revision: 1 },
          },
        },
      },
      {
        ...selected(),
        inner: {
          ...selected().inner,
          applicationTables: [
            { table: "foreign", schemaPresent: true, rowsPresent: true },
          ],
        },
      },
      {
        ...selected(),
        inner: { ...selected().inner, applicationTables: Array(17).fill({}) },
      },
    ];
    for (const value of bad) {
      bridge.inspectStorage.mockResolvedValueOnce(value);
      await expect(
        database().inspectExperimentalStorage(options()),
      ).rejects.toMatchObject({ code: "Persistence" });
    }
    const good = { ...selected(), privatePayload: "secret" };
    bridge.inspectStorage.mockResolvedValueOnce(good);
    expect(
      JSON.stringify(
        await database().inspectExperimentalStorage(options()),
        (_, v) => (typeof v === "bigint" ? String(v) : v),
      ),
    ).not.toMatch(/secret|privatePayload/);
  });
  it("sanitizes native errors, accessor tags, proxy traps and output accessors", async () => {
    const getter = vi.fn(() => {
      throw new Error("private tag");
    });
    for (const error of [
      new Error("private payload"),
      Object.defineProperty({}, "tag", { get: getter }),
      new Proxy(
        {},
        {
          getOwnPropertyDescriptor() {
            throw new Error("private trap");
          },
        },
      ),
    ]) {
      bridge.inspectStorage.mockRejectedValueOnce(error);
      const result = await database()
        .inspectExperimentalStorage(options())
        .catch((e) => e);
      expect(result).toBeInstanceOf(ExperimentalStorageInspectionError);
      expect(result.code).toBe("Persistence");
      expect(String(result)).not.toContain("private");
      expect(result.cause).toBeUndefined();
    }
    bridge.inspectStorage.mockRejectedValueOnce({
      tag: "Unsettled",
      message: "private",
    });
    await expect(
      database().inspectExperimentalStorage(options()),
    ).rejects.toMatchObject({ code: "Unsettled" });
    bridge.inspectStorage.mockResolvedValueOnce(
      Object.defineProperty({}, "tag", { get: getter }),
    );
    await expect(
      database().inspectExperimentalStorage(options()),
    ).rejects.toMatchObject({ code: "Persistence" });
    expect(getter).not.toHaveBeenCalled();
  });
  it("withdraws pre-dispatch and late aborts without a mutation", async () => {
    const controller = new AbortController();
    controller.abort();
    await expect(
      database().inspectExperimentalStorage(options(), {
        signal: controller.signal,
      }),
    ).rejects.toMatchObject({ code: "Aborted" });
    expect(bridge.inspectStorage).not.toHaveBeenCalled();
    const late = new AbortController();
    bridge.inspectStorage.mockImplementationOnce(async () => {
      late.abort();
      return selected();
    });
    await expect(
      database().inspectExperimentalStorage(options(), { signal: late.signal }),
    ).rejects.toMatchObject({ code: "Aborted" });
  });
});
