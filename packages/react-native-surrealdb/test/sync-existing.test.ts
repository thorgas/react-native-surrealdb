import { describe, expect, it, vi } from "vitest";
import type { SurrealDatabaseLike } from "../src/generated/surrealdb_rn_core";

const bindings = vi.hoisted(() => ({
  openExistingSyncClient: vi.fn(),
  openSyncClient: vi.fn(),
  encodeSyncPushRequest: vi.fn(),
  decodeSyncPushResponse: vi.fn(),
  encodeSyncPullRequest: vi.fn(),
  decodeSyncPullResponse: vi.fn(),
  decodeSyncPullResponseWithSource: vi.fn(),
}));
vi.mock("../src/native", () => bindings);
import {
  SurrealClient,
  createExperimentalCanonicalCborSyncHttpCodec,
} from "../src/client";

const options = {
  partitionId: "partition",
  clientId: "account.installation",
  requestedScope: "household-state-v2",
  subscriptionRevision: 1n,
};

describe("existing-only sync attach facade", () => {
  it("exposes the native source-proof decoder through the canonical codec", async () => {
    const bytes = new ArrayBuffer(1);
    bindings.decodeSyncPullResponseWithSource.mockResolvedValueOnce(
      "opaque-source-proof",
    );
    const codec = createExperimentalCanonicalCborSyncHttpCodec();
    expect(codec.decodePullResponseWithSource).toBeTypeOf("function");
    expect(await codec.decodePullResponseWithSource!(bytes)).toBe(
      "opaque-source-proof",
    );
    expect(bindings.decodeSyncPullResponseWithSource).toHaveBeenCalledWith(
      bytes,
    );
  });
  it("uses only the strict native factory and preserves cancellation options", async () => {
    const native = {} as SurrealDatabaseLike;
    const close = vi.fn(async () => undefined);
    bindings.openExistingSyncClient.mockResolvedValueOnce({ close });
    const signal = new AbortController().signal;
    const client = await new SurrealClient(native).openExistingExperimentalSync(
      options,
      { signal },
    );
    expect(bindings.openExistingSyncClient).toHaveBeenLastCalledWith(
      native,
      options,
      { signal },
    );
    await client.close();
    expect(close).toHaveBeenCalledTimes(1);
    expect(bindings.openSyncClient).not.toHaveBeenCalled();
  });
  it("preserves refusal rather than retrying with initialization", async () => {
    const failure = new Error("missing journal");
    bindings.openExistingSyncClient.mockRejectedValueOnce(failure);
    await expect(
      new SurrealClient({} as SurrealDatabaseLike).openExistingExperimentalSync(
        options,
      ),
    ).rejects.toBe(failure);
    expect(bindings.openSyncClient).not.toHaveBeenCalled();
  });
  it("fails closed when the strict native ABI is unavailable", async () => {
    const open = bindings.openExistingSyncClient;
    Object.assign(bindings, { openExistingSyncClient: undefined });
    try {
      await expect(
        new SurrealClient(
          {} as SurrealDatabaseLike,
        ).openExistingExperimentalSync(options),
      ).rejects.toThrow("matching rebuilt native binaries");
      expect(bindings.openSyncClient).not.toHaveBeenCalled();
    } finally {
      bindings.openExistingSyncClient = open;
    }
  });
});
