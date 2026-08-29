import { createServer, type Server } from "node:http";

import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

import type {
  NativeSyncClientLike,
  NativeSyncStatus,
} from "../src/generated/surrealdb_rn_core";
import {
  ExperimentalSyncHttpAdapter,
  experimentalJsonSyncHttpCodec,
} from "../src/sync-http";
import { ExperimentalSyncClient } from "../src/sync";

const status: NativeSyncStatus = {
  revision: 1n,
  pendingCount: 0,
  outcomeCount: 1,
  conflictCount: 0,
};

describe("ExperimentalSyncHttpAdapter network boundary", () => {
  let server: Server;
  let baseUrl: string;
  const requests: Array<{
    authorization: string | undefined;
    body: unknown;
    url: string;
  }> = [];

  beforeAll(async () => {
    server = createServer((request, response) => {
      const chunks: Uint8Array[] = [];
      request.on("data", (chunk: Uint8Array) => chunks.push(chunk));
      request.on("end", () => {
        const body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
        requests.push({
          authorization: request.headers.authorization,
          body,
          url: request.url ?? "",
        });
        response.writeHead(200, { "Content-Type": "application/json" });
        if (request.url === "/v1/sync/push") {
          response.end(
            JSON.stringify({
              schemaVersion: "v1",
              partitionId: "partition-1",
              clientId: "client-1",
              outcome: { status: "accepted" },
            }),
          );
          return;
        }
        response.end(
          JSON.stringify({
            response: "reset",
            reason: "checkpoint_expired",
            checkpoint: {
              token: "checkpoint-1",
              cursor: { epoch: 1, sequence: 1 },
              scope: {
                identity: "all",
                authorizationRevision: 1,
                subscriptionRevision: 1,
              },
            },
            records: [],
          }),
        );
      });
    });
    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    const address = server.address();
    if (address == null || typeof address === "string") {
      throw new Error("sync test server did not bind a TCP port");
    }
    baseUrl = `http://127.0.0.1:${address.port}`;
  });

  afterAll(async () => {
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error == null ? resolve() : reject(error))),
    );
  });

  it("pushes and pulls through a real HTTP server", async () => {
    const native = {
      applyPullResponse: vi.fn(async () => status),
      checkpointToken: vi.fn(async () => "checkpoint-0"),
      close: vi.fn(async () => undefined),
      conflictsJson: vi.fn(async () => []),
      enqueue: vi.fn(async () => status),
      isClosed: vi.fn(() => false),
      pendingJson: vi.fn(async () => [
        JSON.stringify({
          identity: {
            clientCommitId: "commit-1",
            fingerprint:
              "sha256:0000000000000000000000000000000000000000000000000000000000000000",
          },
          operations: [],
        }),
      ]),
      recordPushResponse: vi.fn(async () => status),
      status: vi.fn(async () => status),
    } as NativeSyncClientLike;
    const adapter = new ExperimentalSyncHttpAdapter({
      sync: new ExperimentalSyncClient(native),
      baseUrl,
      allowInsecureLocalhost: true,
      partitionId: "partition-1",
      clientId: "client-1",
      requestedScope: "all",
      subscriptionRevision: 1n,
      accessToken: () => "redacted-network-test-token",
      codec: experimentalJsonSyncHttpCodec,
    });

    await expect(adapter.syncOnce()).resolves.toEqual({
      push: [status],
      pull: status,
    });
    expect(requests.map(({ url }) => url)).toEqual([
      "/v1/sync/push",
      "/v1/sync/pull",
    ]);
    expect(
      requests.every(
        ({ authorization }) =>
          authorization === "Bearer redacted-network-test-token",
      ),
    ).toBe(true);
    expect(requests[1]?.body).toMatchObject({ checkpoint: "checkpoint-0" });
    expect(native.recordPushResponse).toHaveBeenCalledOnce();
    expect(native.applyPullResponse).toHaveBeenCalledOnce();
  });
});
