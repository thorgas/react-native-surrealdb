import { describe, expect, it, vi } from "vitest";
import {
  decodeSyncViewResult,
  encodeSyncViewRequest,
  type SyncViewRequest,
} from "../src/sync-view";
import { ExperimentalSyncClient } from "../src/sync";
import type { NativeSyncClientLike } from "../src/generated/surrealdb_rn_core";
const metadata = {
  revision: "9007199254740993",
  scope: {
    partitionId: "p",
    scopeIdentity: "s",
    subscriptionRevision: "1",
    authorizationRevision: null,
  },
  asOf: null,
  readiness: "local_available_awaiting_coverage",
  coverage: { kind: "unknown" },
};
const work = {
  recordsVisited: 4,
  operationsVisited: 0,
  valueNodesVisited: 8,
  valueBytesVisited: 32,
};
const count: SyncViewRequest = {
  protocolVersion: 1,
  operation: "count",
  selector: { kind: "all" },
};
const page: SyncViewRequest = {
  protocolVersion: 1,
  operation: "page",
  selector: { kind: "all" },
  projection: [["name"]],
  limit: 2,
};
const result = {
  protocolVersion: 1,
  kind: "count",
  observedCount: "2",
  metadata,
  work,
};
function fixture(
  response: () => Promise<string> = async () => JSON.stringify(result),
) {
  const native = {
    readViewJson: vi.fn(response),
    isClosed: vi.fn(() => false),
    close: vi.fn(async () => undefined),
  };
  return {
    native,
    client: new ExperimentalSyncClient(
      native as unknown as NativeSyncClientLike,
    ),
  };
}
describe("bounded local native views", () => {
  it("preserves local coverage and lossless revision without reading snapshots", async () => {
    const { client, native } = fixture();
    expect(await client.readView(count)).toEqual(result);
    expect(native.readViewJson).toHaveBeenCalledWith(
      JSON.stringify(count),
      undefined,
    );
  });
  it("refuses unsupported scalar/path/oversized requests before native work", async () => {
    const { client, native } = fixture();
    for (const selector of [
      { kind: "eq", path: ["x"], value: 9007199254740993 },
      { kind: "eq", path: ["__proto__"], value: "x" },
      { kind: "and", terms: [{ kind: "all" }] },
    ]) {
      await expect(
        client.readView({ ...count, selector } as SyncViewRequest),
      ).rejects.toThrow("bounded sync view");
    }
    expect(native.readViewJson).not.toHaveBeenCalled();
    expect(() =>
      encodeSyncViewRequest({ ...page, limit: 65 } as SyncViewRequest),
    ).toThrow();
  });
  it("checks capability presence instead of falling back to full snapshots", async () => {
    const old = new ExperimentalSyncClient({
      isClosed: () => false,
    } as NativeSyncClientLike);
    await expect(old.readView(count)).rejects.toThrow(
      "matching rebuilt native binaries",
    );
  });
  it("decodes projected values and distinguishes absent from null", () => {
    const rows = [
      {
        recordId: "a",
        pending: [],
        conflicts: [],
        accepted: [],
        fields: [{ path: ["name"], present: true, value: "a" }],
      },
    ];
    expect(
      decodeSyncViewResult(
        JSON.stringify({
          protocolVersion: 1,
          kind: "page",
          metadata,
          work,
          rows,
          nextCursor: null,
        }),
        page,
      ).kind,
    ).toBe("page");
    rows[0]!.fields[0]!.path = ["constructor"];
    expect(() =>
      decodeSyncViewResult(
        JSON.stringify({
          protocolVersion: 1,
          kind: "page",
          metadata,
          work,
          rows,
          nextCursor: null,
        }),
        page,
      ),
    ).toThrow();
  });
  it("decodes whole canonical atoms without rewriting ordinary value keys", () => {
    const request: SyncViewRequest = {
      ...page,
      operation: "page",
      projection: [["amount"], ["bytes"], ["ordinary", "value"]],
      limit: 1,
    };
    const response = {
      protocolVersion: 1,
      kind: "page",
      metadata,
      work,
      nextCursor: null,
      rows: [
        {
          recordId: "a",
          pending: [],
          conflicts: [],
          accepted: [],
          fields: [
            {
              path: ["amount"],
              present: true,
              value: { $surreal: "int", value: "9223372036854775807" },
            },
            {
              path: ["bytes"],
              present: true,
              value: { $surreal: "bytes", base64: "AAH/" },
            },
            { path: ["ordinary", "value"], present: true, value: "42" },
          ],
        },
      ],
    };
    const decoded = decodeSyncViewResult(JSON.stringify(response), request);
    if (decoded.kind !== "page") throw new Error("expected page");
    expect(decoded.rows[0]!.fields[0]!.value).toBe(9223372036854775807n);
    expect(decoded.rows[0]!.fields[1]!.value).toEqual(
      new Uint8Array([0, 1, 255]),
    );
    expect(decoded.rows[0]!.fields[2]!.value).toBe("42");
    expect(() =>
      encodeSyncViewRequest({
        ...count,
        selector: { kind: "eq", path: ["amount", "$surreal"], value: "int" },
      }),
    ).toThrow();
  });
  it("refuses malformed output, output budgets and hidden snapshot fallback", () => {
    for (const bad of [
      { ...result, observedCount: 2 },
      { ...result, observedCount: "02" },
      { ...result, work: { ...work, operationsVisited: 16384 } },
      {
        ...result,
        metadata: { ...metadata, readiness: "authorized_scope_as_of" },
      },
      { protocolVersion: 1, kind: "stale_cursor", metadata, snapshot: {} },
    ]) {
      expect(() => decodeSyncViewResult(JSON.stringify(bad), count)).toThrow();
    }
    expect(() => decodeSyncViewResult(" ".repeat(262145), count)).toThrow();
    expect(
      decodeSyncViewResult(
        JSON.stringify({ protocolVersion: 1, kind: "stale_cursor", metadata }),
        { ...page, cursor: "issued" } as SyncViewRequest,
      ).kind,
    ).toBe("stale_cursor");
  });
  it("withdraws completion after abort and close even if native resolves late", async () => {
    let resolve!: (value: string) => void;
    const { client } = fixture(
      () =>
        new Promise((r) => {
          resolve = r;
        }),
    );
    const controller = new AbortController();
    const pending = client.readView(count, { signal: controller.signal });
    controller.abort();
    resolve(JSON.stringify(result));
    await expect(pending).rejects.toThrow("aborted");
    let finish!: (value: string) => void;
    const other = fixture(
      () =>
        new Promise((r) => {
          finish = r;
        }),
    );
    const read = other.client.readView(count);
    await other.client.close();
    finish(JSON.stringify(result));
    await expect(read).rejects.toThrow("closed");
  });
  it("retains captured request across a caller mutation while awaiting native", async () => {
    let finish!: (value: string) => void;
    const { client } = fixture(
      () =>
        new Promise((r) => {
          finish = r;
        }),
    );
    const mutable = { ...count };
    const read = client.readView(mutable);
    Object.assign(mutable, {
      operation: "page",
      projection: [["evil"]],
      limit: 2,
    });
    finish(JSON.stringify(result));
    expect((await read).kind).toBe("count");
  });
  it("withdraws a pending completion after another call observes scope revocation", async () => {
    let finish!: (value: string) => void;
    const native = {
      readViewJson: vi.fn(
        () =>
          new Promise<string>((resolve) => {
            finish = resolve;
          }),
      ),
      isClosed: () => false,
      applyPullResponse: vi.fn(async () => {
        throw Object.assign(new Error("revoked"), {
          tag: "NeedsNewScopeJournal",
        });
      }),
    };
    const client = new ExperimentalSyncClient(
      native as unknown as NativeSyncClientLike,
    );
    const read = client.readView(count);
    await expect(client.applyPullResponse({})).rejects.toThrow(
      "new authorized journal",
    );
    finish(JSON.stringify(result));
    await expect(read).rejects.toThrow("new authorized journal");
  });
  it("propagates scope revocation and refuses subsequent reads", async () => {
    const { client, native } = fixture(async () => {
      throw Object.assign(new Error("revoked"), {
        tag: "NeedsNewScopeJournal",
      });
    });
    await expect(client.readView(count)).rejects.toThrow(
      "new authorized journal",
    );
    await expect(client.readView(count)).rejects.toThrow(
      "new authorized journal",
    );
    expect(native.readViewJson).toHaveBeenCalledTimes(1);
  });
});
