import { describe, expect, it } from "vitest";
import { ExperimentalSyncTelemetry } from "../src/sync-telemetry";
import { ExperimentalSyncObservedClient } from "../src/sync-observation";

describe("bounded telemetry", () => {
  it("isolates hung, throwing and reentrant sinks with independent FIFO delivery", async () => {
    const t = new ExperimentalSyncTelemetry({ capacity: 2 });
    let hungCalls = 0;
    t.subscribe(() => {
      hungCalls++;
      return new Promise(() => {});
    });
    t.subscribe(() => {
      throw new Error("SECRET");
    });
    const seen: number[] = [];
    const unsubscribe = t.subscribe((event) => {
      seen.push(event.sequence);
      if (seen.length === 1) t.state("idle");
    });
    for (let i = 0; i < 100; i++) t.state("syncing");
    expect(t.snapshot().queued).toBeLessThanOrEqual(6);
    expect(await t.flush(50)).toBe(false);
    expect(hungCalls).toBe(1);
    expect(seen).toEqual([...seen].sort((a, b) => a - b));
    expect(t.snapshot().errors).toBeGreaterThan(0);
    expect(t.snapshot().dropped).toBeGreaterThan(0);
    unsubscribe();
    unsubscribe();
    expect(await t.close(0)).toBe(false);
    expect(t.snapshot().queued).toBe(0);
    const sequence = t.snapshot().sequence;
    t.state("idle");
    expect(t.snapshot().sequence).toBe(sequence);
  });
  it("reconstructs allowlisted events and provides JSON-safe status without secrets", async () => {
    const t = new ExperimentalSyncTelemetry();
    const seen: unknown[] = [];
    t.subscribe((event) => {
      seen.push(event);
    });
    t.record({
      span: 1,
      phase: "transport",
      boundary: "end",
      origin: "client",
      atMs: 2,
      ...{ password: "SECRET", checkpoint: "SECRET", reason: "SECRET" as "ok" },
    });
    const sync = new ExperimentalSyncObservedClient(
      {
        enqueue: async () => status,
        status: async () => status,
        recordPushResponseProtocolJson: async () => status,
      },
      { telemetry: t },
    );
    await sync.enqueue({ identity: { clientCommitId: "SECRET" } });
    expect(await t.flush()).toBe(true);
    expect(JSON.stringify([seen, await sync.supportSnapshot()])).not.toContain(
      "SECRET",
    );
    expect((await sync.supportSnapshot()).consistency).toBe(
      "native_status_only",
    );
  });
  it("keeps durable results identical under sustained backpressure with bounded retained work", async () => {
    const t = new ExperimentalSyncTelemetry({ capacity: 3 });
    t.subscribe(() => new Promise(() => {}));
    const client = {
      enqueue: async () => status,
      status: async () => status,
      recordPushResponseProtocolJson: async () => status,
    };
    const observed = new ExperimentalSyncObservedClient(client, {
      telemetry: t,
    });
    const baseline = new ExperimentalSyncObservedClient(client);
    const start = performance.now();
    for (let i = 0; i < 10000; i++)
      expect(await observed.enqueue({})).toEqual(await baseline.enqueue({}));
    expect(t.snapshot().queued).toBeLessThanOrEqual(3);
    expect(performance.now() - start).toBeLessThan(5000);
    expect(t.snapshot().dropped).toBeGreaterThan(10000);
    await t.close(0);
  });
});
const status = {
  revision: 1n,
  pendingCount: 1,
  outcomeCount: 0,
  conflictCount: 0,
};

it("records phase durations on both success and failure without changing failures", async () => {
  let now = 10;
  const t = new ExperimentalSyncTelemetry({ now: () => now });
  const seen: {
    phase: string;
    durationMs?: number;
    parent?: number;
    reason?: string;
  }[] = [];
  t.subscribe((event) => {
    seen.push(event);
  });
  const cycle = t.begin("push");
  const failure = new Error("PRIVATE body");
  await expect(
    t.measure(
      "encode",
      async () => {
        now += 5;
        throw failure;
      },
      cycle.id,
    ),
  ).rejects.toBe(failure);
  cycle.end();
  await t.flush();
  expect(seen).toContainEqual(
    expect.objectContaining({
      phase: "encode",
      durationMs: 5,
      parent: cycle.id,
      reason: "error",
    }),
  );
  expect(JSON.stringify(seen)).not.toContain("PRIVATE");
  await t.close();
});

it("bounds session commit correlation and strips raw identities", async () => {
  const t = new ExperimentalSyncTelemetry({ capacity: 4096 });
  const events: unknown[] = [];
  t.subscribe((event) => {
    events.push(event);
  });
  const id = t.correlate("SECRET-account-commit");
  expect(t.correlate("SECRET-account-commit")).toBe(id);
  t.milestone("SECRET-account-commit", "local_durable");
  t.milestone("SECRET-account-commit", "authority_accepted");
  for (let i = 0; i < 1100; i++) t.correlate(`SECRET-${i}`);
  expect(t.snapshot().correlationEvictions).toBe(77);
  await t.flush();
  expect(JSON.stringify(events)).not.toContain("SECRET");
  expect(events).toHaveLength(2);
  expect(events[0]).toHaveProperty("commit", id);
  expect(events[1]).toHaveProperty("commit", id);
  await t.close();
});

it("isolates native diagnostics failures and snapshots only numeric allowlisted counters", async () => {
  const client = {
    enqueue: async () => status,
    status: async () => status,
    recordPushResponseProtocolJson: async () => status,
    nativeDiagnostics: () => ({
      schemaVersion: 1,
      storageNs: 50n,
      checkpoint: "SECRET",
      password: "SECRET",
    }),
  };
  const observed = new ExperimentalSyncObservedClient(client);
  expect((await observed.supportSnapshot()).native).toEqual({
    schemaVersion: "1",
    storageNs: "50",
  });
  client.nativeDiagnostics = () => {
    throw new Error("SECRET");
  };
  expect((await observed.supportSnapshot()).native).toBeUndefined();
  expect(await observed.enqueue({})).toEqual(status);
});

it("does not let unsubscribe cycles multiply unresolved exporter promises", async () => {
  const t = new ExperimentalSyncTelemetry();
  for (let i = 0; i < 16; i++) {
    const stop = t.subscribe(() => new Promise(() => {}));
    t.state("idle");
    await new Promise((resolve) => setTimeout(resolve, 2));
    stop();
  }
  expect(t.snapshot().inFlight).toBe(16);
  expect(() => t.subscribe(() => {})).toThrow("limit");
  expect(await t.close(0)).toBe(false);
});
