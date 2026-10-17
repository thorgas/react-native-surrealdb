import { describe, expect, it, vi } from "vitest";
import { observe } from "@legendapp/state";
import {
  ExperimentalSyncReadStore,
  type SyncReadEvent,
  type SyncReadSnapshot,
  type SyncReadRow,
} from "../../../packages/react-native-surrealdb/src/sync-read";
import { createLegendProjection } from "../src/legend";
import { createTanStackProjection } from "../src/tanstack";
import type { EngineReadView } from "../src/contract";

type Value = { name: string; large?: bigint };
function row(id: string): SyncReadRow<Value> {
  return {
    recordId: id,
    confirmed: { state: "absent" },
    confirmedAbsence: "absent_within_authorized_scope",
    optimistic: {
      state: "present",
      value: { name: id, large: 9007199254740993n },
      baseVersion: "absent",
      reference: null,
    },
    pending: [{ clientCommitId: "pending", fingerprint: "fingerprint" }],
    conflicts: [],
    accepted: [],
  };
}
function snapshot(
  revision = "1",
  ids = ["a", "b"],
  authorized = true,
): SyncReadSnapshot<Value> {
  return {
    protocolVersion: 1,
    metadata: {
      revision,
      scope: {
        partitionId: "p",
        scopeIdentity: "s",
        subscriptionRevision: "1",
        authorizationRevision: authorized ? "1" : null,
      },
      asOf: authorized ? { epoch: "1", sequence: "0" } : null,
      readiness: authorized
        ? "authorized_scope_as_of"
        : "local_available_awaiting_coverage",
      coverage: authorized
        ? { kind: "authorized_reset", cursor: { epoch: "1", sequence: "0" } }
        : { kind: "unknown" },
    },
    rows: Object.fromEntries(
      ids.map((id) => [
        id,
        {
          ...row(id),
          confirmedAbsence: authorized
            ? "absent_within_authorized_scope"
            : "unknown",
        },
      ]),
    ),
  };
}
class Source implements EngineReadView<Value> {
  observerErrors = 0;
  listeners = new Set<(event: SyncReadEvent<Value>) => void>();
  last?: (event: SyncReadEvent<Value>) => void;
  watchCollection(listener: (event: SyncReadEvent<Value>) => void) {
    this.last = listener;
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }
  emit(event: SyncReadEvent<Value>) {
    for (const listener of [...this.listeners]) {
      try {
        listener(event);
      } catch {
        this.observerErrors++;
      }
    }
  }
}
function setup() {
  const source = new Source();
  const receipt = {
    milestone: "local_durable",
    clientCommitId: "engine-commit",
  } as const;
  const commands = { commit: vi.fn(async (_command: string) => receipt) };
  const legend = createLegendProjection({ source, commands });
  const tanstack = createTanStackProjection({
    source,
    commands,
    id: `fixture-${Math.random()}`,
  });
  const frames = () => ({
    legend: legend.view.peek(),
    tanstack: tanstack.collection.get("metadata"),
  });
  const close = async () => {
    legend.close();
    await tanstack.close();
    expect(source.observerErrors).toBe(0);
  };
  return { source, commands, receipt, legend, tanstack, frames, close };
}

describe("shared engine projection fixtures", () => {
  it("does not revive readiness after subscriber-triggered scope invalidation", async () => {
    const f = setup();
    let invalidated = false;
    const subscription = f.tanstack.collection.subscribeChanges(() => {
      const entry = f.tanstack.collection.get("metadata");
      if (
        !invalidated &&
        entry?.kind === "metadata" &&
        entry.frame.phase === "ready"
      ) {
        invalidated = true;
        f.source.emit({
          kind: "error",
          invalidated: true,
          error: new Error("revoked during publication"),
        });
      }
    });
    f.source.emit({ kind: "snapshot", snapshot: snapshot() });
    await vi.waitFor(() => expect(f.tanstack.collection.status).toBe("error"));
    expect([...f.tanstack.collection.keys()]).toEqual(["metadata"]);
    expect(f.legend.view.peek().phase).toBe("blocked");
    expect(f.source.listeners.size).toBe(0);
    await expect(f.tanstack.commit("stale")).rejects.toThrow("blocked");
    subscription.unsubscribe();
    await f.close();
  });

  it("does not revive readiness after subscriber-triggered close", async () => {
    const f = setup();
    let closing: Promise<void> | undefined;
    const subscription = f.tanstack.collection.subscribeChanges(() => {
      const entry = f.tanstack.collection.get("metadata");
      if (
        !closing &&
        entry?.kind === "metadata" &&
        entry.frame.phase === "ready"
      )
        closing = f.tanstack.close();
    });
    f.source.emit({ kind: "snapshot", snapshot: snapshot() });
    await vi.waitFor(() => expect(closing).toBeDefined());
    await closing;
    expect(f.tanstack.collection.status).toBe("cleaned-up");
    expect(f.tanstack.collection.has("record:a")).toBe(false);
    await expect(f.tanstack.commit("stale")).rejects.toThrow("closed");
    subscription.unsubscribe();
    await f.close();
  });
  it("publishes full rows and metadata together through each actual library", async () => {
    const f = setup();
    const seen: Array<[string | undefined, number]> = [];
    const stop = observe(() => {
      const current = f.legend.view.get().snapshot;
      if (current)
        seen.push([
          current.metadata.revision,
          Object.keys(current.rows).length,
        ]);
    });
    const changes: Array<Array<string>> = [];
    const subscription = f.tanstack.collection.subscribeChanges((batch) =>
      changes.push(batch.map((change) => change.key)),
    );
    f.source.emit({ kind: "snapshot", snapshot: snapshot() });
    await f.tanstack.collection.preload();
    expect(seen).toEqual([["1", 2]]);
    await vi.waitFor(() =>
      expect(changes.some((batch) => batch.length === 3)).toBe(true),
    );
    expect(changes.find((batch) => batch.length === 3)?.sort()).toEqual([
      "metadata",
      "record:a",
      "record:b",
    ]);
    expect(f.tanstack.collection.status).toBe("ready");
    expect(f.tanstack.collection.get("record:a")).toMatchObject({
      key: "record:a",
      kind: "record",
      row: row("a"),
      $synced: true,
    });
    expect(f.legend.view.peek().snapshot?.rows.a?.optimistic).toEqual(
      row("a").optimistic,
    );
    stop();
    subscription.unsubscribe();
    await f.close();
  });

  it("keeps unknown empty coverage local instead of claiming readiness", async () => {
    const f = setup();
    f.source.emit({ kind: "snapshot", snapshot: snapshot("0", [], false) });
    expect(f.legend.view.peek().phase).toBe("local");
    expect(f.tanstack.collection.status).toBe("loading");
    expect(f.tanstack.collection.size).toBe(1); // Explicit metadata, no fabricated domain row.
    f.source.emit({ kind: "snapshot", snapshot: snapshot("1", []) });
    expect(f.legend.view.peek().phase).toBe("ready");
    expect(f.tanstack.collection.status).toBe("ready");
    await f.close();
  });

  it("applies a complete multirow delta and overflow replacement without stale rows", async () => {
    const f = setup(),
      first = snapshot(),
      next = snapshot("2", ["b", "c"]);
    f.source.emit({ kind: "snapshot", snapshot: first });
    const notifications: string[][] = [];
    const subscription = f.tanstack.collection.subscribeChanges(
      (changes) => notifications.push(changes.map((change) => change.key)),
      { includeInitialState: true },
    );
    await vi.waitFor(() => expect(notifications.flat()).toContain("record:a"));
    notifications.length = 0;
    f.source.emit({
      kind: "batch",
      snapshot: next,
      batch: {
        protocolVersion: 1,
        beforeRevision: "1",
        revision: "2",
        metadata: next.metadata,
        changes: [
          { kind: "remove", recordId: "a" },
          { kind: "upsert", row: row("c") },
        ],
      },
    });
    expect(notifications).toHaveLength(1);
    expect(notifications[0]?.sort()).toEqual([
      "metadata",
      "record:a",
      "record:c",
    ]);
    expect(f.legend.view.peek().snapshot).toBe(next);
    expect(f.tanstack.collection.has("record:a")).toBe(false);
    f.source.emit({
      kind: "snapshot",
      snapshot: snapshot("9", ["replacement"]),
    });
    expect([...f.tanstack.collection.keys()].sort()).toEqual([
      "metadata",
      "record:replacement",
    ]);
    subscription.unsubscribe();
    await f.close();
  });

  it("retains dated rows on ordinary failure and withdraws them on scope rejection", async () => {
    const f = setup(),
      initial = snapshot();
    f.source.emit({ kind: "snapshot", snapshot: initial });
    f.source.emit({
      kind: "error",
      error: new Error("read failed"),
      invalidated: false,
    });
    expect(f.legend.view.peek()).toMatchObject({
      phase: "error",
      snapshot: initial,
    });
    expect(f.tanstack.collection.has("record:a")).toBe(true);
    f.source.emit({
      kind: "error",
      error: new Error("quarantine"),
      invalidated: true,
    });
    expect(f.legend.view.peek()).toMatchObject({ phase: "blocked" });
    expect(f.legend.view.peek().snapshot).toBeUndefined();
    expect([...f.tanstack.collection.keys()]).toEqual(["metadata"]);
    expect(f.tanstack.collection.status).toBe("error");
    expect(f.source.listeners.size).toBe(0);
    await expect(f.legend.commit("edit")).rejects.toThrow("blocked");
    await expect(f.tanstack.commit("edit")).rejects.toThrow("blocked");
    f.source.last?.({ kind: "snapshot", snapshot: initial });
    expect(f.tanstack.collection.has("record:a")).toBe(false);
    await f.close();
  });

  it("delegates commands exactly once and waits for engine events to update views", async () => {
    const f = setup();
    f.source.emit({ kind: "snapshot", snapshot: snapshot() });
    expect(await f.legend.commit("edit-a")).toBe(f.receipt);
    expect(await f.tanstack.commit("edit-b")).toBe(f.receipt);
    expect(f.commands.commit.mock.calls).toEqual([["edit-a"], ["edit-b"]]);
    expect(f.legend.view.peek().snapshot?.metadata.revision).toBe("1");
    f.commands.commit.mockRejectedValueOnce(
      new Error("unknown native outcome"),
    );
    await expect(f.legend.commit("uncertain")).rejects.toThrow(
      "unknown native outcome",
    );
    expect(f.commands.commit).toHaveBeenCalledTimes(3);
    expect("set" in f.legend.view).toBe(false);
    expect(() =>
      f.tanstack.collection.insert({
        key: "invented",
        kind: "record",
        row: row("invented"),
      }),
    ).toThrow();
    expect(f.commands.commit).toHaveBeenCalledTimes(3);
    await f.close();
  });

  it("disposes only adapter subscriptions and ignores late callbacks", async () => {
    const f = setup();
    f.source.emit({ kind: "snapshot", snapshot: snapshot() });
    await f.close();
    await f.close();
    expect(f.source.listeners.size).toBe(0);
    expect(f.legend.view.peek()).toEqual({ phase: "closed" });
    f.source.last?.({ kind: "snapshot", snapshot: snapshot("99") });
    expect(f.legend.view.peek()).toEqual({ phase: "closed" });
    await expect(f.legend.commit("late")).rejects.toThrow("closed");
    await expect(f.tanstack.commit("late")).rejects.toThrow("closed");
    expect(f.commands.commit).not.toHaveBeenCalled();
  });

  it("keeps structural record ids and pending/conflict metadata lossless", async () => {
    const f = setup(),
      current = snapshot("9007199254740993", [
        "__proto__",
        "$surreal",
        "metadata",
      ]);
    f.source.emit({ kind: "snapshot", snapshot: current });
    expect(
      Object.keys(f.legend.view.peek().snapshot?.rows ?? {}).sort(),
    ).toEqual(["$surreal", "__proto__", "metadata"]);
    expect(f.tanstack.collection.get("record:metadata")?.kind).toBe("record");
    expect(f.legend.view.peek().snapshot?.metadata.revision).toBe(
      "9007199254740993",
    );
    expect(f.tanstack.collection.get("record:__proto__")).toMatchObject({
      row: { pending: row("__proto__").pending },
    });
    await f.close();
  });

  it("does not retain ready status when a fresh snapshot has unknown coverage", async () => {
    const f = setup();
    f.source.emit({ kind: "snapshot", snapshot: snapshot() });
    f.source.emit({ kind: "snapshot", snapshot: snapshot("0", [], false) });
    expect(f.legend.view.peek().phase).toBe("local");
    expect(f.tanstack.collection.status).toBe("error");
    expect([...f.tanstack.collection.keys()]).toEqual(["metadata"]);
    f.source.emit({ kind: "snapshot", snapshot: snapshot("1", []) });
    expect(f.tanstack.collection.status).toBe("ready");
    await f.close();
  });

  it("keeps tombstones, pending deletes, conflicts and epoch-unknown receipts explicit", async () => {
    const f = setup(),
      initial = snapshot();
    const retained: SyncReadRow<Value> = {
      ...row("a"),
      confirmed: { state: "tombstone", version: "9007199254740993" },
      confirmedAbsence: "not_absent",
      optimistic: { state: "deleted", baseVersion: "9007199254740993" },
      conflicts: [
        {
          identity: { clientCommitId: "conflict", fingerprint: "cfp" },
          resolution: null,
        },
      ],
      accepted: [
        {
          identity: { clientCommitId: "ack", fingerprint: "afp" },
          sequence: "9007199254740993",
          status: "awaiting_pull_epoch_unknown",
        },
      ],
    };
    const installed = { ...initial, rows: { a: retained } };
    f.source.emit({ kind: "snapshot", snapshot: installed });
    expect(f.legend.view.peek().snapshot?.rows.a).toEqual(retained);
    const entry = f.tanstack.collection.get("record:a");
    expect(entry?.kind).toBe("record");
    if (entry?.kind === "record") expect(entry.row).toEqual(retained);
    await f.close();
  });

  it("releases a source that rejects scope synchronously during subscription", async () => {
    const unsubscribed = vi.fn();
    const source: EngineReadView<Value> = {
      watchCollection(listener) {
        listener({
          kind: "error",
          invalidated: true,
          error: new Error("scope revoked"),
        });
        return unsubscribed;
      },
    };
    const commands = { commit: vi.fn(async () => "receipt") };
    const legend = createLegendProjection({ source, commands });
    const tanstack = createTanStackProjection({
      source,
      commands,
      id: "immediate-scope-rejection",
    });
    expect(unsubscribed).toHaveBeenCalledTimes(2);
    expect(legend.view.peek().phase).toBe("blocked");
    await expect(legend.commit("edit")).rejects.toThrow("blocked");
    await expect(tanstack.commit("edit")).rejects.toThrow("blocked");
    expect(commands.commit).not.toHaveBeenCalled();
    legend.close();
    await tanstack.close();
  });

  it("shares one real SDK read pump across both adapters", async () => {
    const initial = snapshot();
    const readSnapshot = vi.fn(async () => ({
      snapshot: initial,
      cursor: { processEpoch: "one-handle", revision: "1" },
    }));
    const readChanges = vi.fn(() => new Promise<never>(() => {}));
    const source = new ExperimentalSyncReadStore({ readSnapshot, readChanges });
    const commands = { commit: vi.fn(async () => "receipt") };
    const legend = createLegendProjection({ source, commands });
    const tanstack = createTanStackProjection({
      source,
      commands,
      id: "shared-pump",
    });
    await vi.waitFor(() => expect(legend.view.peek().phase).toBe("ready"));
    expect(readSnapshot).toHaveBeenCalledTimes(1);
    expect(readChanges).toHaveBeenCalledTimes(1);
    legend.close();
    await tanstack.close();
    expect(source.getSnapshot()?.metadata.revision).toBe("1");
    source.close();
  });
});
