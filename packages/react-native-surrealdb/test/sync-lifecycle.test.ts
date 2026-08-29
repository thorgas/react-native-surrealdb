import { describe, expect, it, vi } from "vitest";

import { ExperimentalSyncHttpError } from "../src/sync-http";
import {
  type ExperimentalSyncApplicationLifecycle,
  type ExperimentalSyncApplicationState,
  ExperimentalSyncLifecycleCoordinator,
} from "../src/sync-lifecycle";
import { ExperimentalSyncScheduler } from "../src/sync-scheduler";

function lifecycle(initial: ExperimentalSyncApplicationState = "unknown") {
  let state = initial;
  let listener: ((next: ExperimentalSyncApplicationState) => void) | undefined;
  const unsubscribe = vi.fn();
  const source: ExperimentalSyncApplicationLifecycle = {
    current: () => state,
    subscribe: (next) => {
      listener = next;
      return unsubscribe;
    },
  };
  return {
    source,
    emit(next: ExperimentalSyncApplicationState) {
      state = next;
      listener?.(next);
    },
    unsubscribe,
  };
}

function adapter() {
  return {
    pull: vi.fn(async () => undefined as never),
    syncOnce: vi.fn(async () => ({ push: [], pull: {} }) as never),
  };
}

describe("ExperimentalSyncLifecycleCoordinator", () => {
  it("fails closed until active and stops idempotently in background", async () => {
    const application = lifecycle();
    const transport = adapter();
    const scheduler = new ExperimentalSyncScheduler({ adapter: transport });
    const coordinator = new ExperimentalSyncLifecycleCoordinator({
      scheduler,
      lifecycle: application.source,
    });

    coordinator.start();
    coordinator.start();
    expect(transport.syncOnce).not.toHaveBeenCalled();
    application.emit("active");
    await vi.waitFor(() => expect(transport.syncOnce).toHaveBeenCalledOnce());
    application.emit("background");
    application.emit("inactive");
    expect(scheduler.status).toEqual({ state: "stopped" });
    coordinator.stop();
    coordinator.stop();
    expect(application.unsubscribe).toHaveBeenCalledOnce();
  });

  it("refreshes credentials once and resumes with an explicit reason", async () => {
    const application = lifecycle("active");
    const transport = adapter();
    transport.syncOnce
      .mockRejectedValueOnce(
        new ExperimentalSyncHttpError("unauthorized", "http", 401),
      )
      .mockResolvedValueOnce({ push: [], pull: {} } as never);
    const statuses: unknown[] = [];
    const scheduler = new ExperimentalSyncScheduler({ adapter: transport });
    scheduler.subscribe((status) => statuses.push(status));
    const refreshAuthentication = vi.fn(
      async (_status: 401 | 403, _options: { signal: AbortSignal }) => true,
    );
    const coordinator = new ExperimentalSyncLifecycleCoordinator({
      scheduler,
      lifecycle: application.source,
      refreshAuthentication,
    });

    coordinator.start();
    await vi.waitFor(() => expect(transport.syncOnce).toHaveBeenCalledTimes(2));
    expect(refreshAuthentication).toHaveBeenCalledOnce();
    expect(refreshAuthentication.mock.calls[0]?.[0]).toBe(401);
    expect(statuses).toContainEqual({
      state: "syncing",
      reason: "authentication_refresh",
    });
    coordinator.stop();
  });

  it("does not storm when refresh declines and allows explicit retry", async () => {
    const application = lifecycle("active");
    const transport = adapter();
    transport.syncOnce.mockRejectedValue(
      new ExperimentalSyncHttpError("forbidden", "http", 403),
    );
    const scheduler = new ExperimentalSyncScheduler({ adapter: transport });
    const refreshAuthentication = vi.fn(async () => false);
    const coordinator = new ExperimentalSyncLifecycleCoordinator({
      scheduler,
      lifecycle: application.source,
      refreshAuthentication,
    });

    coordinator.start();
    await vi.waitFor(() =>
      expect(refreshAuthentication).toHaveBeenCalledOnce(),
    );
    await Promise.resolve();
    expect(transport.syncOnce).toHaveBeenCalledOnce();
    coordinator.retryAuthentication();
    await vi.waitFor(() =>
      expect(refreshAuthentication).toHaveBeenCalledTimes(2),
    );
    expect(transport.syncOnce).toHaveBeenCalledOnce();
    coordinator.stop();
  });

  it("aborts refresh and ignores stale completion after backgrounding", async () => {
    const application = lifecycle("active");
    const transport = adapter();
    transport.syncOnce.mockRejectedValue(
      new ExperimentalSyncHttpError("unauthorized", "http", 401),
    );
    let finish!: (changed: boolean) => void;
    let refreshSignal: AbortSignal | undefined;
    const scheduler = new ExperimentalSyncScheduler({ adapter: transport });
    const coordinator = new ExperimentalSyncLifecycleCoordinator({
      scheduler,
      lifecycle: application.source,
      refreshAuthentication: (_status, options) => {
        refreshSignal = options.signal;
        return new Promise((resolve) => {
          finish = resolve;
        });
      },
    });

    coordinator.start();
    await vi.waitFor(() => expect(refreshSignal).toBeDefined());
    application.emit("background");
    expect(refreshSignal?.aborted).toBe(true);
    finish(true);
    await Promise.resolve();
    await Promise.resolve();
    expect(transport.syncOnce).toHaveBeenCalledOnce();
    coordinator.stop();
  });
});
