import { afterEach, describe, expect, it, vi } from "vitest";

import type { CallOptions } from "../src/client";
import { ExperimentalSyncHttpError } from "../src/sync-http";
import {
  ExperimentalSyncScheduler,
  type ExperimentalSyncConnectivity,
  type ExperimentalSyncInvalidationSource,
} from "../src/sync-scheduler";

const onceResult = { push: [], pull: {} } as never;
const pullResult = {} as never;

function adapter() {
  return {
    pull: vi.fn(async (_options?: CallOptions) => pullResult),
    syncOnce: vi.fn(async (_options?: CallOptions) => onceResult),
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

afterEach(() => {
  vi.useRealTimers();
});

describe("ExperimentalSyncScheduler", () => {
  it("starts immediately and coalesces concurrent triggers", async () => {
    const first = deferred<never>();
    const transport = adapter();
    transport.syncOnce
      .mockReturnValueOnce(first.promise)
      .mockResolvedValueOnce(onceResult);
    const scheduler = new ExperimentalSyncScheduler({ adapter: transport });

    scheduler.start();
    for (let index = 0; index < 10; index += 1) {
      scheduler.requestSync("local_mutation");
    }
    expect(transport.syncOnce).toHaveBeenCalledOnce();
    first.resolve(onceResult);
    await vi.waitFor(() => expect(transport.syncOnce).toHaveBeenCalledTimes(2));
    expect(scheduler.status.state).toBe("idle");
    scheduler.stop();
  });

  it("waits offline and runs immediately when connectivity returns", async () => {
    let listener: ((online: boolean) => void) | undefined;
    const unsubscribe = vi.fn();
    const connectivity: ExperimentalSyncConnectivity = {
      current: () => false,
      subscribe: (next) => {
        listener = next;
        return unsubscribe;
      },
    };
    const transport = adapter();
    const scheduler = new ExperimentalSyncScheduler({
      adapter: transport,
      connectivity,
    });

    scheduler.start();
    expect(scheduler.status).toEqual({ state: "offline" });
    expect(transport.syncOnce).not.toHaveBeenCalled();
    listener?.(true);
    await vi.waitFor(() => expect(transport.syncOnce).toHaveBeenCalledOnce());
    scheduler.stop();
    expect(unsubscribe).toHaveBeenCalledOnce();
  });

  it("uses bounded full-jitter retry only for retryable failures", async () => {
    vi.useFakeTimers();
    const transport = adapter();
    transport.syncOnce
      .mockRejectedValueOnce(
        new ExperimentalSyncHttpError("unavailable", "http", 503),
      )
      .mockResolvedValueOnce(onceResult);
    const scheduler = new ExperimentalSyncScheduler({
      adapter: transport,
      baseRetryMs: 100,
      maxRetryMs: 1_000,
      now: () => 1_000,
      random: () => 0.5,
    });

    scheduler.start();
    await Promise.resolve();
    await Promise.resolve();
    expect(scheduler.status).toEqual({
      state: "backoff",
      attempt: 1,
      retryAt: 1_050,
    });
    await vi.advanceTimersByTimeAsync(49);
    expect(transport.syncOnce).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(1);
    expect(transport.syncOnce).toHaveBeenCalledTimes(2);
    scheduler.stop();
  });

  it("stops retry storms for authentication and terminal protocol failures", async () => {
    const authentication = adapter();
    authentication.syncOnce.mockRejectedValue(
      new ExperimentalSyncHttpError("forbidden", "http", 403),
    );
    const authScheduler = new ExperimentalSyncScheduler({
      adapter: authentication,
    });
    authScheduler.start();
    await vi.waitFor(() =>
      expect(authScheduler.status).toEqual({
        state: "authentication_required",
        status: 403,
      }),
    );
    expect(authentication.syncOnce).toHaveBeenCalledOnce();
    authScheduler.stop();

    const protocol = adapter();
    protocol.syncOnce.mockRejectedValue(
      new ExperimentalSyncHttpError("invalid", "protocol"),
    );
    const protocolScheduler = new ExperimentalSyncScheduler({
      adapter: protocol,
    });
    protocolScheduler.start();
    await vi.waitFor(() =>
      expect(protocolScheduler.status.state).toBe("failed"),
    );
    expect(protocol.syncOnce).toHaveBeenCalledOnce();
    protocolScheduler.stop();
  });

  it("keeps terminal failures halted until an explicit recovery request", async () => {
    vi.useFakeTimers();
    let hint: (() => void) | undefined;
    const transport = adapter();
    transport.syncOnce
      .mockRejectedValueOnce(
        new ExperimentalSyncHttpError("invalid", "protocol"),
      )
      .mockResolvedValueOnce(onceResult);
    const scheduler = new ExperimentalSyncScheduler({
      adapter: transport,
      invalidations: {
        start: (onHint) => {
          hint = onHint;
          return () => undefined;
        },
      },
    });

    scheduler.start();
    await vi.waitFor(() => expect(scheduler.status.state).toBe("failed"));
    hint?.();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(transport.syncOnce).toHaveBeenCalledOnce();
    expect(transport.pull).not.toHaveBeenCalled();

    scheduler.requestSync("manual");
    await vi.waitFor(() => expect(transport.syncOnce).toHaveBeenCalledTimes(2));
    expect(scheduler.status.state).toBe("idle");
    scheduler.stop();
  });

  it("uses periodic HTTP pull when an invalidation hint is lost", async () => {
    vi.useFakeTimers();
    const transport = adapter();
    const scheduler = new ExperimentalSyncScheduler({ adapter: transport });

    scheduler.start();
    await vi.waitFor(() => expect(transport.syncOnce).toHaveBeenCalledOnce());
    await vi.advanceTimersByTimeAsync(59_999);
    expect(transport.pull).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    await vi.waitFor(() => expect(transport.pull).toHaveBeenCalledOnce());
    scheduler.stop();
  });

  it("treats invalidations as pull-only hints", async () => {
    let hint: (() => void) | undefined;
    const stopHints = vi.fn();
    const invalidations: ExperimentalSyncInvalidationSource = {
      start: (onHint) => {
        hint = onHint;
        return stopHints;
      },
    };
    const transport = adapter();
    const scheduler = new ExperimentalSyncScheduler({
      adapter: transport,
      invalidations,
    });
    scheduler.start();
    await vi.waitFor(() => expect(transport.syncOnce).toHaveBeenCalledOnce());
    hint?.();
    await vi.waitFor(() => expect(transport.pull).toHaveBeenCalledOnce());
    expect(transport.syncOnce).toHaveBeenCalledOnce();
    scheduler.stop();
    expect(stopHints).toHaveBeenCalledOnce();
  });

  it("aborts active work and removes all scheduling on stop", async () => {
    const transport = adapter();
    transport.syncOnce.mockImplementation(
      (options) =>
        new Promise<never>((_resolve, reject) => {
          options?.signal.addEventListener("abort", () =>
            reject(new ExperimentalSyncHttpError("aborted", "aborted")),
          );
        }),
    );
    const scheduler = new ExperimentalSyncScheduler({ adapter: transport });
    scheduler.start();
    await vi.waitFor(() => expect(transport.syncOnce).toHaveBeenCalledOnce());
    const signal = transport.syncOnce.mock.calls[0]?.[0]?.signal;
    scheduler.stop();
    expect(signal?.aborted).toBe(true);
    expect(scheduler.status).toEqual({ state: "stopped" });
  });

  it("ignores stale completion from a stopped lifecycle after restart", async () => {
    const oldCycle = deferred<never>();
    const newCycle = deferred<never>();
    const transport = adapter();
    transport.syncOnce
      .mockReturnValueOnce(oldCycle.promise)
      .mockReturnValueOnce(newCycle.promise);
    const scheduler = new ExperimentalSyncScheduler({ adapter: transport });

    scheduler.start();
    scheduler.stop();
    scheduler.start();
    expect(transport.syncOnce).toHaveBeenCalledTimes(2);
    oldCycle.resolve(onceResult);
    await Promise.resolve();
    await Promise.resolve();
    expect(scheduler.status.state).toBe("syncing");
    newCycle.resolve(onceResult);
    await vi.waitFor(() => expect(scheduler.status.state).toBe("idle"));
    scheduler.stop();
  });
});
