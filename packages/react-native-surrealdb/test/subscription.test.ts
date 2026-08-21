import { describe, expect, it, vi } from "vitest";

import type { LiveNotification } from "../src/client";
import { LiveSubscription } from "../src/subscription";
import type { SurrealValue } from "../src/wire";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, reject, resolve };
}

function createLiveQuery<T = SurrealValue>() {
  const pulls: Array<
    ReturnType<typeof deferred<LiveNotification<T> | undefined>>
  > = [];
  const query = {
    close: vi.fn(async () => {
      pulls.at(-1)?.resolve(undefined);
    }),
    next: vi.fn(() => {
      const pull = deferred<LiveNotification<T> | undefined>();
      pulls.push(pull);
      return pull.promise;
    }),
  };
  return { pulls, query };
}

describe("LiveSubscription", () => {
  it("multicasts notifications while consuming the native stream once", async () => {
    const { pulls, query } = createLiveQuery<{ ready: boolean }>();
    const subscription = new LiveSubscription<{ ready: boolean }>(query);
    const first = vi.fn();
    const second = vi.fn();
    subscription.onNotification(first);
    subscription.onNotification(second);

    const notification: LiveNotification<{ ready: boolean }> = {
      action: "create",
      queryId: "query-id",
      value: { ready: true },
    };
    pulls[0]?.resolve(notification);
    await vi.waitFor(() => expect(first).toHaveBeenCalledOnce());

    expect(first).toHaveBeenCalledWith(notification);
    expect(second).toHaveBeenCalledWith(notification);
    expect(subscription.getSnapshot()).toEqual({
      status: "open",
      notification,
    });
    expect(query.next).toHaveBeenCalledTimes(2);

    await subscription.close();
    expect(query.close).toHaveBeenCalled();
    expect(subscription.isClosed).toBe(true);
  });

  it("surfaces stream errors without notifying data listeners", async () => {
    const { pulls, query } = createLiveQuery();
    const subscription = new LiveSubscription(query);
    const dataListener = vi.fn();
    const storeListener = vi.fn();
    subscription.onNotification(dataListener);
    subscription.subscribe(storeListener);

    const error = new Error("stream failed");
    pulls[0]?.reject(error);
    await vi.waitFor(() => expect(storeListener).toHaveBeenCalledOnce());

    expect(dataListener).not.toHaveBeenCalled();
    expect(subscription.getSnapshot()).toEqual({ status: "error", error });
  });

  it("keeps streaming when one notification listener throws", async () => {
    const { pulls, query } = createLiveQuery();
    const subscription = new LiveSubscription(query);
    const consoleError = vi
      .spyOn(console, "error")
      .mockImplementation(() => {});
    const healthyListener = vi.fn();
    subscription.onNotification(() => {
      throw new Error("listener failed");
    });
    subscription.onNotification(healthyListener);

    pulls[0]?.resolve({
      action: "update",
      queryId: "query-id",
      value: "first",
    });
    await vi.waitFor(() => expect(healthyListener).toHaveBeenCalledOnce());

    expect(query.next).toHaveBeenCalledTimes(2);
    expect(consoleError).toHaveBeenCalledOnce();
    consoleError.mockRestore();
    await subscription.close();
  });
});
