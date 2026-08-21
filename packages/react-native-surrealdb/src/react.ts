import { useEffect, useMemo, useState, useSyncExternalStore } from "react";

import type { LiveAction, LiveNotification, SurrealClient } from "./client";
import type {
  LiveSubscription,
  LiveSubscriptionSnapshot,
} from "./subscription";
import {
  encodeQueryVariables,
  type QueryVariables,
  type SurrealValue,
} from "./wire";

export type UseLiveQueryOptions = {
  client: SurrealClient;
  surql: string;
  variables?: QueryVariables;
  /**
   * Stable identity for the subscription, like TanStack Query's `queryKey`.
   * Include every value used by `surql` and `variables`. When omitted, the
   * query text and encoded variables form the key.
   */
  queryKey?: readonly SurrealValue[];
  enabled?: boolean;
};

export type UseLiveQueryResult<T> = {
  data: T | undefined;
  notification: LiveNotification<T> | undefined;
  action: LiveAction | undefined;
  error: unknown;
  status: "pending" | "success" | "error";
  fetchStatus: "fetching" | "idle";
  isPending: boolean;
  isSuccess: boolean;
  isError: boolean;
  isClosed: boolean;
};

const pendingSnapshot: LiveSubscriptionSnapshot<never> = { status: "open" };
const subscribeNoop = () => () => {};

type SubscriptionState<T> = {
  request: object;
  subscription?: LiveSubscription<T>;
  error?: unknown;
};

/**
 * Subscribe to a SurrealDB `LIVE SELECT` and expose its latest notification
 * with TanStack Query-style status flags.
 */
export function useLiveQuery<T = SurrealValue>(
  options: UseLiveQueryOptions,
): UseLiveQueryResult<T> {
  const { client, enabled = true, queryKey, surql, variables } = options;
  const identity = encodeQueryVariables({
    key: queryKey ?? [surql, variables ?? null],
  });
  const request = useMemo(
    () => ({ client, surql, variables }),
    [client, identity, surql],
  );
  const [state, setState] = useState<SubscriptionState<T>>({ request });
  const currentState = state.request === request ? state : undefined;
  const subscription = currentState?.subscription;

  useEffect(() => {
    let active = true;
    let current: LiveSubscription<T> | undefined;

    setState({ request });
    if (enabled) {
      void request.client
        .subscribe<T>(request.surql, request.variables)
        .then((created) => {
          if (!active) {
            void created.close();
            return;
          }
          current = created;
          setState({ request, subscription: created });
        })
        .catch((error) => {
          if (active) setState({ request, error });
        });
    }

    return () => {
      active = false;
      if (current) void current.close();
    };
  }, [enabled, request]);

  const snapshot = useSyncExternalStore(
    subscription?.subscribe ?? subscribeNoop,
    subscription?.getSnapshot ?? (() => pendingSnapshot),
  ) as LiveSubscriptionSnapshot<T>;

  const error = currentState?.error ?? snapshot.error;
  const isError = error !== undefined;
  const isPending = !subscription && !isError;
  const notification = snapshot.notification;

  return {
    data: notification?.value,
    notification,
    action: notification?.action,
    error,
    status: isError ? "error" : isPending ? "pending" : "success",
    fetchStatus: enabled && isPending ? "fetching" : "idle",
    isPending,
    isSuccess: !isError && !isPending,
    isError,
    isClosed: snapshot.status === "closed",
  };
}
