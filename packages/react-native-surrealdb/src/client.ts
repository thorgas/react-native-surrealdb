import {
  LiveAction as NativeLiveAction,
  NativeOutputEncoding,
  NativeSyncError,
  NativeSyncError_Tags,
  SurrealRnError,
  SurrealRnError_Tags,
  benchmarkBoundaryNoop as nativeBenchmarkBoundaryNoop,
  connect as nativeConnect,
  decodeSyncPullResponse as nativeDecodeSyncPullResponse,
  decodeSyncPushResponse as nativeDecodeSyncPushResponse,
  encodeSyncPullRequest as nativeEncodeSyncPullRequest,
  encodeSyncPushRequest as nativeEncodeSyncPushRequest,
  openSyncClient as nativeOpenSyncClient,
  type ConnectOptions,
  type LiveQueryLike,
  type NativeBatchQuery,
  type NativeProfiledQueryResult,
  type SurrealDatabaseLike,
  type SurrealTransactionLike as NativeSurrealTransactionLike,
} from "./native";
import {
  decodeSurrealValue,
  encodeQueryVariables,
  type QueryVariables,
  type SurrealDecodeMode,
  type SurrealValue,
} from "./wire";
import { LiveSubscription } from "./subscription";
import {
  ExperimentalSyncClient,
  type ExperimentalSyncOpenOptions,
} from "./sync";
import {
  experimentalCanonicalCborSyncHttpCodec,
  type ExperimentalSyncHttpCodec,
} from "./sync-http";

export {
  NativeSyncError,
  NativeSyncError_Tags,
  SurrealRnError,
  SurrealRnError_Tags,
};
export type { ConnectOptions };

/** Create the native, bounded `surrealdb-sync/1` canonical CBOR codec. */
export function createExperimentalCanonicalCborSyncHttpCodec(): ExperimentalSyncHttpCodec {
  return experimentalCanonicalCborSyncHttpCodec({
    encodeSyncPushRequest: nativeEncodeSyncPushRequest,
    decodeSyncPushResponse: nativeDecodeSyncPushResponse,
    encodeSyncPullRequest: nativeEncodeSyncPullRequest,
    decodeSyncPullResponse: nativeDecodeSyncPullResponse,
  });
}
export { LiveSubscription } from "./subscription";
export type {
  LiveSubscriptionSnapshot,
  LiveSubscriptionStatus,
} from "./subscription";

export type CallOptions = { signal: AbortSignal };
export type QueryProfileOptions = {
  signal?: AbortSignal;
  decodeMode?: SurrealDecodeMode;
  nativeOutputEncoding?: "tree" | "streaming";
};

export type QueryStatement<T = SurrealValue> = {
  statementIndex: number;
  value: T;
};

export type BatchQuery = {
  surql: string;
  variables?: QueryVariables;
};

export type BatchQueryResult<T = SurrealValue> = {
  queryIndex: number;
  results: Array<QueryStatement<T>>;
};

export type QueryProfile = {
  inputEncodeMs: number;
  nativeInputDecodeMs: number;
  engineMs: number;
  nativeOutputEncodeMs: number;
  bindingAndSchedulingMs: number;
  outputDecodeMs: number;
  totalMs: number;
};

export type ProfiledQuery<T = SurrealValue> = {
  results: Array<QueryStatement<T>>;
  profile: QueryProfile;
};

export type LiveAction = "create" | "update" | "delete" | "error" | "unknown";

export type LiveNotification<T = SurrealValue> = {
  queryId: string;
  action: LiveAction;
  value: T;
};

export class LiveQuery<T = SurrealValue> implements AsyncIterable<
  LiveNotification<T>
> {
  readonly #native: LiveQueryLike;

  constructor(native: LiveQueryLike) {
    this.#native = native;
  }

  async next(options?: CallOptions): Promise<LiveNotification<T> | undefined> {
    const notification = await this.#native.next(options);
    if (!notification) return undefined;
    return {
      queryId: notification.queryId,
      action: decodeLiveAction(notification.action),
      value: decodeSurrealValue(notification.valueJson) as T,
    };
  }

  close(options?: CallOptions) {
    return this.#native.close(options);
  }

  get isClosed() {
    return this.#native.isClosed();
  }

  async *[Symbol.asyncIterator](): AsyncGenerator<LiveNotification<T>> {
    try {
      while (true) {
        const notification = await this.next();
        if (!notification) return;
        yield notification;
      }
    } finally {
      await this.close();
    }
  }
}

/**
 * A JavaScript handle for one native SurrealDB transaction.
 *
 * Queries execute immediately and share the native transaction ID. Commit and
 * cancel are idempotent; no new queries are accepted after either operation.
 */
export class SurrealTransaction {
  readonly #native: NativeSurrealTransactionLike;
  readonly #options?: CallOptions;

  constructor(native: NativeSurrealTransactionLike, options?: CallOptions) {
    this.#native = native;
    this.#options = options;
  }

  async query<T = SurrealValue>(
    surql: string,
    variables?: QueryVariables,
    options?: CallOptions,
  ): Promise<Array<QueryStatement<T>>> {
    const results = await this.#native.query(
      surql,
      variables === undefined ? undefined : encodeQueryVariables(variables),
      options ?? this.#options,
    );

    return decodeQueryResults<T>(results);
  }

  /**
   * Execute multiple parameterized queries inside this transaction using one
   * asynchronous native call. Query order and per-query statement results are
   * preserved.
   */
  async queryBatch<T = SurrealValue>(
    queries: readonly BatchQuery[],
  ): Promise<Array<BatchQueryResult<T>>> {
    const nativeQueries: NativeBatchQuery[] = queries.map(
      ({ surql, variables }) => ({
        surql,
        variablesJson:
          variables === undefined ? undefined : encodeQueryVariables(variables),
      }),
    );
    const batchResults = await this.#native.queryBatch(
      nativeQueries,
      this.#options,
    );
    return batchResults.map(({ queryIndex, results }) => ({
      queryIndex,
      results: decodeQueryResults<T>(results),
    }));
  }

  /**
   * Execute one parameterized query for every variables object using one
   * asynchronous native call. Intended for `RETURN NONE` bulk writes.
   */
  executeBatch(
    surql: string,
    variables: readonly QueryVariables[],
  ): Promise<number> {
    return this.#native.executeBatch(
      surql,
      variables.map(encodeQueryVariables),
      this.#options,
    );
  }

  /** Execute a query with benchmark attribution; do not use for production telemetry. */
  async queryProfiled<T = SurrealValue>(
    surql: string,
    variables?: QueryVariables,
    options?: QueryProfileOptions,
  ): Promise<ProfiledQuery<T>> {
    const inputStarted = performance.now();
    const encodedVariables =
      variables === undefined ? undefined : encodeQueryVariables(variables);
    const inputEncodeMs = performance.now() - inputStarted;
    const nativeStarted = performance.now();
    const profiled = await this.#native.queryProfiledWithEncoding(
      surql,
      encodedVariables,
      nativeOutputEncoding(options?.nativeOutputEncoding),
      options?.signal ? { signal: options.signal } : this.#options,
    );
    const nativeCallMs = performance.now() - nativeStarted;
    return decodeProfiledQuery<T>(
      profiled,
      inputEncodeMs,
      nativeCallMs,
      options?.decodeMode,
    );
  }

  commit(options?: CallOptions) {
    return this.#native.commit(options ?? this.#options);
  }

  cancel(options?: CallOptions) {
    return this.#native.cancel(options ?? this.#options);
  }

  get isClosed() {
    return this.#native.isClosed();
  }
}

/**
 * A stable, hand-written facade over generated UniFFI bindings.
 *
 * This layer owns the lossless wire codec and keeps generated native types out
 * of application code, allowing bindings to be regenerated independently.
 */
export class SurrealClient {
  readonly #native: SurrealDatabaseLike;

  constructor(native: SurrealDatabaseLike) {
    this.#native = native;
  }

  async query<T = SurrealValue>(
    surql: string,
    variables?: QueryVariables,
    options?: CallOptions,
  ): Promise<Array<QueryStatement<T>>> {
    const results = await this.#native.query(
      surql,
      variables === undefined ? undefined : encodeQueryVariables(variables),
      options,
    );

    return decodeQueryResults<T>(results);
  }

  /** Execute a query with benchmark attribution; do not use for production telemetry. */
  async queryProfiled<T = SurrealValue>(
    surql: string,
    variables?: QueryVariables,
    options?: QueryProfileOptions,
  ): Promise<ProfiledQuery<T>> {
    const inputStarted = performance.now();
    const encodedVariables =
      variables === undefined ? undefined : encodeQueryVariables(variables);
    const inputEncodeMs = performance.now() - inputStarted;
    const nativeStarted = performance.now();
    const profiled = await this.#native.queryProfiledWithEncoding(
      surql,
      encodedVariables,
      nativeOutputEncoding(options?.nativeOutputEncoding),
      options?.signal ? { signal: options.signal } : undefined,
    );
    const nativeCallMs = performance.now() - nativeStarted;
    return decodeProfiledQuery<T>(
      profiled,
      inputEncodeMs,
      nativeCallMs,
      options?.decodeMode,
    );
  }

  /** Begin a manually managed transaction that must be committed or cancelled. */
  async beginTransaction(options?: CallOptions): Promise<SurrealTransaction> {
    return new SurrealTransaction(
      await this.#native.beginTransaction(options),
      options,
    );
  }

  /**
   * Run a callback in one native transaction.
   *
   * Resolving commits and returns the callback value. Throwing cancels and
   * rethrows the original callback error.
   */
  async transaction<T>(
    run: (transaction: SurrealTransaction) => Promise<T>,
    options?: CallOptions,
  ): Promise<T> {
    const transaction = await this.beginTransaction(options);
    try {
      const result = await run(transaction);
      await transaction.commit();
      return result;
    } catch (error) {
      try {
        await transaction.cancel();
      } catch {
        // Preserve the callback or commit error that caused the rollback.
      }
      throw error;
    }
  }

  async live<T = SurrealValue>(
    surql: string,
    variables?: QueryVariables,
    options?: CallOptions,
  ): Promise<LiveQuery<T>> {
    const liveQuery = await this.#native.liveQuery(
      surql,
      variables === undefined ? undefined : encodeQueryVariables(variables),
      options,
    );
    return new LiveQuery<T>(liveQuery);
  }

  /**
   * Start a multicast subscription backed by one native live query.
   *
   * Consumers can listen imperatively or use the React integration. Closing
   * the subscription cancels the server-side live query.
   */
  async subscribe<T = SurrealValue>(
    surql: string,
    variables?: QueryVariables,
    options?: CallOptions,
  ): Promise<LiveSubscription<T>> {
    return new LiveSubscription(await this.live<T>(surql, variables, options));
  }

  /** Open the unreleased, transport-free sync protocol prototype. */
  async openExperimentalSync(
    options: ExperimentalSyncOpenOptions,
    callOptions?: CallOptions,
  ): Promise<ExperimentalSyncClient> {
    return new ExperimentalSyncClient(
      await nativeOpenSyncClient(this.#native, options, callOptions),
    );
  }

  use(namespace: string, database: string, options?: CallOptions) {
    return this.#native.useNamespaceDatabase(namespace, database, options);
  }

  authenticate(accessToken: string, options?: CallOptions) {
    return this.#native.authenticate(accessToken, options);
  }

  signInRoot(username: string, password: string, options?: CallOptions) {
    return this.#native.signInRoot(username, password, options);
  }

  signInDatabase(
    namespace: string,
    database: string,
    username: string,
    password: string,
    options?: CallOptions,
  ) {
    return this.#native.signInDatabase(
      namespace,
      database,
      username,
      password,
      options,
    );
  }

  invalidate(options?: CallOptions) {
    return this.#native.invalidate(options);
  }

  close(options?: CallOptions) {
    return this.#native.close(options);
  }

  get isClosed() {
    return this.#native.isClosed();
  }
}

function decodeQueryResults<T>(
  results: ReadonlyArray<{ statementIndex: number; valueJson: string }>,
  decodeMode: SurrealDecodeMode = "in-place",
): Array<QueryStatement<T>> {
  return results.map(({ statementIndex, valueJson }) => ({
    statementIndex,
    value: decodeSurrealValue(valueJson, decodeMode) as T,
  }));
}

function decodeProfiledQuery<T>(
  profiled: NativeProfiledQueryResult,
  inputEncodeMs: number,
  nativeCallMs: number,
  decodeMode: SurrealDecodeMode = "in-place",
): ProfiledQuery<T> {
  const outputStarted = performance.now();
  const results = decodeQueryResults<T>(profiled.results, decodeMode);
  const outputDecodeMs = performance.now() - outputStarted;
  const nativeInputDecodeMs = nanosecondsToMilliseconds(
    profiled.timing.inputDecodeNs,
  );
  const engineMs = nanosecondsToMilliseconds(profiled.timing.engineNs);
  const nativeOutputEncodeMs = nanosecondsToMilliseconds(
    profiled.timing.outputEncodeNs,
  );
  const bindingAndSchedulingMs = Math.max(
    0,
    nativeCallMs - nativeInputDecodeMs - engineMs - nativeOutputEncodeMs,
  );

  return {
    results,
    profile: {
      inputEncodeMs,
      nativeInputDecodeMs,
      engineMs,
      nativeOutputEncodeMs,
      bindingAndSchedulingMs,
      outputDecodeMs,
      totalMs: inputEncodeMs + nativeCallMs + outputDecodeMs,
    },
  };
}

function nanosecondsToMilliseconds(value: bigint): number {
  return Number(value) / 1_000_000;
}

function nativeOutputEncoding(
  encoding: QueryProfileOptions["nativeOutputEncoding"] = "streaming",
): NativeOutputEncoding {
  return encoding === "tree"
    ? NativeOutputEncoding.Tree
    : NativeOutputEncoding.Streaming;
}

/** Measure the minimum async UniFFI/JSI round-trip cost without database work. */
export async function benchmarkNativeBoundary(iterations: number): Promise<{
  iterations: number;
  totalMs: number;
  averageMs: number;
}> {
  if (!Number.isSafeInteger(iterations) || iterations < 1) {
    throw new RangeError("iterations must be a positive integer");
  }
  const started = performance.now();
  for (let index = 0; index < iterations; index += 1) {
    if (!(await nativeBenchmarkBoundaryNoop())) {
      throw new Error("native benchmark boundary no-op returned false");
    }
  }
  const totalMs = performance.now() - started;
  return { iterations, totalMs, averageMs: totalMs / iterations };
}

export async function connect(
  options: ConnectOptions,
  callOptions?: CallOptions,
): Promise<SurrealClient> {
  return new SurrealClient(await nativeConnect(options, callOptions));
}

function decodeLiveAction(action: NativeLiveAction): LiveAction {
  switch (action) {
    case NativeLiveAction.Create:
      return "create";
    case NativeLiveAction.Update:
      return "update";
    case NativeLiveAction.Delete:
      return "delete";
    case NativeLiveAction.Error:
      return "error";
    default:
      return "unknown";
  }
}
