import {
  LiveAction as NativeLiveAction,
  SurrealRnError,
  SurrealRnError_Tags,
  connect as nativeConnect,
  type ConnectOptions,
  type LiveQueryLike,
  type SurrealDatabaseLike,
} from "./native";
import {
  decodeSurrealValue,
  encodeQueryVariables,
  type QueryVariables,
  type SurrealValue,
} from "./wire";

export { SurrealRnError, SurrealRnError_Tags };
export type { ConnectOptions };

export type CallOptions = { signal: AbortSignal };

export type QueryStatement<T = SurrealValue> = {
  statementIndex: number;
  value: T;
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

    return results.map(({ statementIndex, valueJson }) => ({
      statementIndex,
      value: decodeSurrealValue(valueJson) as T,
    }));
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
