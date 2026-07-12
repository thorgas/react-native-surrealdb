import {
  SurrealRnError,
  SurrealRnError_Tags,
  connect as nativeConnect,
  type ConnectOptions,
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
