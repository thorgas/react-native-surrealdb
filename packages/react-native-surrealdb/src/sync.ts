import type {
  NativeSyncClientLike,
  NativeSyncStatus,
} from "./generated/surrealdb_rn_core";

export type SyncCallOptions = { signal: AbortSignal };

/** JSON values accepted by the prototype protocol boundary. */
export type SyncJsonValue =
  | null
  | boolean
  | number
  | string
  | ReadonlyArray<SyncJsonValue>
  | { readonly [key: string]: SyncJsonValue };

export type ExperimentalSyncOpenOptions = {
  partitionId: string;
  clientId: string;
  requestedScope: string;
  subscriptionRevision: bigint;
};

export type ExperimentalSyncStatus = NativeSyncStatus;

/**
 * Experimental transport-free facade over the native durable sync runtime.
 *
 * Protocol payloads are JSON-compatible values, not the package's lossless
 * SurrealDB value codec. This API is a prototype and is not release-ready.
 */
export class ExperimentalSyncClient {
  readonly #native: NativeSyncClientLike;

  constructor(native: NativeSyncClientLike) {
    this.#native = native;
  }

  enqueue(
    commit: SyncJsonValue,
    options?: SyncCallOptions,
  ): Promise<ExperimentalSyncStatus> {
    return this.#native.enqueue(encodeProtocolJson(commit), options);
  }

  recordPushResponse(
    response: SyncJsonValue,
    options?: SyncCallOptions,
  ): Promise<ExperimentalSyncStatus> {
    return this.#native.recordPushResponse(
      encodeProtocolJson(response),
      options,
    );
  }

  applyPullResponse(
    response: SyncJsonValue,
    options?: SyncCallOptions,
  ): Promise<ExperimentalSyncStatus> {
    return this.#native.applyPullResponse(
      encodeProtocolJson(response),
      options,
    );
  }

  async pending<T extends SyncJsonValue = SyncJsonValue>(
    options?: SyncCallOptions,
  ): Promise<T[]> {
    return decodeProtocolJson<T>(await this.#native.pendingJson(options));
  }

  async conflicts<T extends SyncJsonValue = SyncJsonValue>(
    options?: SyncCallOptions,
  ): Promise<T[]> {
    return decodeProtocolJson<T>(await this.#native.conflictsJson(options));
  }

  status(options?: SyncCallOptions): Promise<ExperimentalSyncStatus> {
    return this.#native.status(options);
  }

  close(options?: SyncCallOptions): Promise<void> {
    return this.#native.close(options);
  }

  get isClosed(): boolean {
    return this.#native.isClosed();
  }
}

function encodeProtocolJson(value: SyncJsonValue): string {
  const encoded = JSON.stringify(value);
  if (encoded === undefined) {
    throw new TypeError("sync protocol value must be JSON serializable");
  }
  return encoded;
}

function decodeProtocolJson<T>(values: ReadonlyArray<string>): T[] {
  return values.map((value) => JSON.parse(value) as T);
}
