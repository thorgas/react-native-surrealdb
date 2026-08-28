import type {
  NativeSyncClientLike,
  NativeSyncStatus,
} from "./generated/surrealdb_rn_core";
import type { CallOptions } from "./client";
import {
  decodeSurrealValue,
  encodeSurrealValue,
  type SurrealScalar,
} from "./wire";

/** Lossless values accepted by the experimental protocol boundary. */
export type SyncJsonValue =
  | SurrealScalar
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
 * Payloads use the package's tagged lossless JSON bridge. Native Rust validates
 * record values against the narrower canonical protocol profile and computes
 * commit fingerprints. This API is a prototype and is not release-ready.
 */
export class ExperimentalSyncClient {
  readonly #native: NativeSyncClientLike;

  constructor(native: NativeSyncClientLike) {
    this.#native = native;
  }

  enqueue(
    commit: SyncJsonValue,
    options?: CallOptions,
  ): Promise<ExperimentalSyncStatus> {
    return this.#native.enqueue(encodeProtocolJson(commit), options);
  }

  recordPushResponse(
    response: SyncJsonValue,
    options?: CallOptions,
  ): Promise<ExperimentalSyncStatus> {
    return this.#native.recordPushResponse(
      encodeProtocolJson(response),
      options,
    );
  }

  applyPullResponse(
    response: SyncJsonValue,
    options?: CallOptions,
  ): Promise<ExperimentalSyncStatus> {
    return this.#native.applyPullResponse(
      encodeProtocolJson(response),
      options,
    );
  }

  async pending<T extends SyncJsonValue = SyncJsonValue>(
    options?: CallOptions,
  ): Promise<T[]> {
    return decodeProtocolJson<T>(await this.#native.pendingJson(options));
  }

  async conflicts<T extends SyncJsonValue = SyncJsonValue>(
    options?: CallOptions,
  ): Promise<T[]> {
    return decodeProtocolJson<T>(await this.#native.conflictsJson(options));
  }

  status(options?: CallOptions): Promise<ExperimentalSyncStatus> {
    return this.#native.status(options);
  }

  close(options?: CallOptions): Promise<void> {
    return this.#native.close(options);
  }

  get isClosed(): boolean {
    return this.#native.isClosed();
  }
}

function encodeProtocolJson(value: SyncJsonValue): string {
  return encodeSurrealValue(value);
}

function decodeProtocolJson<T>(values: ReadonlyArray<string>): T[] {
  return values.map((value) => decodeSurrealValue(value) as T);
}
