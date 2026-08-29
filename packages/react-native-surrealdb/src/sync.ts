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
    return decodeProtocolJson<T>(await this.pendingProtocolJson(options));
  }

  /** @internal Exact tagged JSON used by the native HTTP codec bridge. */
  pendingProtocolJson(options?: CallOptions): Promise<string[]> {
    return this.#native.pendingJson(options);
  }

  /** @internal Applies one decoded HTTP response without a lossy JS round trip. */
  recordPushResponseProtocolJson(
    responseJson: string,
    options?: CallOptions,
  ): Promise<ExperimentalSyncStatus> {
    return this.#native.recordPushResponse(responseJson, options);
  }

  /** @internal Applies one decoded pull response without a lossy JS round trip. */
  applyPullResponseProtocolJson(
    responseJson: string,
    options?: CallOptions,
  ): Promise<ExperimentalSyncStatus> {
    return this.#native.applyPullResponse(responseJson, options);
  }

  async conflicts<T extends SyncJsonValue = SyncJsonValue>(
    options?: CallOptions,
  ): Promise<T[]> {
    return decodeProtocolJson<T>(await this.#native.conflictsJson(options));
  }

  /** Token from the last complete pull persisted with native client state. */
  checkpointToken(options?: CallOptions): Promise<string | undefined> {
    return this.#native.checkpointToken(options);
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
