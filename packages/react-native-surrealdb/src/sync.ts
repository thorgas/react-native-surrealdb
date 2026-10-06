import type {
  NativeSyncClientLike,
  NativeSyncStatus,
} from "./generated/surrealdb_rn_core";
import { NativeConflictPolicy } from "./generated/surrealdb_rn_core";
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

/** Local disposition after a durable authority conflict; never sent to the server. */
export type ExperimentalConflictPolicy = "manual" | "preferServerV1";

const nativeConflictPolicy = (
  policy: ExperimentalConflictPolicy,
): NativeConflictPolicy => {
  if (policy === "manual") return NativeConflictPolicy.Manual;
  if (policy === "preferServerV1") return NativeConflictPolicy.PreferServerV1;
  throw new TypeError("Unknown experimental sync conflict policy");
};

/**
 * Experimental transport-free facade over the native durable sync runtime.
 *
 * Payloads use the package's tagged lossless JSON bridge. Native Rust validates
 * record values against the narrower canonical protocol profile and computes
 * commit fingerprints. The protocol record_id is the SurrealDB row identity:
 * ordinary queries of its optimistic projection return a SurrealRecordId in
 * the row's id field, even if an upsert value supplied a separate string id.
 * Applications map their own logical IDs at the boundary. This API is a
 * prototype and is not release-ready.
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

  /** Explicit opt-in; the default response method always remains manual. */
  recordPushResponseWithPolicy(
    response: SyncJsonValue,
    policy: ExperimentalConflictPolicy,
    options?: CallOptions,
  ): Promise<ExperimentalSyncStatus> {
    return this.#native.recordPushResponseWithPolicy(
      encodeProtocolJson(response),
      nativeConflictPolicy(policy),
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

  /** Accept the current authority value and durably close a conflict. */
  resolveConflictKeepServer(
    conflictedCommitId: string,
    options?: CallOptions,
  ): Promise<ExperimentalSyncStatus> {
    return this.#native.resolveConflictKeepServer(conflictedCommitId, options);
  }

  /** Retry the complete retained local batch with a fresh commit identity. */
  resolveConflictKeepLocal(
    conflictedCommitId: string,
    replacementCommitId: string,
    options?: CallOptions,
  ): Promise<ExperimentalSyncStatus> {
    return this.#native.resolveConflictKeepLocal(
      conflictedCommitId,
      replacementCommitId,
      options,
    );
  }

  /** Queue an application-provided merged batch with a fresh commit identity. */
  resolveConflictMerge(
    conflictedCommitId: string,
    replacementCommit: SyncJsonValue,
    options?: CallOptions,
  ): Promise<ExperimentalSyncStatus> {
    return this.#native.resolveConflictMerge(
      conflictedCommitId,
      encodeProtocolJson(replacementCommit),
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

  /** @internal Applies a decoded response with an explicit local policy. */
  recordPushResponseWithPolicyProtocolJson(
    responseJson: string,
    policy: ExperimentalConflictPolicy,
    options?: CallOptions,
  ): Promise<ExperimentalSyncStatus> {
    return this.#native.recordPushResponseWithPolicy(
      responseJson,
      nativeConflictPolicy(policy),
      options,
    );
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
