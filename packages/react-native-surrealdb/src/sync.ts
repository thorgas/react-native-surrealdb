import {
  bindExperimentalBootstrap,
  experimentalBootstrapCapabilities,
  type ExperimentalBootstrapContext,
  type ExperimentalBootstrapSession,
  type ExperimentalBootstrapCapabilities,
} from "./sync-bootstrap";
import type {
  NativeSyncClientLike,
  NativeSyncStatus,
  NativeSyncDiagnostics,
} from "./generated/surrealdb_rn_core";
import { NativeConflictPolicy } from "./generated/surrealdb_rn_core";
import type { CallOptions } from "./client";
import {
  decodeSurrealValue,
  encodeSurrealValue,
  type SurrealScalar,
} from "./wire";

import {
  decodeSyncReadBootstrap,
  decodeSyncReadChanges,
  decodeSyncReadCapabilities,
} from "./sync-read-codec";
import {
  ExperimentalSyncReadStore,
  syncReadScopeError,
  readSyncRecord,
  type SyncReadBootstrap,
  type SyncReadChanges,
  type SyncReadCursor,
  type SyncReadCapabilities,
  type SyncReadRecord,
  type SyncScopeTransitionDecision,
  type SyncReadScopeError,
} from "./sync-read";

import {
  ExperimentalSyncWindowClient,
  type NativeWindowPort,
} from "./sync-window";

interface NativeReadPort {
  readCapabilitiesJson(): string;
  readSnapshotJson(options?: CallOptions): Promise<string>;
  readChangesJson(
    cursorJson: string,
    maxBatches: number,
    options?: CallOptions,
  ): Promise<string>;
  requiresAuthorizedReset(options?: CallOptions): Promise<boolean>;
  scopeTransitionDecision(
    scope: string,
    subscriptionRevision: bigint,
    options?: CallOptions,
  ): Promise<string>;
}
function missingReadAbi(): never {
  throw new Error(
    "Sync read protocol v1 requires matching rebuilt native binaries; update the native build and its runtime fingerprint",
  );
}

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
  readonly #native: NativeSyncClientLike &
    Partial<NativeReadPort> &
    Partial<NativeWindowPort>;
  #readCapabilities?: SyncReadCapabilities;
  readonly #readStores = new Set<ExperimentalSyncReadStore>();
  readonly #windowClients = new Set<ExperimentalSyncWindowClient>();
  #scopeBlocked?: SyncReadScopeError;
  #closed = false;

  constructor(
    native: NativeSyncClientLike &
      Partial<NativeReadPort> &
      Partial<NativeWindowPort>,
  ) {
    this.#native = native;
  }

  /** Additive opaque-byte bootstrap; never falls back to v1 reset/open. */
  bootstrapCapabilities(): ExperimentalBootstrapCapabilities {
    return experimentalBootstrapCapabilities();
  }
  bindBootstrapSession(
    expected: ExperimentalBootstrapContext,
    options?: CallOptions,
  ): Promise<ExperimentalBootstrapSession> {
    return bindExperimentalBootstrap(
      this.#native,
      () => this.isClosed,
      expected,
      options,
    );
  }

  #readFailure(error: unknown): unknown {
    const blocked = syncReadScopeError(error);
    if (blocked) {
      this.#scopeBlocked = blocked;
      for (const store of [...this.#readStores]) store.invalidateScope(blocked);
      for (const client of [...this.#windowClients])
        client.invalidateScope(blocked);
    }
    return blocked ?? error;
  }

  /** Separate ordered-window capability; full-scope v1 semantics remain unchanged. */
  createWindowClient(): ExperimentalSyncWindowClient {
    const client = new ExperimentalSyncWindowClient(this.#native, (error) =>
      this.#readFailure(error),
    );
    client.capabilities();
    if (this.#scopeBlocked) client.invalidateScope(this.#scopeBlocked);
    this.#windowClients.add(client);
    return client;
  }

  /** Additive read capability; generated bindings still require a matching binary. */
  readCapabilities(): SyncReadCapabilities {
    if (!this.#readCapabilities) {
      const method = this.#native.readCapabilitiesJson;
      if (!method) return missingReadAbi();
      this.#readCapabilities = decodeSyncReadCapabilities(
        method.call(this.#native),
      );
    }
    return this.#readCapabilities;
  }

  async readSnapshot(options?: CallOptions): Promise<SyncReadBootstrap> {
    if (this.#scopeBlocked) throw this.#scopeBlocked;
    this.readCapabilities();
    const method = this.#native.readSnapshotJson;
    if (!method) return missingReadAbi();
    return decodeSyncReadBootstrap(
      await method.call(this.#native, options).catch((error) => {
        throw this.#readFailure(error);
      }),
    );
  }

  async readChanges(
    cursor: SyncReadCursor,
    maxBatches = 32,
    options?: CallOptions,
  ): Promise<SyncReadChanges> {
    if (this.#scopeBlocked) throw this.#scopeBlocked;
    this.readCapabilities();
    if (!Number.isInteger(maxBatches) || maxBatches < 1 || maxBatches > 128)
      throw new RangeError("Sync read batch limit must be between 1 and 128");
    const method = this.#native.readChangesJson;
    if (!method) return missingReadAbi();
    return decodeSyncReadChanges(
      await method
        .call(this.#native, JSON.stringify(cursor), maxBatches, options)
        .catch((error) => {
          throw this.#readFailure(error);
        }),
    );
  }

  async readRecord(
    recordId: string,
    options?: CallOptions,
  ): Promise<SyncReadRecord> {
    const { snapshot } = await this.readSnapshot(options);
    return readSyncRecord(snapshot, recordId);
  }

  /** True for legacy caches until an authorized reset establishes coverage. */
  requiresAuthorizedReset(options?: CallOptions): Promise<boolean> {
    this.readCapabilities();
    const method = this.#native.requiresAuthorizedReset;
    if (!method) return missingReadAbi();
    return method.call(this.#native, options);
  }

  async scopeTransitionDecision(
    scope: string,
    subscriptionRevision: bigint,
    options?: CallOptions,
  ): Promise<SyncScopeTransitionDecision> {
    this.readCapabilities();
    const method = this.#native.scopeTransitionDecision;
    if (!method) return missingReadAbi();
    const result = await method.call(
      this.#native,
      scope,
      subscriptionRevision,
      options,
    );
    if (
      result !== "unchanged" &&
      result !== "needs_new_scope_journal" &&
      result !== "pending_quarantine_required"
    )
      throw new Error("Unsupported native scope transition decision");
    return result;
  }

  createReadStore(options?: {
    pollMs?: number;
    maxBatches?: number;
  }): ExperimentalSyncReadStore {
    this.readCapabilities();
    const store = new ExperimentalSyncReadStore(this, options, () => {
      this.#readStores.delete(store);
    });
    if (this.#scopeBlocked) store.invalidateScope(this.#scopeBlocked);
    this.#readStores.add(store);
    return store;
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
    return this.#native
      .applyPullResponse(encodeProtocolJson(response), options)
      .catch((error) => {
        throw this.#readFailure(error);
      });
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
    return this.#native
      .applyPullResponse(responseJson, options)
      .catch((error) => {
        throw this.#readFailure(error);
      });
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

  /** Metadata-only Rust measurements; never a snapshot of application rows. */
  nativeDiagnostics(): NativeSyncDiagnostics {
    return this.#native.diagnostics();
  }

  close(options?: CallOptions): Promise<void> {
    this.#closed = true;
    for (const store of [...this.#readStores]) store.close();
    for (const client of [...this.#windowClients]) client.close();
    this.#windowClients.clear();
    return this.#native.close(options);
  }

  get isClosed(): boolean {
    return this.#closed || this.#native.isClosed();
  }
}

function encodeProtocolJson(value: SyncJsonValue): string {
  return encodeSurrealValue(value);
}

function decodeProtocolJson<T>(values: ReadonlyArray<string>): T[] {
  return values.map((value) => decodeSurrealValue(value) as T);
}
