import {
  encodeSyncViewRequest,
  decodeSyncViewResult,
  type SyncViewRequest,
  type SyncViewResult,
} from "./sync-view";
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
  type SurrealValue,
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
  readViewJson(requestJson: string, options?: CallOptions): Promise<string>;
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
interface NativeLanePort {
  laneCapabilitiesJson(): string;
  observeRecordJson(recordId: string, options?: CallOptions): Promise<string>;
  laneConflictSnapshotJson(recordId: string, options?: CallOptions): Promise<string | undefined>;
  laneResolutionObservationJson(snapshotJson: string, replacementIdentityJson: string, options?: CallOptions): Promise<string>;
  enqueueIntentJson(intentJson: string, observationJson: string, options?: CallOptions): Promise<NativeSyncStatus>;
  materializeNextIntent(recordId: string, identityJson: string, options?: CallOptions): Promise<NativeSyncStatus>;
  resolveLaneConflictJson(resolutionId: string, snapshotJson: string, resolutionJson: string, options?: CallOptions): Promise<NativeSyncStatus>;
  laneResolutionReceiptJson(resolutionId: string, options?: CallOptions): Promise<string>;
  acknowledgeLaneResolutionReceipt(resolutionId: string, options?: CallOptions): Promise<NativeSyncStatus>;
  readIntentRecordJson(recordId: string, options?: CallOptions): Promise<string>;
  prepareLaneResolutionJson(snapshotJson: string, choiceJson: string, options?: CallOptions): Promise<ExperimentalPreparedLaneResolution>;
  projectLaneConflictSnapshotJson(snapshotJson: string, options?: CallOptions): string;
  inspectLaneResolutionJson(snapshotJson: string, resolutionId?: string, requestJson?: string, options?: CallOptions): Promise<string>;
  materializeNextIntentWithCommitId(recordId: string, commitId: string, options?: CallOptions): Promise<NativeSyncStatus>;
}
/** Existing native clients may predate lane bindings; source proof checks them at runtime. */
export type ExperimentalSyncNativePort =
  Omit<NativeSyncClientLike, keyof NativeLanePort> &
  Partial<NativeLanePort> &
  Partial<NativeReadPort> &
  Partial<NativeWindowPort>;
export type ExperimentalLaneResolutionChoice =
  | Readonly<{ strategy: "keep_server" }>
  | Readonly<{ strategy: "keep_local" }>
  | Readonly<{ strategy: "merge"; value: SyncJsonValue; reference?: string | null }>;
export type ExperimentalPreparedLaneResolution = Readonly<{
  resolutionId: string;
  requestJson: string;
}>;
export type ExperimentalLaneResolutionInspection = Readonly<{
  state: "absent" | "unknown" | "keep_server_applied" | "awaiting_acceptance" | "awaiting_pull" | "reconciled" | "needs_review";
  receiptJson: string | null;
}>;
export type ExperimentalLaneConflictProjection = Readonly<{
  recordId: string;
  coveredIntentPrefixJson: string;
  local: SurrealValue | null;
  remote: SurrealValue | null;
  observed: SurrealValue | null;
}>;
export type ExperimentalIntentRecord = Readonly<{
  record: SyncReadRecord;
  lane: Readonly<{
    pendingIntentIds: readonly string[];
    blockedSnapshotJson: string | null;
    blockedReason: string | null;
    headIdentityJson: string | null;
    resolutionRevision: string;
  }>;
}>;
function laneAbiMissing(): never {
  throw new Error("Lane intents require matching rebuilt native source-proof bindings");
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
  readonly #native: ExperimentalSyncNativePort;
  #readCapabilities?: SyncReadCapabilities;
  readonly #readStores = new Set<ExperimentalSyncReadStore>();
  readonly #windowClients = new Set<ExperimentalSyncWindowClient>();
  #scopeBlocked?: SyncReadScopeError;
  #closed = false;
  #laneSourceProof = false;

  /** @internal Enabled only by an HTTP adapter with the source-v2 decoder. */
  enableLaneSourceProof(): void {
    const native = this.#native;
    if (
      typeof native.laneCapabilitiesJson !== "function" ||
      typeof native.observeRecordJson !== "function" ||
      typeof native.enqueueIntentJson !== "function" ||
      typeof native.materializeNextIntent !== "function" ||
      typeof native.laneConflictSnapshotJson !== "function" ||
      typeof native.laneResolutionObservationJson !== "function" ||
      typeof native.laneResolutionReceiptJson !== "function" ||
      typeof native.acknowledgeLaneResolutionReceipt !== "function" ||
      typeof native.readIntentRecordJson !== "function" ||
      typeof native.prepareLaneResolutionJson !== "function" ||
      typeof native.projectLaneConflictSnapshotJson !== "function" ||
      typeof native.inspectLaneResolutionJson !== "function" ||
      typeof native.materializeNextIntentWithCommitId !== "function" ||
      typeof native.resolveLaneConflictJson !== "function"
    ) laneAbiMissing();
    let capabilities: unknown;
    try { capabilities = JSON.parse(native.laneCapabilitiesJson()); }
    catch { laneAbiMissing(); }
    if (
      typeof capabilities !== "object" || capabilities === null ||
      !("schemaVersion" in capabilities) || capabilities.schemaVersion !== 2 ||
      !("storageFormat" in capabilities) || capabilities.storageFormat !== 5 ||
      !("pullSourceProfile" in capabilities) || capabilities.pullSourceProfile !== "pull-source-v2"
    ) laneAbiMissing();
    this.#laneSourceProof = true;
  }

  constructor(
    native: ExperimentalSyncNativePort,
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

  /** Bounded local observed counts/pages; coverage is always explicit. */
  async readView(
    request: SyncViewRequest,
    options?: CallOptions,
  ): Promise<SyncViewResult> {
    if (this.#scopeBlocked) throw this.#scopeBlocked;
    if (this.isClosed) throw new Error("Sync client is closed");
    if (options?.signal?.aborted) throw new Error("Sync view read aborted");
    const encoded = encodeSyncViewRequest(request);
    const captured = JSON.parse(encoded) as SyncViewRequest;
    const method = this.#native.readViewJson;
    if (!method) return missingReadAbi();
    const response = await method
      .call(this.#native, encoded, options)
      .catch((error) => {
        throw this.#readFailure(error);
      });
    if (this.#scopeBlocked) throw this.#scopeBlocked;
    if (this.isClosed) throw new Error("Sync client is closed");
    if (options?.signal?.aborted) throw new Error("Sync view read aborted");
    return decodeSyncViewResult(response, captured);
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

  /** Opaque observation token for a later edit of this record. */
  async observeRecord(recordId: string, options?: CallOptions): Promise<string> {
    const method = this.#native.observeRecordJson ?? laneAbiMissing();
    return method.call(this.#native, recordId, options);
  }

  async enqueueIntent(intent: SyncJsonValue, observationJson: string, options?: CallOptions): Promise<ExperimentalSyncStatus> {
    if (!this.#laneSourceProof) throw new Error("Lane intents require source-proof pull capability");
    const method = this.#native.enqueueIntentJson ?? laneAbiMissing();
    return method.call(this.#native, encodeProtocolJson(intent), observationJson, options);
  }

  materializeNextIntent(recordId: string, identity: SyncJsonValue, options?: CallOptions): Promise<ExperimentalSyncStatus> {
    if (!this.#laneSourceProof) throw new Error("Lane intents require source-proof pull capability");
    const method = this.#native.materializeNextIntent ?? laneAbiMissing();
    return method.call(this.#native, recordId, encodeProtocolJson(identity), options);
  }

  async laneConflictSnapshot(recordId: string, options?: CallOptions): Promise<string | undefined> {
    const method = this.#native.laneConflictSnapshotJson ?? laneAbiMissing();
    return method.call(this.#native, recordId, options);
  }

  /** Engine-derived phase; unknown means the original snapshot binding is unavailable. */
  laneResolutionObservation(snapshotJson: string, replacementIdentity: SyncJsonValue, options?: CallOptions): Promise<string> {
    const method = this.#native.laneResolutionObservationJson ?? laneAbiMissing();
    return method.call(this.#native, snapshotJson, encodeProtocolJson(replacementIdentity), options);
  }

  resolveLaneConflict(resolutionId: string, snapshotJson: string, resolution: SyncJsonValue, options?: CallOptions): Promise<ExperimentalSyncStatus> {
    if (!this.#laneSourceProof) throw new Error("Lane receipts require matching native source-proof bindings");
    const method = this.#native.resolveLaneConflictJson ?? laneAbiMissing();
    return method.call(this.#native, resolutionId, snapshotJson, encodeProtocolJson(resolution), options);
  }

  /** Opaque durable receipt JSON; unknown means this ID has no retained receipt. */
  laneResolutionReceipt(resolutionId: string, options?: CallOptions): Promise<string> {
    const method = this.#native.laneResolutionReceiptJson ?? laneAbiMissing();
    return method.call(this.#native, resolutionId, options);
  }

  acknowledgeLaneResolutionReceipt(resolutionId: string, options?: CallOptions): Promise<ExperimentalSyncStatus> {
    if (!this.#laneSourceProof) throw new Error("Lane receipts require matching native source-proof bindings");
    const method = this.#native.acknowledgeLaneResolutionReceipt ?? laneAbiMissing();
    return method.call(this.#native, resolutionId, options);
  }

  prepareLaneResolution(snapshotJson: string, choice: ExperimentalLaneResolutionChoice, options?: CallOptions): Promise<ExperimentalPreparedLaneResolution> {
    if (!this.#laneSourceProof) throw new Error("Lane resolution requires matching native source-proof bindings");
    const method = this.#native.prepareLaneResolutionJson ?? laneAbiMissing();
    return method.call(this.#native, snapshotJson, encodeProtocolJson(choice), options);
  }

  /** Replay the exact request persisted with the draft; no protocol JSON parse in JS. */
  submitPreparedLaneResolution(resolutionId: string, snapshotJson: string, requestJson: string, options?: CallOptions): Promise<ExperimentalSyncStatus> {
    if (!this.#laneSourceProof) throw new Error("Lane resolution requires matching native source-proof bindings");
    const method = this.#native.resolveLaneConflictJson ?? laneAbiMissing();
    return method.call(this.#native, resolutionId, snapshotJson, requestJson, options);
  }

  /** Native validates the saved snapshot, request and ID before projecting a phase. */
  async inspectLaneResolution(snapshotJson: string, attempt: { resolutionId: string; requestJson: string } | null, options?: CallOptions): Promise<ExperimentalLaneResolutionInspection> {
    if (!this.#laneSourceProof) throw new Error("Lane receipts require matching native source-proof bindings");
    const method = this.#native.inspectLaneResolutionJson ?? laneAbiMissing();
    const parsed: unknown = JSON.parse(await method.call(this.#native, snapshotJson, attempt?.resolutionId, attempt?.requestJson, options));
    if (typeof parsed !== "object" || parsed === null || !("state" in parsed) ||
        !["absent", "unknown", "keep_server_applied", "awaiting_acceptance", "awaiting_pull", "reconciled", "needs_review"].includes(String(parsed.state)) ||
        !("receiptJson" in parsed) || !(parsed.receiptJson === null || typeof parsed.receiptJson === "string"))
      throw new Error("Invalid native lane receipt projection");
    if ((parsed.state === "absent" || parsed.state === "unknown") !== (parsed.receiptJson === null))
      throw new Error("Invalid native lane receipt projection");
    return { state: parsed.state as ExperimentalLaneResolutionInspection["state"], receiptJson: parsed.receiptJson };
  }

  /** Domain values only; the full protocol snapshot stays opaque. */
  projectLaneConflictSnapshot(snapshotJson: string): ExperimentalLaneConflictProjection {
    if (!this.#laneSourceProof) throw new Error("Lane snapshots require matching native source-proof bindings");
    const method = this.#native.projectLaneConflictSnapshotJson ?? laneAbiMissing();
    const parsed: unknown = JSON.parse(method.call(this.#native, snapshotJson));
    if (typeof parsed !== "object" || parsed === null ||
        !("recordId" in parsed) || typeof parsed.recordId !== "string" ||
        !("coveredIntentPrefixJson" in parsed) || typeof parsed.coveredIntentPrefixJson !== "string" ||
        !("localJson" in parsed) || !(parsed.localJson === null || typeof parsed.localJson === "string") ||
        !("remoteJson" in parsed) || !(parsed.remoteJson === null || typeof parsed.remoteJson === "string") ||
        !("observedJson" in parsed) || !(parsed.observedJson === null || typeof parsed.observedJson === "string"))
      throw new Error("Invalid native lane snapshot projection");
    return {
      recordId: parsed.recordId,
      coveredIntentPrefixJson: parsed.coveredIntentPrefixJson,
      local: parsed.localJson === null ? null : decodeSurrealValue(parsed.localJson),
      remote: parsed.remoteJson === null ? null : decodeSurrealValue(parsed.remoteJson),
      observed: parsed.observedJson === null ? null : decodeSurrealValue(parsed.observedJson),
    };
  }

  materializeNextIntentWithCommitId(recordId: string, commitId: string, options?: CallOptions): Promise<ExperimentalSyncStatus> {
    if (!this.#laneSourceProof) throw new Error("Lane materialization requires matching native source-proof bindings");
    const method = this.#native.materializeNextIntentWithCommitId ?? laneAbiMissing();
    return method.call(this.#native, recordId, commitId, options);
  }

  /** One native revision of the optimistic row and its unmaterialized intent lane. */
  async readIntentRecord(recordId: string, options?: CallOptions): Promise<ExperimentalIntentRecord> {
    if (!this.#laneSourceProof) throw new Error("Lane reads require matching native source-proof bindings");
    const method = this.#native.readIntentRecordJson ?? laneAbiMissing();
    const parsed: unknown = JSON.parse(await method.call(this.#native, recordId, options));
    if (typeof parsed !== "object" || parsed === null ||
        !("viewJson" in parsed) || typeof parsed.viewJson !== "string" ||
        !("lane" in parsed) || typeof parsed.lane !== "object" || parsed.lane === null)
      throw new Error("Invalid native intent read");
    const lane = parsed.lane;
    if (!("pendingIntentIds" in lane) || !Array.isArray(lane.pendingIntentIds) ||
        !lane.pendingIntentIds.every((id: unknown) => typeof id === "string" && id.length > 0) ||
        !("blockedSnapshotJson" in lane) || !(lane.blockedSnapshotJson === null || typeof lane.blockedSnapshotJson === "string") ||
        !("blockedReason" in lane) || !(lane.blockedReason === null || typeof lane.blockedReason === "string") ||
        !("headIdentityJson" in lane) || !(lane.headIdentityJson === null || typeof lane.headIdentityJson === "string") ||
        !("resolutionRevision" in lane) || typeof lane.resolutionRevision !== "string" ||
        !/^(0|[1-9][0-9]*)$/.test(lane.resolutionRevision))
      throw new Error("Invalid native intent read");
    const view = decodeSyncReadBootstrap(parsed.viewJson);
    return {
      record: readSyncRecord(view.snapshot, recordId),
      lane: {
        pendingIntentIds: lane.pendingIntentIds,
        blockedSnapshotJson: lane.blockedSnapshotJson,
        blockedReason: lane.blockedReason,
        headIdentityJson: lane.headIdentityJson,
        resolutionRevision: lane.resolutionRevision,
      },
    };
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
