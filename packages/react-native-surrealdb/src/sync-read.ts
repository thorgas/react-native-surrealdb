import type { SyncJsonValue } from "./sync";

/** Canonical decimal u64 strings avoid rounding at JavaScript JSON boundaries. */
export type SyncReadCounter = string;
export type SyncReadIdentity = Readonly<{
  clientCommitId: string;
  fingerprint: string;
}>;
export type SyncReadCursor = Readonly<{
  processEpoch: string;
  revision: SyncReadCounter;
}>;
export type SyncReadFrontier = Readonly<{
  epoch: SyncReadCounter;
  sequence: SyncReadCounter;
}>;
export type SyncReadMetadata = Readonly<{
  revision: SyncReadCounter;
  scope: Readonly<{
    partitionId: string;
    scopeIdentity: string;
    subscriptionRevision: SyncReadCounter;
    authorizationRevision: SyncReadCounter | null;
  }>;
  asOf: SyncReadFrontier | null;
  readiness: "local_available_awaiting_coverage" | "authorized_scope_as_of";
  coverage:
    | Readonly<{ kind: "unknown" }>
    | Readonly<{ kind: "authorized_reset"; cursor: SyncReadFrontier }>;
}>;
export type SyncReadBaseVersion =
  "absent" | Readonly<{ exact: SyncReadCounter }>;
export type SyncReadConfirmed<T = SyncJsonValue> =
  | Readonly<{ state: "absent" }>
  | Readonly<{
      state: "present";
      value: T;
      version: SyncReadCounter;
      reference: string | null;
    }>
  | Readonly<{ state: "tombstone"; version: SyncReadCounter }>;
export type SyncReadOptimistic<T = SyncJsonValue> =
  | Readonly<{
      state: "present";
      value: T;
      baseVersion: SyncReadBaseVersion;
      reference: string | null;
    }>
  | Readonly<{ state: "deleted"; baseVersion: SyncReadCounter }>;
export type SyncReadResolution =
  | Readonly<{ strategy: "keep_server" | "prefer_server_policy_v1" }>
  | Readonly<{
      strategy: "keep_local" | "merge";
      replacement: SyncReadIdentity;
    }>;
export type SyncReadRow<T = SyncJsonValue> = Readonly<{
  recordId: string;
  confirmed: SyncReadConfirmed<T>;
  confirmedAbsence: "not_absent" | "unknown" | "absent_within_authorized_scope";
  optimistic: SyncReadOptimistic<T> | null;
  pending: readonly SyncReadIdentity[];
  conflicts: readonly Readonly<{
    identity: SyncReadIdentity;
    resolution: SyncReadResolution | null;
  }>[];
  accepted: readonly Readonly<{
    identity: SyncReadIdentity;
    sequence: SyncReadCounter;
    status: "covered_by_authorized_reset" | "awaiting_pull_epoch_unknown";
  }>[];
}>;
export type SyncReadSnapshot<T = SyncJsonValue> = Readonly<{
  protocolVersion: 1;
  metadata: SyncReadMetadata;
  rows: Readonly<Record<string, SyncReadRow<T>>>;
}>;
export type SyncReadBatch<T = SyncJsonValue> = Readonly<{
  protocolVersion: 1;
  beforeRevision: SyncReadCounter;
  revision: SyncReadCounter;
  metadata: SyncReadMetadata;
  changes: readonly (
    | Readonly<{ kind: "upsert"; row: SyncReadRow<T> }>
    | Readonly<{ kind: "remove"; recordId: string }>
  )[];
}>;
export type SyncReadBootstrap<T = SyncJsonValue> = Readonly<{
  snapshot: SyncReadSnapshot<T>;
  cursor: SyncReadCursor;
}>;
export type SyncReadChanges<T = SyncJsonValue> =
  | Readonly<{
      kind: "batches";
      batches: readonly SyncReadBatch<T>[];
      cursor: SyncReadCursor;
    }>
  | Readonly<{
      kind: "reset_required";
      snapshot: SyncReadSnapshot<T>;
      cursor: SyncReadCursor;
    }>;
export type SyncReadRecord<T = SyncJsonValue> = Readonly<{
  metadata: SyncReadMetadata;
  row: SyncReadRow<T> | undefined;
  absence: "unknown" | "absent_within_authorized_scope" | "not_absent";
}>;

function validateReadBatch<T>(
  metadata: SyncReadMetadata,
  batch: SyncReadBatch<T>,
): void {
  if (
    batch.protocolVersion !== 1 ||
    batch.beforeRevision !== metadata.revision ||
    batch.revision !== batch.metadata.revision ||
    BigInt(batch.revision) !== BigInt(batch.beforeRevision) + 1n ||
    batch.metadata.scope.partitionId !== metadata.scope.partitionId ||
    batch.metadata.scope.scopeIdentity !== metadata.scope.scopeIdentity ||
    batch.metadata.scope.subscriptionRevision !==
      metadata.scope.subscriptionRevision
  )
    throw new Error("Sync read batch requires a fresh consistent snapshot");
  const touched = new Set<string>();
  for (const change of batch.changes) {
    const id = change.kind === "upsert" ? change.row.recordId : change.recordId;
    if (touched.has(id)) throw new Error("Sync read batch repeats a record");
    touched.add(id);
  }
}

/** Pure reactive projection only: no journal, persistence, network or retries. */
export function applySyncReadBatch<T>(
  snapshot: SyncReadSnapshot<T>,
  batch: SyncReadBatch<T>,
): SyncReadSnapshot<T> {
  validateReadBatch(snapshot.metadata, batch);
  const rows = { ...snapshot.rows };
  for (const change of batch.changes) {
    const id = change.kind === "upsert" ? change.row.recordId : change.recordId;
    if (change.kind === "remove") delete rows[id];
    else
      Object.defineProperty(rows, id, {
        value: change.row,
        enumerable: true,
        writable: true,
        configurable: true,
      });
  }
  return { protocolVersion: 1, metadata: batch.metadata, rows };
}

/** Missing cached rows stay unknown until complete authorized scope coverage. */
export function readSyncRecord<T>(
  snapshot: SyncReadSnapshot<T>,
  recordId: string,
): SyncReadRecord<T> {
  const row = Object.prototype.hasOwnProperty.call(snapshot.rows, recordId)
    ? snapshot.rows[recordId]
    : undefined;
  return {
    metadata: snapshot.metadata,
    row,
    absence:
      row?.confirmedAbsence ??
      (snapshot.metadata.coverage.kind === "authorized_reset"
        ? "absent_within_authorized_scope"
        : "unknown"),
  };
}

export interface SyncReadSource<T = SyncJsonValue> {
  readSnapshot(options?: {
    signal?: AbortSignal;
  }): Promise<SyncReadBootstrap<T>>;
  readChanges(
    cursor: SyncReadCursor,
    maxBatches: number,
    options?: { signal?: AbortSignal },
  ): Promise<SyncReadChanges<T>>;
}
export type SyncReadEvent<T = SyncJsonValue> =
  | Readonly<{ kind: "snapshot"; snapshot: SyncReadSnapshot<T> }>
  | Readonly<{
      kind: "batch";
      snapshot: SyncReadSnapshot<T>;
      batch: SyncReadBatch<T>;
    }>
  | Readonly<{ kind: "error"; error: unknown; invalidated: boolean }>
  | Readonly<{ kind: "closed" }>;

export class SyncReadScopeError extends Error {
  readonly reason: "needs_new_scope_journal" | "pending_quarantine_required";
  readonly originalCause: unknown;
  constructor(
    reason: "needs_new_scope_journal" | "pending_quarantine_required",
    originalCause?: unknown,
  ) {
    super(
      "Sync scope requires a new authorized journal; retained edits and history are preserved",
    );
    this.name = "SyncReadScopeError";
    this.reason = reason;
    this.originalCause = originalCause;
  }
}
/** @internal Translate stable native error tags without importing a newer ABI eagerly. */
export function syncReadScopeError(
  error: unknown,
): SyncReadScopeError | undefined {
  if (error instanceof SyncReadScopeError) return error;
  if (typeof error !== "object" || error === null || !("tag" in error))
    return undefined;
  if (error.tag === "NeedsNewScopeJournal")
    return new SyncReadScopeError("needs_new_scope_journal", error);
  if (error.tag === "PendingQuarantineRequired")
    return new SyncReadScopeError("pending_quarantine_required", error);
  return undefined;
}

/** One cancellable native-read pump; the engine remains the sole durability owner. */
export class ExperimentalSyncReadStore<T = SyncJsonValue> {
  readonly #source: SyncReadSource<T>;
  readonly #pollMs: number;
  readonly #maxBatches: number;
  readonly #listeners = new Set<(event: SyncReadEvent<T>) => void>();
  #snapshot?: SyncReadSnapshot<T>;
  #controller?: AbortController;
  #closed = false;
  #observerErrors = 0;
  #scopeBlocked?: SyncReadScopeError;
  readonly #onClose?: () => void;

  constructor(
    source: SyncReadSource<T>,
    options: { pollMs?: number; maxBatches?: number } = {},
    onClose?: () => void,
  ) {
    this.#source = source;
    this.#onClose = onClose;
    this.#pollMs = options.pollMs ?? 50;
    this.#maxBatches = options.maxBatches ?? 32;
    if (
      !Number.isInteger(this.#pollMs) ||
      this.#pollMs < 1 ||
      this.#pollMs > 60_000 ||
      !Number.isInteger(this.#maxBatches) ||
      this.#maxBatches < 1 ||
      this.#maxBatches > 128
    )
      throw new RangeError(
        "Sync read polling limits are outside supported bounds",
      );
  }

  getSnapshot(): SyncReadSnapshot<T> | undefined {
    return this.#snapshot;
  }
  get observerErrors(): number {
    return this.#observerErrors;
  }

  watchCollection(listener: (event: SyncReadEvent<T>) => void): () => void {
    if (this.#closed) throw new Error("Sync read store is closed");
    if (this.#scopeBlocked) throw this.#scopeBlocked;
    this.#listeners.add(listener);
    if (this.#controller && this.#snapshot)
      this.#notify(listener, { kind: "snapshot", snapshot: this.#snapshot });
    if (!this.#controller) this.#start();
    return () => {
      this.#listeners.delete(listener);
      if (this.#listeners.size === 0) this.#stop();
    };
  }

  /** An explicit restart after a read failure; never retries network or mutations. */
  refresh(): void {
    if (this.#closed) throw new Error("Sync read store is closed");
    if (this.#scopeBlocked) throw this.#scopeBlocked;
    this.#stop();
    if (this.#listeners.size) this.#start();
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#stop();
    this.#snapshot = undefined;
    this.#emit({ kind: "closed" });
    this.#listeners.clear();
    this.#onClose?.();
  }

  /** Preserve durable state while withdrawing an old scope from reactive consumers. */
  invalidateScope(error: SyncReadScopeError): void {
    if (this.#closed || this.#scopeBlocked) return;
    this.#scopeBlocked = error;
    this.#stop();
    this.#snapshot = undefined;
    this.#emit({ kind: "error", error, invalidated: true });
  }

  #stop(): void {
    this.#controller?.abort();
    this.#controller = undefined;
  }
  #start(): void {
    const controller = new AbortController();
    this.#controller = controller;
    void this.#pump(controller);
  }
  #current(controller: AbortController): boolean {
    return (
      !controller.signal.aborted &&
      this.#controller === controller &&
      !this.#closed
    );
  }
  #notify(
    listener: (event: SyncReadEvent<T>) => void,
    event: SyncReadEvent<T>,
  ): void {
    try {
      listener(event);
    } catch {
      this.#observerErrors = Math.min(
        Number.MAX_SAFE_INTEGER,
        this.#observerErrors + 1,
      );
    }
  }
  #emit(event: SyncReadEvent<T>): void {
    for (const listener of [...this.#listeners]) {
      if (
        (event.kind === "snapshot" || event.kind === "batch") &&
        (this.#closed ||
          this.#scopeBlocked ||
          this.#snapshot !== event.snapshot)
      )
        return;
      if (this.#listeners.has(listener)) this.#notify(listener, event);
    }
  }
  async #pump(controller: AbortController): Promise<void> {
    try {
      const initial = await this.#source.readSnapshot({
        signal: controller.signal,
      });
      if (!this.#current(controller)) return;
      if (initial.cursor.revision !== initial.snapshot.metadata.revision)
        throw new Error(
          "Sync read bootstrap cursor does not match its snapshot",
        );
      this.#snapshot = initial.snapshot;
      let cursor = initial.cursor;
      this.#emit({ kind: "snapshot", snapshot: initial.snapshot });
      while (this.#current(controller)) {
        const result = await this.#source.readChanges(
          cursor,
          this.#maxBatches,
          { signal: controller.signal },
        );
        if (!this.#current(controller)) return;
        if (result.kind === "reset_required") {
          if (result.cursor.revision !== result.snapshot.metadata.revision)
            throw new Error(
              "Sync read reset cursor does not match its snapshot",
            );
          this.#snapshot = result.snapshot;
          cursor = result.cursor;
          this.#emit({ kind: "snapshot", snapshot: result.snapshot });
        } else {
          if (result.cursor.processEpoch !== cursor.processEpoch)
            throw new Error(
              "Sync read process changed without a fresh snapshot",
            );
          if (result.batches.length > this.#maxBatches)
            throw new Error("Sync read response exceeds its batch limit");
          let metadata: SyncReadMetadata | undefined = this.#snapshot?.metadata;
          if (!metadata) throw new Error("Sync read snapshot is unavailable");
          for (const batch of result.batches) {
            validateReadBatch(metadata, batch);
            metadata = batch.metadata;
          }
          if (result.cursor.revision !== metadata.revision)
            throw new Error(
              "Sync read change cursor does not match committed batches",
            );
          for (const batch of result.batches) {
            if (!this.#current(controller)) return;
            const current: SyncReadSnapshot<T> | undefined = this.#snapshot;
            if (!current) throw new Error("Sync read snapshot is unavailable");
            this.#snapshot = applySyncReadBatch(current, batch);
            this.#emit({ kind: "batch", snapshot: this.#snapshot, batch });
          }
          if (result.cursor.revision !== this.#snapshot?.metadata.revision)
            throw new Error(
              "Sync read change cursor does not match committed batches",
            );
          cursor = result.cursor;
        }
        if (result.kind === "reset_required" || result.batches.length === 0)
          await readPollDelay(this.#pollMs, controller.signal);
      }
    } catch (error) {
      if (this.#current(controller)) {
        const blocked = syncReadScopeError(error);
        if (blocked) this.invalidateScope(blocked);
        else this.#emit({ kind: "error", error, invalidated: false });
      }
    } finally {
      if (this.#controller === controller) this.#controller = undefined;
    }
  }
}

function readPollDelay(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    const finish = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", finish);
      resolve();
    };
    const timer = setTimeout(finish, ms);
    signal.addEventListener("abort", finish, { once: true });
  });
}

export type SyncReadCapabilities = Readonly<{
  protocolVersion: 1;
  maxJournalBatches: number;
  maxJournalBytes: number;
  maxOutputBytes: number;
  requiresMatchingNativeAbi: true;
  predicateSubscriptions: false;
}>;
export type SyncScopeTransitionDecision =
  "unchanged" | "needs_new_scope_journal" | "pending_quarantine_required";
