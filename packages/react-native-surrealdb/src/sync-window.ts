import { ExperimentalSyncWindowStore } from "./sync-window-store";
import { syncReadScopeError } from "./sync-read";
import type { CallOptions } from "./client";
import type { SyncJsonValue } from "./sync";
import {
  decodeWindowCapabilities,
  decodeWindowFrame,
  decodeWindowChanges,
  validateWindowRequest,
} from "./sync-window-codec";

/** Structural counters are decimal strings; payload values retain SurrealDB tags. */
export type WindowScalar =
  | { kind: "bool"; value: boolean }
  | { kind: "int"; value: string }
  | { kind: "text"; value: string };
export type WindowCommand =
  | {
      kind: "start";
      subscriptionId: string;
      generation: string;
      definitionId: string;
      parameters: Readonly<Record<string, WindowScalar>>;
      limit: number;
    }
  | {
      kind: "resume";
      subscriptionId: string;
      generation: string;
      token: string;
    }
  | { kind: "cancel"; subscriptionId: string; generation: string };
export type WindowRequest = {
  partitionId: string;
  clientId: string;
  requestedScope: string;
  subscriptionRevision: string;
  command: WindowCommand;
};
export type WindowCoverage = {
  definitionId: string;
  limit: number;
  orderField: string;
  orderKind: "bool" | "int" | "text";
  direction: "ascending" | "descending";
};
export type WindowMetadata = {
  revision: string;
  subscriptionId: string;
  generation: string;
  planFingerprint: string;
  scope: {
    identity: string;
    authorizationRevision: string;
    subscriptionRevision: string;
  };
  frontier: { epoch: string; sequence: string };
  viewRevision: string;
  coverage: WindowCoverage;
};
export type WindowRow<T = SyncJsonValue> = {
  recordId: string;
  value: T;
  version: string;
  sortKey: WindowScalar;
};
export type WindowSnapshot<T = SyncJsonValue> = {
  metadata: WindowMetadata;
  rows: readonly WindowRow<T>[];
  pendingOutsideWindow: readonly string[] | null;
};
export type WindowCursor = {
  processEpoch: string;
  subscriptionId: string;
  revision: string;
};
export type WindowFrame<T = SyncJsonValue> = {
  snapshot: WindowSnapshot<T> | null;
  request: WindowRequest | null;
  cancelledThrough: string | null;
  cursor: WindowCursor;
};
export type WindowBatch<T = SyncJsonValue> = Omit<WindowFrame<T>, "cursor"> & {
  beforeRevision: string;
  revision: string;
};
export type WindowChanges<T = SyncJsonValue> =
  | ({ kind: "reset_required" } & WindowFrame<T>)
  | {
      kind: "batches";
      cursor: WindowCursor;
      batches: readonly WindowBatch<T>[];
    };
export type WindowCapabilities = {
  protocolVersion: 1;
  maxSubscriptions: 32;
  maxWindowRows: 32;
  maxJournalBatches: 128;
  maxJournalBytes: number;
  maxAggregateJournalBytes: number;
  maxOutputBytes: number;
  requiresMatchingNativeAbi: true;
};

/** Optional ABI, independent of the v1 full-scope read capability. */
export interface NativeWindowPort {
  subscriptionCapabilitiesJson(): string;
  subscriptionRequestJson(
    subscriptionId: string,
    options?: CallOptions,
  ): Promise<string>;
  startSubscriptionJson(
    requestJson: string,
    options?: CallOptions,
  ): Promise<unknown>;
  applySubscriptionResponseJson(
    responseJson: string,
    options?: CallOptions,
  ): Promise<unknown>;
  cancelSubscription(
    subscriptionId: string,
    generation: bigint,
    options?: CallOptions,
  ): Promise<unknown>;
  subscriptionSnapshotJson(
    subscriptionId: string,
    options?: CallOptions,
  ): Promise<string>;
  subscriptionChangesJson(
    subscriptionId: string,
    cursorJson: string,
    maxBatches: number,
    options?: CallOptions,
  ): Promise<string>;
}
function unsupported(): never {
  throw new Error(
    "Ordered subscriptions require matching rebuilt native binaries",
  );
}
function id(value: string): void {
  if (!value || new TextEncoder().encode(value).length > 256)
    throw new TypeError("Invalid subscription identity");
}

/** Explicit native operations only: no HTTP, storage, write queue or automatic retry. */
export class ExperimentalSyncWindowClient {
  readonly #native: Partial<NativeWindowPort>;
  readonly #failure: (error: unknown) => unknown;
  readonly #stores = new Set<ExperimentalSyncWindowStore>();
  #closed = false;
  #blocked: unknown;
  constructor(
    native: Partial<NativeWindowPort>,
    failure: (error: unknown) => unknown = (error) => error,
  ) {
    this.#native = native;
    this.#failure = failure;
  }
  #check(allowBlocked = false): void {
    if (this.#closed) throw new Error("Subscription client is closed");
    if (this.#blocked && !allowBlocked) throw this.#blocked;
    this.capabilities();
  }
  capabilities(): WindowCapabilities {
    const method = this.#native.subscriptionCapabilitiesJson;
    if (!method) return unsupported();
    return decodeWindowCapabilities(method.call(this.#native));
  }
  async #call<T>(
    operation: () => Promise<T>,
    allowBlocked = false,
  ): Promise<T> {
    this.#check(allowBlocked);
    try {
      const result = await operation();
      if (this.#closed) throw new Error("Subscription client is closed");
      if (this.#blocked && !allowBlocked) throw this.#blocked;
      return result;
    } catch (error) {
      const blocked = syncReadScopeError(error);
      if (blocked) this.invalidateScope(blocked);
      throw this.#failure(blocked ?? error);
    }
  }
  createStore(
    subscriptionId: string,
    pollMs = 50,
  ): ExperimentalSyncWindowStore {
    id(subscriptionId);
    this.#check();
    const store = new ExperimentalSyncWindowStore(
      this,
      subscriptionId,
      pollMs,
      () => this.#stores.delete(store),
    );
    this.#stores.add(store);
    return store;
  }
  async #mutation(
    operation: () => Promise<unknown>,
    subscriptionId?: string,
    allowBlocked = false,
  ): Promise<unknown> {
    const receipt = await this.#call(operation, allowBlocked);
    for (const store of [...this.#stores])
      if (
        subscriptionId === undefined ||
        store.subscriptionId === subscriptionId
      )
        store.refresh();
    return receipt;
  }
  start(request: WindowRequest, options?: CallOptions): Promise<unknown> {
    validateWindowRequest(request);
    if (request.command.kind !== "start")
      throw new TypeError("Start requires a start command");
    return this.#mutation(() => {
      const method = this.#native.startSubscriptionJson;
      if (!method) return unsupported();
      return method.call(this.#native, JSON.stringify(request), options);
    });
  }
  applyResponse(responseJson: string, options?: CallOptions): Promise<unknown> {
    return this.#mutation(() => {
      const method = this.#native.applySubscriptionResponseJson;
      if (!method) return unsupported();
      return method.call(this.#native, responseJson, options);
    });
  }
  cancel(
    subscriptionId: string,
    generation: bigint,
    options?: CallOptions,
  ): Promise<unknown> {
    id(subscriptionId);
    if (generation < 1n || generation > 18446744073709551615n)
      throw new RangeError("Invalid subscription generation");
    return this.#mutation(
      () => {
        const method = this.#native.cancelSubscription;
        if (!method) return unsupported();
        return method.call(this.#native, subscriptionId, generation, options);
      },
      subscriptionId,
      true,
    );
  }
  request(
    subscriptionId: string,
    options?: CallOptions,
  ): Promise<WindowRequest | null> {
    id(subscriptionId);
    return this.#call(async () => {
      const method = this.#native.subscriptionRequestJson;
      if (!method) return unsupported();
      const json = await method.call(this.#native, subscriptionId, options);
      if (json.length > 262144)
        throw new TypeError("Subscription request exceeds its byte limit");
      const value: unknown = JSON.parse(json);
      if (value === null) return null;
      validateWindowRequest(value);
      if (value.command.subscriptionId !== subscriptionId)
        throw new TypeError("Unexpected subscription request identity");
      return value;
    });
  }
  snapshot(
    subscriptionId: string,
    options?: CallOptions,
  ): Promise<WindowFrame> {
    id(subscriptionId);
    return this.#call(async () => {
      const method = this.#native.subscriptionSnapshotJson;
      if (!method) return unsupported();
      return decodeWindowFrame(
        await method.call(this.#native, subscriptionId, options),
        subscriptionId,
      );
    });
  }
  changes(
    cursor: WindowCursor,
    maxBatches = 32,
    options?: CallOptions,
  ): Promise<WindowChanges> {
    id(cursor.subscriptionId);
    if (!Number.isInteger(maxBatches) || maxBatches < 1 || maxBatches > 128)
      throw new RangeError(
        "Subscription batch limit must be between 1 and 128",
      );
    return this.#call(async () => {
      const method = this.#native.subscriptionChangesJson;
      if (!method) return unsupported();
      const result = decodeWindowChanges(
        await method.call(
          this.#native,
          cursor.subscriptionId,
          JSON.stringify(cursor),
          maxBatches,
          options,
        ),
        cursor,
        maxBatches,
      );
      return result;
    });
  }
  /** Withdraw in-memory views without deleting any durable state. */
  invalidateScope(error: unknown): void {
    this.#blocked = error;
    for (const store of [...this.#stores]) store.invalidateScope(error);
  }
  close(): void {
    this.#closed = true;
    for (const store of [...this.#stores]) store.close();
    this.#stores.clear();
  }
}
