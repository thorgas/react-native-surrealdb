import * as bindings from "./generated/surrealdb_rn_core";
import type { CallOptions } from "./client";

/** Independently supplied by authenticated transport, never inferred from wire bytes. */
export type ExperimentalBootstrapContext =
  Readonly<bindings.NativeBootstrapContext>;
export type ExperimentalBootstrapBytes = ArrayBuffer | ArrayBufferView;
export type ExperimentalBootstrapCapabilities =
  Readonly<bindings.NativeBootstrapCapabilities>;
export type ExperimentalBootstrapErrorCode =
  bindings.NativeBootstrapError_Tags | "UnsupportedAbi" | "Aborted";

/** Deliberately contains no native error, payload, checkpoint, identity or cause. */
export class ExperimentalBootstrapError extends Error {
  constructor(readonly code: ExperimentalBootstrapErrorCode) {
    super(
      code === "UnsupportedAbi"
        ? "Bootstrap v1 requires matching rebuilt native binaries and runtime fingerprint"
        : `Bootstrap operation failed: ${code}`,
    );
    this.name = "ExperimentalBootstrapError";
  }
}
const fail = (code: ExperimentalBootstrapErrorCode): never => {
  throw new ExperimentalBootstrapError(code);
};
const nativeCodes = new Set<string>(
  Object.values(bindings.NativeBootstrapError_Tags),
);
function redacted(
  error: unknown,
  mutating: boolean,
): ExperimentalBootstrapError {
  try {
    // Even instanceof and descriptor lookup can invoke hostile Proxy traps.
    // Never read accessors, inherited tags, messages or causes from the input.
    if (error instanceof ExperimentalBootstrapError) {
      const code = Object.getOwnPropertyDescriptor(error, "code");
      if (
        code &&
        "value" in code &&
        typeof code.value === "string" &&
        (nativeCodes.has(code.value) ||
          code.value === "UnsupportedAbi" ||
          code.value === "Aborted")
      ) {
        return new ExperimentalBootstrapError(
          code.value as ExperimentalBootstrapErrorCode,
        );
      }
    }
    if (typeof error === "object" && error !== null) {
      const tag = Object.getOwnPropertyDescriptor(error, "tag");
      if (
        tag &&
        "value" in tag &&
        typeof tag.value === "string" &&
        nativeCodes.has(tag.value)
      ) {
        return new ExperimentalBootstrapError(
          tag.value as bindings.NativeBootstrapError_Tags,
        );
      }
    }
  } catch {
    // Uninspectable failures carry no trusted classification or public payload.
  }
  // Cancellation/unknown bridge failure after a mutation may have committed.
  return new ExperimentalBootstrapError(
    mutating
      ? bindings.NativeBootstrapError_Tags.RecoveryRequired
      : bindings.NativeBootstrapError_Tags.Storage,
  );
}

export function experimentalBootstrapCapabilities(): ExperimentalBootstrapCapabilities {
  try {
    if (
      typeof bindings.bootstrapCapabilities !== "function" ||
      typeof bindings.bindBootstrapSession !== "function"
    )
      return fail("UnsupportedAbi");
    const value = bindings.bootstrapCapabilities();
    if (
      value.protocol !== "surrealdb-sync-bootstrap/1" ||
      value.maxFrameBytes !== 4194304n ||
      value.maxPersistenceBytes !== 67108864n ||
      value.maxComponents !== 16384 ||
      value.nativeKeyBytes !== 512 ||
      value.completionDeadlineSeconds !== 30 ||
      value.requiresMatchingNativeAbi !== true
    )
      return fail("UnsupportedAbi");
    return Object.freeze({ ...value });
  } catch {
    return fail("UnsupportedAbi");
  }
}
function context(
  value: ExperimentalBootstrapContext,
): bindings.NativeBootstrapContext {
  const copy = { ...value };
  for (const [name, max] of [
    ["authorityId", 1024],
    ["principalId", 1024],
    ["journalGeneration", 1024],
    ["partitionId", 512],
    ["clientId", 512],
    ["requestedScope", 512],
  ] as const) {
    const key = copy[name];
    if (
      typeof key !== "string" ||
      key.length === 0 ||
      key.length > max ||
      new TextEncoder().encode(key).byteLength > max
    )
      fail(bindings.NativeBootstrapError_Tags.InvalidInput);
  }
  for (const number of [
    copy.subscriptionRevision,
    copy.authorizationRevision,
  ]) {
    if (
      typeof number !== "bigint" ||
      number < 0n ||
      number > 18446744073709551615n
    )
      fail(bindings.NativeBootstrapError_Tags.InvalidInput);
  }
  return copy;
}
function frame(input: ExperimentalBootstrapBytes): ArrayBuffer {
  let view: Uint8Array;
  if (input instanceof ArrayBuffer) {
    if (input.byteLength > 4194304)
      return fail(bindings.NativeBootstrapError_Tags.InvalidInput);
    view = new Uint8Array(input);
  } else if (ArrayBuffer.isView(input)) {
    if (input.byteLength > 4194304)
      return fail(bindings.NativeBootstrapError_Tags.InvalidInput);
    view = new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
  } else return fail(bindings.NativeBootstrapError_Tags.InvalidInput);
  return view.slice().buffer;
}

declare const stageBrand: unique symbol;
/** Opaque process-local object. After restart reacquire it through progress. */
export interface ExperimentalBootstrapStage {
  readonly [stageBrand]: true;
}
export type ExperimentalBootstrapProgress =
  | { readonly kind: "missing" }
  | {
      readonly kind: "staging" | "cancelled";
      readonly stage: ExperimentalBootstrapStage;
      readonly receivedIndices: readonly bigint[];
      readonly expectedPages: bigint;
    }
  | {
      readonly kind: "consumed";
      readonly stage: ExperimentalBootstrapStage;
      readonly receivedIndices: readonly bigint[];
      readonly expectedPages: bigint;
      readonly successfulRevision: bigint;
    };
export type ExperimentalBootstrapCompletion = "applied" | "historical_success";

/** Internal composition entry; callers use ExperimentalSyncClient.bindBootstrapSession. */
export async function bindExperimentalBootstrap(
  client: bindings.NativeSyncClientLike,
  closed: () => boolean,
  expected: ExperimentalBootstrapContext,
  options?: CallOptions,
): Promise<ExperimentalBootstrapSession> {
  const caps = experimentalBootstrapCapabilities();
  const independent = context(expected);
  if (closed()) return fail(bindings.NativeBootstrapError_Tags.Closed);
  if (options?.signal.aborted) return fail("Aborted");
  let native: bindings.NativeBootstrapSessionLike;
  try {
    native = await bindings.bindBootstrapSession(client, independent, options);
  } catch (error) {
    throw redacted(error, false);
  }
  if (closed()) return fail(bindings.NativeBootstrapError_Tags.Closed);
  if (
    !native ||
    typeof native !== "object" ||
    [
      native.begin,
      native.page,
      native.progress,
      native.cancel,
      native.complete,
      native.requestStart,
      native.requestPage,
      native.requestComplete,
    ].some((method) => typeof method !== "function")
  )
    return fail("UnsupportedAbi");
  return new ExperimentalBootstrapSession(native, closed, caps);
}

/** Native clock/deadline and durable incarnation fencing remain authoritative. */
export class ExperimentalBootstrapSession {
  readonly #stages = new WeakMap<
    ExperimentalBootstrapStage,
    bindings.NativeBootstrapStageLike
  >();
  #closed = false;
  constructor(
    private readonly native: bindings.NativeBootstrapSessionLike,
    private readonly parentClosed: () => boolean,
    readonly capabilities: ExperimentalBootstrapCapabilities,
  ) {}
  /** Releases only this JS wrapper; never cancels or deletes a durable stage. */
  close(): void {
    this.#closed = true;
  }
  get isClosed(): boolean {
    return this.#closed || this.parentClosed();
  }
  #check(options?: CallOptions): void {
    if (this.isClosed) fail(bindings.NativeBootstrapError_Tags.Closed);
    if (options?.signal.aborted) fail("Aborted");
  }
  async #call<T>(
    mutating: boolean,
    options: CallOptions | undefined,
    call: () => Promise<T>,
  ): Promise<T> {
    this.#check(options);
    try {
      const value = await call();
      this.#check();
      return value;
    } catch (error) {
      throw redacted(error, mutating);
    }
  }
  #stage(
    native: bindings.NativeBootstrapStageLike,
  ): ExperimentalBootstrapStage {
    if (native === null || typeof native !== "object")
      return fail(bindings.NativeBootstrapError_Tags.CorruptStage);
    const stage = Object.freeze({}) as ExperimentalBootstrapStage;
    this.#stages.set(stage, native);
    return stage;
  }
  #nativeStage(
    stage: ExperimentalBootstrapStage,
  ): bindings.NativeBootstrapStageLike {
    const native = this.#stages.get(stage);
    if (!native) return fail(bindings.NativeBootstrapError_Tags.StageMismatch);
    return native;
  }
  /** Canonical native request bytes derived only from the independently bound context. */
  async requestStart(
    requestId: string,
    options?: CallOptions,
  ): Promise<ArrayBuffer> {
    if (
      typeof requestId !== "string" ||
      requestId.length === 0 ||
      requestId.length > 512 ||
      new TextEncoder().encode(requestId).length > 512
    )
      return fail(bindings.NativeBootstrapError_Tags.InvalidInput);
    return frame(
      await this.#call(false, options, () =>
        this.native.requestStart(requestId, options),
      ),
    );
  }
  /** Identity/digest remains inside the opaque stage; JS supplies only the page index. */
  async requestPage(
    stage: ExperimentalBootstrapStage,
    index: bigint,
    options?: CallOptions,
  ): Promise<ArrayBuffer> {
    const native = this.#nativeStage(stage);
    if (
      typeof index !== "bigint" ||
      index < 0n ||
      index > 18446744073709551615n
    )
      return fail(bindings.NativeBootstrapError_Tags.InvalidInput);
    return frame(
      await this.#call(false, options, () =>
        this.native.requestPage(native, index, options),
      ),
    );
  }
  async requestComplete(
    stage: ExperimentalBootstrapStage,
    options?: CallOptions,
  ): Promise<ArrayBuffer> {
    const native = this.#nativeStage(stage);
    return frame(
      await this.#call(false, options, () =>
        this.native.requestComplete(native, options),
      ),
    );
  }
  async begin(
    start: ExperimentalBootstrapBytes,
    manifest: ExperimentalBootstrapBytes,
    options?: CallOptions,
  ): Promise<ExperimentalBootstrapStage> {
    const a = frame(start),
      b = frame(manifest);
    return this.#stage(
      await this.#call(true, options, () => this.native.begin(a, b, options)),
    );
  }
  async page(
    stage: ExperimentalBootstrapStage,
    bytes: ExperimentalBootstrapBytes,
    options?: CallOptions,
  ): Promise<void> {
    const native = this.#nativeStage(stage),
      raw = frame(bytes);
    return this.#call(true, options, () =>
      this.native.page(native, raw, options),
    );
  }
  async progress(
    manifest: ExperimentalBootstrapBytes,
    options?: CallOptions,
  ): Promise<ExperimentalBootstrapProgress> {
    const raw = frame(manifest);
    const progress = await this.#call(false, options, () =>
      this.native.progress(raw, options),
    );
    if (!progress || typeof progress !== "object")
      return fail(bindings.NativeBootstrapError_Tags.CorruptStage);
    if (progress.state === bindings.NativeBootstrapStageState.Missing) {
      if (
        progress.stage !== undefined ||
        progress.receivedIndices !== undefined ||
        progress.expectedPages !== undefined ||
        progress.successfulRevision !== undefined
      )
        return fail(bindings.NativeBootstrapError_Tags.CorruptStage);
      return { kind: "missing" };
    }
    const { expectedPages, receivedIndices, successfulRevision } = progress;
    if (
      typeof expectedPages !== "bigint" ||
      expectedPages < 0n ||
      expectedPages > 16384n ||
      !Array.isArray(receivedIndices) ||
      receivedIndices.length > 16384 ||
      receivedIndices.some(
        (index, n) =>
          typeof index !== "bigint" ||
          index < 0n ||
          index >= expectedPages ||
          (n > 0 && index <= receivedIndices[n - 1]!),
      )
    )
      return fail(bindings.NativeBootstrapError_Tags.CorruptStage);
    if (progress.stage === undefined)
      return fail(bindings.NativeBootstrapError_Tags.CorruptStage);
    const common = {
      stage: this.#stage(progress.stage),
      expectedPages,
      receivedIndices: Object.freeze([...receivedIndices]),
    };
    if (progress.state === bindings.NativeBootstrapStageState.Consumed) {
      if (
        typeof successfulRevision !== "bigint" ||
        successfulRevision <= 0n ||
        successfulRevision > 18446744073709551615n
      )
        return fail(bindings.NativeBootstrapError_Tags.CorruptStage);
      return { kind: "consumed", ...common, successfulRevision };
    }
    if (successfulRevision !== undefined)
      return fail(bindings.NativeBootstrapError_Tags.CorruptStage);
    if (progress.state === bindings.NativeBootstrapStageState.Staging)
      return { kind: "staging", ...common };
    if (progress.state === bindings.NativeBootstrapStageState.Cancelled)
      return { kind: "cancelled", ...common };
    return fail(bindings.NativeBootstrapError_Tags.CorruptStage);
  }
  async cancel(
    stage: ExperimentalBootstrapStage,
    options?: CallOptions,
  ): Promise<void> {
    const native = this.#nativeStage(stage);
    return this.#call(true, options, () => this.native.cancel(native, options));
  }
  async complete(
    stage: ExperimentalBootstrapStage,
    receipt: ExperimentalBootstrapBytes,
    options?: CallOptions,
  ): Promise<ExperimentalBootstrapCompletion> {
    const native = this.#nativeStage(stage),
      raw = frame(receipt);
    const disposition = await this.#call(true, options, () =>
      this.native.complete(native, raw, options),
    );
    if (disposition === bindings.NativeBootstrapCompletion.Applied)
      return "applied";
    if (disposition === bindings.NativeBootstrapCompletion.HistoricalSuccess)
      return "historical_success";
    return fail(bindings.NativeBootstrapError_Tags.RecoveryRequired);
  }
}
