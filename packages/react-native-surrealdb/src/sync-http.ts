import { decodeSurrealValue } from "./wire";
import type { WindowRequest } from "./sync-window";
import { validateWindowRequest } from "./sync-window-codec";
import type { ExperimentalSyncTelemetry } from "./sync-telemetry";
import type { CallOptions } from "./client";
import {
  ExperimentalSyncClient,
  type ExperimentalConflictPolicy,
  type ExperimentalSyncStatus,
  type SyncJsonValue,
} from "./sync";

export type ExperimentalSyncHttpCodec = {
  readonly mediaType: string;
  encodePushRequest(
    request: ExperimentalSyncPushRequest,
    options?: CallOptions,
  ): Promise<ExperimentalSyncHttpBody>;
  decodePushResponse(
    bytes: ArrayBuffer,
    options?: CallOptions,
  ): Promise<string>;
  encodePullRequest(
    request: ExperimentalSyncPullRequest,
    options?: CallOptions,
  ): Promise<ExperimentalSyncHttpBody>;
  decodePullResponse(
    bytes: ArrayBuffer,
    options?: CallOptions,
  ): Promise<string>;
};

export type ExperimentalSyncHttpBody = string | ArrayBuffer;

export type ExperimentalSyncHttpLimits = {
  requestTimeoutMs: number;
  maxRequestBytes: number;
  maxResponseBytes: number;
};

export type ExperimentalSyncHttpErrorKind =
  | "aborted"
  | "timeout"
  | "network"
  | "http"
  | "content_type"
  | "body_limit"
  | "protocol";

export type ExperimentalSyncPushRequest = {
  partitionId: string;
  clientId: string;
  commitJson: string;
};

export type ExperimentalSyncPullRequest = {
  partitionId: string;
  clientId: string;
  checkpoint?: string;
  requestedScope: string;
  subscriptionRevision: bigint;
};

export type ExperimentalSyncHttpNativeBridge = {
  encodeSyncPushRequest(
    partitionId: string,
    clientId: string,
    commitJson: string,
    options?: CallOptions,
  ): Promise<ArrayBuffer>;
  decodeSyncPushResponse(
    bytes: ArrayBuffer,
    options?: CallOptions,
  ): Promise<string>;
  encodeSyncPullRequest(
    partitionId: string,
    clientId: string,
    checkpoint: string | undefined,
    requestedScope: string,
    subscriptionRevision: bigint,
    options?: CallOptions,
  ): Promise<ArrayBuffer>;
  decodeSyncPullResponse(
    bytes: ArrayBuffer,
    options?: CallOptions,
  ): Promise<string>;
};

export type ExperimentalSyncHttpOptions = {
  sync: ExperimentalSyncClient;
  telemetry?: ExperimentalSyncTelemetry;
  /** Defaults to manual; only an explicit opt-in changes local conflict disposition. */
  conflictPolicy?: ExperimentalConflictPolicy;
  baseUrl: string;
  partitionId: string;
  clientId: string;
  requestedScope: string;
  subscriptionRevision: bigint;
  accessToken: (options?: CallOptions) => string | Promise<string>;
  codec: ExperimentalSyncHttpCodec;
  fetch?: typeof globalThis.fetch;
  limits?: Partial<ExperimentalSyncHttpLimits>;
  allowInsecureLocalhost?: boolean;
};

export type ExperimentalSyncOnceResult = {
  push: ExperimentalSyncStatus[];
  pull: ExperimentalSyncStatus;
};

/** Transport failure. Protocol conflicts and rejections remain normal HTTP 200 outcomes. */
export class ExperimentalSyncHttpError extends Error {
  constructor(
    message: string,
    readonly kind: ExperimentalSyncHttpErrorKind,
    readonly status?: number,
  ) {
    super(message);
    this.name = "ExperimentalSyncHttpError";
  }
}

/**
 * Explicit prototype codec. It is not the canonical SurrealDB sync wire codec.
 * BigInts are accepted only when they fit safely in a JSON number.
 */
export const experimentalJsonSyncHttpCodec: ExperimentalSyncHttpCodec = {
  mediaType: "application/json",
  async encodePushRequest(request) {
    return encodeJson({
      schemaVersion: "v1",
      partitionId: request.partitionId,
      clientId: request.clientId,
      commit: parseProtocolJson(request.commitJson),
    });
  },
  async decodePushResponse(bytes) {
    return decodeJsonResponse(bytes);
  },
  async encodePullRequest(request) {
    return encodeJson({
      schemaVersion: "v1",
      partitionId: request.partitionId,
      clientId: request.clientId,
      checkpoint: request.checkpoint ?? null,
      requestedScope: request.requestedScope,
      subscriptionRevision: request.subscriptionRevision,
    });
  },
  async decodePullResponse(bytes) {
    return decodeJsonResponse(bytes);
  },
};

/** Complete, bounded surrealdb-sync/1 CBOR codec backed by native Rust. */
export function experimentalCanonicalCborSyncHttpCodec(
  native: ExperimentalSyncHttpNativeBridge,
): ExperimentalSyncHttpCodec {
  return {
    mediaType: "application/vnd.surrealdb-sync.v1+cbor",
    async encodePushRequest(request, options) {
      return native.encodeSyncPushRequest(
        request.partitionId,
        request.clientId,
        request.commitJson,
        options,
      );
    },
    async decodePushResponse(bytes, options) {
      return native.decodeSyncPushResponse(bytes, options);
    },
    async encodePullRequest(request, options) {
      return native.encodeSyncPullRequest(
        request.partitionId,
        request.clientId,
        request.checkpoint,
        request.requestedScope,
        request.subscriptionRevision,
        options,
      );
    },
    async decodePullResponse(bytes, options) {
      return native.decodeSyncPullResponse(bytes, options);
    },
  };
}

/**
 * Application-owned HTTP push/pull orchestration for the experimental client.
 *
 * The adapter has no scheduler or implicit retry loop. Calls are serialized,
 * and WebSockets may only trigger a later `pull`; they do not carry state.
 */
export class ExperimentalSyncHttpAdapter {
  readonly #sync: ExperimentalSyncClient;
  readonly #conflictPolicy: ExperimentalConflictPolicy;
  readonly #baseUrl: string;
  readonly #partitionId: string;
  readonly #clientId: string;
  readonly #requestedScope: string;
  readonly #subscriptionRevision: bigint;
  readonly #accessToken: ExperimentalSyncHttpOptions["accessToken"];
  readonly #codec: ExperimentalSyncHttpCodec;
  readonly #fetch: typeof globalThis.fetch;
  readonly #limits: ExperimentalSyncHttpLimits;
  readonly #telemetry?: ExperimentalSyncTelemetry;
  #cycle?: number;
  #tail: Promise<void> = Promise.resolve();

  constructor(options: ExperimentalSyncHttpOptions) {
    const baseUrl = options.baseUrl.replace(/\/+$/, "");
    assertSecureHttpUrl(baseUrl, options.allowInsecureLocalhost === true);
    if (options.codec.mediaType.trim().length === 0) {
      throw new TypeError("sync HTTP codec mediaType is required");
    }
    this.#limits = limits(options.limits);
    this.#sync = options.sync;
    this.#telemetry = options.telemetry;
    this.#conflictPolicy = options.conflictPolicy ?? "manual";
    this.#baseUrl = baseUrl;
    this.#partitionId = options.partitionId;
    this.#clientId = options.clientId;
    this.#requestedScope = options.requestedScope;
    this.#subscriptionRevision = options.subscriptionRevision;
    this.#accessToken = options.accessToken;
    this.#codec = options.codec;
    this.#fetch = options.fetch ?? globalThis.fetch.bind(globalThis);
  }

  push(options?: CallOptions): Promise<ExperimentalSyncStatus[]> {
    return this.#serialize(() =>
      this.#measureCycle("push", () => this.#push(options)),
    );
  }

  pull(options?: CallOptions): Promise<ExperimentalSyncStatus> {
    return this.#serialize(() =>
      this.#measureCycle("pull", () => this.#pull(options)),
    );
  }

  /** Acquire complete authorized read coverage without discarding pending intent. */
  bootstrapReadCoverage(
    options?: CallOptions,
  ): Promise<ExperimentalSyncStatus> {
    this.#sync.readCapabilities();
    return this.#serialize(() =>
      this.#measureCycle("pull", () => this.#pull(options, true)),
    );
  }

  syncOnce(options?: CallOptions): Promise<ExperimentalSyncOnceResult> {
    return this.#serialize(() =>
      this.#measureCycle("push", async () => ({
        push: await this.#push(options),
        pull: await this.#pull(options),
      })),
    );
  }

  async #measureCycle<T>(
    phase: "push" | "pull",
    operation: () => Promise<T>,
  ): Promise<T> {
    const span = this.#telemetry?.begin(phase);
    this.#cycle = span?.id;
    try {
      const result = await operation();
      span?.end();
      return result;
    } catch (error) {
      span?.end("error");
      throw error;
    } finally {
      this.#cycle = undefined;
    }
  }

  async #push(options?: CallOptions): Promise<ExperimentalSyncStatus[]> {
    const commits = await this.#sync.pendingProtocolJson(options);
    const statuses: ExperimentalSyncStatus[] = [];

    for (const commit of commits) {
      const responseJson = await this.#postPush(
        "/v1/sync/push",
        {
          partitionId: this.#partitionId,
          clientId: this.#clientId,
          commitJson: commit,
        },
        options,
      );
      const persisted = this.#telemetry?.begin(
        "native_persistence",
        this.#cycle,
      );
      try {
        statuses.push(
          await (this.#conflictPolicy === "manual"
            ? this.#sync.recordPushResponseProtocolJson(responseJson, options)
            : this.#sync.recordPushResponseWithPolicyProtocolJson(
                responseJson,
                this.#conflictPolicy,
                options,
              )),
        );
        let reason:
          | "authority_accepted"
          | "authority_conflict"
          | "authority_rejected"
          | "ok" = "ok";
        try {
          const kind = (
            JSON.parse(responseJson) as { outcome?: { status?: string } }
          ).outcome?.status;
          reason =
            kind === "accepted"
              ? "authority_accepted"
              : kind === "conflict"
                ? "authority_conflict"
                : kind === "rejected"
                  ? "authority_rejected"
                  : "ok";
        } catch {
          /* Never expose protocol bodies in diagnostics. */
        }
        persisted?.end(reason);
        if (this.#telemetry && reason !== "ok") {
          try {
            const outcome = (
              decodeSurrealValue(responseJson) as {
                outcome?: { identity?: { clientCommitId?: string } };
              }
            ).outcome;
            const id = outcome?.identity?.clientCommitId;
            if (typeof id === "string" && id.length <= 128)
              this.#telemetry.milestone(id, reason);
          } catch {
            /* Metadata is optional. */
          }
        }
      } catch (error) {
        persisted?.end("error");
        throw error;
      }
    }

    return statuses;
  }

  async #pull(
    options?: CallOptions,
    forceReset = false,
  ): Promise<ExperimentalSyncStatus> {
    const span = this.#telemetry?.begin("pull", this.#cycle);
    try {
      const checkpoint = forceReset
        ? undefined
        : await this.#sync.checkpointToken(options);
      const responseJson = await this.#postPull(
        "/v1/sync/pull",
        {
          partitionId: this.#partitionId,
          clientId: this.#clientId,
          checkpoint,
          requestedScope: this.#requestedScope,
          subscriptionRevision: this.#subscriptionRevision,
        },
        options,
      );
      const result = await (this.#telemetry
        ? this.#telemetry.measure(
            "projection_checkpoint",
            () =>
              this.#sync.applyPullResponseProtocolJson(responseJson, options),
            this.#cycle,
            "locally_visible",
          )
        : this.#sync.applyPullResponseProtocolJson(responseJson, options));
      span?.end();
      return result;
    } catch (error) {
      span?.end("error");
      throw error;
    }
  }

  async #postPush(
    path: string,
    request: ExperimentalSyncPushRequest,
    options?: CallOptions,
  ): Promise<string> {
    return this.#withDeadline(options, async (boundedOptions) =>
      this.#post(
        path,
        await (this.#telemetry
          ? this.#telemetry.measure("encode", () =>
              this.#codec.encodePushRequest(request, boundedOptions),
            )
          : this.#codec.encodePushRequest(request, boundedOptions)),
        (response) => this.#codec.decodePushResponse(response, boundedOptions),
        boundedOptions,
      ),
    );
  }

  async #postPull(
    path: string,
    request: ExperimentalSyncPullRequest,
    options?: CallOptions,
  ): Promise<string> {
    return this.#withDeadline(options, async (boundedOptions) =>
      this.#post(
        path,
        await (this.#telemetry
          ? this.#telemetry.measure("encode", () =>
              this.#codec.encodePullRequest(request, boundedOptions),
            )
          : this.#codec.encodePullRequest(request, boundedOptions)),
        (response) => this.#codec.decodePullResponse(response, boundedOptions),
        boundedOptions,
      ),
    );
  }

  async #post(
    path: string,
    body: ExperimentalSyncHttpBody,
    decode: (bytes: ArrayBuffer) => Promise<string>,
    options?: CallOptions,
  ): Promise<string> {
    const token = await this.#accessToken(options);
    if (token.trim().length === 0) {
      throw new ExperimentalSyncHttpError(
        "sync access token is empty",
        "protocol",
      );
    }
    if (bodyBytes(body) > this.#limits.maxRequestBytes) {
      throw new ExperimentalSyncHttpError(
        "sync HTTP request exceeds the configured byte limit",
        "body_limit",
      );
    }
    if (options?.signal.aborted) {
      throw new ExperimentalSyncHttpError(
        "sync HTTP request was aborted",
        "aborted",
      );
    }

    const transport = this.#telemetry?.begin("transport", this.#cycle);
    let receivedResponse = false;
    try {
      const response = await this.#fetch(`${this.#baseUrl}${path}`, {
        method: "POST",
        headers: {
          Accept: this.#codec.mediaType,
          Authorization: `Bearer ${token}`,
          "Content-Type": this.#codec.mediaType,
        },
        body,
        signal: options?.signal,
      });
      receivedResponse = true;
      if (response.status !== 200) {
        throw new ExperimentalSyncHttpError(
          `sync HTTP request failed with status ${response.status}`,
          "http",
          response.status,
        );
      }
      const contentType = response.headers
        .get("Content-Type")
        ?.split(";", 1)[0]
        ?.trim();
      if (contentType !== this.#codec.mediaType) {
        throw new ExperimentalSyncHttpError(
          "sync HTTP response has an unexpected content type",
          "content_type",
        );
      }
      const bytes = await boundedResponseBytes(
        response,
        this.#limits.maxResponseBytes,
      );
      const requestId = response.headers
        .get("x-sync-request-id")
        ?.match(/^sync-([0-9]{1,15})$/u)?.[1];
      transport?.end(
        "ok",
        bodyBytes(body),
        bytes.byteLength,
        requestId === undefined ? undefined : Number(requestId),
      );
      return await (this.#telemetry
        ? this.#telemetry.measure("decode", () => decode(bytes), transport?.id)
        : decode(bytes));
    } catch (error) {
      transport?.end("error", bodyBytes(body));
      if (options?.signal.aborted)
        throw new ExperimentalSyncHttpError(
          "sync HTTP request was aborted",
          "aborted",
        );
      if (error instanceof ExperimentalSyncHttpError) throw error;
      throw new ExperimentalSyncHttpError(
        receivedResponse
          ? "sync HTTP response failed protocol decoding"
          : "sync HTTP network request failed",
        receivedResponse ? "protocol" : "network",
      );
    }
  }

  async #withDeadline<T>(
    options: CallOptions | undefined,
    operation: (options: CallOptions) => Promise<T>,
  ): Promise<T> {
    if (options?.signal.aborted) {
      throw new ExperimentalSyncHttpError(
        "sync HTTP request was aborted",
        "aborted",
      );
    }
    const controller = new AbortController();
    let timedOut = false;
    const onAbort = () => controller.abort();
    options?.signal.addEventListener("abort", onAbort, { once: true });
    const timeout = setTimeout(() => {
      timedOut = true;
      controller.abort();
    }, this.#limits.requestTimeoutMs);
    try {
      return await Promise.race([
        operation({ signal: controller.signal }),
        abortFailure(controller.signal),
      ]);
    } catch (error) {
      if (timedOut) {
        throw new ExperimentalSyncHttpError(
          "sync HTTP request timed out",
          "timeout",
        );
      }
      if (options?.signal.aborted) {
        throw new ExperimentalSyncHttpError(
          "sync HTTP request was aborted",
          "aborted",
        );
      }
      throw error;
    } finally {
      clearTimeout(timeout);
      options?.signal.removeEventListener("abort", onAbort);
    }
  }

  #serialize<T>(operation: () => Promise<T>): Promise<T> {
    const wait = this.#telemetry?.begin("queue_wait");
    const run = () => {
      wait?.end();
      return operation();
    };
    const result = this.#tail.then(run, run);
    this.#tail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }
}

function abortFailure(signal: AbortSignal): Promise<never> {
  return new Promise((_, reject) => {
    signal.addEventListener(
      "abort",
      () =>
        reject(
          new ExperimentalSyncHttpError(
            "sync HTTP request was aborted",
            "aborted",
          ),
        ),
      { once: true },
    );
  });
}

function assertSecureHttpUrl(baseUrl: string, allowLocal: boolean): void {
  let url: URL;
  try {
    url = new URL(baseUrl);
  } catch {
    throw new TypeError("sync HTTP baseUrl must be an absolute URL");
  }
  if (url.protocol === "https:") return;
  const local = ["localhost", "127.0.0.1", "[::1]", "10.0.2.2"].includes(
    url.hostname,
  );
  if (url.protocol !== "http:" || !allowLocal || !local) {
    throw new TypeError(
      "sync HTTP baseUrl must use https unless insecure localhost is explicitly enabled",
    );
  }
}

const DEFAULT_HTTP_LIMITS: ExperimentalSyncHttpLimits = {
  requestTimeoutMs: 30_000,
  maxRequestBytes: 4 * 1024 * 1024,
  maxResponseBytes: 4 * 1024 * 1024,
};

function limits(
  configured?: Partial<ExperimentalSyncHttpLimits>,
): ExperimentalSyncHttpLimits {
  const result = { ...DEFAULT_HTTP_LIMITS, ...configured };
  for (const [name, value] of Object.entries(result)) {
    if (!Number.isSafeInteger(value) || value <= 0) {
      throw new TypeError(`sync HTTP ${name} must be a positive safe integer`);
    }
  }
  return result;
}

function bodyBytes(body: ExperimentalSyncHttpBody): number {
  if (typeof body === "string")
    return new TextEncoder().encode(body).byteLength;
  return body.byteLength;
}

async function boundedResponseBytes(
  response: Response,
  maximum: number,
): Promise<ArrayBuffer> {
  const declared = response.headers.get("Content-Length");
  if (declared != null) {
    if (!/^\d+$/u.test(declared)) {
      throw new ExperimentalSyncHttpError(
        "sync HTTP response has an invalid content length",
        "body_limit",
      );
    }
    if (Number(declared) > maximum) {
      throw new ExperimentalSyncHttpError(
        "sync HTTP response exceeds the configured byte limit",
        "body_limit",
      );
    }
  }

  const reader = response.body?.getReader();
  if (reader != null) {
    const chunks: Uint8Array[] = [];
    let length = 0;
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      length += value.byteLength;
      if (length > maximum) {
        await reader.cancel();
        throw new ExperimentalSyncHttpError(
          "sync HTTP response exceeds the configured byte limit",
          "body_limit",
        );
      }
      chunks.push(value);
    }
    const bytes = new Uint8Array(length);
    let offset = 0;
    for (const chunk of chunks) {
      bytes.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return bytes.buffer;
  }
  if (declared == null) {
    throw new ExperimentalSyncHttpError(
      "sync HTTP response cannot be bounded before allocation",
      "body_limit",
    );
  }
  const bytes = await response.arrayBuffer();
  if (bytes.byteLength > maximum) {
    throw new ExperimentalSyncHttpError(
      "sync HTTP response exceeds the configured byte limit",
      "body_limit",
    );
  }
  return bytes;
}

function encodeJson(value: unknown): string {
  const encoded = JSON.stringify(value, (_key, item: unknown) => {
    if (typeof item !== "bigint") {
      return item;
    }
    const number = Number(item);
    if (!Number.isSafeInteger(number)) {
      throw new RangeError("sync JSON integer exceeds the safe range");
    }
    return number;
  });
  if (encoded === undefined) {
    throw new TypeError("sync HTTP value must be JSON serializable");
  }
  return encoded;
}

function parseProtocolJson(json: string): SyncJsonValue {
  const value: unknown = JSON.parse(json);
  if (!isSyncJsonValue(value)) {
    throw new TypeError("sync protocol JSON is not a finite JSON value");
  }
  return value;
}

async function decodeJsonResponse(bytes: ArrayBuffer): Promise<string> {
  // Hermes exposes Fetch response decoding even on runtimes without a global
  // TextDecoder. Keep the JSON-only prototype on that existing boundary.
  const value: unknown = JSON.parse(await new Response(bytes).text());
  if (!isSyncJsonValue(value)) {
    throw new TypeError("sync HTTP response is not a finite JSON value");
  }
  return JSON.stringify(value);
}

function isSyncJsonValue(value: unknown): value is SyncJsonValue {
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "string"
  ) {
    return true;
  }
  if (typeof value === "number") {
    return Number.isFinite(value);
  }
  if (Array.isArray(value)) {
    return value.every(isSyncJsonValue);
  }
  if (typeof value !== "object") {
    return false;
  }
  return Object.values(value).every(isSyncJsonValue);
}

export interface ExperimentalSubscriptionHttpNativeBridge {
  encodeSubscriptionRequestJson(
    requestJson: string,
    options?: CallOptions,
  ): Promise<ArrayBuffer>;
  decodeSubscriptionResponse(
    bytes: ArrayBuffer,
    options?: CallOptions,
  ): Promise<string>;
}

/** One bounded CBOR exchange. Callers durably start/cancel and install responses. */
export function experimentalCanonicalSubscriptionHttpTransport(config: {
  baseUrl: string;
  native: ExperimentalSubscriptionHttpNativeBridge;
  accessToken: (options?: CallOptions) => string | Promise<string>;
  fetch?: typeof globalThis.fetch;
  requestTimeoutMs?: number;
  allowInsecureLocalhost?: boolean;
}): {
  exchange(request: WindowRequest, options?: CallOptions): Promise<string>;
} {
  assertSecureHttpUrl(config.baseUrl, config.allowInsecureLocalhost ?? false);
  const endpoint = new URL(config.baseUrl);
  if (
    endpoint.username ||
    endpoint.password ||
    endpoint.search ||
    endpoint.hash
  )
    throw new TypeError(
      "Subscription base URL must not contain credentials, query or fragment",
    );
  const timeoutMs = config.requestTimeoutMs ?? 30000;
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 300000)
    throw new RangeError("Invalid subscription HTTP deadline");
  const mediaType = "application/vnd.surrealdb-subscription.v1+cbor";
  const fetcher = config.fetch ?? globalThis.fetch;
  return {
    async exchange(request, options) {
      validateWindowRequest(request);
      if (options?.signal.aborted)
        throw new ExperimentalSyncHttpError(
          "Subscription request was aborted",
          "aborted",
        );
      const controller = new AbortController();
      let timedOut = false;
      const onAbort = () => controller.abort();
      options?.signal.addEventListener("abort", onAbort, { once: true });
      const timeout = setTimeout(() => {
        timedOut = true;
        controller.abort();
      }, timeoutMs);
      const boundedOptions = { signal: controller.signal };
      let receivedResponse = false;
      const operation = async () => {
        const json = JSON.stringify(request);
        if (new TextEncoder().encode(json).byteLength > 262144)
          throw new ExperimentalSyncHttpError(
            "Subscription request exceeds its byte limit",
            "body_limit",
          );
        const body = await config.native.encodeSubscriptionRequestJson(
          json,
          boundedOptions,
        );
        if (body.byteLength > 262144)
          throw new ExperimentalSyncHttpError(
            "Subscription request exceeds its byte limit",
            "body_limit",
          );
        if (controller.signal.aborted)
          throw new ExperimentalSyncHttpError(
            "Subscription request was aborted",
            "aborted",
          );
        const token = await config.accessToken(boundedOptions);
        if (!token.trim())
          throw new ExperimentalSyncHttpError(
            "Subscription access token is empty",
            "protocol",
          );
        if (controller.signal.aborted)
          throw new ExperimentalSyncHttpError(
            "Subscription request was aborted",
            "aborted",
          );
        const response = await fetcher(
          `${config.baseUrl.replace(/\/$/, "")}/v1/sync/subscriptions`,
          {
            method: "POST",
            headers: {
              Accept: mediaType,
              "Content-Type": mediaType,
              Authorization: `Bearer ${token}`,
            },
            body,
            signal: controller.signal,
            redirect: "error",
          },
        );
        receivedResponse = true;
        if (response.status !== 200)
          throw new ExperimentalSyncHttpError(
            `Subscription request failed with status ${response.status}`,
            "http",
            response.status,
          );
        if (
          response.headers.get("Content-Type")?.split(";", 1)[0]?.trim() !==
          mediaType
        )
          throw new ExperimentalSyncHttpError(
            "Unexpected subscription content type",
            "content_type",
          );
        const bytes = await boundedResponseBytes(response, 262144);
        if (controller.signal.aborted)
          throw new ExperimentalSyncHttpError(
            "Subscription request was aborted",
            "aborted",
          );
        const decoded = await config.native.decodeSubscriptionResponse(
          bytes,
          boundedOptions,
        );
        if (controller.signal.aborted)
          throw new ExperimentalSyncHttpError(
            "Subscription request was aborted",
            "aborted",
          );
        return decoded;
      };
      try {
        return await Promise.race([
          operation(),
          abortFailure(controller.signal),
        ]);
      } catch (error) {
        if (timedOut)
          throw new ExperimentalSyncHttpError(
            "Subscription request timed out",
            "timeout",
          );
        if (options?.signal.aborted)
          throw new ExperimentalSyncHttpError(
            "Subscription request was aborted",
            "aborted",
          );
        if (error instanceof ExperimentalSyncHttpError) throw error;
        throw new ExperimentalSyncHttpError(
          "Subscription exchange failed",
          receivedResponse ? "protocol" : "network",
        );
      } finally {
        clearTimeout(timeout);
        options?.signal.removeEventListener("abort", onAbort);
      }
    },
  };
}
