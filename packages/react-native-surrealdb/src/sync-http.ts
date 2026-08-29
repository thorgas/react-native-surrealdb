import type { CallOptions } from "./client";
import {
  ExperimentalSyncClient,
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
  baseUrl: string;
  partitionId: string;
  clientId: string;
  requestedScope: string;
  subscriptionRevision: bigint;
  accessToken: () => string | Promise<string>;
  codec: ExperimentalSyncHttpCodec;
  fetch?: typeof globalThis.fetch;
  limits?: Partial<ExperimentalSyncHttpLimits>;
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
  readonly #baseUrl: string;
  readonly #partitionId: string;
  readonly #clientId: string;
  readonly #requestedScope: string;
  readonly #subscriptionRevision: bigint;
  readonly #accessToken: () => string | Promise<string>;
  readonly #codec: ExperimentalSyncHttpCodec;
  readonly #fetch: typeof globalThis.fetch;
  readonly #limits: ExperimentalSyncHttpLimits;
  #tail: Promise<void> = Promise.resolve();

  constructor(options: ExperimentalSyncHttpOptions) {
    const baseUrl = options.baseUrl.replace(/\/+$/, "");
    if (!/^https?:\/\//u.test(baseUrl)) {
      throw new TypeError("sync HTTP baseUrl must use http or https");
    }
    if (options.codec.mediaType.trim().length === 0) {
      throw new TypeError("sync HTTP codec mediaType is required");
    }
    this.#limits = limits(options.limits);
    this.#sync = options.sync;
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
    return this.#serialize(() => this.#push(options));
  }

  pull(options?: CallOptions): Promise<ExperimentalSyncStatus> {
    return this.#serialize(() => this.#pull(options));
  }

  syncOnce(options?: CallOptions): Promise<ExperimentalSyncOnceResult> {
    return this.#serialize(async () => ({
      push: await this.#push(options),
      pull: await this.#pull(options),
    }));
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
      statuses.push(
        await this.#sync.recordPushResponseProtocolJson(responseJson, options),
      );
    }

    return statuses;
  }

  async #pull(options?: CallOptions): Promise<ExperimentalSyncStatus> {
    const checkpoint = await this.#sync.checkpointToken(options);
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
    return this.#sync.applyPullResponseProtocolJson(responseJson, options);
  }

  async #postPush(
    path: string,
    request: ExperimentalSyncPushRequest,
    options?: CallOptions,
  ): Promise<string> {
    return this.#post(
      path,
      await this.#codec.encodePushRequest(request, options),
      (response) => this.#codec.decodePushResponse(response, options),
      options,
    );
  }

  async #postPull(
    path: string,
    request: ExperimentalSyncPullRequest,
    options?: CallOptions,
  ): Promise<string> {
    return this.#post(
      path,
      await this.#codec.encodePullRequest(request, options),
      (response) => this.#codec.decodePullResponse(response, options),
      options,
    );
  }

  async #post(
    path: string,
    body: ExperimentalSyncHttpBody,
    decode: (bytes: ArrayBuffer) => Promise<string>,
    options?: CallOptions,
  ): Promise<string> {
    const token = await this.#accessToken();
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

    const controller = new AbortController();
    let timedOut = false;
    const onAbort = () => controller.abort();
    options?.signal.addEventListener("abort", onAbort, { once: true });
    const timeout = setTimeout(() => {
      timedOut = true;
      controller.abort();
    }, this.#limits.requestTimeoutMs);
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
        signal: controller.signal,
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
      return await decode(bytes);
    } catch (error) {
      if (timedOut)
        throw new ExperimentalSyncHttpError(
          "sync HTTP request timed out",
          "timeout",
        );
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
    } finally {
      clearTimeout(timeout);
      options?.signal.removeEventListener("abort", onAbort);
    }
  }

  #serialize<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.#tail.then(operation, operation);
    this.#tail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
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
