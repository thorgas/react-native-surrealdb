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
  ): Promise<BodyInit>;
  decodePushResponse(
    response: Response,
    options?: CallOptions,
  ): Promise<string>;
  encodePullRequest(
    request: ExperimentalSyncPullRequest,
    options?: CallOptions,
  ): Promise<BodyInit>;
  decodePullResponse(
    response: Response,
    options?: CallOptions,
  ): Promise<string>;
};

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
};

export type ExperimentalSyncOnceResult = {
  push: ExperimentalSyncStatus[];
  pull: ExperimentalSyncStatus;
};

/** Transport failure. Protocol conflicts and rejections remain normal HTTP 200 outcomes. */
export class ExperimentalSyncHttpError extends Error {
  constructor(
    message: string,
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
  async decodePushResponse(response) {
    return decodeJsonResponse(response);
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
  async decodePullResponse(response) {
    return decodeJsonResponse(response);
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
    async decodePushResponse(response, options) {
      return native.decodeSyncPushResponse(
        await responseBytes(response),
        options,
      );
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
    async decodePullResponse(response, options) {
      return native.decodeSyncPullResponse(
        await responseBytes(response),
        options,
      );
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
  #tail: Promise<void> = Promise.resolve();

  constructor(options: ExperimentalSyncHttpOptions) {
    const baseUrl = options.baseUrl.replace(/\/+$/, "");
    if (!/^https?:\/\//u.test(baseUrl)) {
      throw new TypeError("sync HTTP baseUrl must use http or https");
    }
    if (options.codec.mediaType.trim().length === 0) {
      throw new TypeError("sync HTTP codec mediaType is required");
    }
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
    body: BodyInit,
    decode: (response: Response) => Promise<string>,
    options?: CallOptions,
  ): Promise<string> {
    const token = await this.#accessToken();
    if (token.trim().length === 0) {
      throw new ExperimentalSyncHttpError("sync access token is empty");
    }

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
    if (response.status !== 200) {
      throw new ExperimentalSyncHttpError(
        `sync HTTP request failed with status ${response.status}`,
        response.status,
      );
    }
    return decode(response);
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

async function decodeJsonResponse(response: Response): Promise<string> {
  const value: unknown = await response.json();
  if (!isSyncJsonValue(value)) {
    throw new TypeError("sync HTTP response is not a finite JSON value");
  }
  return JSON.stringify(value);
}

async function responseBytes(response: Response): Promise<ArrayBuffer> {
  return response.arrayBuffer();
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
