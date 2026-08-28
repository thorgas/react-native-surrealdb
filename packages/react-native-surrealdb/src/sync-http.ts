import type { CallOptions } from "./client";
import {
  ExperimentalSyncClient,
  type ExperimentalSyncStatus,
  type SyncJsonValue,
} from "./sync";

export type ExperimentalSyncHttpCodec = {
  readonly mediaType: string;
  encode(value: unknown): BodyInit;
  decode(response: Response): Promise<SyncJsonValue>;
};

/** Application-owned durable storage for the last completely applied pull checkpoint. */
export type ExperimentalSyncCheckpointStore = {
  load(): string | undefined | Promise<string | undefined>;
  save(checkpoint: string): void | Promise<void>;
};

export type ExperimentalSyncHttpOptions = {
  sync: ExperimentalSyncClient;
  baseUrl: string;
  partitionId: string;
  clientId: string;
  requestedScope: string;
  subscriptionRevision: bigint;
  accessToken: () => string | Promise<string>;
  checkpointStore: ExperimentalSyncCheckpointStore;
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
  encode(value) {
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
  },
  async decode(response) {
    const value: unknown = await response.json();
    if (!isSyncJsonValue(value)) {
      throw new TypeError("sync HTTP response is not a finite JSON value");
    }
    return value;
  },
};

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
  readonly #checkpointStore: ExperimentalSyncCheckpointStore;
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
    if (options.checkpointStore == null) {
      throw new TypeError("sync HTTP checkpointStore is required");
    }

    this.#sync = options.sync;
    this.#baseUrl = baseUrl;
    this.#partitionId = options.partitionId;
    this.#clientId = options.clientId;
    this.#requestedScope = options.requestedScope;
    this.#subscriptionRevision = options.subscriptionRevision;
    this.#accessToken = options.accessToken;
    this.#checkpointStore = options.checkpointStore;
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
    const commits = await this.#sync.pending(options);
    const statuses: ExperimentalSyncStatus[] = [];

    for (const commit of commits) {
      const response = await this.#post(
        "/v1/sync/push",
        {
          schemaVersion: "v1",
          partitionId: this.#partitionId,
          clientId: this.#clientId,
          commit,
        },
        options,
      );
      statuses.push(await this.#sync.recordPushResponse(response, options));
    }

    return statuses;
  }

  async #pull(options?: CallOptions): Promise<ExperimentalSyncStatus> {
    const checkpoint = await this.#checkpointStore.load();
    const response = await this.#post(
      "/v1/sync/pull",
      {
        schemaVersion: "v1",
        partitionId: this.#partitionId,
        clientId: this.#clientId,
        checkpoint: checkpoint ?? null,
        requestedScope: this.#requestedScope,
        subscriptionRevision: this.#subscriptionRevision,
      },
      options,
    );
    const nextCheckpoint = pullCheckpoint(response);
    const status = await this.#sync.applyPullResponse(response, options);
    await this.#checkpointStore.save(nextCheckpoint);
    return status;
  }

  async #post(
    path: string,
    body: unknown,
    options?: CallOptions,
  ): Promise<SyncJsonValue> {
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
      body: this.#codec.encode(body),
      signal: options?.signal,
    });
    if (response.status !== 200) {
      throw new ExperimentalSyncHttpError(
        `sync HTTP request failed with status ${response.status}`,
        response.status,
      );
    }
    return this.#codec.decode(response);
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

function pullCheckpoint(response: SyncJsonValue): string {
  if (!isSyncJsonObject(response)) {
    throw new TypeError("sync pull response must be an object");
  }

  const endFrame = Array.isArray(response.frames)
    ? response.frames.at(-1)
    : undefined;
  const checkpoint =
    response.response === "reset"
      ? response.checkpoint
      : response.response === "batch" &&
          isSyncJsonObject(endFrame) &&
          endFrame.frame === "end"
        ? endFrame.checkpoint
        : undefined;
  if (
    !isSyncJsonObject(checkpoint) ||
    typeof checkpoint.token !== "string" ||
    checkpoint.token.length === 0
  ) {
    throw new TypeError("sync pull response has no complete checkpoint");
  }
  return checkpoint.token;
}

function isSyncJsonObject(
  value: SyncJsonValue | undefined,
): value is { readonly [key: string]: SyncJsonValue } {
  return value !== null && typeof value === "object" && !Array.isArray(value);
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
