import type { ExperimentalSyncInvalidationSource } from "./sync-scheduler";

export type ExperimentalSyncWebSocketHintsOptions = {
  url: () => string | Promise<string>;
  webSocketFactory?: (url: string) => WebSocket;
  maxMessageBytes?: number;
  minimumHintIntervalMs?: number;
  baseReconnectMs?: number;
  maxReconnectMs?: number;
  now?: () => number;
  random?: () => number;
  setTimer?: (
    callback: () => void,
    delayMs: number,
  ) => ReturnType<typeof setTimeout>;
  clearTimer?: (timer: ReturnType<typeof setTimeout>) => void;
  allowInsecureLocalhost?: boolean;
};

/** Best-effort WebSocket wakeups. Messages never carry protocol state or ordering. */
export class ExperimentalSyncWebSocketHints implements ExperimentalSyncInvalidationSource {
  readonly #options: Required<
    Omit<
      ExperimentalSyncWebSocketHintsOptions,
      "webSocketFactory" | "allowInsecureLocalhost"
    >
  > & { webSocketFactory: (url: string) => WebSocket };
  readonly #allowInsecureLocalhost: boolean;
  #stop?: () => void;

  constructor(options: ExperimentalSyncWebSocketHintsOptions) {
    const baseReconnectMs = positive(
      options.baseReconnectMs ?? 1_000,
      "baseReconnectMs",
    );
    const maxReconnectMs = positive(
      options.maxReconnectMs ?? 60_000,
      "maxReconnectMs",
    );
    if (maxReconnectMs < baseReconnectMs) {
      throw new TypeError(
        "sync hint maxReconnectMs must not be below baseReconnectMs",
      );
    }
    this.#options = {
      url: options.url,
      webSocketFactory:
        options.webSocketFactory ?? ((url) => new WebSocket(url)),
      maxMessageBytes: positive(
        options.maxMessageBytes ?? 1_024,
        "maxMessageBytes",
      ),
      minimumHintIntervalMs: positive(
        options.minimumHintIntervalMs ?? 250,
        "minimumHintIntervalMs",
      ),
      baseReconnectMs,
      maxReconnectMs,
      now: options.now ?? Date.now,
      random: options.random ?? Math.random,
      setTimer: options.setTimer ?? setTimeout,
      clearTimer: options.clearTimer ?? clearTimeout,
    };
    this.#allowInsecureLocalhost = options.allowInsecureLocalhost === true;
  }

  start(onHint: () => void, onFailure: (error: unknown) => void): () => void {
    if (this.#stop != null)
      throw new Error("sync WebSocket hints are already started");
    let stopped = false;
    let socket: WebSocket | undefined;
    let reconnect: ReturnType<typeof setTimeout> | undefined;
    let attempt = 0;
    let lastHintAt = Number.NEGATIVE_INFINITY;

    const schedule = () => {
      if (stopped || reconnect != null) return;
      attempt += 1;
      const cap = Math.min(
        this.#options.maxReconnectMs,
        this.#options.baseReconnectMs * 2 ** Math.min(attempt - 1, 30),
      );
      const sample = this.#options.random();
      const delay = Math.floor(
        Math.max(0, Math.min(Number.isFinite(sample) ? sample : 1, 1)) * cap,
      );
      reconnect = this.#options.setTimer(() => {
        reconnect = undefined;
        void connect();
      }, delay);
    };

    const connect = async () => {
      if (stopped) return;
      try {
        const url = await this.#options.url();
        assertSecureWebSocketUrl(url, this.#allowInsecureLocalhost);
        if (stopped) return;
        const next = this.#options.webSocketFactory(url);
        socket = next;
        next.onopen = () => {
          attempt = 0;
        };
        next.onmessage = (event) => {
          if (messageBytes(event.data) > this.#options.maxMessageBytes) {
            onFailure(new Error("sync WebSocket hint exceeds the byte limit"));
            next.close(1009, "hint too large");
            return;
          }
          const current = this.#options.now();
          if (current - lastHintAt < this.#options.minimumHintIntervalMs)
            return;
          lastHintAt = current;
          onHint();
        };
        next.onerror = () =>
          onFailure(new Error("sync WebSocket hint connection failed"));
        next.onclose = () => {
          if (socket !== next) return;
          socket = undefined;
          schedule();
        };
      } catch (error) {
        onFailure(error);
        schedule();
      }
    };

    void connect();
    this.#stop = () => {
      if (stopped) return;
      stopped = true;
      if (reconnect != null) this.#options.clearTimer(reconnect);
      reconnect = undefined;
      socket?.close(1000, "sync hints stopped");
      socket = undefined;
      this.#stop = undefined;
    };
    return this.#stop;
  }
}

function positive(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`sync hint ${name} must be a positive safe integer`);
  }
  return value;
}

function messageBytes(value: unknown): number {
  if (typeof value === "string")
    return new TextEncoder().encode(value).byteLength;
  if (value instanceof ArrayBuffer) return value.byteLength;
  if (ArrayBuffer.isView(value)) return value.byteLength;
  if (typeof Blob !== "undefined" && value instanceof Blob) return value.size;
  return Number.POSITIVE_INFINITY;
}

function assertSecureWebSocketUrl(value: string, allowLocal: boolean): void {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new TypeError("sync hint URL must be absolute");
  }
  if (url.protocol === "wss:") return;
  const local = ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname);
  if (url.protocol !== "ws:" || !allowLocal || !local) {
    throw new TypeError(
      "sync hint URL must use wss unless insecure localhost is explicitly enabled",
    );
  }
}
