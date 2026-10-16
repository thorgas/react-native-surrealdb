/** Metadata-only telemetry. Never pass payloads, checkpoint tokens or error text. */
export type SyncPhase =
  | "enqueue"
  | "native_persistence"
  | "push"
  | "pull"
  | "encode"
  | "decode"
  | "transport"
  | "projection_checkpoint"
  | "queue_wait"
  | "scheduler"
  | "authority"
  | "native_storage"
  | "native_encoding";
export type SyncReason =
  | "ok"
  | "error"
  | "local_durable"
  | "authority_accepted"
  | "authority_conflict"
  | "authority_rejected"
  | "locally_visible"
  | "offline"
  | "backoff"
  | "authentication_required"
  | "scope_changed"
  | "recovery_required"
  | "stopped"
  | "idle"
  | "syncing"
  | "failed";
export type SyncTelemetryEvent = Readonly<{
  schemaVersion: 1;
  sequence: number;
  span: number;
  parent?: number;
  commit?: number;
  phase: SyncPhase;
  boundary: "start" | "end" | "state";
  origin: "client" | "native" | "server";
  atMs: number;
  durationMs?: number;
  reason?: SyncReason;
  bytes?: number;
  responseBytes?: number;
  serverRequest?: number;
}>;
export type SyncExporter = (event: SyncTelemetryEvent) => void | Promise<void>;
type Sink = {
  send: SyncExporter;
  queue: SyncTelemetryEvent[];
  busy: boolean;
  errors: number;
  dropped: number;
  delivered: number;
  active: boolean;
};
const phases = new Set<SyncPhase>([
  "enqueue",
  "native_persistence",
  "push",
  "pull",
  "encode",
  "decode",
  "transport",
  "projection_checkpoint",
  "queue_wait",
  "scheduler",
  "authority",
  "native_storage",
  "native_encoding",
]);
const reasons = new Set<SyncReason>([
  "ok",
  "error",
  "local_durable",
  "authority_accepted",
  "authority_conflict",
  "authority_rejected",
  "locally_visible",
  "offline",
  "backoff",
  "authentication_required",
  "scope_changed",
  "recovery_required",
  "stopped",
  "idle",
  "syncing",
  "failed",
]);

/**
 * Independent FIFO per exporter, drop-newest backpressure, at most one unresolved
 * callback per sink. A timed-out flush does not detach a hung callback and retry
 * it: that would accumulate unbounded promises. Exporters must yield to JS.
 */
export class ExperimentalSyncTelemetry {
  readonly #capacity: number;
  readonly #now: () => number;
  readonly #sinks = new Set<Sink>();
  #sequence = 0;
  #span = 0;
  readonly #commits = new Map<string, number>();
  #correlationEvictions = 0;
  #closed = false;
  #lastSchedulerReason?: SyncReason;
  #lastPushSuccessAtMs?: number;
  #lastPullSuccessAtMs?: number;
  #retired = { delivered: 0, dropped: 0, errors: 0 };
  constructor(options: { capacity?: number; now?: () => number } = {}) {
    this.#capacity = options.capacity ?? 64;
    if (
      !Number.isSafeInteger(this.#capacity) ||
      this.#capacity < 1 ||
      this.#capacity > 65536
    )
      throw new TypeError("Invalid telemetry capacity");
    this.#now = options.now ?? (() => performance.now());
  }
  now(): number {
    try {
      const n = this.#now();
      return Number.isFinite(n) ? n : 0;
    } catch {
      return 0;
    }
  }
  subscribe(send: SyncExporter): () => void {
    if (this.#closed) throw new Error("Telemetry closed");
    if (this.#sinks.size >= 16) throw new Error("Telemetry exporter limit");
    const sink: Sink = {
      send,
      queue: [],
      busy: false,
      errors: 0,
      dropped: 0,
      delivered: 0,
      active: true,
    };
    this.#sinks.add(sink);
    return () => {
      if (!sink.active) return;
      sink.active = false;
      sink.dropped += sink.queue.length;
      sink.queue.length = 0;
      if (!sink.busy) this.#retire(sink);
    };
  }
  begin(
    phase: SyncPhase,
    parent?: number,
    origin: SyncTelemetryEvent["origin"] = "client",
  ): {
    id: number;
    end: (
      reason?: SyncReason,
      bytes?: number,
      responseBytes?: number,
      serverRequest?: number,
    ) => void;
  } {
    const id = ++this.#span;
    const start = this.now();
    this.record({
      span: id,
      parent,
      phase,
      boundary: "start",
      origin,
      atMs: start,
    });
    let ended = false;
    return {
      id,
      end: (reason = "ok", bytes, responseBytes, serverRequest) => {
        if (ended) return;
        ended = true;
        this.record({
          span: id,
          parent,
          phase,
          boundary: "end",
          origin,
          atMs: this.now(),
          durationMs: Math.max(0, this.now() - start),
          reason,
          bytes,
          responseBytes,
          serverRequest,
        });
      },
    };
  }
  /** Session-local commit ordinal, bounded to 1024 identities; never exported raw. */
  correlate(identity: string): number {
    if (typeof identity !== "string" || identity.length > 128)
      return ++this.#span;
    const existing = this.#commits.get(identity);
    if (existing !== undefined) return existing;
    if (this.#commits.size >= 1024) {
      this.#commits.delete(this.#commits.keys().next().value!);
      this.#correlationEvictions++;
    }
    const ordinal = ++this.#span;
    this.#commits.set(identity, ordinal);
    return ordinal;
  }
  milestone(identity: string, reason: SyncReason): void {
    this.record({
      span: ++this.#span,
      commit: this.correlate(identity),
      phase: "native_persistence",
      boundary: "state",
      origin: "client",
      atMs: this.now(),
      reason,
    });
  }

  async measure<T>(
    phase: SyncPhase,
    operation: () => Promise<T>,
    parent?: number,
    success: SyncReason = "ok",
  ): Promise<T> {
    const span = this.begin(phase, parent);
    try {
      const value = await operation();
      span.end(success);
      return value;
    } catch (error) {
      span.end("error");
      throw error;
    }
  }
  state(reason: SyncReason): void {
    this.record({
      span: ++this.#span,
      phase: "scheduler",
      boundary: "state",
      origin: "client",
      atMs: this.now(),
      reason,
    });
  }
  /** For native/server producers only when their clocks actually measured the phase. */
  record(input: Omit<SyncTelemetryEvent, "schemaVersion" | "sequence">): void {
    if (this.#closed || !phases.has(input.phase)) return;
    // Reconstruct, never spread caller metadata (including structurally typed extras).
    const event: SyncTelemetryEvent = Object.freeze({
      schemaVersion: 1,
      sequence: ++this.#sequence,
      span: safe(input.span),
      parent: input.parent === undefined ? undefined : safe(input.parent),
      commit: input.commit === undefined ? undefined : safe(input.commit),
      phase: input.phase,
      boundary: ["start", "end", "state"].includes(input.boundary)
        ? input.boundary
        : "state",
      origin: ["native", "server"].includes(input.origin)
        ? input.origin
        : "client",
      atMs: finite(input.atMs),
      durationMs:
        input.durationMs === undefined ? undefined : finite(input.durationMs),
      reason:
        input.reason && reasons.has(input.reason) ? input.reason : undefined,
      bytes: input.bytes === undefined ? undefined : safe(input.bytes),
      responseBytes:
        input.responseBytes === undefined
          ? undefined
          : safe(input.responseBytes),
      serverRequest:
        input.serverRequest === undefined
          ? undefined
          : safe(input.serverRequest),
    });
    if (event.phase === "scheduler" && event.reason)
      this.#lastSchedulerReason = event.reason;
    if (event.boundary === "end" && event.reason === "ok") {
      if (event.phase === "push") this.#lastPushSuccessAtMs = event.atMs;
      if (event.phase === "pull") this.#lastPullSuccessAtMs = event.atMs;
    }
    for (const sink of this.#sinks) {
      if (sink.queue.length >= this.#capacity) {
        sink.dropped++;
        continue;
      }
      sink.queue.push(event);
      this.#pump(sink);
    }
  }
  snapshot() {
    let queued = 0,
      inFlight = 0;
    const totals = { ...this.#retired };
    for (const sink of this.#sinks) {
      queued += sink.queue.length;
      inFlight += Number(sink.busy);
      totals.delivered += sink.delivered;
      totals.dropped += sink.dropped;
      totals.errors += sink.errors;
    }
    return Object.freeze({
      schemaVersion: 1 as const,
      sequence: this.#sequence,
      closed: this.#closed,
      lastSchedulerReason: this.#lastSchedulerReason,
      lastPushSuccessAtMs: this.#lastPushSuccessAtMs,
      lastPullSuccessAtMs: this.#lastPullSuccessAtMs,
      capacityPerExporter: this.#capacity,
      exporters: this.#sinks.size,
      correlationEvictions: this.#correlationEvictions,
      queued,
      inFlight,
      ...totals,
    });
  }
  async flush(deadlineMs = 1000): Promise<boolean> {
    if (!Number.isFinite(deadlineMs) || deadlineMs < 0 || deadlineMs > 60000)
      throw new TypeError("Invalid flush deadline");
    const until = Date.now() + deadlineMs;
    while ([...this.#sinks].some((s) => s.busy || s.queue.length)) {
      if (Date.now() >= until) return false;
      await new Promise((resolve) =>
        setTimeout(resolve, Math.min(5, Math.max(0, until - Date.now()))),
      );
    }
    return true;
  }
  async close(deadlineMs = 1000): Promise<boolean> {
    this.#closed = true;
    const flushed = await this.flush(deadlineMs);
    for (const sink of this.#sinks) {
      sink.dropped += sink.queue.length;
      sink.queue.length = 0;
      sink.active = false;
    }
    return flushed;
  }
  #retire(sink: Sink): void {
    if (!this.#sinks.delete(sink)) return;
    this.#retired.delivered += sink.delivered;
    this.#retired.dropped += sink.dropped;
    this.#retired.errors += sink.errors;
  }
  #pump(sink: Sink): void {
    if (sink.busy || !sink.active || !sink.queue.length) return;
    sink.busy = true;
    // Timer task rather than a chained microtask: reentrant sinks cannot starve I/O.
    setTimeout(() => {
      if (!sink.active) {
        sink.busy = false;
        this.#retire(sink);
        return;
      }
      const event = sink.queue.shift()!;
      Promise.resolve()
        .then(() => sink.send(event))
        .then(
          () => {
            sink.delivered++;
          },
          () => {
            sink.errors++;
          },
        )
        .finally(() => {
          sink.busy = false;
          if (sink.active) this.#pump(sink);
          else this.#retire(sink);
        });
    }, 0);
  }
}
function finite(value: number): number {
  return Number.isFinite(value) ? Math.max(0, value) : 0;
}
function safe(value: number): number {
  return Number.isSafeInteger(value) && value >= 0 ? value : 0;
}
