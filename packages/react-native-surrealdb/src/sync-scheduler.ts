import {
  ExperimentalSyncHttpError,
  type ExperimentalSyncHttpAdapter,
} from "./sync-http";

export interface ExperimentalSyncConnectivity {
  current(): boolean | undefined;
  subscribe(listener: (online: boolean) => void): () => void;
}

export interface ExperimentalSyncInvalidationSource {
  start(onHint: () => void, onFailure: (error: unknown) => void): () => void;
}

export type ExperimentalSyncReason =
  | "startup"
  | "local_mutation"
  | "manual"
  | "online"
  | "invalidation"
  | "periodic"
  | "retry";

export type ExperimentalSyncSchedulerStatus =
  | { state: "stopped" }
  | { state: "offline" }
  | { state: "idle"; lastSuccessAt?: number }
  | { state: "syncing"; reason: ExperimentalSyncReason }
  | { state: "backoff"; attempt: number; retryAt: number }
  | { state: "authentication_required"; status: 401 | 403 }
  | { state: "failed"; error: ExperimentalSyncHttpError };

export type ExperimentalSyncSchedulerOptions = {
  adapter: Pick<ExperimentalSyncHttpAdapter, "pull" | "syncOnce">;
  connectivity?: ExperimentalSyncConnectivity;
  invalidations?: ExperimentalSyncInvalidationSource;
  baseRetryMs?: number;
  maxRetryMs?: number;
  periodicPullMs?: number;
  now?: () => number;
  random?: () => number;
  setTimer?: (
    callback: () => void,
    delayMs: number,
  ) => ReturnType<typeof setTimeout>;
  clearTimer?: (timer: ReturnType<typeof setTimeout>) => void;
};

type Cycle = "pull" | "sync";

/**
 * Advisory in-memory scheduling around the durable native sync state machine.
 * Losing this object loses only timing; restart immediately re-reads the outbox/checkpoint.
 */
export class ExperimentalSyncScheduler {
  readonly #adapter: ExperimentalSyncSchedulerOptions["adapter"];
  readonly #connectivity?: ExperimentalSyncConnectivity;
  readonly #invalidations?: ExperimentalSyncInvalidationSource;
  readonly #baseRetryMs: number;
  readonly #maxRetryMs: number;
  readonly #periodicPullMs?: number;
  readonly #now: () => number;
  readonly #random: () => number;
  readonly #setTimer: NonNullable<ExperimentalSyncSchedulerOptions["setTimer"]>;
  readonly #clearTimer: NonNullable<
    ExperimentalSyncSchedulerOptions["clearTimer"]
  >;
  readonly #listeners = new Set<
    (status: ExperimentalSyncSchedulerStatus) => void
  >();
  #status: ExperimentalSyncSchedulerStatus = { state: "stopped" };
  #started = false;
  #online = true;
  #pending?: { cycle: Cycle; reason: ExperimentalSyncReason };
  #active?: { controller: AbortController; cycle: Cycle };
  #retryAttempt = 0;
  #retryTimer?: ReturnType<typeof setTimeout>;
  #periodicTimer?: ReturnType<typeof setTimeout>;
  #unsubscribeConnectivity?: () => void;
  #stopInvalidations?: () => void;
  #authenticationBlocked = false;

  constructor(options: ExperimentalSyncSchedulerOptions) {
    this.#adapter = options.adapter;
    this.#connectivity = options.connectivity;
    this.#invalidations = options.invalidations;
    this.#baseRetryMs = positive(options.baseRetryMs ?? 1_000, "baseRetryMs");
    this.#maxRetryMs = positive(options.maxRetryMs ?? 60_000, "maxRetryMs");
    if (this.#maxRetryMs < this.#baseRetryMs) {
      throw new TypeError(
        "sync scheduler maxRetryMs must not be below baseRetryMs",
      );
    }
    this.#periodicPullMs =
      options.periodicPullMs == null
        ? undefined
        : positive(options.periodicPullMs, "periodicPullMs");
    this.#now = options.now ?? Date.now;
    this.#random = options.random ?? Math.random;
    this.#setTimer = options.setTimer ?? setTimeout;
    this.#clearTimer = options.clearTimer ?? clearTimeout;
  }

  get status(): ExperimentalSyncSchedulerStatus {
    return this.#status;
  }

  start(): void {
    if (this.#started) return;
    this.#started = true;
    this.#online = this.#connectivity?.current() !== false;
    this.#unsubscribeConnectivity = this.#connectivity?.subscribe((online) =>
      this.#setOnline(online),
    );
    if (!this.#online) {
      this.#pending = { cycle: "sync", reason: "startup" };
      this.#publish({ state: "offline" });
      return;
    }
    this.#startInvalidations();
    this.#queue("sync", "startup", true);
  }

  stop(): void {
    if (!this.#started) return;
    this.#started = false;
    this.#pending = undefined;
    this.#active?.controller.abort();
    this.#active = undefined;
    this.#clearTimers();
    this.#unsubscribeConnectivity?.();
    this.#unsubscribeConnectivity = undefined;
    this.#stopInvalidationSource();
    this.#publish({ state: "stopped" });
  }

  requestSync(reason: "local_mutation" | "manual" = "manual"): void {
    this.#authenticationBlocked = false;
    this.#cancelRetry();
    this.#queue("sync", reason, true);
  }

  requestPull(reason: "invalidation" | "periodic" = "invalidation"): void {
    this.#queue("pull", reason, false);
  }

  subscribe(
    listener: (status: ExperimentalSyncSchedulerStatus) => void,
  ): () => void {
    this.#listeners.add(listener);
    listener(this.#status);
    return () => this.#listeners.delete(listener);
  }

  #queue(
    cycle: Cycle,
    reason: ExperimentalSyncReason,
    explicit: boolean,
  ): void {
    if (!this.#started) return;
    if (explicit) this.#authenticationBlocked = false;
    if (explicit || this.#pending?.cycle !== "sync") {
      this.#pending = { cycle, reason };
    }
    if (!this.#online) {
      this.#publish({ state: "offline" });
      return;
    }
    if (
      this.#authenticationBlocked ||
      this.#active != null ||
      this.#retryTimer != null
    )
      return;
    void this.#drain();
  }

  async #drain(): Promise<void> {
    const pending = this.#pending;
    if (
      !this.#started ||
      !this.#online ||
      pending == null ||
      this.#active != null
    )
      return;
    this.#pending = undefined;
    const controller = new AbortController();
    this.#active = { controller, cycle: pending.cycle };
    this.#publish({ state: "syncing", reason: pending.reason });
    try {
      if (pending.cycle === "sync") {
        await this.#adapter.syncOnce({ signal: controller.signal });
      } else {
        await this.#adapter.pull({ signal: controller.signal });
      }
      if (!this.#started) return;
      this.#retryAttempt = 0;
      this.#publish({ state: "idle", lastSuccessAt: this.#now() });
      this.#schedulePeriodic();
    } catch (error) {
      if (!this.#started) return;
      if (!this.#online) {
        this.#pending = prefer(this.#pending, pending);
        this.#publish({ state: "offline" });
        return;
      }
      const failure = normalize(error);
      if (failure.kind === "aborted") return;
      if (
        failure.kind === "http" &&
        (failure.status === 401 || failure.status === 403)
      ) {
        this.#authenticationBlocked = true;
        this.#publish({
          state: "authentication_required",
          status: failure.status,
        });
        return;
      }
      if (!retryable(failure)) {
        this.#publish({ state: "failed", error: failure });
        return;
      }
      this.#pending = prefer(this.#pending, {
        cycle: pending.cycle,
        reason: "retry",
      });
      this.#scheduleRetry();
    } finally {
      if (this.#active?.controller === controller) this.#active = undefined;
      if (
        this.#started &&
        this.#retryTimer == null &&
        !this.#authenticationBlocked
      ) {
        void this.#drain();
      }
    }
  }

  #setOnline(online: boolean): void {
    if (!this.#started || this.#online === online) return;
    this.#online = online;
    if (!online) {
      if (this.#active != null) {
        this.#pending = prefer(this.#pending, {
          cycle: this.#active.cycle,
          reason: "online",
        });
        this.#active.controller.abort();
      }
      this.#cancelRetry();
      this.#stopInvalidationSource();
      this.#publish({ state: "offline" });
      return;
    }
    this.#authenticationBlocked = false;
    this.#startInvalidations();
    this.#queue("sync", "online", true);
  }

  #scheduleRetry(): void {
    this.#retryAttempt += 1;
    const exponent = Math.min(this.#retryAttempt - 1, 30);
    const cap = Math.min(this.#maxRetryMs, this.#baseRetryMs * 2 ** exponent);
    const sample = this.#random();
    const delay = Math.floor(
      (Number.isFinite(sample) ? Math.max(0, Math.min(sample, 1)) : 1) * cap,
    );
    this.#publish({
      state: "backoff",
      attempt: this.#retryAttempt,
      retryAt: this.#now() + delay,
    });
    this.#retryTimer = this.#setTimer(() => {
      this.#retryTimer = undefined;
      void this.#drain();
    }, delay);
  }

  #schedulePeriodic(): void {
    if (
      !this.#started ||
      this.#periodicPullMs == null ||
      this.#periodicTimer != null
    )
      return;
    this.#periodicTimer = this.#setTimer(() => {
      this.#periodicTimer = undefined;
      this.requestPull("periodic");
    }, this.#periodicPullMs);
  }

  #startInvalidations(): void {
    if (this.#stopInvalidations != null) return;
    this.#stopInvalidations = this.#invalidations?.start(
      () => this.requestPull("invalidation"),
      () => this.#schedulePeriodic(),
    );
  }

  #stopInvalidationSource(): void {
    this.#stopInvalidations?.();
    this.#stopInvalidations = undefined;
  }

  #cancelRetry(): void {
    if (this.#retryTimer == null) return;
    this.#clearTimer(this.#retryTimer);
    this.#retryTimer = undefined;
    this.#retryAttempt = 0;
  }

  #clearTimers(): void {
    this.#cancelRetry();
    if (this.#periodicTimer != null) this.#clearTimer(this.#periodicTimer);
    this.#periodicTimer = undefined;
  }

  #publish(status: ExperimentalSyncSchedulerStatus): void {
    this.#status = status;
    for (const listener of this.#listeners) listener(status);
  }
}

function prefer(
  current: { cycle: Cycle; reason: ExperimentalSyncReason } | undefined,
  incoming: { cycle: Cycle; reason: ExperimentalSyncReason },
) {
  return current?.cycle === "sync" ? current : incoming;
}

function positive(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(
      `sync scheduler ${name} must be a positive safe integer`,
    );
  }
  return value;
}

function normalize(error: unknown): ExperimentalSyncHttpError {
  return error instanceof ExperimentalSyncHttpError
    ? error
    : new ExperimentalSyncHttpError(
        "sync cycle failed outside the HTTP transport",
        "protocol",
      );
}

function retryable(error: ExperimentalSyncHttpError): boolean {
  if (error.kind === "network" || error.kind === "timeout") return true;
  return (
    error.kind === "http" &&
    [408, 425, 429, 500, 502, 503, 504].includes(error.status ?? 0)
  );
}
