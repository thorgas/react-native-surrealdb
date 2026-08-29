import type {
  ExperimentalSyncScheduler,
  ExperimentalSyncSchedulerStatus,
} from "./sync-scheduler";

export type ExperimentalSyncApplicationState =
  "active" | "inactive" | "background" | "unknown";

export interface ExperimentalSyncApplicationLifecycle {
  current(): ExperimentalSyncApplicationState | undefined;
  subscribe(
    listener: (state: ExperimentalSyncApplicationState) => void,
  ): () => void;
}

export type ExperimentalSyncAuthenticationRefresh = (
  status: 401 | 403,
  options: { signal: AbortSignal },
) => Promise<boolean>;

export type ExperimentalSyncLifecycleCoordinatorOptions = {
  scheduler: Pick<
    ExperimentalSyncScheduler,
    "resumeAfterAuthentication" | "start" | "stop" | "subscribe"
  >;
  lifecycle: ExperimentalSyncApplicationLifecycle;
  refreshAuthentication?: ExperimentalSyncAuthenticationRefresh;
  onRefreshError?: (error: unknown) => void;
};

/**
 * Owns the in-memory scheduler for one application lifecycle. Durable sync state remains native.
 * Unknown lifecycle state fails closed; applications map React Native AppState into this interface.
 */
export class ExperimentalSyncLifecycleCoordinator {
  readonly #scheduler: ExperimentalSyncLifecycleCoordinatorOptions["scheduler"];
  readonly #lifecycle: ExperimentalSyncApplicationLifecycle;
  readonly #refreshAuthentication?: ExperimentalSyncAuthenticationRefresh;
  readonly #onRefreshError?: (error: unknown) => void;
  #started = false;
  #active = false;
  #generation = 0;
  #refreshAttempted = false;
  #lastAuthenticationStatus?: 401 | 403;
  #refresh?: AbortController;
  #unsubscribeLifecycle?: () => void;
  #unsubscribeScheduler?: () => void;

  constructor(options: ExperimentalSyncLifecycleCoordinatorOptions) {
    this.#scheduler = options.scheduler;
    this.#lifecycle = options.lifecycle;
    this.#refreshAuthentication = options.refreshAuthentication;
    this.#onRefreshError = options.onRefreshError;
  }

  start(): void {
    if (this.#started) return;
    this.#started = true;
    this.#generation += 1;
    this.#unsubscribeScheduler = this.#scheduler.subscribe((status) =>
      this.#onSchedulerStatus(status),
    );
    this.#unsubscribeLifecycle = this.#lifecycle.subscribe((state) =>
      this.#setApplicationState(state),
    );
    this.#setApplicationState(this.#lifecycle.current() ?? "unknown");
  }

  stop(): void {
    if (!this.#started) return;
    this.#started = false;
    this.#active = false;
    this.#generation += 1;
    this.#refreshAttempted = false;
    this.#lastAuthenticationStatus = undefined;
    this.#refresh?.abort();
    this.#refresh = undefined;
    this.#unsubscribeLifecycle?.();
    this.#unsubscribeLifecycle = undefined;
    this.#unsubscribeScheduler?.();
    this.#unsubscribeScheduler = undefined;
    this.#scheduler.stop();
  }

  /** Explicitly retries a failed or declined credential refresh without creating a retry loop. */
  retryAuthentication(): void {
    if (
      !this.#started ||
      !this.#active ||
      this.#lastAuthenticationStatus == null
    )
      return;
    this.#refreshAttempted = false;
    this.#beginAuthenticationRefresh(this.#lastAuthenticationStatus);
  }

  #setApplicationState(state: ExperimentalSyncApplicationState): void {
    if (!this.#started) return;
    const active = state === "active";
    if (active === this.#active) return;
    this.#active = active;
    this.#generation += 1;
    this.#refreshAttempted = false;
    this.#refresh?.abort();
    this.#refresh = undefined;
    if (active) this.#scheduler.start();
    else this.#scheduler.stop();
  }

  #onSchedulerStatus(status: ExperimentalSyncSchedulerStatus): void {
    if (status.state === "idle") {
      this.#refreshAttempted = false;
      this.#lastAuthenticationStatus = undefined;
      return;
    }
    if (status.state === "authentication_required") {
      this.#lastAuthenticationStatus = status.status;
      this.#beginAuthenticationRefresh(status.status);
    }
  }

  #beginAuthenticationRefresh(status: 401 | 403): void {
    if (
      !this.#started ||
      !this.#active ||
      this.#refreshAuthentication == null ||
      this.#refreshAttempted ||
      this.#refresh != null
    )
      return;
    this.#refreshAttempted = true;
    const generation = this.#generation;
    const controller = new AbortController();
    this.#refresh = controller;
    void this.#refreshAuthentication(status, { signal: controller.signal })
      .then((changed) => {
        if (
          changed &&
          !controller.signal.aborted &&
          this.#started &&
          this.#active &&
          generation === this.#generation
        ) {
          this.#scheduler.resumeAfterAuthentication();
        }
      })
      .catch((error: unknown) => {
        if (!controller.signal.aborted) this.#onRefreshError?.(error);
      })
      .finally(() => {
        if (this.#refresh === controller) this.#refresh = undefined;
      });
  }
}
