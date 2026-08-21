import type { CallOptions, LiveNotification } from "./client";
import type { SurrealValue } from "./wire";

export type LiveSubscriptionStatus = "open" | "error" | "closed";

export type LiveSubscriptionSnapshot<T = SurrealValue> = {
  status: LiveSubscriptionStatus;
  notification?: LiveNotification<T>;
  error?: unknown;
};

type StoreListener = () => void;
type NotificationListener<T> = (notification: LiveNotification<T>) => void;
type LiveQuerySource<T> = {
  next(options?: CallOptions): Promise<LiveNotification<T> | undefined>;
  close(options?: CallOptions): Promise<void>;
};

/** A multicast, observable facade over one pull-based live query. */
export class LiveSubscription<T = SurrealValue> {
  readonly #query: LiveQuerySource<T>;
  readonly #storeListeners = new Set<StoreListener>();
  readonly #notificationListeners = new Set<NotificationListener<T>>();
  #snapshot: LiveSubscriptionSnapshot<T> = { status: "open" };
  #closePromise?: Promise<void>;
  #started = false;

  constructor(query: LiveQuerySource<T>) {
    this.#query = query;
  }

  /** Subscribe to snapshot changes. Compatible with `useSyncExternalStore`. */
  subscribe = (listener: StoreListener): (() => void) => {
    this.#storeListeners.add(listener);
    this.#start();
    return () => this.#storeListeners.delete(listener);
  };

  /** Listen for live-query notifications without consuming the native stream. */
  onNotification(listener: NotificationListener<T>): () => void {
    this.#notificationListeners.add(listener);
    this.#start();
    return () => this.#notificationListeners.delete(listener);
  }

  getSnapshot = (): LiveSubscriptionSnapshot<T> => this.#snapshot;

  close(options?: CallOptions): Promise<void> {
    if (!this.#closePromise) {
      this.#closePromise = this.#close(options);
    }
    return this.#closePromise;
  }

  get isClosed(): boolean {
    return this.#snapshot.status === "closed";
  }

  #start(): void {
    if (this.#started || this.#snapshot.status === "closed") return;
    this.#started = true;
    void this.#pump();
  }

  async #pump(): Promise<void> {
    try {
      while (this.#snapshot.status !== "closed") {
        const notification = await this.#query.next();
        if (!notification) break;
        this.#snapshot = { status: "open", notification };
        this.#emit(notification);
      }
      this.#setClosed();
    } catch (error) {
      if (this.#snapshot.status === "closed") return;
      this.#snapshot = { status: "error", error };
      this.#emit();
    } finally {
      try {
        await this.#query.close();
      } catch (error) {
        if (this.#snapshot.status !== "closed") {
          this.#snapshot = { status: "error", error };
          this.#emit();
        }
      }
    }
  }

  async #close(options?: CallOptions): Promise<void> {
    this.#setClosed();
    await this.#query.close(options);
  }

  #setClosed(): void {
    if (this.#snapshot.status === "closed") return;
    this.#snapshot = {
      status: "closed",
      notification: this.#snapshot.notification,
    };
    this.#emit();
  }

  #emit(notification?: LiveNotification<T>): void {
    for (const listener of this.#storeListeners) this.#notify(listener);
    if (notification) {
      for (const listener of this.#notificationListeners) {
        this.#notify(() => listener(notification));
      }
    }
  }

  #notify(listener: StoreListener): void {
    try {
      listener();
    } catch (error) {
      // A consumer callback must not terminate the shared native stream.
      console.error("LiveSubscription listener failed", error);
    }
  }
}
