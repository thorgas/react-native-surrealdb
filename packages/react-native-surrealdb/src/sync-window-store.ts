import type { CallOptions } from "./client";
import type { ExperimentalSyncWindowClient, WindowFrame } from "./sync-window";
import { syncReadScopeError } from "./sync-read";
export type WindowEvent =
  | { kind: "frame"; frame: WindowFrame }
  | { kind: "loading" }
  | { kind: "error"; error: unknown; invalidated: boolean }
  | { kind: "closed" };
/** Polls committed native frames only. It never requests or mutates authority data. */
export class ExperimentalSyncWindowStore {
  readonly #source: Pick<ExperimentalSyncWindowClient, "snapshot" | "changes">;
  readonly subscriptionId: string;
  readonly #pollMs: number;
  readonly #listeners = new Set<(event: WindowEvent) => void>();
  #frame?: WindowFrame;
  #controller?: AbortController;
  #closed = false;
  #blocked = false;
  #observerErrors = 0;
  readonly #onClose?: () => void;
  constructor(
    source: Pick<ExperimentalSyncWindowClient, "snapshot" | "changes">,
    subscriptionId: string,
    pollMs = 50,
    onClose?: () => void,
  ) {
    if (!Number.isInteger(pollMs) || pollMs < 1 || pollMs > 60000)
      throw new RangeError("Invalid window polling interval");
    this.#source = source;
    this.subscriptionId = subscriptionId;
    this.#pollMs = pollMs;
    this.#onClose = onClose;
  }
  getSnapshot(): WindowFrame | undefined {
    return this.#frame;
  }
  get observerErrors(): number {
    return this.#observerErrors;
  }
  subscribe(listener: (event: WindowEvent) => void): () => void {
    if (this.#closed || this.#blocked)
      throw new Error("Window store is unavailable");
    this.#listeners.add(listener);
    if (this.#controller && this.#frame)
      this.#notify(listener, { kind: "frame", frame: this.#frame });
    if (
      !this.#controller &&
      !this.#closed &&
      !this.#blocked &&
      this.#listeners.has(listener)
    )
      this.#start();
    return () => {
      this.#listeners.delete(listener);
      if (!this.#listeners.size) this.#stop();
    };
  }
  refresh(): void {
    if (this.#closed || this.#blocked) return;
    this.#stop();
    this.#frame = undefined;
    this.#emit({ kind: "loading" });
    if (
      this.#listeners.size &&
      !this.#controller &&
      !this.#closed &&
      !this.#blocked
    )
      this.#start();
  }
  invalidateScope(error: unknown): void {
    if (this.#closed || this.#blocked) return;
    this.#blocked = true;
    this.#stop();
    this.#frame = undefined;
    this.#emit({ kind: "error", error, invalidated: true });
  }
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#stop();
    this.#frame = undefined;
    this.#emit({ kind: "closed" });
    this.#listeners.clear();
    this.#onClose?.();
  }
  #stop(): void {
    this.#controller?.abort();
    this.#controller = undefined;
  }
  #current(controller: AbortController): boolean {
    return (
      this.#controller === controller &&
      !controller.signal.aborted &&
      !this.#closed &&
      !this.#blocked
    );
  }
  #notify(listener: (event: WindowEvent) => void, event: WindowEvent): void {
    try {
      listener(event);
    } catch {
      this.#observerErrors = Math.min(
        Number.MAX_SAFE_INTEGER,
        this.#observerErrors + 1,
      );
    }
  }
  #emit(event: WindowEvent): void {
    for (const listener of [...this.#listeners]) {
      if (
        event.kind === "frame" &&
        (this.#closed || this.#blocked || this.#frame !== event.frame)
      )
        return;
      if (this.#listeners.has(listener)) this.#notify(listener, event);
    }
  }
  #start(): void {
    const controller = new AbortController();
    this.#controller = controller;
    void this.#pump(controller);
  }
  async #pump(controller: AbortController): Promise<void> {
    const options: CallOptions = { signal: controller.signal };
    try {
      const initial = await this.#source.snapshot(this.subscriptionId, options);
      if (!this.#current(controller)) return;
      this.#frame = initial;
      let cursor = initial.cursor;
      this.#emit({ kind: "frame", frame: initial });
      while (this.#current(controller)) {
        const result = await this.#source.changes(cursor, 32, options);
        if (!this.#current(controller)) return;
        if (result.kind === "reset_required") {
          this.#frame = result;
          this.#emit({ kind: "frame", frame: result });
        } else {
          for (const batch of result.batches) {
            if (!this.#current(controller)) return;
            const frame: WindowFrame = {
              snapshot: batch.snapshot,
              request: batch.request,
              cancelledThrough: batch.cancelledThrough,
              cursor: { ...result.cursor, revision: batch.revision },
            };
            this.#frame = frame;
            this.#emit({ kind: "frame", frame });
          }
        }
        cursor = result.cursor;
        if (result.kind === "reset_required" || result.batches.length === 0)
          await delay(this.#pollMs, controller.signal);
      }
    } catch (error) {
      if (this.#current(controller)) {
        if (syncReadScopeError(error)) this.invalidateScope(error);
        else this.#emit({ kind: "error", error, invalidated: false });
      }
    } finally {
      if (this.#controller === controller) this.#controller = undefined;
    }
  }
}
function delay(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }
    const finish = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", finish);
      resolve();
    };
    const timer = setTimeout(finish, ms);
    signal.addEventListener("abort", finish, { once: true });
  });
}
