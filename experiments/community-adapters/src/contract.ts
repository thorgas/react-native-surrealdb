import type {
  SyncReadEvent,
  SyncReadSnapshot,
} from "../../../packages/react-native-surrealdb/src/sync-read";

export type ProjectionFrame<T> = Readonly<{
  phase: "loading" | "local" | "ready" | "error" | "blocked" | "closed";
  snapshot?: SyncReadSnapshot<T>;
  error?: unknown;
}>;

/** Pass the same engine-owned read store to every projection. */
export interface EngineReadView<T> {
  watchCollection(listener: (event: SyncReadEvent<T>) => void): () => void;
}

/** Commands go straight to the application adapter; no library mutation queue. */
export interface EngineCommands<C, R> {
  commit(command: C): Promise<R>;
}

export type ProjectionOptions<T, C, R> = Readonly<{
  source: EngineReadView<T>;
  commands: EngineCommands<C, R>;
}>;

/** A disposable subscription, with no storage, transport, timers or retries. */
export function subscribeProjection<T, C, R>(
  options: ProjectionOptions<T, C, R>,
  publish: (frame: ProjectionFrame<T>, event?: SyncReadEvent<T>) => void,
) {
  let frame: ProjectionFrame<T> = { phase: "loading" };
  let stop: (() => void) | undefined;
  let stopped = false;
  const retire = () => {
    stopped = true;
    stop?.();
    stop = undefined;
  };
  const unsubscribe = options.source.watchCollection((event) => {
    if (stopped) return;
    switch (event.kind) {
      case "snapshot":
      case "batch":
        frame = {
          phase:
            event.snapshot.metadata.coverage.kind === "authorized_reset"
              ? "ready"
              : "local",
          snapshot: event.snapshot,
        };
        break;
      case "error":
        frame = event.invalidated
          ? { phase: "blocked", error: event.error }
          : { phase: "error", snapshot: frame.snapshot, error: event.error };
        if (event.invalidated) retire();
        break;
      case "closed":
        frame = { phase: "closed" };
        retire();
        break;
    }
    publish(frame, event);
  });
  if (stopped) unsubscribe();
  else stop = unsubscribe;
  return {
    async commit(command: C): Promise<R> {
      if (stopped) throw new Error("Engine projection is closed or blocked");
      return options.commands.commit(command);
    },
    close(): void {
      if (stopped) return;
      retire();
      frame = { phase: "closed" };
      publish(frame);
    },
  };
}
