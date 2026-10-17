import { createCollection } from "@tanstack/db";
import type {
  SyncReadMetadata,
  SyncReadRow,
} from "../../../packages/react-native-surrealdb/src/sync-read";
import {
  subscribeProjection,
  type ProjectionFrame,
  type ProjectionOptions,
} from "./contract";

/** Metadata is a row so an empty view and its coverage publish in the same transaction. */
export type EngineCollectionEntry<T> =
  | Readonly<{
      key: "metadata";
      kind: "metadata";
      frame: Readonly<{
        phase: ProjectionFrame<T>["phase"];
        metadata?: SyncReadMetadata;
        error?: unknown;
      }>;
    }>
  | Readonly<{ key: string; kind: "record"; row: SyncReadRow<T> }>;

/** Eager local projection only. Query-driven remote replication is a different protocol. */
export function createTanStackProjection<T, C, R>(
  options: ProjectionOptions<T, C, R> & { id: string },
) {
  let session: ReturnType<typeof subscribeProjection<T, C, R>> | undefined;
  let closed = false;
  let publicationGeneration = 0;
  const collection = createCollection<EngineCollectionEntry<T>, string>({
    id: options.id,
    getKey: (entry) => entry.key,
    startSync: false,
    gcTime: 0,
    sync: {
      sync({
        collection,
        begin,
        write,
        commit,
        truncate,
        markReady,
        markError,
      }) {
        if (closed) {
          markError(new Error("Engine projection is closed"));
          return;
        }
        session = subscribeProjection(options, (frame, event) => {
          const generation = ++publicationGeneration;
          begin();
          const replacing = event?.kind === "snapshot" || !frame.snapshot;
          if (event?.kind === "batch") {
            for (const change of event.batch.changes) {
              if (change.kind === "remove")
                write({ type: "delete", key: `record:${change.recordId}` });
              else
                write({
                  type: collection.has(`record:${change.row.recordId}`)
                    ? "update"
                    : "insert",
                  value: {
                    key: `record:${change.row.recordId}`,
                    kind: "record",
                    row: change.row,
                  },
                });
            }
          } else if (replacing) {
            truncate({ markReady: false });
            for (const row of Object.values(frame.snapshot?.rows ?? {})) {
              write({
                type: "insert",
                value: { key: `record:${row.recordId}`, kind: "record", row },
              });
            }
          }
          write({
            type:
              replacing || !collection.has("metadata") ? "insert" : "update",
            value: {
              key: "metadata",
              kind: "metadata",
              frame: {
                phase: frame.phase,
                metadata: frame.snapshot?.metadata,
                error: frame.error,
              },
            },
          });
          const applied = commit();
          const terminal =
            frame.phase === "blocked" || frame.phase === "closed";
          if (terminal) markError(frame.error);
          const finalize = () => {
            if (generation !== publicationGeneration || closed) return;
            if (frame.snapshot?.metadata.coverage.kind === "authorized_reset")
              markReady();
            else if (frame.phase === "local" && collection.status === "ready")
              markError(new Error("Authorized coverage is unavailable"));
            else if (frame.phase === "error" && !frame.snapshot)
              markError(frame.error);
          };
          // Reentrant library publications may queue. An older receipt must never
          // revive a closed, invalidated, or newer projection's readiness.
          if (applied === true) finalize();
          else
            void applied.then(finalize, (error: unknown) => {
              if (generation === publicationGeneration && !closed)
                markError(error);
            });
        });
        return () => session?.close();
      },
    },
  });
  collection.startSyncImmediate();
  return Object.freeze({
    collection,
    commit(command: C): Promise<R> {
      if (closed || !session)
        return Promise.reject(new Error("Engine projection is closed"));
      return session.commit(command);
    },
    async close(): Promise<void> {
      if (closed) return;
      closed = true;
      session?.close();
      await collection.cleanup();
    },
  });
}
