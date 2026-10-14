import type { CallOptions } from "./client";
import type { ExperimentalSyncStatus, SyncJsonValue } from "./sync";
import { decodeSurrealValue } from "./wire";

/** Existing alpha.3 calls only; no native ABI or storage changes. */
export interface ExperimentalSyncObservationPort {
  enqueue(
    commit: SyncJsonValue,
    options?: CallOptions,
  ): Promise<ExperimentalSyncStatus>;
  recordPushResponseProtocolJson(
    response: string,
    options?: CallOptions,
  ): Promise<ExperimentalSyncStatus>;
  status(options?: CallOptions): Promise<ExperimentalSyncStatus>;
}

/** Advisory observations over one application-owned durable sync client. */
export class ExperimentalSyncObservedClient {
  readonly #client: ExperimentalSyncObservationPort;
  readonly #observers = new Set<(event: ExperimentalSyncEvent) => void>();
  readonly #logger?: ExperimentalSyncClientOptions["logger"];
  #logging = false;
  #droppedLogs = 0;
  #loggerErrors = 0;

  constructor(
    client: ExperimentalSyncObservationPort,
    options: ExperimentalSyncClientOptions = {},
  ) {
    this.#client = client;
    this.#logger = options.logger;
  }

  enqueue(
    commit: SyncJsonValue,
    options?: CallOptions,
  ): Promise<ExperimentalSyncStatus> {
    return this.#persist(
      this.#client.enqueue(commit, options),
      "local_durable",
      commitId(commit),
    );
  }

  recordPushResponseProtocolJson(
    response: string,
    options?: CallOptions,
  ): Promise<ExperimentalSyncStatus> {
    return this.#persistOutcome(
      this.#client.recordPushResponseProtocolJson(response, options),
      response,
    );
  }

  /** One native status read; not an atomic snapshot of application rows. */
  async diagnosticSnapshot(
    options?: CallOptions,
  ): Promise<ExperimentalSyncDiagnosticSnapshot> {
    const status = await this.#client.status(options);
    return Object.freeze({
      ...status,
      logging: Object.freeze({
        inFlight: this.#logging,
        droppedCount: this.#droppedLogs,
        errorCount: this.#loggerErrors,
      }),
    });
  }

  /** Session-only observations. Durable outcomes remain native, not this listener set. */
  subscribeEvents(
    listener: (event: ExperimentalSyncEvent) => void,
  ): () => void {
    this.#observers.add(listener);
    return () => this.#observers.delete(listener);
  }

  async #persistOutcome(
    persisted: Promise<ExperimentalSyncStatus>,
    response: unknown,
  ): Promise<ExperimentalSyncStatus> {
    const status = await persisted;
    let decoded = response;
    try {
      if (typeof response === "string") decoded = decodeSurrealValue(response);
    } catch {
      return status;
    }
    const outcome = object(decoded)?.outcome;
    const kind = object(outcome)?.status;
    const milestone =
      kind === "accepted"
        ? "authority_accepted"
        : kind === "conflict"
          ? "authority_conflict"
          : kind === "rejected"
            ? "authority_rejected"
            : undefined;
    return this.#persist(Promise.resolve(status), milestone, commitId(outcome));
  }

  async #persist(
    persisted: Promise<ExperimentalSyncStatus>,
    milestone?: ExperimentalSyncMilestone,
    clientCommitId?: string,
  ): Promise<ExperimentalSyncStatus> {
    const status = await persisted;
    if (!milestone || !clientCommitId) return status;
    const event = Object.freeze({
      milestone,
      clientCommitId,
      status: Object.freeze({ ...status }),
    });
    // Consumers cannot turn a durable success into a failed mutation.
    for (const listener of this.#observers) {
      try {
        listener(event);
      } catch {
        /* Advisory observer. */
      }
    }
    if (this.#logger && this.#logging) this.#droppedLogs += 1;
    if (this.#logger && !this.#logging) {
      this.#logging = true;
      const metadata = Object.freeze({
        milestone,
        revision: status.revision,
        pendingCount: status.pendingCount,
        outcomeCount: status.outcomeCount,
        conflictCount: status.conflictCount,
      });
      // Never await diagnostics; one hung sink drops subsequent logs rather than accumulating work.
      queueMicrotask(() => {
        try {
          Promise.resolve(this.#logger?.(metadata))
            .catch(() => {
              this.#loggerErrors += 1;
            })
            .finally(() => {
              this.#logging = false;
            });
        } catch {
          this.#loggerErrors += 1;
          this.#logging = false;
        }
      });
    }
    return status;
  }
}

export type ExperimentalSyncMilestone =
  | "local_durable"
  | "authority_accepted"
  | "authority_conflict"
  | "authority_rejected";
export type ExperimentalSyncEvent = Readonly<{
  milestone: ExperimentalSyncMilestone;
  clientCommitId: string;
  status: Readonly<ExperimentalSyncStatus>;
}>;
export type ExperimentalSyncDiagnosticEvent = Readonly<
  Pick<ExperimentalSyncEvent, "milestone"> &
    Pick<
      ExperimentalSyncStatus,
      "revision" | "pendingCount" | "outcomeCount" | "conflictCount"
    >
>;
export type ExperimentalSyncClientOptions = {
  logger?: (event: ExperimentalSyncDiagnosticEvent) => void | Promise<void>;
};

function object(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}
function commitId(value: unknown): string | undefined {
  const id = object(object(value)?.identity)?.clientCommitId;
  return typeof id === "string" && id.length > 0 && id.length <= 128
    ? id
    : undefined;
}

export type ExperimentalSyncDiagnosticSnapshot = Readonly<
  ExperimentalSyncStatus & {
    logging: Readonly<{
      inFlight: boolean;
      droppedCount: number;
      errorCount: number;
    }>;
  }
>;
