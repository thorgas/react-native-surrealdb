import type { CallOptions } from "./client";
import type {
  ExperimentalSyncClient,
  ExperimentalSyncStatus,
  SyncJsonValue,
} from "./sync";

/** Existing protocol preconditions; native validation remains authoritative. */
export type ExperimentalSyncOperation =
  | {
      readonly kind: "upsert";
      readonly record_id: string;
      readonly base_version: "absent" | { readonly exact: number | bigint };
      readonly value: SyncJsonValue;
      readonly reference?: string | null;
    }
  | {
      readonly kind: "delete";
      readonly record_id: string;
      readonly base_version: number | bigint;
    };

export type ExperimentalSyncCommit = {
  readonly identity: {
    readonly clientCommitId: string;
    readonly fingerprint: string;
  };
  readonly operations: ReadonlyArray<ExperimentalSyncOperation>;
};

export type ExperimentalSyncCommitReceipt = Readonly<{
  clientCommitId: string;
  milestone: "local_durable";
  status: Readonly<ExperimentalSyncStatus>;
}>;

export type ExperimentalSyncApplicationAdapterOptions<TCommand> = {
  sync: Pick<ExperimentalSyncClient, "enqueue">;
  encodeCommand: (command: TCommand) => ExperimentalSyncCommit;
  /** Advisory scheduler wakeup; a thrown callback never invalidates durability. */
  requestSync?: () => void | Promise<void>;
};

/** Application command mapping without a second queue, database, or transport. */
export class ExperimentalSyncApplicationAdapter<TCommand> {
  readonly #options: ExperimentalSyncApplicationAdapterOptions<TCommand>;
  constructor(options: ExperimentalSyncApplicationAdapterOptions<TCommand>) {
    this.#options = options;
  }

  async commit(
    command: TCommand,
    options?: CallOptions,
  ): Promise<ExperimentalSyncCommitReceipt> {
    const commit = this.#options.encodeCommand(command);
    const clientCommitId = commit.identity.clientCommitId;
    if (!clientCommitId || clientCommitId.length > 128)
      throw new TypeError("A bounded commit identity is required");
    const status = await this.#options.sync.enqueue(
      commit as SyncJsonValue,
      options,
    );
    try {
      const wakeup = this.#options.requestSync?.();
      if (wakeup) void Promise.resolve(wakeup).catch(() => {});
    } catch {
      /* Native success already durable. */
    }
    return Object.freeze({
      clientCommitId,
      milestone: "local_durable",
      status: Object.freeze({ ...status }),
    });
  }
}
