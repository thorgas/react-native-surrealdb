import * as bindings from "./generated/surrealdb_rn_core";
import type { CallOptions } from "./client";

export interface ExperimentalStorageInspectionOptions {
  readonly namespace: string;
  readonly database: string;
  readonly expectedJournal: Readonly<bindings.NativeSyncOpenOptions>;
  readonly applicationTables: readonly string[];
}
export type ExperimentalStorageInspectionErrorCode =
  bindings.NativeStorageInspectionError_Tags | "UnsupportedAbi" | "Aborted";
/** Fixed classification only: never retains a native error, identity or cause. */
export class ExperimentalStorageInspectionError extends Error {
  constructor(readonly code: ExperimentalStorageInspectionErrorCode) {
    super(`Storage inspection failed: ${code}`);
    this.name = "ExperimentalStorageInspectionError";
  }
}
export type ExperimentalStorageJournalInspection =
  | { readonly kind: "missing" }
  | {
      readonly kind: "present";
      readonly revision: bigint;
      readonly pendingCount: number;
      readonly outcomeCount: number;
      readonly conflictCount: number;
      readonly barrier:
        "none" | "needs_new_scope_journal" | "pending_quarantine_required";
    };
export type ExperimentalStorageInspection =
  | { readonly kind: "pristine_unselected" }
  | {
      readonly kind: "selected";
      readonly native: ExperimentalStorageJournalInspection;
      readonly applicationTables: readonly {
        readonly table: string;
        readonly schemaPresent: boolean;
        readonly rowsPresent: boolean;
      }[];
    };
const codes = new Set<string>(
  Object.values(bindings.NativeStorageInspectionError_Tags),
);
const fail = (code: ExperimentalStorageInspectionErrorCode): never => {
  throw new ExperimentalStorageInspectionError(code);
};
function redacted(error: unknown): ExperimentalStorageInspectionError {
  try {
    if (typeof error === "object" && error !== null) {
      for (const key of ["code", "tag"]) {
        const descriptor = Object.getOwnPropertyDescriptor(error, key);
        if (
          descriptor &&
          "value" in descriptor &&
          typeof descriptor.value === "string" &&
          (codes.has(descriptor.value) ||
            (key === "code" &&
              error instanceof ExperimentalStorageInspectionError &&
              ["UnsupportedAbi", "Aborted"].includes(descriptor.value)))
        ) {
          return new ExperimentalStorageInspectionError(
            descriptor.value as ExperimentalStorageInspectionErrorCode,
          );
        }
      }
    }
  } catch {
    /* Proxy traps are untrusted too. */
  }
  return new ExperimentalStorageInspectionError(
    bindings.NativeStorageInspectionError_Tags.Persistence,
  );
}
function own(value: unknown, key: string): unknown {
  if (
    (typeof value !== "object" && typeof value !== "function") ||
    value === null
  )
    throw new Error();
  const descriptor = Object.getOwnPropertyDescriptor(value, key);
  if (!descriptor || !("value" in descriptor)) throw new Error();
  return descriptor.value;
}
function boundedArray(value: unknown, max: number): unknown[] {
  if (!Array.isArray(value)) throw new Error();
  const length = own(value, "length");
  if (
    typeof length !== "number" ||
    !Number.isInteger(length) ||
    length < 0 ||
    length > max
  )
    throw new Error();
  const result: unknown[] = [];
  for (let i = 0; i < length; i++) result.push(own(value, String(i)));
  return result;
}
const u64 = (value: unknown): value is bigint =>
  typeof value === "bigint" && value >= 0n && value <= 18446744073709551615n;
const u32 = (value: unknown): value is number =>
  typeof value === "number" &&
  Number.isInteger(value) &&
  value >= 0 &&
  value <= 4294967295;
function input(
  value: ExperimentalStorageInspectionOptions,
): bindings.NativeStorageInspectionOptions {
  try {
    const namespace = own(value, "namespace"),
      database = own(value, "database");
    if (
      typeof namespace !== "string" ||
      typeof database !== "string" ||
      !/^[A-Za-z0-9_-]{1,64}$/.test(namespace) ||
      !/^[A-Za-z0-9_-]{1,64}$/.test(database)
    )
      throw new Error();
    const journal = own(value, "expectedJournal");
    const partitionId = own(journal, "partitionId"),
      clientId = own(journal, "clientId"),
      requestedScope = own(journal, "requestedScope"),
      subscriptionRevision = own(journal, "subscriptionRevision");
    for (const field of [partitionId, clientId, requestedScope]) {
      if (
        typeof field !== "string" ||
        field.length === 0 ||
        field.length > 512 ||
        new TextEncoder().encode(field).byteLength > 512
      )
        throw new Error();
    }
    if (!u64(subscriptionRevision)) throw new Error();
    const applicationTables = boundedArray(own(value, "applicationTables"), 16);
    if (
      applicationTables.some(
        (table) =>
          typeof table !== "string" ||
          !/^[A-Za-z_][A-Za-z0-9_]{0,63}$/.test(table) ||
          table.startsWith("_sync_"),
      ) ||
      new Set(applicationTables).size !== applicationTables.length
    )
      throw new Error();
    return {
      namespace,
      database,
      expectedJournal: {
        partitionId: partitionId as string,
        clientId: clientId as string,
        requestedScope: requestedScope as string,
        subscriptionRevision,
      },
      applicationTables: applicationTables as string[],
    };
  } catch {
    return fail(bindings.NativeStorageInspectionError_Tags.InvalidOptions);
  }
}
function output(
  value: unknown,
  requested: readonly string[],
): ExperimentalStorageInspection {
  const tag = own(value, "tag");
  if (tag === "PristineUnselected")
    return Object.freeze({ kind: "pristine_unselected" });
  if (tag !== "Selected") throw new Error();
  const inner = own(value, "inner"),
    native = own(inner, "native"),
    nativeTag = own(native, "tag");
  let journal: ExperimentalStorageJournalInspection;
  if (nativeTag === "Missing") journal = Object.freeze({ kind: "missing" });
  else if (nativeTag === "Present") {
    const detail = own(native, "inner"),
      revision = own(detail, "revision"),
      pendingCount = own(detail, "pendingCount"),
      outcomeCount = own(detail, "outcomeCount"),
      conflictCount = own(detail, "conflictCount"),
      barrier = own(detail, "barrier");
    if (
      !u64(revision) ||
      !u32(pendingCount) ||
      !u32(outcomeCount) ||
      !u32(conflictCount) ||
      (barrier !== 0 && barrier !== 1 && barrier !== 2)
    )
      throw new Error();
    journal = Object.freeze({
      kind: "present",
      revision,
      pendingCount,
      outcomeCount,
      conflictCount,
      barrier: (
        [
          "none",
          "needs_new_scope_journal",
          "pending_quarantine_required",
        ] as const
      )[barrier],
    });
  } else throw new Error();
  const seen = new Set<string>();
  const applicationTables = boundedArray(
    own(inner, "applicationTables"),
    16,
  ).map((row) => {
    const table = own(row, "table"),
      schemaPresent = own(row, "schemaPresent"),
      rowsPresent = own(row, "rowsPresent");
    if (
      typeof table !== "string" ||
      !requested.includes(table) ||
      seen.has(table) ||
      typeof schemaPresent !== "boolean" ||
      typeof rowsPresent !== "boolean" ||
      (rowsPresent && !schemaPresent)
    )
      throw new Error();
    seen.add(table);
    return Object.freeze({ table, schemaPresent, rowsPresent });
  });
  if (seen.size !== requested.length) throw new Error();
  return Object.freeze({
    kind: "selected",
    native: journal,
    applicationTables: Object.freeze(applicationTables),
  });
}
/** @internal SurrealClient owns the native database handle; this helper never opens one. */
export async function inspectExperimentalStorage(
  database: bindings.SurrealDatabaseLike,
  options: ExperimentalStorageInspectionOptions,
  callOptions?: CallOptions,
): Promise<ExperimentalStorageInspection> {
  try {
    const snapshot = input(options);
    if (callOptions?.signal.aborted) return fail("Aborted");
    if (typeof bindings.inspectStorage !== "function")
      return fail("UnsupportedAbi");
    const value = await bindings.inspectStorage(
      database,
      snapshot,
      callOptions,
    );
    if (callOptions?.signal.aborted) return fail("Aborted");
    return output(value, snapshot.applicationTables);
  } catch (error) {
    throw redacted(error);
  }
}
