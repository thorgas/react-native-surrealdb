import { decodeSurrealValue } from "./wire";
import type { SyncReadBootstrap, SyncReadChanges } from "./sync-read";

const MAX_READ_JSON_CHARS = 16 * 1024 * 1024;
function invalid(): never {
  throw new TypeError("Invalid sync read protocol v1 payload");
}
function object(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    return invalid();
  return value as Record<string, unknown>;
}
function text(value: unknown): void {
  if (typeof value !== "string" || value.length === 0 || value.length > 4096)
    invalid();
}
function counter(value: unknown): void {
  if (
    typeof value !== "string" ||
    !/^(0|[1-9][0-9]{0,19})$/.test(value) ||
    BigInt(value) > 18446744073709551615n
  )
    invalid();
}
function array(value: unknown): readonly unknown[] {
  if (!Array.isArray(value) || value.length > 100_000) return invalid();
  return value;
}
function frontier(value: unknown): void {
  const row = object(value);
  counter(row.epoch);
  counter(row.sequence);
}
function identity(value: unknown): void {
  const row = object(value);
  text(row.clientCommitId);
  text(row.fingerprint);
}
function cursor(value: unknown): void {
  const row = object(value);
  text(row.processEpoch);
  counter(row.revision);
}
function metadata(value: unknown): void {
  const row = object(value),
    scope = object(row.scope),
    coverage = object(row.coverage);
  counter(row.revision);
  text(scope.partitionId);
  text(scope.scopeIdentity);
  counter(scope.subscriptionRevision);
  if (scope.authorizationRevision !== null)
    counter(scope.authorizationRevision);
  if (row.asOf !== null) frontier(row.asOf);
  if (coverage.kind === "unknown") {
    if (row.readiness !== "local_available_awaiting_coverage") invalid();
  } else if (coverage.kind === "authorized_reset") {
    frontier(coverage.cursor);
    if (scope.authorizationRevision === null || object(row.asOf).epoch === "0")
      invalid();
    if (row.readiness !== "authorized_scope_as_of" || row.asOf === null)
      invalid();
    const at = object(row.asOf),
      reset = object(coverage.cursor);
    if (
      at.epoch !== reset.epoch ||
      BigInt(String(reset.sequence)) > BigInt(String(at.sequence))
    )
      invalid();
  } else invalid();
}
function decodePayload(value: unknown): unknown {
  const encoded = JSON.stringify(value);
  if (typeof encoded !== "string") invalid();
  return decodeSurrealValue(encoded);
}
function row(value: unknown, meta: unknown): void {
  const entry = object(value),
    confirmed = object(entry.confirmed);
  text(entry.recordId);
  if (
    !["not_absent", "unknown", "absent_within_authorized_scope"].includes(
      String(entry.confirmedAbsence),
    )
  )
    invalid();
  if (confirmed.state === "present") {
    counter(confirmed.version);
    if (!Object.prototype.hasOwnProperty.call(confirmed, "value")) invalid();
    if (confirmed.reference !== null) text(confirmed.reference);
  } else if (confirmed.state === "tombstone") counter(confirmed.version);
  else if (confirmed.state !== "absent") invalid();
  const coverage = object(object(meta).coverage).kind;
  const expectedAbsence =
    confirmed.state !== "absent"
      ? "not_absent"
      : coverage === "authorized_reset"
        ? "absent_within_authorized_scope"
        : "unknown";
  if (entry.confirmedAbsence !== expectedAbsence) invalid();
  if (confirmed.state === "present")
    confirmed.value = decodePayload(confirmed.value);
  if (entry.optimistic !== null) {
    const optimistic = object(entry.optimistic);
    if (optimistic.state === "present") {
      if (!Object.prototype.hasOwnProperty.call(optimistic, "value")) invalid();
      optimistic.value = decodePayload(optimistic.value);
      if (optimistic.reference !== null) text(optimistic.reference);
      if (optimistic.baseVersion !== "absent")
        counter(object(optimistic.baseVersion).exact);
    } else if (optimistic.state === "deleted") counter(optimistic.baseVersion);
    else invalid();
  }
  for (const pending of array(entry.pending)) identity(pending);
  for (const value of array(entry.conflicts)) {
    const conflict = object(value);
    identity(conflict.identity);
    if (conflict.resolution !== null) {
      const resolution = object(conflict.resolution);
      if (
        resolution.strategy === "keep_local" ||
        resolution.strategy === "merge"
      )
        identity(resolution.replacement);
      else if (
        resolution.strategy !== "keep_server" &&
        resolution.strategy !== "prefer_server_policy_v1"
      )
        invalid();
    }
  }
  for (const value of array(entry.accepted)) {
    const accepted = object(value);
    identity(accepted.identity);
    counter(accepted.sequence);
    if (
      accepted.status !== "covered_by_authorized_reset" &&
      accepted.status !== "awaiting_pull_epoch_unknown"
    )
      invalid();
  }
}
function snapshot(value: unknown): void {
  const result = object(value);
  if (result.protocolVersion !== 1) invalid();
  metadata(result.metadata);
  const rows = object(result.rows),
    entries = Object.entries(rows);
  if (entries.length > 100_000) invalid();
  for (const [id, value] of entries) {
    row(value, result.metadata);
    if (object(value).recordId !== id) invalid();
  }
}
function decode(json: string): unknown {
  if (json.length > MAX_READ_JSON_CHARS) invalid();
  return JSON.parse(json) as unknown;
}

export function decodeSyncReadBootstrap(json: string): SyncReadBootstrap {
  const value = object(decode(json));
  snapshot(value.snapshot);
  cursor(value.cursor);
  if (
    object(value.cursor).revision !==
    object(object(value.snapshot).metadata).revision
  )
    invalid();
  return value as SyncReadBootstrap;
}
export function decodeSyncReadChanges(json: string): SyncReadChanges {
  const value = object(decode(json));
  cursor(value.cursor);
  if (value.kind === "reset_required") {
    snapshot(value.snapshot);
    if (
      object(value.cursor).revision !==
      object(object(value.snapshot).metadata).revision
    )
      invalid();
  } else if (value.kind === "batches") {
    const batches = array(value.batches);
    if (batches.length > 128) invalid();
    for (const value of batches) {
      const batch = object(value);
      if (batch.protocolVersion !== 1) invalid();
      counter(batch.beforeRevision);
      counter(batch.revision);
      metadata(batch.metadata);
      if (batch.revision !== object(batch.metadata).revision) invalid();
      for (const value of array(batch.changes)) {
        const change = object(value);
        if (change.kind === "upsert") row(change.row, batch.metadata);
        else if (change.kind === "remove") text(change.recordId);
        else invalid();
      }
    }
  } else invalid();
  return value as SyncReadChanges;
}

export function decodeSyncReadCapabilities(
  json: string,
): import("./sync-read").SyncReadCapabilities {
  const value = object(decode(json));
  if (
    value.protocolVersion !== 1 ||
    value.requiresMatchingNativeAbi !== true ||
    value.predicateSubscriptions !== false
  )
    invalid();
  for (const key of [
    "maxJournalBatches",
    "maxJournalBytes",
    "maxOutputBytes",
  ]) {
    if (
      typeof value[key] !== "number" ||
      !Number.isSafeInteger(value[key]) ||
      value[key] < 1
    )
      invalid();
  }
  if (
    Number(value.maxJournalBatches) > 128 ||
    Number(value.maxOutputBytes) > MAX_READ_JSON_CHARS
  )
    invalid();
  return Object.freeze(value) as import("./sync-read").SyncReadCapabilities;
}
