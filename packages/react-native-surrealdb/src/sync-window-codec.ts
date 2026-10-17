import { decodeSurrealValue } from "./wire";
import type {
  WindowCapabilities,
  WindowChanges,
  WindowCursor,
  WindowFrame,
  WindowRequest,
} from "./sync-window";
function invalid(): never {
  throw new TypeError("Invalid ordered subscription payload");
}
function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value))
    return invalid();
  return value as Record<string, unknown>;
}
function text(value: unknown): asserts value is string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    new TextEncoder().encode(value).length > 256
  )
    invalid();
}
function counter(value: unknown, positive = false): asserts value is string {
  if (
    typeof value !== "string" ||
    !/^(0|[1-9][0-9]{0,19})$/.test(value) ||
    BigInt(value) > 18446744073709551615n ||
    (positive && value === "0")
  )
    invalid();
}
function list(value: unknown, max: number): unknown[] {
  if (!Array.isArray(value) || value.length > max) return invalid();
  return value;
}
function scalar(value: unknown): string {
  const row = object(value);
  if (row.kind === "int") {
    if (
      typeof row.value !== "string" ||
      !/^(0|-?[1-9][0-9]{0,18})$/.test(row.value) ||
      BigInt(row.value) < -9223372036854775808n ||
      BigInt(row.value) > 9223372036854775807n
    )
      invalid();
  } else if (row.kind === "text") {
    if (
      typeof row.value !== "string" ||
      new TextEncoder().encode(row.value).length > 256
    )
      invalid();
  } else if (row.kind === "bool") {
    if (typeof row.value !== "boolean") invalid();
  } else invalid();
  return String(row.kind);
}
export function validateWindowRequest(
  value: unknown,
): asserts value is WindowRequest {
  const row = object(value);
  text(row.partitionId);
  text(row.clientId);
  text(row.requestedScope);
  counter(row.subscriptionRevision);
  const command = object(row.command);
  text(command.subscriptionId);
  counter(command.generation, true);
  if (command.kind === "start") {
    text(command.definitionId);
    if (
      !Number.isInteger(command.limit) ||
      Number(command.limit) < 1 ||
      Number(command.limit) > 32
    )
      invalid();
    const parameters = Object.entries(object(command.parameters));
    if (parameters.length > 8) invalid();
    for (const [key, value] of parameters) {
      text(key);
      scalar(value);
    }
  } else if (command.kind === "resume") text(command.token);
  else if (command.kind !== "cancel") invalid();
}
function cursor(value: unknown, id: string): void {
  const row = object(value);
  text(row.processEpoch);
  if (row.subscriptionId !== id) invalid();
  counter(row.revision);
}
function compareText(a: string, b: string): number {
  const left = new TextEncoder().encode(a),
    right = new TextEncoder().encode(b);
  for (let i = 0; i < Math.min(left.length, right.length); i++) {
    if (left[i] !== right[i]) return left[i]! < right[i]! ? -1 : 1;
  }
  return left.length - right.length;
}
function compareRows(a: unknown, b: unknown, descending: boolean): number {
  const left = object(a),
    right = object(b),
    x = object(left.sortKey),
    y = object(right.sortKey);
  const xv = x.kind === "int" ? BigInt(String(x.value)) : x.value;
  const yv = y.kind === "int" ? BigInt(String(y.value)) : y.value;
  const comparison =
    x.kind === "text"
      ? compareText(String(xv), String(yv))
      : xv === yv
        ? 0
        : (xv as bigint | boolean) < (yv as bigint | boolean)
          ? -1
          : 1;
  return (
    (descending ? -comparison : comparison) ||
    compareText(String(left.recordId), String(right.recordId))
  );
}
function frame(value: unknown, id: string, revision: string): void {
  const row = object(value);
  if (row.cancelledThrough !== null) counter(row.cancelledThrough, true);
  if (row.request !== null) {
    validateWindowRequest(row.request);
    if (row.request.command.subscriptionId !== id) invalid();
    if (row.request.command.kind === "cancel") {
      if (
        row.snapshot !== null ||
        row.cancelledThrough !== row.request.command.generation
      )
        invalid();
    } else if (
      row.cancelledThrough !== null &&
      BigInt(row.request.command.generation) <=
        BigInt(String(row.cancelledThrough))
    )
      invalid();
  }
  if (row.snapshot === null) return;
  const snapshot = object(row.snapshot),
    meta = object(snapshot.metadata),
    scope = object(meta.scope),
    frontier = object(meta.frontier),
    coverage = object(meta.coverage);
  if (meta.revision !== revision || meta.subscriptionId !== id) invalid();
  counter(meta.generation, true);
  text(meta.planFingerprint);
  text(scope.identity);
  counter(scope.authorizationRevision);
  counter(scope.subscriptionRevision);
  counter(frontier.epoch, true);
  counter(frontier.sequence);
  counter(meta.viewRevision, true);
  text(coverage.definitionId);
  text(coverage.orderField);
  if (
    !["bool", "int", "text"].includes(String(coverage.orderKind)) ||
    !["ascending", "descending"].includes(String(coverage.direction)) ||
    !Number.isInteger(coverage.limit) ||
    Number(coverage.limit) < 1 ||
    Number(coverage.limit) > 32
  )
    invalid();
  if (
    row.request === null ||
    object(object(row.request).command).kind === "cancel"
  )
    invalid();
  const request = object(row.request);
  const command = object(request.command);
  if (
    command.generation !== meta.generation ||
    request.requestedScope !== scope.identity ||
    request.subscriptionRevision !== scope.subscriptionRevision
  )
    invalid();
  const rows = list(snapshot.rows, Number(coverage.limit));
  const ids = new Set<string>();
  for (const value of rows) {
    const entry = object(value);
    text(entry.recordId);
    if (ids.has(entry.recordId)) invalid();
    ids.add(entry.recordId);
    counter(entry.version, true);
    if (
      scalar(entry.sortKey) !== coverage.orderKind ||
      !Object.hasOwn(entry, "value")
    )
      invalid();
  }
  for (let i = 1; i < rows.length; i++)
    if (
      compareRows(rows[i - 1], rows[i], coverage.direction === "descending") >=
      0
    )
      invalid();
  const pending = new Set<string>();
  for (const value of list(
    snapshot.pendingOutsideWindow === null ? [] : snapshot.pendingOutsideWindow,
    100000,
  )) {
    text(value);
    if (ids.has(value) || pending.has(value)) invalid();
    pending.add(value);
  }
  for (const value of rows) {
    const entry = object(value);
    entry.value = decodeSurrealValue(JSON.stringify(entry.value));
  }
}
function parse(json: string): Record<string, unknown> {
  if (json.length > 16777216) invalid();
  return object(JSON.parse(json));
}
export function decodeWindowFrame(json: string, id: string): WindowFrame {
  const row = parse(json);
  cursor(row.cursor, id);
  frame(row, id, String(object(row.cursor).revision));
  return row as unknown as WindowFrame;
}
/** Preflight every replacement before publishing any earlier batch. */
export function decodeWindowChanges(
  json: string,
  previous: WindowCursor,
  maxBatches: number,
): WindowChanges {
  const row = parse(json);
  cursor(row.cursor, previous.subscriptionId);
  const next = object(row.cursor);
  if (row.kind === "reset_required") {
    frame(row, previous.subscriptionId, String(next.revision));
  } else if (row.kind === "batches") {
    if (next.processEpoch !== previous.processEpoch) invalid();
    let revision = previous.revision;
    counter(revision);
    for (const value of list(row.batches, maxBatches)) {
      const batch = object(value);
      counter(batch.beforeRevision);
      counter(batch.revision);
      if (
        batch.beforeRevision !== revision ||
        BigInt(String(batch.revision)) <= BigInt(revision)
      )
        invalid();
      frame(batch, previous.subscriptionId, String(batch.revision));
      revision = String(batch.revision);
    }
    if (next.revision !== revision) invalid();
  } else invalid();
  return row as unknown as WindowChanges;
}
export function decodeWindowCapabilities(json: string): WindowCapabilities {
  const row = parse(json);
  if (
    row.protocolVersion !== 1 ||
    row.maxSubscriptions !== 32 ||
    row.maxWindowRows !== 32 ||
    row.maxJournalBatches !== 128 ||
    row.requiresMatchingNativeAbi !== true ||
    !Number.isSafeInteger(row.maxJournalBytes) ||
    Number(row.maxJournalBytes) < 1 ||
    Number(row.maxJournalBytes) > 33554432 ||
    !Number.isSafeInteger(row.maxAggregateJournalBytes) ||
    Number(row.maxAggregateJournalBytes) < Number(row.maxJournalBytes) ||
    Number(row.maxAggregateJournalBytes) > 16777216 ||
    !Number.isSafeInteger(row.maxOutputBytes) ||
    Number(row.maxOutputBytes) < 1 ||
    Number(row.maxOutputBytes) > 16777216
  )
    invalid();
  return row as unknown as WindowCapabilities;
}
