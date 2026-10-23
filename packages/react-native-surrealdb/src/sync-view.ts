import type { SyncJsonValue } from "./sync";
import type { SyncReadMetadata, SyncReadRow } from "./sync-read";
import {
  validateSyncReadMetadata,
  validateSyncReadSummary,
} from "./sync-read-codec";
import { decodeSurrealValue } from "./wire";

/** Local structural predicates only; numeric comparison is deliberately unsupported. */
export type SyncViewSelector =
  | Readonly<{ kind: "all" }>
  | Readonly<{
      kind: "eq";
      path: readonly string[];
      value: string | boolean | null;
    }>
  | Readonly<{
      kind: "and";
      terms: readonly Readonly<{
        kind: "eq";
        path: readonly string[];
        value: string | boolean | null;
      }>[];
    }>;
export type SyncViewRequest =
  | Readonly<{
      protocolVersion: 1;
      operation: "count";
      selector: SyncViewSelector;
    }>
  | Readonly<{
      protocolVersion: 1;
      operation: "page";
      selector: SyncViewSelector;
      projection: readonly (readonly string[])[];
      limit: number;
      cursor?: string | null;
    }>;
export type SyncViewWork = Readonly<{
  recordsVisited: number;
  operationsVisited: number;
  valueNodesVisited: number;
  valueBytesVisited: number;
}>;
export type SyncViewRow = Pick<
  SyncReadRow,
  "recordId" | "pending" | "conflicts" | "accepted"
> &
  Readonly<{
    fields: readonly Readonly<{
      path: readonly string[];
      present: boolean;
      value: SyncJsonValue | null;
    }>[];
  }>;
export type SyncViewResult = Readonly<{
  protocolVersion: 1;
  metadata: SyncReadMetadata;
}> &
  (
    | Readonly<{ kind: "stale_cursor" }>
    | Readonly<{ kind: "count"; observedCount: string; work: SyncViewWork }>
    | Readonly<{
        kind: "page";
        rows: readonly SyncViewRow[];
        nextCursor: string | null;
        work: SyncViewWork;
      }>
  );
function invalid(): never {
  throw new TypeError("Invalid bounded sync view protocol v1 payload");
}
function object(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    invalid();
  return value as Record<string, unknown>;
}
function path(value: unknown): void {
  if (
    !Array.isArray(value) ||
    value.length < 1 ||
    value.length > 8 ||
    value.some(
      (s: unknown) =>
        typeof s !== "string" ||
        s.length < 1 ||
        s.length > 128 ||
        /[\u0000-\u001f\u007f-\u009f]/.test(s) ||
        ["__proto__", "prototype", "constructor", "$surreal"].includes(s),
    )
  )
    invalid();
}
function selector(value: unknown, conjunction = true): void {
  const s = object(value);
  if (s.kind === "all") return;
  if (s.kind === "eq") {
    path(s.path);
    if (
      s.value !== null &&
      typeof s.value !== "boolean" &&
      !(typeof s.value === "string" && s.value.length <= 1024)
    )
      invalid();
    return;
  }
  if (
    s.kind === "and" &&
    conjunction &&
    Array.isArray(s.terms) &&
    s.terms.length > 0 &&
    s.terms.length <= 8
  ) {
    for (const term of s.terms) {
      if (object(term).kind !== "eq") invalid();
      selector(term, false);
    }
    return;
  }
  invalid();
}
export function encodeSyncViewRequest(value: SyncViewRequest): string {
  const req = object(value);
  if (req.protocolVersion !== 1) invalid();
  selector(req.selector);
  if (req.operation === "page") {
    if (
      !Number.isInteger(req.limit) ||
      Number(req.limit) < 1 ||
      Number(req.limit) > 64 ||
      !Array.isArray(req.projection) ||
      req.projection.length < 1 ||
      req.projection.length > 16
    )
      invalid();
    for (const p of req.projection) path(p);
    if (
      new Set(req.projection.map((p) => JSON.stringify(p))).size !==
      req.projection.length
    )
      invalid();
    if (
      req.cursor !== undefined &&
      req.cursor !== null &&
      (typeof req.cursor !== "string" || req.cursor.length > 8192)
    )
      invalid();
  } else if (req.operation !== "count") invalid();
  const encoded = JSON.stringify(value);
  if (encoded.length > 16 * 1024) invalid();
  return encoded;
}
export function decodeSyncViewResult(
  json: string,
  request: SyncViewRequest,
): SyncViewResult {
  if (json.length > 256 * 1024) invalid();
  const result = object(JSON.parse(json));
  if (result.protocolVersion !== 1 || "snapshot" in result) invalid();
  validateSyncReadMetadata(result.metadata);
  if (result.kind === "stale_cursor") {
    if (
      request.operation !== "page" ||
      !request.cursor ||
      "rows" in result ||
      "snapshot" in result ||
      "work" in result
    )
      invalid();
    return result as SyncViewResult;
  }
  const work = object(result.work);
  for (const k of [
    "recordsVisited",
    "operationsVisited",
    "valueNodesVisited",
    "valueBytesVisited",
  ]) {
    if (!Number.isSafeInteger(work[k]) || Number(work[k]) < 0) invalid();
  }
  if (
    Number(work.recordsVisited) + Number(work.operationsVisited) > 16384 ||
    Number(work.valueNodesVisited) > 65536 ||
    Number(work.valueBytesVisited) > 4194304
  )
    invalid();
  if (result.kind === "count" && request.operation === "count") {
    if (
      typeof result.observedCount !== "string" ||
      !/^(0|[1-9][0-9]{0,4})$/.test(result.observedCount) ||
      BigInt(result.observedCount) > 16384n ||
      "rows" in result
    )
      invalid();
  } else if (result.kind === "page" && request.operation === "page") {
    if (
      !Array.isArray(result.rows) ||
      result.rows.length > request.limit ||
      (result.nextCursor !== null &&
        (typeof result.nextCursor !== "string" ||
          result.nextCursor.length > 8192 ||
          result.rows.length !== request.limit))
    )
      invalid();
    const ids = new Set<string>();
    for (const item of result.rows) {
      const row = object(item);
      if (
        typeof row.recordId !== "string" ||
        !row.recordId ||
        row.recordId.length > 4096 ||
        ids.has(row.recordId)
      )
        invalid();
      ids.add(row.recordId);
      validateSyncReadSummary(row);
      if (
        !Array.isArray(row.fields) ||
        row.fields.length !== request.projection.length
      )
        invalid();
      for (let i = 0; i < row.fields.length; i++) {
        const f = object(row.fields[i]);
        if (
          JSON.stringify(f.path) !== JSON.stringify(request.projection[i]) ||
          typeof f.present !== "boolean" ||
          !("value" in f) ||
          (!f.present && f.value !== null)
        )
          invalid();
        if (f.present) f.value = decodeSurrealValue(JSON.stringify(f.value));
      }
    }
  } else invalid();
  return result as SyncViewResult;
}
