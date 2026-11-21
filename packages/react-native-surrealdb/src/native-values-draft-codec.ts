/**
 * Internal, unwired implementation of the native-values draft value-node
 * format. This module is not exported by the package entry points.
 *
 * It is deliberately a draft codec: Set duplicate/cardinality validation
 * requires pinned SurrealDB native equality and is not approximated here.
 */
export type DraftNativeValue =
  | { kind: "none" | "null" }
  | { kind: "bool"; value: boolean }
  | { kind: "int"; value: bigint }
  | { kind: "float"; bitsHex: string }
  | { kind: "string"; value: string }
  | { kind: "bytes" | "uuid"; hex: string }
  | { kind: "array" | "set"; values: DraftNativeValue[] }
  | { kind: "object"; entries: [string, DraftNativeValue][] }
  | { kind: "datetime"; seconds: bigint; nanos: number }
  | {
      kind: "decimal";
      negative: boolean;
      coefficientHex: string;
      scale: number;
    }
  | {
      kind: "recordId";
      table: string;
      key:
        | { kind: "int"; value: bigint }
        | { kind: "string"; value: string }
        | { kind: "uuid"; hex: string };
    };

const TAG = 60000n;
const MAX_INPUT = 4 * 1024 * 1024;
const MAX_DEPTH = 32;
const MAX_ITEMS = 1024;
const MAX_BYTES = 1024 * 1024;
const MAX_DECIMAL = 79228162514264337593543950335n;
// Pinned Chrono 0.4.45 NaiveDateTime UTC bounds used by SurrealDB 3.2.4.
const MIN_DATETIME_SECONDS = -8334601228800n;
const MAX_DATETIME_SECONDS = 8210266876799n;
const kindIds = [
  "none",
  "null",
  "bool",
  "int",
  "float",
  "string",
  "bytes",
  "array",
  "object",
  "datetime",
  "decimal",
  "uuid",
  "set",
  "recordId",
] as const;

type Cbor =
  | null
  | boolean
  | bigint
  | string
  | Uint8Array
  | Cbor[]
  | { tag: bigint; value: Cbor };

function head(major: number, n: bigint): number[] {
  if (n < 0n) throw new Error("InvalidCbor");
  if (n < 24n) return [(major << 5) | Number(n)];
  const width = n <= 0xffn ? 1 : n <= 0xffffn ? 2 : n <= 0xffffffffn ? 4 : 8;
  if (n >= 1n << BigInt(width * 8)) throw new Error("InvalidCbor");
  const out = [
    (major << 5) |
      ({ 1: 24, 2: 25, 4: 26, 8: 27 } as const)[width as 1 | 2 | 4 | 8],
  ];
  for (let shift = (width - 1) * 8; shift >= 0; shift -= 8)
    out.push(Number((n >> BigInt(shift)) & 255n));
  return out;
}

function bytesHex(hex: string, length?: number): Uint8Array {
  if (
    !/^(?:[0-9a-f]{2})*$/i.test(hex) ||
    (length !== undefined && hex.length !== length * 2)
  )
    throw new Error("InvalidValue");
  return Uint8Array.from(hex.match(/.{2}/g)?.map((x) => parseInt(x, 16)) ?? []);
}
function utf8(value: string): Uint8Array {
  const encoded = new TextEncoder().encode(value);
  if (new TextDecoder("utf-8", { fatal: true }).decode(encoded) !== value)
    throw new Error("InvalidUnicode");
  return encoded;
}
function enc(n: Cbor): number[] {
  if (n === null) return [0xf6];
  if (n === false) return [0xf4];
  if (n === true) return [0xf5];
  if (typeof n === "bigint") return n >= 0 ? head(0, n) : head(1, -1n - n);
  if (typeof n === "string") {
    const b = utf8(n);
    return [...head(3, BigInt(b.length)), ...b];
  }
  if (n instanceof Uint8Array) return [...head(2, BigInt(n.length)), ...n];
  if (Array.isArray(n))
    return [...head(4, BigInt(n.length)), ...n.flatMap(enc)];
  return [...head(6, n.tag), ...enc(n.value)];
}

function asCbor(v: DraftNativeValue, depth = 1): Cbor {
  if (depth > MAX_DEPTH) throw new Error("DepthLimit");
  switch (v.kind) {
    case "none":
    case "null":
      return { tag: TAG, value: [BigInt(kindIds.indexOf(v.kind)), null] };
    case "bool":
      return { tag: TAG, value: [2n, v.value] };
    case "int":
      if (v.value < -(1n << 63n) || v.value > (1n << 63n) - 1n)
        throw new Error("InvalidValue");
      return { tag: TAG, value: [3n, v.value] };
    case "float": {
      const b = bytesHex(v.bitsHex, 8);
      const dv = new DataView(b.buffer, b.byteOffset, 8);
      if (!Number.isFinite(dv.getFloat64(0, false)))
        throw new Error("InvalidFloat");
      return { tag: TAG, value: [4n, b] };
    }
    case "string":
      return { tag: TAG, value: [5n, v.value] };
    case "bytes": {
      const b = bytesHex(v.hex);
      if (b.length > MAX_BYTES) throw new Error("ByteLimit");
      return { tag: TAG, value: [6n, b] };
    }
    case "array":
    case "set": {
      if (v.values.length > MAX_ITEMS) throw new Error("ItemLimit");
      const xs = v.values.map((x) => asCbor(x, depth + 1));
      if (v.kind === "set") {
        const sorted = xs
          .map((x) => Uint8Array.from(enc(x)))
          .sort(compareBytes);
        for (let i = 1; i < sorted.length; i++)
          if (compareBytes(sorted[i - 1]!, sorted[i]!) >= 0)
            throw new Error("SetOrderOrDuplicate");
        return { tag: TAG, value: [12n, sorted.map((b) => parse(b))] };
      }
      return { tag: TAG, value: [7n, xs] };
    }
    case "object": {
      if (v.entries.length > MAX_ITEMS) throw new Error("ItemLimit");
      const entries = v.entries.map(
        ([k, x]) => [k, asCbor(x, depth + 1)] as Cbor[],
      );
      for (let i = 1; i < entries.length; i++)
        if (
          compareBytes(
            utf8(entries[i - 1]![0] as string),
            utf8(entries[i]![0] as string),
          ) >= 0
        )
          throw new Error("ObjectOrderOrDuplicate");
      return { tag: TAG, value: [8n, entries] };
    }
    case "datetime":
      if (
        v.seconds < MIN_DATETIME_SECONDS ||
        v.seconds > MAX_DATETIME_SECONDS ||
        !Number.isInteger(v.nanos) ||
        v.nanos < 0 ||
        v.nanos >= 1_000_000_000 ||
        (v.seconds === MIN_DATETIME_SECONDS && v.nanos !== 0)
      )
        throw new Error("InvalidDatetime");
      return { tag: TAG, value: [9n, [v.seconds, BigInt(v.nanos)]] };
    case "decimal": {
      const b = bytesHex(v.coefficientHex, 12);
      const c = BigInt(`0x${v.coefficientHex}`);
      if (
        c > MAX_DECIMAL ||
        !Number.isInteger(v.scale) ||
        v.scale < 0 ||
        v.scale > 28
      )
        throw new Error("InvalidDecimal");
      return { tag: TAG, value: [10n, [v.negative, b, BigInt(v.scale)]] };
    }
    case "uuid":
      return { tag: TAG, value: [11n, bytesHex(v.hex, 16)] };
    case "recordId": {
      if (!v.table.length || utf8(v.table).length > 1024)
        throw new Error("InvalidRecordId");
      if (
        v.key.kind === "int" &&
        (v.key.value < -(1n << 63n) || v.key.value > (1n << 63n) - 1n)
      )
        throw new Error("InvalidRecordId");
      if (
        v.key.kind === "string" &&
        (!v.key.value.length || utf8(v.key.value).length > 1024)
      )
        throw new Error("InvalidRecordId");
      const key: Cbor =
        v.key.kind === "int"
          ? [0n, v.key.value]
          : v.key.kind === "string"
            ? [1n, v.key.value]
            : [2n, bytesHex(v.key.hex, 16)];
      return { tag: TAG, value: [13n, [v.table, key]] };
    }
  }
}

function compareBytes(a: Uint8Array, b: Uint8Array): number {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) if (a[i] !== b[i]) return a[i]! - b[i]!;
  return a.length - b.length;
}

class Reader {
  i = 0;
  constructor(readonly b: Uint8Array) {}
  take(n: number): Uint8Array {
    if (n < 0 || this.i + n > this.b.length) throw new Error("InvalidCbor");
    const x = this.b.slice(this.i, this.i + n);
    this.i += n;
    return x;
  }
  arg(ai: number): bigint {
    if (ai < 24) return BigInt(ai);
    const w = ai === 24 ? 1 : ai === 25 ? 2 : ai === 26 ? 4 : ai === 27 ? 8 : 0;
    if (!w) throw new Error("InvalidCbor");
    let n = 0n;
    for (const x of this.take(w)) n = (n << 8n) | BigInt(x);
    if (
      (w === 1 && n < 24n) ||
      (w === 2 && n <= 255n) ||
      (w === 4 && n <= 65535n) ||
      (w === 8 && n <= 0xffffffffn)
    )
      throw new Error("NonCanonical");
    return n;
  }
  node(depth = 0): Cbor {
    if (depth > MAX_DEPTH * 3 + 8) throw new Error("DepthLimit");
    const h = this.take(1)[0]!;
    const m = h >> 5;
    const ai = h & 31;
    if (m === 7) {
      if (h === 0xf4) return false;
      if (h === 0xf5) return true;
      if (h === 0xf6) return null;
      throw new Error("InvalidCbor");
    }
    const n = this.arg(ai);
    if (m === 0) return n;
    if (m === 1) return -1n - n;
    if (m === 2 || m === 3) {
      if (n > BigInt(MAX_INPUT)) throw new Error("EncodedLimit");
      const b = this.take(Number(n));
      return m === 2 ? b : new TextDecoder("utf-8", { fatal: true }).decode(b);
    }
    if (m === 4) {
      if (n > BigInt(MAX_ITEMS * 2 + 8)) throw new Error("ItemLimit");
      return Array.from({ length: Number(n) }, () => this.node(depth + 1));
    }
    if (m === 6) return { tag: n, value: this.node(depth + 1) };
    throw new Error("InvalidCbor");
  }
}
function parse(b: Uint8Array): Cbor {
  const r = new Reader(b);
  const x = r.node();
  if (r.i !== b.length) throw new Error("TrailingData");
  return x;
}
function arr(x: Cbor): Cbor[] {
  if (!Array.isArray(x)) throw new Error("InvalidValue");
  return x;
}
function tagged(x: Cbor, expected: bigint): Cbor {
  if (
    !x ||
    typeof x !== "object" ||
    x instanceof Uint8Array ||
    Array.isArray(x) ||
    !("tag" in x) ||
    x.tag !== expected
  )
    throw new Error("InvalidTag");
  return x.value;
}
function int(x: Cbor): bigint {
  if (typeof x !== "bigint") throw new Error("InvalidValue");
  return x;
}
function text(x: Cbor): string {
  if (typeof x !== "string") throw new Error("InvalidValue");
  return x;
}
function bin(x: Cbor): Uint8Array {
  if (!(x instanceof Uint8Array)) throw new Error("InvalidValue");
  return x;
}
function valueFrom(x: Cbor, depth: number): DraftNativeValue {
  if (depth > MAX_DEPTH) throw new Error("DepthLimit");
  const [ki, p] = arr(tagged(x, TAG));
  const k = Number(int(ki));
  if (k < 0 || k >= kindIds.length) throw new Error("InvalidKind");
  switch (k) {
    case 0:
      if (p !== null) throw new Error("InvalidValue");
      return { kind: "none" };
    case 1:
      if (p !== null) throw new Error("InvalidValue");
      return { kind: "null" };
    case 2:
      if (typeof p !== "boolean") throw new Error("InvalidValue");
      return { kind: "bool", value: p };
    case 3: {
      const n = int(p);
      if (n < -(1n << 63n) || n > (1n << 63n) - 1n)
        throw new Error("InvalidValue");
      return { kind: "int", value: n };
    }
    case 4: {
      const b = bin(p);
      if (b.length !== 8) throw new Error("InvalidFloat");
      if (
        !Number.isFinite(
          new DataView(b.buffer, b.byteOffset, 8).getFloat64(0, false),
        )
      )
        throw new Error("InvalidFloat");
      return {
        kind: "float",
        bitsHex: [...b].map((x) => x.toString(16).padStart(2, "0")).join(""),
      };
    }
    case 5:
      return { kind: "string", value: text(p) };
    case 6: {
      const b = bin(p);
      if (b.length > MAX_BYTES) throw new Error("ByteLimit");
      return { kind: "bytes", hex: toHex(b) };
    }
    case 7:
    case 12: {
      const a = arr(p);
      if (a.length > MAX_ITEMS) throw new Error("ItemLimit");
      const vals = a.map((v) => valueFrom(v, depth + 1));
      if (k === 12) {
        const encs = a.map((v) => Uint8Array.from(enc(v)));
        for (let i = 1; i < encs.length; i++)
          if (compareBytes(encs[i - 1]!, encs[i]!) >= 0)
            throw new Error("SetOrderOrDuplicate");
      }
      return k === 7
        ? { kind: "array", values: vals }
        : { kind: "set", values: vals };
    }
    case 8: {
      const pairs = arr(p);
      if (pairs.length > MAX_ITEMS) throw new Error("ItemLimit");
      const entries = pairs.map((e) => {
        const pair = arr(e);
        if (pair.length !== 2) throw new Error("InvalidValue");
        return [text(pair[0]!), valueFrom(pair[1]!, depth + 1)] as [
          string,
          DraftNativeValue,
        ];
      });
      for (let i = 1; i < entries.length; i++)
        if (compareBytes(utf8(entries[i - 1]![0]), utf8(entries[i]![0])) >= 0)
          throw new Error("ObjectOrderOrDuplicate");
      return { kind: "object", entries };
    }
    case 9: {
      const [s, ns] = arr(p);
      const seconds = int(s!);
      const nanos = int(ns!);
      if (
        seconds < MIN_DATETIME_SECONDS ||
        seconds > MAX_DATETIME_SECONDS ||
        nanos < 0n ||
        nanos >= 1_000_000_000n ||
        (seconds === MIN_DATETIME_SECONDS && nanos !== 0n)
      )
        throw new Error("InvalidDatetime");
      return { kind: "datetime", seconds, nanos: Number(nanos) };
    }
    case 10: {
      const [neg, coeff, scale] = arr(p);
      const b = bin(coeff!);
      const sc = int(scale!);
      if (
        typeof neg !== "boolean" ||
        b.length !== 12 ||
        sc < 0n ||
        sc > 28n ||
        BigInt(`0x${toHex(b)}`) > MAX_DECIMAL
      )
        throw new Error("InvalidDecimal");
      return {
        kind: "decimal",
        negative: neg,
        coefficientHex: toHex(b),
        scale: Number(sc),
      };
    }
    case 11: {
      const b = bin(p);
      if (b.length !== 16) throw new Error("InvalidValue");
      return { kind: "uuid", hex: toHex(b) };
    }
    case 13: {
      const [tableRaw, keyRaw] = arr(p);
      const table = text(tableRaw!);
      const [keyType, key] = arr(keyRaw!);
      const kt = int(keyType!);
      if (!table.length || utf8(table).length > 1024)
        throw new Error("InvalidRecordId");
      if (kt === 0n) {
        const n = int(key!);
        if (n < -(1n << 63n) || n > (1n << 63n) - 1n)
          throw new Error("InvalidRecordId");
        return { kind: "recordId", table, key: { kind: "int", value: n } };
      }
      if (kt === 1n) {
        const s = text(key!);
        if (!s.length || utf8(s).length > 1024)
          throw new Error("InvalidRecordId");
        return { kind: "recordId", table, key: { kind: "string", value: s } };
      }
      if (kt === 2n) {
        const b = bin(key!);
        if (b.length !== 16) throw new Error("InvalidRecordId");
        return {
          kind: "recordId",
          table,
          key: { kind: "uuid", hex: toHex(b) },
        };
      }
      throw new Error("InvalidRecordId");
    }
    default:
      throw new Error("InvalidKind");
  }
}
function toHex(b: Uint8Array): string {
  return [...b].map((x) => x.toString(16).padStart(2, "0")).join("");
}

export function encodeDraftNativeValue(value: DraftNativeValue): Uint8Array {
  return Uint8Array.from(enc(asCbor(value)));
}
export function decodeDraftNativeValue(bytes: Uint8Array): DraftNativeValue {
  if (bytes.length > MAX_INPUT) throw new Error("EncodedLimit");
  const node = parse(bytes);
  const decoded = valueFrom(node, 1);
  if (compareBytes(encodeDraftNativeValue(decoded), bytes) !== 0)
    throw new Error("NonCanonical");
  return decoded;
}
