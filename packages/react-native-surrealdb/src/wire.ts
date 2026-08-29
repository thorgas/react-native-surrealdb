const TAG = "$surreal" as const;

export class SurrealDecimal {
  readonly kind = "decimal";
  readonly value: string;

  constructor(value: string) {
    this.value = value;
  }
}

export class SurrealRecordId {
  readonly kind = "record";
  readonly value: string;

  constructor(value: string) {
    this.value = value;
  }
}

export class SurrealUuid {
  readonly kind = "uuid";
  readonly value: string;

  constructor(value: string) {
    this.value = value;
  }
}

export class SurrealNone {
  readonly kind = "none";
  private constructor() {}
  static readonly value = new SurrealNone();
}

export const NONE = SurrealNone.value;

export type SurrealSqlKind =
  "datetime" | "duration" | "file" | "geometry" | "range" | "regex" | "table";

/** A lossless SurrealQL-formatted value not yet represented by a JS class. */
export class SurrealSqlValue {
  readonly kind: SurrealSqlKind;
  readonly value: string;

  constructor(kind: SurrealSqlKind, value: string) {
    this.kind = kind;
    this.value = value;
  }
}

export type SurrealScalar =
  | null
  | boolean
  | string
  | number
  | bigint
  | Uint8Array
  | SurrealDecimal
  | SurrealRecordId
  | SurrealUuid
  | SurrealNone
  | SurrealSqlValue;

export type SurrealValue =
  | SurrealScalar
  | SurrealValue[]
  | Set<SurrealValue>
  | { [key: string]: SurrealValue };

export type QueryVariables = Record<string, unknown>;
export type SurrealDecodeMode = "copy" | "in-place";

type Tagged = {
  [TAG]: string;
  value?: unknown;
  base64?: unknown;
  values?: unknown;
};

export function encodeQueryVariables(variables: QueryVariables): string {
  if (!isPlainObject(variables)) {
    throw new TypeError("query variables must be a plain object");
  }
  return encodeSurrealValue(variables);
}

/** Encode one lossless SurrealDB value tree for a native JSON boundary. */
export function encodeSurrealValue(value: unknown): string {
  return JSON.stringify(encodeValue(value, new WeakSet<object>()));
}

export function decodeSurrealValue(
  json: string,
  mode: SurrealDecodeMode = "in-place",
): SurrealValue {
  const parsed = JSON.parse(json) as unknown;
  return mode === "copy" ? decodeValueCopy(parsed) : decodeValueInPlace(parsed);
}

function encodeValue(value: unknown, ancestors: WeakSet<object>): unknown {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean"
  ) {
    return value;
  }
  if (typeof value === "bigint") {
    return { [TAG]: "int", value: value.toString() };
  }
  if (typeof value === "number") {
    return Number.isFinite(value)
      ? value
      : { [TAG]: "float", value: value.toString() };
  }
  if (value === NONE) return { [TAG]: "none" };
  if (value instanceof SurrealDecimal)
    return taggedString("decimal", value.value);
  if (value instanceof SurrealRecordId)
    return taggedString("record", value.value);
  if (value instanceof SurrealUuid) return taggedString("uuid", value.value);
  if (value instanceof Uint8Array) {
    return { [TAG]: "bytes", base64: encodeBase64(value) };
  }
  if (value instanceof SurrealSqlValue) {
    throw new TypeError(
      `Surreal ${value.kind} variables are not supported yet`,
    );
  }
  if (Array.isArray(value)) {
    return encodeContainer(value, ancestors, () =>
      value.map((item) => encodeValue(item, ancestors)),
    );
  }
  if (value instanceof Set) {
    return encodeContainer(value, ancestors, () => ({
      [TAG]: "set",
      values: [...value].map((item) => encodeValue(item, ancestors)),
    }));
  }
  if (isPlainObject(value)) {
    return encodeContainer(value, ancestors, () =>
      Object.fromEntries(
        Object.entries(value).map(([key, item]) => [
          key,
          encodeValue(item, ancestors),
        ]),
      ),
    );
  }
  throw new TypeError(`unsupported query variable: ${describe(value)}`);
}

function encodeContainer<T>(
  value: object,
  ancestors: WeakSet<object>,
  encode: () => T,
): T {
  if (ancestors.has(value))
    throw new TypeError("query variables cannot be cyclic");
  ancestors.add(value);
  try {
    return encode();
  } finally {
    ancestors.delete(value);
  }
}

function decodeValueCopy(value: unknown): SurrealValue {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean" ||
    typeof value === "number"
  ) {
    return value;
  }
  if (Array.isArray(value)) return value.map(decodeValueCopy);
  if (!isPlainObject(value))
    throw new TypeError("invalid SurrealDB wire value");

  if (TAG in value) return decodeTagged(value as Tagged, decodeValueCopy);

  return Object.fromEntries(
    Object.entries(value).map(([key, item]) => [key, decodeValueCopy(item)]),
  );
}

function decodeValueInPlace(value: unknown): SurrealValue {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean" ||
    typeof value === "number"
  ) {
    return value;
  }
  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index += 1) {
      value[index] = decodeValueInPlace(value[index]);
    }
    return value as SurrealValue[];
  }
  if (!isPlainObject(value))
    throw new TypeError("invalid SurrealDB wire value");

  if (TAG in value) return decodeTagged(value as Tagged, decodeValueInPlace);

  for (const key of Object.keys(value)) {
    value[key] = decodeValueInPlace(value[key]);
  }
  return value as { [key: string]: SurrealValue };
}

function decodeTagged(
  value: Tagged,
  decodeNested: (value: unknown) => SurrealValue,
): SurrealValue {
  switch (value[TAG]) {
    case "none":
      return NONE;
    case "int":
      return BigInt(requireString(value.value, "value"));
    case "float": {
      const number = Number(requireString(value.value, "value"));
      if (!Number.isNaN(number) || value.value === "NaN") return number;
      throw new TypeError("invalid tagged float");
    }
    case "decimal":
      return new SurrealDecimal(requireString(value.value, "value"));
    case "record":
      return new SurrealRecordId(requireString(value.value, "value"));
    case "uuid":
      return new SurrealUuid(requireString(value.value, "value"));
    case "bytes":
      return decodeBase64(requireString(value.base64, "base64"));
    case "set":
      if (!Array.isArray(value.values))
        throw new TypeError("tagged set requires values");
      return new Set(value.values.map(decodeNested));
    case "datetime":
    case "duration":
    case "file":
    case "geometry":
    case "range":
    case "regex":
    case "table":
      return new SurrealSqlValue(
        value[TAG],
        requireString(value.value, "value"),
      );
    default:
      throw new TypeError(`unsupported SurrealDB wire tag: ${value[TAG]}`);
  }
}

function taggedString(kind: string, value: string) {
  return { [TAG]: kind, value };
}

function requireString(value: unknown, field: string): string {
  if (typeof value !== "string")
    throw new TypeError(`tag requires ${field} string`);
  return value;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object") return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function describe(value: unknown): string {
  if (value === undefined) return "undefined";
  if (typeof value === "function" || typeof value === "symbol")
    return typeof value;
  return Object.prototype.toString.call(value);
}

const BASE64 =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

function encodeBase64(bytes: Uint8Array): string {
  let output = "";
  for (let index = 0; index < bytes.length; index += 3) {
    const a = bytes[index] ?? 0;
    const b = bytes[index + 1] ?? 0;
    const c = bytes[index + 2] ?? 0;
    const bits = (a << 16) | (b << 8) | c;
    output += BASE64[(bits >>> 18) & 63];
    output += BASE64[(bits >>> 12) & 63];
    output += index + 1 < bytes.length ? BASE64[(bits >>> 6) & 63] : "=";
    output += index + 2 < bytes.length ? BASE64[bits & 63] : "=";
  }
  return output;
}

function decodeBase64(input: string): Uint8Array {
  if (input.length % 4 !== 0 || !/^[A-Za-z0-9+/]*={0,2}$/.test(input)) {
    throw new TypeError("invalid base64 bytes");
  }
  const padding = input.endsWith("==") ? 2 : input.endsWith("=") ? 1 : 0;
  const output = new Uint8Array((input.length / 4) * 3 - padding);
  let offset = 0;
  for (let index = 0; index < input.length; index += 4) {
    const a = BASE64.indexOf(input[index]!);
    const b = BASE64.indexOf(input[index + 1]!);
    const c = input[index + 2] === "=" ? 0 : BASE64.indexOf(input[index + 2]!);
    const d = input[index + 3] === "=" ? 0 : BASE64.indexOf(input[index + 3]!);
    if (a < 0 || b < 0 || c < 0 || d < 0)
      throw new TypeError("invalid base64 bytes");
    const bits = (a << 18) | (b << 12) | (c << 6) | d;
    if (offset < output.length) output[offset++] = (bits >>> 16) & 255;
    if (offset < output.length) output[offset++] = (bits >>> 8) & 255;
    if (offset < output.length) output[offset++] = bits & 255;
  }
  return output;
}
