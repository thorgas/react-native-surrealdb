import { readFileSync } from "node:fs";
import { URL } from "node:url";
import { describe, expect, it } from "vitest";
import {
  decodeDraftNativeValue,
  encodeDraftNativeValue,
  type DraftNativeValue,
} from "../src/native-values-draft-codec";

type Fixture = {
  valid: { id: string; cborHex: string; expected: string }[];
  rejected: { id: string; cborHex: string; error: string }[];
  oversizedEncodedInput: { length: number; error: string };
};
const fixture = JSON.parse(
  readFileSync(
    new URL("./fixtures/native-values-decode-vectors.json", import.meta.url),
    "utf8",
  ),
) as Fixture;
const hex = (s: string) =>
  Uint8Array.from(s.match(/.{2}/g)?.map((x) => parseInt(x, 16)) ?? []);
const decimalMax = "ffffffffffffffffffffffff";
const uuid = "00112233445566778899aabbccddeeff";

function expected(id: string): DraftNativeValue {
  switch (id) {
    case "none":
      return { kind: "none" };
    case "null":
      return { kind: "null" };
    case "int-one":
      return { kind: "int", value: 1n };
    case "string-one":
      return { kind: "string", value: "1" };
    case "float-negative-zero":
      return { kind: "float", bitsHex: "8000000000000000" };
    case "decimal-max-coefficient-scale-zero":
      return {
        kind: "decimal",
        negative: false,
        coefficientHex: decimalMax,
        scale: 0,
      };
    case "decimal-max-coefficient-scale-28":
      return {
        kind: "decimal",
        negative: false,
        coefficientHex: decimalMax,
        scale: 28,
      };
    case "datetime-min-utc":
      return { kind: "datetime", seconds: -8334601228800n, nanos: 0 };
    case "datetime-max-utc":
      return { kind: "datetime", seconds: 8210266876799n, nanos: 999999999 };
    case "datetime-fractional-nanos":
      return { kind: "datetime", seconds: 1n, nanos: 999999999 };
    case "uuid-00112233":
      return { kind: "uuid", hex: uuid };
    case "record-key-integer-seven":
      return {
        kind: "recordId",
        table: "note",
        key: { kind: "int", value: 7n },
      };
    case "record-key-string-seven":
      return {
        kind: "recordId",
        table: "note",
        key: { kind: "string", value: "7" },
      };
    case "record-key-uuid":
      return {
        kind: "recordId",
        table: "note",
        key: { kind: "uuid", hex: uuid },
      };
    case "set-retained-float-one":
      return {
        kind: "set",
        values: [{ kind: "float", bitsHex: "3ff0000000000000" }],
      };
    case "set-retained-decimal-scale-two":
      return {
        kind: "set",
        values: [
          {
            kind: "decimal",
            negative: false,
            coefficientHex: "000000000000000000000064",
            scale: 2,
          },
        ],
      };
    case "set-wire-order-differs-from-native-order":
      return {
        kind: "set",
        values: [
          { kind: "int", value: 1n },
          {
            kind: "decimal",
            negative: false,
            coefficientHex: "000000000000000000000000",
            scale: 2,
          },
        ],
      };
    case "ordinary-map-with-tag-lookalike-keys":
      return {
        kind: "object",
        entries: [
          ["$surreal", { kind: "string", value: "tag:60000" }],
          [
            "surrealdb-sync/native-values/1",
            { kind: "string", value: "native-values/1" },
          ],
        ],
      };
    case "depth-32-array-chain": {
      let v: DraftNativeValue = { kind: "int", value: 1n };
      for (let i = 0; i < 31; i++) v = { kind: "array", values: [v] };
      return v;
    }
    default:
      throw new Error(`unknown literal fixture id: ${id}`);
  }
}

describe("unwired native-values draft codec", () => {
  it("decodes independent literal bytes and encodes explicit expected ASTs", () => {
    for (const vector of fixture.valid) {
      const bytes = hex(vector.cborHex);
      const expectedValue = expected(vector.expected);
      expect(decodeDraftNativeValue(bytes), vector.id).toEqual(expectedValue);
      expect(encodeDraftNativeValue(expectedValue), vector.id).toEqual(bytes);
    }
  });

  it("rejects independently authored malformed and over-limit literals", () => {
    for (const vector of fixture.rejected) {
      expect(
        () => decodeDraftNativeValue(hex(vector.cborHex)),
        vector.id,
      ).toThrow(vector.error);
    }
    expect(() =>
      decodeDraftNativeValue(
        new Uint8Array(fixture.oversizedEncodedInput.length),
      ),
    ).toThrow(fixture.oversizedEncodedInput.error);
  });

  it("rejects truncated, overlong, map, and non-increasing Set encodings", () => {
    for (const bad of [
      "d9ea6082",
      "d9ea608200f618",
      "d9ea6080a0",
      "d9ea6082031801",
      "d9ea6082031b8000000000000000",
      "d9ea60820e00",
      "d9ea60820c82d9ea60820301d9ea60820301",
    ]) {
      expect(() => decodeDraftNativeValue(hex(bad))).toThrow();
    }
  });

  it("preserves signed zero bits and differentiates structural record keys", () => {
    const minusZero = expected("float-negative-zero");
    expect(decodeDraftNativeValue(encodeDraftNativeValue(minusZero))).toEqual(
      minusZero,
    );
    expect(
      encodeDraftNativeValue(expected("record-key-integer-seven")),
    ).not.toEqual(encodeDraftNativeValue(expected("record-key-string-seven")));
  });

  it("enforces encoder depth and rejects invalid Unicode scalar strings", () => {
    let tooDeep: DraftNativeValue = { kind: "int", value: 1n };
    for (let i = 0; i < 32; i++) tooDeep = { kind: "array", values: [tooDeep] };
    expect(() => encodeDraftNativeValue(tooDeep)).toThrow("DepthLimit");
    expect(() =>
      encodeDraftNativeValue({ kind: "string", value: "lone-\ud800" }),
    ).toThrow("InvalidUnicode");
  });
});
