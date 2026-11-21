import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath, URL } from "node:url";
import { describe, expect, it } from "vitest";
import {
  decodeDraftNativeValue,
  encodeDraftNativeValue,
  type DraftNativeValue,
} from "../src/native-values-draft-codec";

type Fixture = {
  valid: { id: string; cborHex: string; expected: string }[];
  rejected: { id: string; cborHex: string; error: string; tsError?: string }[];
  oversizedEncodedInput: { length: number; error: string };
  nativeSetCardinalityCase: {
    id: string;
    cborHex: string;
    rustError: string;
    typescriptDisposition: string;
  };
  resourceBoundaryCases: {
    maxTotalItems: number;
    overTotalItems: number;
    maxAggregateStringBytes: number;
    overAggregateStringBytes: number;
    maxEncodedBytes: number;
    recordIdentityTableBytes: number;
    recordIdentityKeyBytes: number;
    emptyRecordStringKeyBytes: number;
    overEncodedOutputBytes: number;
    nativeSetEqualityCheckedInTypescript: boolean;
  };
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
const runRustInterop = process.env.RUN_NATIVE_VALUES_RUST_INTEROP === "1";

function expected(id: string): DraftNativeValue {
  switch (id) {
    case "none":
      return { kind: "none" };
    case "null":
      return { kind: "null" };
    case "int-one":
      return { kind: "int", value: 1n };
    case "int64-min":
      return { kind: "int", value: -(1n << 63n) };
    case "int64-max":
      return { kind: "int", value: (1n << 63n) - 1n };
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
    case "record-key-empty-string":
      return {
        kind: "recordId",
        table: "note",
        key: { kind: "string", value: "" },
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
  it.skipIf(!runRustInterop)(
    "round-trips accepted Sets and reports native Set collapse through Rust",
    () => {
      const tempDir = mkdtempSync(
        join(tmpdir(), "native-values-draft-interop-"),
      );
      const requestPath = join(tempDir, "request.json");
      const responsePath = join(tempDir, "response.json");
      const cases: {
        id: string;
        cborHex: string;
        expected?: string;
        expectError?: string;
      }[] = fixture.valid.map((vector) => ({
        id: vector.expected,
        cborHex: Buffer.from(
          encodeDraftNativeValue(expected(vector.expected)),
        ).toString("hex"),
        expected: vector.expected,
      }));
      const setCollapse = fixture.nativeSetCardinalityCase;
      cases.push({
        id: setCollapse.id,
        cborHex: Buffer.from(
          encodeDraftNativeValue({
            kind: "set",
            values: [
              { kind: "int", value: 1n },
              { kind: "float", bitsHex: "3ff0000000000000" },
            ],
          }),
        ).toString("hex"),
        expectError: setCollapse.rustError,
      });
      writeFileSync(requestPath, JSON.stringify({ cases }), "utf8");
      try {
        const repoRoot = fileURLToPath(new URL("../../..", import.meta.url));
        const result = spawnSync(
          process.env.CARGO ?? "cargo",
          [
            "test",
            "-p",
            "surrealdb-rn-core",
            "native_values_draft_codec::tests::typescript_bytes_cross_rust_decoder",
            "--lib",
            "--locked",
            "--offline",
            "--jobs",
            "1",
            "--",
            "--ignored",
            "--exact",
          ],
          {
            cwd: repoRoot,
            env: {
              ...process.env,
              NATIVE_VALUES_DRAFT_INTEROP_REQUEST: requestPath,
              NATIVE_VALUES_DRAFT_INTEROP_RESPONSE: responsePath,
            },
            encoding: "utf8",
            timeout: 120_000,
            maxBuffer: 2 * 1024 * 1024,
          },
        );
        expect(result.status, `${result.stdout}\n${result.stderr}`).toBe(0);
        const response = JSON.parse(readFileSync(responsePath, "utf8")) as {
          results: {
            id: string;
            status: string;
            cborHex?: string;
            error?: string;
          }[];
        };
        expect(response.results).toHaveLength(cases.length);
        expect(response.results.map(({ id }) => id).sort()).toEqual(
          cases.map(({ id }) => id).sort(),
        );
        for (const entry of response.results) {
          const source = cases.find(({ id }) => id === entry.id)!;
          if (entry.status === "rejected") {
            expect(source.expectError).toBe("SetCardinality");
            expect(entry.error).toBe(source.expectError);
            continue;
          }
          expect(entry.status).toBe("accepted");
          expect(source.expected).toBeDefined();
          const expectedValue = expected(entry.id);
          expect(decodeDraftNativeValue(hex(entry.cborHex!))).toEqual(
            expectedValue,
          );
        }
      } finally {
        rmSync(tempDir, { recursive: true, force: true });
      }
    },
    150_000,
  );

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
      ).toThrow(vector.tsError ?? vector.error);
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

  it("matches aggregate resource bounds and native-only Set rejection cases", () => {
    const limits = fixture.resourceBoundaryCases;
    expect(limits).toMatchObject({
      maxTotalItems: 4096,
      overTotalItems: 4097,
      maxAggregateStringBytes: 1048576,
      overAggregateStringBytes: 1048577,
      maxEncodedBytes: 4194304,
      recordIdentityKeyBytes: 1025,
      overEncodedOutputBytes: 4194305,
      nativeSetEqualityCheckedInTypescript: false,
    });

    const buildTree = (extraLeaf: boolean): DraftNativeValue => {
      const branches: DraftNativeValue[] = [];
      for (let i = 0; i < 64; i++) {
        const leafCount = i < 63 ? 63 : 62 + Number(extraLeaf);
        branches.push({
          kind: "array",
          values: Array.from({ length: leafCount }, () => ({
            kind: "null" as const,
          })),
        });
      }
      return { kind: "array", values: branches };
    };
    const atItemLimit = encodeDraftNativeValue(buildTree(false));
    expect(atItemLimit.length).toBeLessThan(limits.maxEncodedBytes);
    expect(decodeDraftNativeValue(atItemLimit)).toEqual(buildTree(false));
    expect(() => encodeDraftNativeValue(buildTree(true))).toThrow(
      "TotalItemsLimit",
    );

    const maxBytes = limits.maxAggregateStringBytes;
    const atStringLimit = encodeDraftNativeValue({
      kind: "array",
      values: [
        { kind: "string", value: "a".repeat(maxBytes / 2) },
        { kind: "bytes", hex: "00".repeat(maxBytes / 2) },
      ],
    });
    expect(atStringLimit.length).toBeLessThan(limits.maxEncodedBytes);
    expect(decodeDraftNativeValue(atStringLimit)).toEqual({
      kind: "array",
      values: [
        { kind: "string", value: "a".repeat(maxBytes / 2) },
        { kind: "bytes", hex: "00".repeat(maxBytes / 2) },
      ],
    });
    expect(() =>
      encodeDraftNativeValue({
        kind: "array",
        values: [
          { kind: "string", value: "a".repeat(maxBytes / 2) },
          { kind: "bytes", hex: "00".repeat(maxBytes / 2 + 1) },
        ],
      }),
    ).toThrow("StringLimit");

    const table = "t".repeat(limits.recordIdentityTableBytes);
    const longRecord: DraftNativeValue = {
      kind: "recordId",
      table,
      key: {
        kind: "string",
        value: "k".repeat(limits.recordIdentityKeyBytes),
      },
    };
    expect(decodeDraftNativeValue(encodeDraftNativeValue(longRecord))).toEqual(
      longRecord,
    );
    const setCase = fixture.nativeSetCardinalityCase;
    expect(setCase.rustError).toBe("SetCardinality");
    expect(setCase.typescriptDisposition).toBe(
      "representation-only-accepts-both-members",
    );
    expect(decodeDraftNativeValue(hex(setCase.cborHex))).toEqual({
      kind: "set",
      values: [
        { kind: "int", value: 1n },
        { kind: "float", bitsHex: "3ff0000000000000" },
      ],
    });
    expect(() =>
      encodeDraftNativeValue({
        kind: "bytes",
        hex: "00".repeat(limits.overEncodedOutputBytes),
      }),
    ).toThrow("StringLimit");
    expect(
      decodeDraftNativeValue(
        encodeDraftNativeValue({
          kind: "recordId",
          table: "note",
          key: { kind: "string", value: "" },
        }),
      ),
    ).toEqual(expected("record-key-empty-string"));
  });
});
