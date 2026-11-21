import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { URL } from "node:url";
import { describe, expect, it } from "vitest";

type UpsertFingerprintFixture = {
  status: string;
  sourceProfileDraft: string;
  sourceProfileDraftCommit: string;
  sourceSchema: string;
  derivation: Record<string, string>;
  vectors: {
    id: string;
    operation: string;
    baseVersion: [0] | [1, number];
    reference: null | [string, [0 | 1 | 2, number | string | Uint8Array]];
    objectInputOrder: string[];
    canonicalObjectOrder: string[];
    decimal: { coefficient: string; scale: number };
    floatBits: string;
    preimageHex: string;
    fingerprint: string;
  }[];
};

const fixture = JSON.parse(
  readFileSync(
    new URL(
      "./fixtures/native-values-upsert-fingerprint-vectors.json",
      import.meta.url,
    ),
    "utf8",
  ),
) as UpsertFingerprintFixture;

const hashLiteralPreimage = (preimageHex: string): string => {
  const bytes = Buffer.from(preimageHex, "hex");
  expect(bytes.toString("hex")).toBe(preimageHex);
  return `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
};

const byId = (id: string) => {
  const vector = fixture.vectors.find((candidate) => candidate.id === id);
  expect(vector).toBeDefined();
  return vector!;
};

describe("native-values Upsert fingerprint draft literals", () => {
  it("checks independently derived six-field CBOR preimages with standard SHA-256", () => {
    expect(fixture.status).toBe(
      "draft-contract-hand-derived-literal-not-runtime-verified",
    );
    expect(fixture.sourceProfileDraft).toBe("surrealdb-sync/native-values/1");
    expect(fixture.sourceProfileDraftCommit).toBe(
      "df88ce632bf8784b8bed439a19da224ef9936583",
    );
    expect(fixture.vectors).toHaveLength(6);
    for (const vector of fixture.vectors) {
      expect(vector.preimageHex).toMatch(/^(?:[0-9a-f]{2})+$/);
      expect(hashLiteralPreimage(vector.preimageHex)).toBe(vector.fingerprint);
      expect(vector.canonicalObjectOrder).toEqual(["a", "z"]);
      expect(vector.decimal.scale).toBeGreaterThanOrEqual(1);
      expect(vector.floatBits).toMatch(/^[0-9a-f]{16}$/);
    }
  });

  it("makes base, reference, Decimal scale and signed-zero representation affect identity", () => {
    const baseline = byId(
      "upsert-absent-null-ref-scale2-negative-zero-object-reverse-input",
    );
    const sortedObject = byId(
      "upsert-absent-null-ref-scale2-negative-zero-object-sorted-input",
    );
    const exactBase = byId("upsert-exact7-null-ref-scale2-negative-zero");
    const typedReference = byId(
      "upsert-absent-typed-string-reference-scale2-negative-zero",
    );
    const scaledDecimal = byId("upsert-absent-null-ref-scale1-negative-zero");
    const positiveZero = byId("upsert-absent-null-ref-scale2-positive-zero");

    expect(baseline.objectInputOrder).toEqual(["z", "a"]);
    expect(sortedObject.objectInputOrder).toEqual(["a", "z"]);
    expect(sortedObject.preimageHex).toBe(baseline.preimageHex);
    expect(sortedObject.fingerprint).toBe(baseline.fingerprint);

    expect(exactBase.baseVersion).toEqual([1, 7]);
    expect(exactBase.preimageHex).not.toBe(baseline.preimageHex);
    expect(typedReference.reference).toEqual(["pillar", [1, "north"]]);
    expect(typedReference.preimageHex).not.toBe(baseline.preimageHex);
    expect(scaledDecimal.decimal).toEqual({ coefficient: "10", scale: 1 });
    expect(scaledDecimal.preimageHex).not.toBe(baseline.preimageHex);
    expect(positiveZero.floatBits).toBe("0000000000000000");
    expect(positiveZero.preimageHex).not.toBe(baseline.preimageHex);
  });

  it("labels complete-operation literals as draft vectors, not an SDK fingerprint API", () => {
    expect(fixture.derivation.independence).toContain(
      "do not call the draft codec or engine encoder",
    );
    expect(fixture.derivation.digest).toContain("Node standard crypto");
    expect(fixture.sourceSchema).toContain(
      "NATIVE_VALUES_PROFILE_PRECISION_REVIEW.md",
    );
  });
});
