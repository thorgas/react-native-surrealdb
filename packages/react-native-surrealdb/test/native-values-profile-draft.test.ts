import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { URL } from "node:url";
import { describe, expect, it } from "vitest";

type DraftFixture = {
  status: string;
  profile: string;
  fingerprintDomain: string;
  fingerprintEnvelope: string[];
  fingerprintVectors: {
    id: string;
    recordKey: { kind: "integer" | "string"; value: string };
    preimageHex: string;
    fingerprint: string;
  }[];
  setRule: {
    nativeEqualityUse: string;
    representative: string;
    wireOrder: string;
    retry: string;
    memberEncodingTagsAssigned: boolean;
    nativeRuntimeEvidenceRequired: boolean;
  };
  setVectors: {
    id: string;
    input: unknown[];
    expectedNativeRepresentative: unknown;
    expectedNativeCardinality: number;
    expectedWireIdentity: string;
    nativeRuntimeRequired: boolean;
  }[];
  decoderRejectVectors: {
    id: string;
    encodedMembers: unknown[];
    expectedNativeCardinality: number;
    nativeRuntimeRequired: boolean;
  }[];
  missingNativeContract: string[];
};

const draft = JSON.parse(
  readFileSync(
    new URL("./fixtures/native-values-profile-draft.json", import.meta.url),
    "utf8",
  ),
) as DraftFixture;

function fingerprint(preimageHex: string): string {
  const digest = createHash("sha256")
    .update(Buffer.from(preimageHex, "hex"))
    .digest("hex");
  return `sha256:${digest}`;
}

describe("native-values profile draft contract fixtures", () => {
  it("pins the two candidate structural record-key preimages and digests", () => {
    expect(draft.status).toBe(
      "draft-contract-hand-derived-not-runtime-verified",
    );
    expect(draft.profile).toBe("surrealdb-sync/native-values/1");
    expect(draft.fingerprintDomain).toBe(
      "surrealdb-sync/native-values/1/commit-fingerprint/sha256",
    );
    expect(draft.fingerprintEnvelope).toEqual([
      "domain",
      "profile",
      "partitionId",
      "clientId",
      "clientCommitId",
      "orderedOperations",
    ]);

    const integerKey = draft.fingerprintVectors.find(
      ({ id }) => id === "delete-integer-record-key-7",
    );
    const stringKey = draft.fingerprintVectors.find(
      ({ id }) => id === "delete-string-record-key-7",
    );
    expect(integerKey).toBeDefined();
    expect(stringKey).toBeDefined();
    expect(integerKey!.recordKey).toEqual({ kind: "integer", value: "7" });
    expect(stringKey!.recordKey).toEqual({ kind: "string", value: "7" });
    expect(integerKey!.preimageHex).not.toBe(stringKey!.preimageHex);
    expect(Buffer.from(integerKey!.preimageHex, "hex").toString("hex")).toBe(
      integerKey!.preimageHex,
    );
    expect(Buffer.from(stringKey!.preimageHex, "hex").toString("hex")).toBe(
      stringKey!.preimageHex,
    );
    expect(fingerprint(integerKey!.preimageHex)).toBe(integerKey!.fingerprint);
    expect(fingerprint(stringKey!.preimageHex)).toBe(stringKey!.fingerprint);
    expect(integerKey!.fingerprint).not.toBe(stringKey!.fingerprint);
  });

  it("keeps exact candidate bytes stable for an explicit replay", () => {
    for (const vector of draft.fingerprintVectors) {
      // This byte-copy example is not an SDK persistence/retry integration test.
      const originalBytes = Buffer.from(vector.preimageHex, "hex");
      expect(originalBytes.toString("hex")).toBe(vector.preimageHex);
      const persistedReplayBytes = Buffer.from(originalBytes);
      expect(persistedReplayBytes).toEqual(originalBytes);
      expect(fingerprint(persistedReplayBytes.toString("hex"))).toBe(
        vector.fingerprint,
      );
    }
  });

  // This checks fixture scope and coverage only. JS Set equality is not a
  // substitute for the pinned SurrealDB Rust Set/Number::Ord behavior.
  it("records Set cases as native-runtime contract vectors", () => {
    expect(draft.setRule).toEqual({
      nativeEqualityUse: "duplicate-and-decoded-cardinality-validation-only",
      representative:
        "preserve-the-member-already-retained-by-native-set-construction",
      wireOrder:
        "strict-ascending-unsigned-lexicographic-complete-canonical-member-bytes",
      retry: "preserve-and-replay-exact-canonical-bytes-and-fingerprint",
      memberEncodingTagsAssigned: false,
      nativeRuntimeEvidenceRequired: true,
    });
    expect(draft.setVectors.map(({ id }) => id)).toEqual([
      "int-one-before-float-one",
      "float-one-before-int-one",
      "decimal-scale-one-before-scale-two",
      "decimal-scale-two-before-scale-one",
      "negative-zero-before-positive-zero",
      "positive-zero-before-negative-zero",
    ]);
    expect(
      draft.setVectors.every(
        (vector) =>
          vector.nativeRuntimeRequired &&
          vector.expectedNativeCardinality === 1 &&
          JSON.stringify(vector.expectedNativeRepresentative) ===
            JSON.stringify(vector.input[0]),
      ),
    ).toBe(true);
    expect(draft.decoderRejectVectors.map(({ id }) => id)).toEqual([
      "int-one-and-float-one",
      "decimal-scale-one-and-scale-two",
      "negative-zero-and-positive-zero",
    ]);
    expect(
      draft.decoderRejectVectors.every(
        (vector) =>
          vector.nativeRuntimeRequired &&
          vector.expectedNativeCardinality === 1 &&
          vector.encodedMembers.length === 2,
      ),
    ).toBe(true);
    expect(draft.missingNativeContract).toContain(
      "semantic CBOR tags and payload encodings for native values",
    );
  });

  it.todo("native codec preserves the actual SurrealDB Set representative");
  it.todo("native codec rejects Set members equal under pinned Number::Ord");
  it.todo("native codec decode-encode preserves accepted canonical Set bytes");
});
