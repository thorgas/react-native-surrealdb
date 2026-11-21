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
  nativeValueSchema: {
    familyTag: number;
    familyTagStatus: string;
    kinds: { id: number; name: string; payload: string }[];
    recordKeyKinds: { id: number; name: string; payload: string }[];
    messageEnvelope: unknown[];
    messageKinds: { id: number; name: string }[];
    pushRequestPayload: string[];
    pushResponsePayload: string[];
    pullRequestPayload: string[];
    pullResponsePayload: string[];
    operationKinds: { id: number; name: string; shape: string }[];
    baseVersion: unknown[];
    clientCommit: string[];
    durableOutcomes: { id: number; name: string; shape: string }[];
    recordStates: { id: number; name: string; shape: string }[];
    nativePullResponse: string[];
    pullFrames: { id: number; name: string; shape: string }[];
    checkpoint: unknown[];
    pullCommit: string[];
    rejectReasons: { id: number; name: string }[];
    resetReasons: { id: number; name: string }[];
    nativeCodecErrors: string[];
    errorBoundary: Record<string, boolean>;
    limits: Record<string, number>;
    limitSemantics: Record<string, string | number | boolean>;
    constructorRules: Record<string, string | number | boolean>;
    jsonAst: string[];
    jsonObjectPayload: string;
    profileBindingToken: [number, string];
    compatibilityReservations: {
      v1: { family: string; mediaType: string; valueTags: number[] };
      pullSourceV2: {
        family: string;
        path: string;
        mediaType: string;
        schema: number;
        pullResponseKind: number;
      };
      acceptanceReceiptV2: {
        family: string;
        mediaType: string;
        pathNamespace: string;
        valueTagsSpecifiedInInspectedDraft: boolean;
      };
      nativeValues: {
        family: string;
        mediaType: string;
        routeAssigned: boolean;
        familyTag: number;
      };
    };
    nativeValueVectors: { id: string; kind: number; cborHex: string }[];
    rejectedNativeValueVectors: {
      id: string;
      kind: number;
      cborHex: string;
      error: string;
    }[];
    pendingExternalDecisions: string[];
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

type DecodeVectorFixture = {
  status: string;
  profileTag: number;
  sourceBasis: string;
  valid: { id: string; cborHex: string; expected: string }[];
  rejected: { id: string; cborHex: string; error: string }[];
  oversizedEncodedInput: { length: number; error: string };
};

const decodeVectors = JSON.parse(
  readFileSync(
    new URL("./fixtures/native-values-decode-vectors.json", import.meta.url),
    "utf8",
  ),
) as DecodeVectorFixture;

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
      memberEncodingTagsAssigned: true,
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
            JSON.stringify(vector.input.at(-1)),
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
    expect(draft.missingNativeContract).toEqual([]);
  });

  it("pins the proposed closed native tag, kind, and message assignments", () => {
    const schema = draft.nativeValueSchema;
    expect(schema.familyTag).toBe(60_000);
    expect(schema.familyTagStatus).toBe("internal-proposal-not-registered");
    expect(schema.kinds).toEqual([
      { id: 0, name: "none", payload: "null" },
      { id: 1, name: "null", payload: "null" },
      { id: 2, name: "bool", payload: "CBOR boolean" },
      { id: 3, name: "int", payload: "signed i64 CBOR integer" },
      {
        id: 4,
        name: "float",
        payload: "8-byte big-endian IEEE-754 binary64",
      },
      { id: 5, name: "string", payload: "CBOR text" },
      { id: 6, name: "bytes", payload: "CBOR byte string" },
      { id: 7, name: "array", payload: "array of NativeValue" },
      {
        id: 8,
        name: "object",
        payload: "sorted array of [keyText, NativeValue] pairs",
      },
      {
        id: 9,
        name: "datetime",
        payload: "[signedUnixSecondsI64, nanosecondsU32]",
      },
      {
        id: 10,
        name: "decimal",
        payload: "[negativeBool, coefficient12ByteBigEndian, scaleU8]",
      },
      { id: 11, name: "uuid", payload: "16-byte string" },
      { id: 12, name: "set", payload: "sorted array of NativeValue" },
      { id: 13, name: "recordId", payload: "NativeRecordIdentity" },
    ]);
    expect(new Set(schema.kinds.map(({ name }) => name)).size).toBe(14);
    expect(schema.recordKeyKinds.map(({ id }) => id)).toEqual([0, 1, 2]);
    expect(schema.messageEnvelope).toEqual([
      "surrealdb-sync/native-values/1",
      0,
      "messageKind",
      "payload",
    ]);
    expect(schema.messageKinds).toEqual([
      { id: 0, name: "pushRequest" },
      { id: 1, name: "pushResponse" },
      { id: 2, name: "pullRequest" },
      { id: 3, name: "pullResponse" },
    ]);
    expect(schema.pushRequestPayload).toEqual([
      "profileId",
      "ProfileBindingToken",
      "partitionId",
      "clientId",
      "ClientCommit",
    ]);
    expect(schema.pushResponsePayload).toEqual([
      "profileId",
      "partitionId",
      "clientId",
      "DurableOutcome",
    ]);
    expect(schema.pullRequestPayload).toEqual([
      "profileId",
      "ProfileBindingToken",
      "partitionId",
      "clientId",
      "checkpointOrNull",
      "requestedScope",
      "subscriptionRevision",
    ]);
    expect(schema.pullResponsePayload).toEqual([
      "profileId",
      "partitionId",
      "clientId",
      "NativePullResponse",
    ]);
    expect(schema.operationKinds).toEqual([
      {
        id: 0,
        name: "upsert",
        shape: "[0, identity, baseVersion, value, referenceOrNull]",
      },
      {
        id: 1,
        name: "delete",
        shape: "[1, identity, baseVersionU64]",
      },
    ]);
    expect(schema.baseVersion).toEqual([[0], [1, "versionU64"]]);
    expect(schema.clientCommit).toEqual([
      "clientCommitId",
      "fingerprintText",
      "operations",
    ]);
    expect(schema.durableOutcomes.map(({ id, name }) => [id, name])).toEqual([
      [0, "accepted"],
      [1, "conflict"],
      [2, "rejected"],
    ]);
    expect(schema.recordStates.map(({ id, name }) => [id, name])).toEqual([
      [0, "absent"],
      [1, "present"],
      [2, "tombstone"],
    ]);
    expect(schema.nativePullResponse).toEqual([
      "[0, frames]",
      "[1, resetReasonU64, checkpoint, snapshotRecords]",
    ]);
    expect(schema.pullFrames.map(({ id, name }) => [id, name])).toEqual([
      [0, "start"],
      [1, "commit"],
      [2, "end"],
    ]);
    expect(schema.pullCommit).toEqual([
      "sequenceU64",
      "sourceOrNullRequired",
      "appliedRecords",
    ]);
    expect(schema.rejectReasons.map(({ id }) => id)).toEqual([0, 1, 2, 3]);
    expect(schema.resetReasons.map(({ id }) => id)).toEqual([0, 1, 2, 3, 4, 5]);
    expect(schema.nativeCodecErrors).toContain("NativeSetCardinalityChanged");
    expect(schema.errorBoundary).toEqual({
      wireErrorMessageKind: false,
      transportStatusAssigned: false,
      localValidationBeforeOutboxMutation: true,
      receiverValidationBeforeDurableMutation: true,
      decoderFailureIsDurableRejectedOutcome: false,
      partialValueExposureOnFailure: false,
    });
    expect(schema.limits).toEqual({
      depth: 32,
      containerItems: 1024,
      totalItems: 4096,
      stringOrBytes: 1048576,
      encodedCbor: 4194304,
      profileBindingToken: 4096,
      serializedJsonBridge: 4194304,
    });
    expect(schema.limitSemantics).toEqual({
      depthCountsRootAs: 1,
      containerItemsAreDirectMembers: true,
      totalItemsCount: "tagged native value nodes",
      stringsAndBytesMeasure: "aggregate UTF-8 text plus bytes",
      encodedCborMeasures: "complete single CBOR message",
      maximaInclusive: true,
      claimedLengthValidatedBeforeAllocation: true,
    });
    expect(schema.constructorRules).toEqual({
      floatWidthBytes: 8,
      finiteFloatOnly: true,
      rejectNaNAndInfinity: true,
      preserveSignedFloatZero: true,
      decimalMaxCoefficient: "79228162514264337593543950335",
      decimalMaxScale: 28,
      decimalMustRoundTripConstructor: true,
      datetimeConstructor: "surrealdb_types::Datetime::from_timestamp",
      uuidWidthBytes: 16,
      recordIdTableMustBeNonempty: true,
      recordIdStringKeyMayBeEmpty: true,
      recordIdFieldsHaveNoIndividualByteCap: true,
    });
    expect(schema.profileBindingToken).toEqual([0, "opaqueTokenBytes:1..4096"]);
    expect(schema.jsonAst).toEqual([
      "surrealdb-sync/native-values/1",
      "kindId",
      "payload",
    ]);
    expect(schema.jsonObjectPayload).toBe(
      "sorted array of [keyText, NativeJsonValue] pairs",
    );
    expect(schema.compatibilityReservations.v1.valueTags).toEqual([6, 8]);
    expect(schema.familyTag).not.toBe(6);
    expect(schema.familyTag).not.toBe(8);
    expect(schema.compatibilityReservations.pullSourceV2).toMatchObject({
      family: "surrealdb-sync/pull-source/2",
      schema: 0,
      pullResponseKind: 3,
    });
    expect(schema.compatibilityReservations.acceptanceReceiptV2).toMatchObject({
      family: "surrealdb-sync/2",
      mediaType: "application/vnd.surrealdb-sync.v2+cbor",
      valueTagsSpecifiedInInspectedDraft: false,
    });
    expect(schema.compatibilityReservations.nativeValues.routeAssigned).toBe(
      false,
    );
    expect(schema.compatibilityReservations.nativeValues.mediaType).not.toBe(
      schema.compatibilityReservations.v1.mediaType,
    );
    expect(schema.compatibilityReservations.nativeValues.family).not.toBe(
      schema.compatibilityReservations.pullSourceV2.family,
    );
    expect(schema.compatibilityReservations.nativeValues.family).not.toBe(
      schema.compatibilityReservations.acceptanceReceiptV2.family,
    );

    const vectors = schema.nativeValueVectors;
    expect(vectors.map(({ id }) => id)).toEqual([
      "none",
      "null",
      "int-one",
      "string-one",
      "float-negative-zero",
      "decimal-one-scale-two",
      "set-containing-int-one",
      "record-note-integer-seven",
    ]);
    expect(
      vectors.every(
        ({ cborHex }) =>
          cborHex.startsWith("d9ea60") &&
          Buffer.from(cborHex, "hex").toString("hex") === cborHex,
      ),
    ).toBe(true);
    expect(vectors[0]!.cborHex).not.toBe(vectors[1]!.cborHex);
    expect(
      schema.rejectedNativeValueVectors.map(({ id, error }) => [id, error]),
    ).toEqual([
      ["float-nan-payload-one", "InvalidFloat"],
      ["float-positive-infinity", "InvalidFloat"],
      ["float-negative-infinity", "InvalidFloat"],
    ]);
    expect(schema.pendingExternalDecisions).toEqual([
      "check and complete any required external CBOR tag registration for 60000",
      "assign a native-values HTTP route after acceptance-receipt-v2 router collision testing",
    ]);
  });

  it("keeps independent literal CBOR decode vectors available cross-layer", () => {
    expect(decodeVectors.status).toBe(
      "hand-authored-cbor-vectors-not-generated-by-sdk-codec",
    );
    expect(decodeVectors.profileTag).toBe(60_000);
    expect(decodeVectors.sourceBasis).toContain("rust_decimal 1.42.1");
    expect(decodeVectors.valid.map(({ id }) => id)).toContain(
      "decimal-max-coefficient-scale-28",
    );
    expect(decodeVectors.valid.map(({ id }) => id)).toContain(
      "datetime-min-utc",
    );
    expect(decodeVectors.valid.map(({ id }) => id)).toContain(
      "datetime-max-utc",
    );
    expect(decodeVectors.valid.map(({ id }) => id)).toContain(
      "ordinary-map-with-tag-lookalike-keys",
    );
    expect(
      [...decodeVectors.valid, ...decodeVectors.rejected].every(
        ({ cborHex }) =>
          /^[0-9a-f]+$/.test(cborHex) && cborHex.length % 2 === 0,
      ),
    ).toBe(true);
    expect(decodeVectors.rejected.map(({ id, error }) => [id, error])).toEqual([
      ["decimal-scale-29", "InvalidDecimal"],
      ["datetime-nanos-one-billion", "InvalidDatetime"],
      ["datetime-one-second-past-max", "InvalidDatetime"],
      ["depth-33-array-chain", "DepthLimit"],
      ["empty-record-table", "InvalidRecordId"],
      ["malformed-utf8-string", "Decode"],
    ]);
    expect(decodeVectors.oversizedEncodedInput).toEqual({
      length: 4_194_305,
      error: "EncodedLimit",
    });
  });

  it.todo(
    "public SDK/native Set construction exposes the surviving representative through a value conversion path",
  );
});
