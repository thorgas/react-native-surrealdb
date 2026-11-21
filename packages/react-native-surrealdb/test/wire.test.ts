import { describe, expect, it } from "vitest";

import {
  NONE,
  SurrealDecimal,
  SurrealRecordId,
  SurrealSqlValue,
  SurrealUuid,
  decodeSurrealValue,
  encodeQueryVariables,
  encodeSurrealValue,
} from "../src/wire";

describe("SurrealDB wire codec", () => {
  it("preserves integers beyond JavaScript safe integer precision", () => {
    const encoded = encodeQueryVariables({ id: 9_007_199_254_740_993n });
    expect(encoded).toBe(
      '{"id":{"$surreal":"int","value":"9007199254740993"}}',
    );
    expect(
      decodeSurrealValue('{"$surreal":"int","value":"9007199254740993"}'),
    ).toBe(9_007_199_254_740_993n);
  });

  it("round-trips tagged input values without Node globals", () => {
    const encoded = encodeQueryVariables({
      bytes: new Uint8Array([0, 1, 254, 255]),
      decimal: new SurrealDecimal("1234567890.0000000001"),
      id: new SurrealRecordId("person:ada"),
      missing: NONE,
      set: new Set([1n, "two"]),
      uuid: new SurrealUuid("2f1b0ff8-0c2d-4b2b-bdce-497784869c2f"),
    });

    expect(JSON.parse(encoded)).toEqual({
      bytes: { $surreal: "bytes", base64: "AAH+/w==" },
      decimal: { $surreal: "decimal", value: "1234567890.0000000001" },
      id: { $surreal: "record", value: "person:ada" },
      missing: { $surreal: "none" },
      set: {
        $surreal: "set",
        values: [{ $surreal: "int", value: "1" }, "two"],
      },
      uuid: {
        $surreal: "uuid",
        value: "2f1b0ff8-0c2d-4b2b-bdce-497784869c2f",
      },
    });
  });

  it("decodes bytes, sets, records, and SurrealQL-formatted output", () => {
    const decoded = decodeSurrealValue(
      '{"bytes":{"$surreal":"bytes","base64":"AAH+/w=="},' +
        '"id":{"$surreal":"record","value":"person:ada"},' +
        '"set":{"$surreal":"set","values":[true,"x"]},' +
        '"when":{"$surreal":"datetime","value":"d\'2026-01-01T00:00:00Z\'"}}',
    ) as Record<string, unknown>;

    expect(decoded.bytes).toEqual(new Uint8Array([0, 1, 254, 255]));
    expect(decoded.id).toEqual(new SurrealRecordId("person:ada"));
    expect(decoded.set).toEqual(new Set([true, "x"]));
    expect(decoded.when).toEqual(
      new SurrealSqlValue("datetime", "d'2026-01-01T00:00:00Z'"),
    );
  });

  it("round-trips a nanosecond datetime and nested lossless values", () => {
    const encoded =
      '{"when":{"$surreal":"datetime","value":"d\'2026-01-01T00:00:00.123456789Z\'"},' +
      '"nested":{"integer":{"$surreal":"int","value":"9007199254740993"},' +
      '"decimal":{"$surreal":"decimal","value":"1234567890.0000000001"},' +
      '"bytes":{"$surreal":"bytes","base64":"AAH+/w=="},' +
      '"record":{"$surreal":"record","value":"person:ada"},' +
      '"missing":{"$surreal":"none"},"null":null}}';
    const decoded = decodeSurrealValue(encoded);
    expect(JSON.parse(encodeSurrealValue(decoded))).toEqual(
      JSON.parse(encoded),
    );
  });

  it("produces equivalent values with copy and in-place decoding", () => {
    const ids = encodeQueryVariables({
      app: new SurrealRecordId("note:`550e8400-e29b-41d4-a716-446655440000`"),
      number: new SurrealRecordId("note:123"),
      numericString: new SurrealRecordId("note:`123`"),
    });
    expect(JSON.parse(ids)).toEqual({
      app: {
        $surreal: "record",
        value: "note:`550e8400-e29b-41d4-a716-446655440000`",
      },
      number: { $surreal: "record", value: "note:123" },
      numericString: { $surreal: "record", value: "note:`123`" },
    });
    const nativeId = decodeSurrealValue(
      '{"$surreal":"record","value":"note:`550e8400-e29b-41d4-a716-446655440000`"}',
    );
    expect(nativeId).toEqual(
      new SurrealRecordId("note:`550e8400-e29b-41d4-a716-446655440000`"),
    );
    expect(JSON.parse(encodeQueryVariables({ id: nativeId }))).toEqual({
      id: {
        $surreal: "record",
        value: "note:`550e8400-e29b-41d4-a716-446655440000`",
      },
    });

    const json =
      '{"rows":[{"sequence":{"$surreal":"int","value":"42"},' +
      '"name":"answer","nested":{"values":[true,{"$surreal":"none"}]}}],' +
      '"set":{"$surreal":"set","values":[{"$surreal":"int","value":"7"},"x"]}}';

    expect(decodeSurrealValue(json, "in-place")).toEqual(
      decodeSurrealValue(json, "copy"),
    );
  });

  it("rejects cyclic, undefined, and unsupported variables", () => {
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    expect(() => encodeQueryVariables(cyclic)).toThrow(/cyclic/);
    expect(() => encodeQueryVariables({ value: undefined })).toThrow(
      /undefined/,
    );
    expect(() => encodeQueryVariables({ value: new Date() })).toThrow(
      /unsupported/,
    );
    expect(() =>
      encodeQueryVariables({
        value: new SurrealSqlValue("geometry", "point(1,2)"),
      }),
    ).toThrow(/not supported/);
  });
});
