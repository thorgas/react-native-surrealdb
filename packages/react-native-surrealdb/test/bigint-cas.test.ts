import { describe, expect, it } from "vitest";
import fixtures from "./fixtures/bigint-cas.json";
import { encodeSurrealValue } from "../src/wire";

// The Rust native-operation tests consume this same checked-in wire output.
describe("bigint CAS native input fixtures", () => {
  for (const fixture of fixtures) {
    it(`encodes ${fixture.commit.identity.clientCommitId} exactly`, () => {
      const operation = fixture.commit.operations[0]!;
      const version = BigInt(fixture.version);
      const commit = {
        identity: fixture.commit.identity,
        operations: [
          {
            kind: operation.kind,
            record_id: "person:ada",
            base_version:
              operation.kind === "upsert" ? { exact: version } : version,
            ...(operation.kind === "upsert"
              ? {
                  value: { count: 5n, nested: { base_version: { exact: 5n } } },
                  reference: null,
                }
              : {}),
          },
        ],
      };
      expect(JSON.parse(encodeSurrealValue(commit))).toEqual(fixture.commit);
    });
  }
});
