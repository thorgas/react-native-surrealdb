import assert from "node:assert/strict";
import { test } from "node:test";
import { assertNpmPackSize } from "./parse-npm-pack-report.mjs";

test("package size guard uses compressed tarball size", () => {
  const artifact = {
    size: 158_568_920,
    unpackedSize: 523_633_111,
  };

  assert.doesNotThrow(() => assertNpmPackSize(artifact, 180_000_000));
  assert.doesNotThrow(() =>
    assertNpmPackSize({ ...artifact, size: 180_000_000 }, 180_000_000),
  );
  assert.throws(
    () => assertNpmPackSize({ ...artifact, size: 180_000_001 }, 180_000_000),
    /npm tarball is 180000001 bytes; release limit is 180000000 bytes/,
  );
});

test("package size guard rejects invalid limits", () => {
  for (const maximumPackedBytes of [
    0,
    -1,
    Number.NaN,
    Number.POSITIVE_INFINITY,
  ]) {
    assert.throws(() => assertNpmPackSize({ size: 1 }, maximumPackedBytes));
  }
});
