import assert from "node:assert/strict";
import { test } from "node:test";
import { parseNpmPackReport } from "./parse-npm-pack-report.mjs";

const artifact = {
  name: "react-native-surrealdb",
  version: "0.1.0-alpha.7.tdt.20261121.2",
  size: 58_027_244,
  unpackedSize: 179_995_076,
};

test("accepts historical one-item npm reports", () => {
  assert.deepEqual(parseNpmPackReport([artifact], artifact.name), artifact);
});

test("accepts npm 12 keyed reports", () => {
  assert.deepEqual(
    parseNpmPackReport({ [artifact.name]: artifact }, artifact.name),
    artifact,
  );
});

test("rejects zero, multiple, and malformed artifacts", () => {
  for (const report of [
    [],
    {},
    [artifact, artifact],
    { a: artifact, b: artifact },
  ]) {
    assert.throws(() => parseNpmPackReport(report, artifact.name));
  }
  for (const invalid of [
    null,
    "artifact",
    { ...artifact, name: "another-package" },
    { ...artifact, version: "" },
    { ...artifact, size: Number.NaN },
    { ...artifact, size: -1 },
    { ...artifact, unpackedSize: "179995076" },
  ]) {
    assert.throws(() => parseNpmPackReport([invalid], artifact.name));
  }
});
