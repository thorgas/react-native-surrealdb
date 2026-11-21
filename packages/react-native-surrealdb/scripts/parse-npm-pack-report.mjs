export function parseNpmPackReport(report, expectedName) {
  const artifacts = Array.isArray(report)
    ? report
    : report && typeof report === "object"
      ? Object.values(report)
      : [];
  if (artifacts.length !== 1) {
    throw new Error("npm pack returned an unexpected report");
  }

  const artifact = artifacts[0];
  if (
    !artifact ||
    typeof artifact !== "object" ||
    artifact.name !== expectedName ||
    typeof artifact.version !== "string" ||
    artifact.version.length === 0 ||
    !Number.isSafeInteger(artifact.size) ||
    artifact.size < 0 ||
    !Number.isSafeInteger(artifact.unpackedSize) ||
    artifact.unpackedSize < 0
  ) {
    throw new Error("npm pack returned an invalid artifact");
  }
  return artifact;
}

export function assertNpmPackSize(artifact, maximumPackedBytes) {
  if (!Number.isSafeInteger(maximumPackedBytes) || maximumPackedBytes <= 0) {
    throw new Error("maximum packed size must be a positive safe integer");
  }
  if (artifact.size > maximumPackedBytes) {
    throw new Error(
      `npm tarball is ${artifact.size} bytes; release limit is ${maximumPackedBytes} bytes`,
    );
  }
}
