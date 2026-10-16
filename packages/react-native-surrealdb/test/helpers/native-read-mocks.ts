import { vi } from "vitest";

/** Required ABI surface for tests that exercise other native-client methods. */
export function createNativeReadMocks() {
  return {
    readCapabilitiesJson: vi.fn(() =>
      JSON.stringify({
        protocolVersion: 1,
        maxJournalBatches: 128,
        maxJournalBytes: 4194304,
        maxOutputBytes: 16777216,
        requiresMatchingNativeAbi: true,
        predicateSubscriptions: false,
      }),
    ),
    readSnapshotJson: vi.fn(async () => {
      throw new Error("Native read snapshot is not configured by this fixture");
    }),
    readChangesJson: vi.fn(async () => {
      throw new Error("Native read replay is not configured by this fixture");
    }),
    requiresAuthorizedReset: vi.fn(async () => true),
    scopeTransitionDecision: vi.fn(async () => "unchanged"),
  };
}
