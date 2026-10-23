import { vi } from "vitest";

/** Required ABI surface for tests that exercise other native-client methods. */
export function createNativeReadMocks() {
  const unavailable = vi.fn(async () => {
    throw new Error("Native subscription is not configured by this fixture");
  });
  return {
    subscriptionCapabilitiesJson: vi.fn(() =>
      JSON.stringify({
        protocolVersion: 1,
        maxSubscriptions: 32,
        maxWindowRows: 32,
        maxJournalBatches: 128,
        maxJournalBytes: 1048576,
        maxAggregateJournalBytes: 16777216,
        maxOutputBytes: 16777216,
        requiresMatchingNativeAbi: true,
      }),
    ),
    startSubscriptionJson: unavailable,
    applySubscriptionResponseJson: unavailable,
    cancelSubscription: unavailable,
    subscriptionRequestJson: unavailable,
    subscriptionSnapshotJson: unavailable,
    subscriptionChangesJson: unavailable,
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
    readViewJson: vi.fn(async () => {
      throw new Error(
        "Native bounded read view is not configured by this fixture",
      );
    }),
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
