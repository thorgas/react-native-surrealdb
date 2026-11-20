//! Compile-only contract for the engine surfaces already consumed by the SDK.
//!
//! The library's own check is exhaustive; this fixture gives the integrated
//! engine branch a short, named compatibility gate before repinning the SDK.

use serde_json::Value;
use surrealdb_sync_client::{
    AutomaticConflictPolicy, ClientRuntime, CommittedReadBatchV1, DurableClientState,
    DurableSubscriptions, ResetCoverage, optimistic_operations,
};
use surrealdb_sync_protocol::{bootstrap, bootstrap_codec, subscription, subscription_codec};

#[test]
fn sdk_engine_api_surface_compiles() {
    type Runtime = ClientRuntime<Value>;
    type State = DurableClientState<Value>;

    let _ = std::mem::size_of::<AutomaticConflictPolicy>();
    let _ = std::mem::size_of::<CommittedReadBatchV1<Value>>();
    let _ = std::mem::size_of::<DurableSubscriptions<Value>>();
    let _ = std::mem::size_of::<ResetCoverage>();
    let _ = std::mem::size_of::<bootstrap::BootstrapContext>();
    let _ = std::mem::size_of::<subscription::SubscriptionRequest>();
    let _ = bootstrap_codec::bootstrap_manifest_digest;
    let _ = subscription_codec::encode_subscription_request;
    let _ = optimistic_operations::<Value>;

    let _ = Runtime::prepare_push_response_with_policy;
    let _ = Runtime::install_with_read_batch;
    let _ = Runtime::prepare_bootstrap_snapshot;
    let _ = Runtime::read_metadata;
    let _ = Runtime::read_snapshot;
    let _ = Runtime::scope_transition_decision;
    let _ = Runtime::prepare_start_subscription;
    let _ = Runtime::prepare_cancel_subscription;
    let _ = Runtime::prepare_subscription_response;
    let _ = Runtime::prepare_resolve_lane_conflict;
    let _ = Runtime::prepare_ack_lane_resolution_receipt;
    let _ = Runtime::subscription_request;
    let _ = Runtime::subscription_snapshot;

    // Durable fields must coexist with lane state in one persisted replacement.
    let assert_fields = |state: &State| {
        let _ = (&state.reset_coverage, &state.subscriptions);
        let _ = (&state.local_intents, &state.lanes, &state.last_reset_cursor);
        let _ = &state.resolution_receipts;
    };
    let _ = assert_fields;
}
