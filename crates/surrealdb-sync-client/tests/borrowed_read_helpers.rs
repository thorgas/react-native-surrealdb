use surrealdb_sync_client::{
    ClientRuntime, DurableClientState, ResolvedCommit, optimistic_operations,
};
use surrealdb_sync_protocol::{
    BaseVersion, ClientCommit, ClientCommitId, ClientId, CommitIdentity, DurableOutcome,
    Fingerprint, Operation, PartitionId, RecordId, ScopeIdentity,
};
fn state<V>() -> DurableClientState<V> {
    DurableClientState::empty(
        PartitionId("p".into()),
        ClientId("c".into()),
        ScopeIdentity("s".into()),
        1,
    )
}
fn commit<V>(id: &str, value: V) -> ClientCommit<V> {
    ClientCommit {
        identity: CommitIdentity {
            client_commit_id: ClientCommitId(id.into()),
            fingerprint: Fingerprint(format!("f-{id}")),
        },
        operations: vec![Operation::Upsert {
            record_id: RecordId("item:a".into()),
            base_version: BaseVersion::Absent,
            value,
            reference: None,
        }],
    }
}
#[test]
fn borrowed_overlay_needs_no_clone_and_preserves_accepted_then_pending_order() {
    struct NoClone;
    let mut state = state();
    let accepted = commit("accepted", NoClone);
    state.outcomes.push(ResolvedCommit {
        outcome: DurableOutcome::Accepted {
            identity: accepted.identity.clone(),
            sequence: 1,
            id_mappings: vec![],
        },
        local_commit: accepted,
        resolution: None,
        covered_by_reset: false,
    });
    let covered = commit("covered", NoClone);
    state.outcomes.push(ResolvedCommit {
        outcome: DurableOutcome::Accepted {
            identity: covered.identity.clone(),
            sequence: 2,
            id_mappings: vec![],
        },
        local_commit: covered,
        resolution: None,
        covered_by_reset: true,
    });
    state.outbox.push(commit("pending", NoClone));
    let borrowed: Vec<_> = optimistic_operations(&state).collect();
    assert_eq!(borrowed.len(), 2);
    assert!(std::ptr::eq(
        borrowed[0],
        &state.outcomes[0].local_commit.operations[0]
    ));
    assert!(std::ptr::eq(borrowed[1], &state.outbox[0].operations[0]));
}
#[test]
fn metadata_matches_existing_snapshot_without_materializing_payloads() {
    let mut state = state::<String>();
    state.revision = u64::MAX;
    let runtime = ClientRuntime::open(state).unwrap();
    assert_eq!(runtime.read_metadata(), runtime.read_snapshot().metadata);
}
