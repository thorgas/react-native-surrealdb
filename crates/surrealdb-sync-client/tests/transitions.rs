use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use surrealdb_sync_client::{
    ClientError, ClientRuntime, DurableClientState, OptimisticRecord, ResolvedCommit,
};
use surrealdb_sync_protocol::{
    AppliedRecord, BaseVersion, Checkpoint, ClientCommit, ClientCommitId, ClientId, CommitIdentity,
    Cursor, DurableOutcome, Fingerprint, IdMapping, OpaqueCheckpoint, Operation, PartitionId,
    PullBatch, PullCommit, PullFrame, PullResponse, PushResponse, RecordId, RecordState,
    RejectReason, ResetReason, ResetSnapshot, SchemaVersion, ScopeIdentity, ScopeSnapshot,
    SnapshotRecord,
};

type Value = String;

fn record_id(value: &str) -> RecordId {
    RecordId(value.to_owned())
}

fn empty_state() -> DurableClientState<Value> {
    DurableClientState::empty(
        PartitionId("partition".to_owned()),
        ClientId("client".to_owned()),
        ScopeIdentity("scope".to_owned()),
        1,
    )
}

fn identity(id: &str) -> CommitIdentity {
    CommitIdentity {
        client_commit_id: ClientCommitId(id.to_owned()),
        fingerprint: Fingerprint(format!("fingerprint-{id}")),
    }
}

fn upsert(id: &str, value: &str, reference: Option<&str>) -> Operation<Value> {
    Operation::Upsert {
        record_id: record_id(id),
        base_version: BaseVersion::Absent,
        value: value.to_owned(),
        reference: reference.map(record_id),
    }
}

fn commit(id: &str, operations: Vec<Operation<Value>>) -> ClientCommit<Value> {
    ClientCommit {
        identity: identity(id),
        operations,
    }
}

fn checkpoint(token: &str, epoch: u64, sequence: u64) -> Checkpoint {
    Checkpoint {
        token: OpaqueCheckpoint(token.to_owned()),
        cursor: Cursor { epoch, sequence },
        scope: ScopeSnapshot {
            identity: ScopeIdentity("scope".to_owned()),
            authorization_revision: 1,
            subscription_revision: 1,
        },
    }
}

fn push_response(outcome: DurableOutcome<Value>) -> PushResponse<Value> {
    PushResponse {
        schema_version: SchemaVersion::V1,
        partition_id: PartitionId("partition".to_owned()),
        client_id: ClientId("client".to_owned()),
        outcome,
    }
}

fn pull_batch(checkpoint: Checkpoint, commits: Vec<PullCommit<Value>>) -> PullResponse<Value> {
    let token = checkpoint.token.clone();
    let frames = std::iter::once(PullFrame::Start {
        checkpoint: checkpoint.clone(),
    })
    .chain(commits.into_iter().map(|commit| PullFrame::Commit {
        checkpoint: token.clone(),
        commit,
    }))
    .chain(std::iter::once(PullFrame::End { checkpoint }))
    .collect();
    PullResponse::Batch(PullBatch { frames })
}

fn install<V: Clone + Eq>(
    runtime: &mut ClientRuntime<V>,
    prepared: surrealdb_sync_client::PreparedTransition<V>,
) -> DurableClientState<V> {
    let persisted = prepared.state().clone();
    runtime.install(prepared).expect("install should succeed");
    persisted
}

#[test]
fn enqueue_is_visible_only_after_atomic_persistence_and_survives_restart() {
    let mut runtime = ClientRuntime::open(empty_state()).expect("empty state is valid");
    let prepared = runtime
        .prepare_enqueue(commit("one", vec![upsert("a", "local", None)]))
        .expect("enqueue should prepare");

    assert!(runtime.pending_commits().next().is_none());
    assert!(runtime.optimistic().is_empty());

    let persisted = install(&mut runtime, prepared);
    assert_eq!(runtime.pending_commits().count(), 1);
    assert!(matches!(
        runtime.optimistic().get(&record_id("a")),
        Some(OptimisticRecord::Present { value, .. }) if value == "local"
    ));

    let restarted = ClientRuntime::open(persisted).expect("persisted state should reopen");
    assert_eq!(restarted.pending_commits().count(), 1);
    assert!(restarted.optimistic().contains_key(&record_id("a")));
}

#[test]
fn failed_persistence_and_stale_prepared_transition_cannot_change_memory() {
    let mut runtime = ClientRuntime::open(empty_state()).expect("empty state is valid");
    let abandoned = runtime
        .prepare_enqueue(commit("abandoned", vec![upsert("a", "lost", None)]))
        .expect("enqueue should prepare");
    drop(abandoned);
    assert!(runtime.state().outbox.is_empty());

    let stale = runtime
        .prepare_enqueue(commit("stale", vec![upsert("a", "stale", None)]))
        .expect("enqueue should prepare");
    let current = runtime
        .prepare_enqueue(commit("current", vec![upsert("b", "current", None)]))
        .expect("enqueue should prepare");
    install(&mut runtime, current);
    assert!(matches!(
        runtime.install(stale),
        Err(ClientError::StaleTransition { .. })
    ));
    assert_eq!(runtime.state().outbox[0].identity, identity("current"));
}

#[test]
fn conflict_is_durable_and_retains_local_intent() {
    let mut state = empty_state();
    state.confirmed.insert(
        record_id("a"),
        RecordState::Present {
            value: "server".to_owned(),
            version: 2,
            reference: None,
        },
    );
    let mut runtime = ClientRuntime::open(state).expect("state is valid");
    let local = commit(
        "conflict",
        vec![Operation::Upsert {
            record_id: record_id("a"),
            base_version: BaseVersion::Exact(1),
            value: "local".to_owned(),
            reference: None,
        }],
    );
    let prepared = runtime
        .prepare_enqueue(local.clone())
        .expect("enqueue should prepare");
    install(&mut runtime, prepared);
    let outcome = DurableOutcome::Conflict {
        identity: local.identity.clone(),
        record_id: record_id("a"),
        authoritative: runtime.state().confirmed[&record_id("a")].clone(),
    };
    let prepared = runtime
        .prepare_push_response(push_response(outcome.clone()))
        .expect("conflict should prepare");
    let persisted = install(&mut runtime, prepared);

    assert!(runtime.state().outbox.is_empty());
    assert_eq!(
        runtime.state().outcomes,
        vec![ResolvedCommit {
            local_commit: local,
            outcome,
        }]
    );
    assert_eq!(
        ClientRuntime::open(persisted)
            .expect("conflict should reopen")
            .state()
            .outcomes
            .len(),
        1
    );
}

#[test]
fn complete_pull_is_atomic_and_duplicate_is_idempotent() {
    let mut runtime = ClientRuntime::open(empty_state()).expect("empty state is valid");
    let end = checkpoint("cp-1", 1, 2);
    let response = pull_batch(
        end.clone(),
        vec![
            PullCommit {
                sequence: 1,
                source: None,
                records: vec![AppliedRecord {
                    record_id: record_id("a"),
                    state: RecordState::Present {
                        value: "one".to_owned(),
                        version: 1,
                        reference: None,
                    },
                }],
            },
            PullCommit {
                sequence: 2,
                source: None,
                records: Vec::new(),
            },
        ],
    );
    let prepared = runtime
        .prepare_pull_response(response.clone())
        .expect("complete pull should prepare");
    assert!(runtime.state().checkpoint.is_none());
    install(&mut runtime, prepared);
    let revision = runtime.state().revision;
    assert_eq!(runtime.state().checkpoint, Some(end));
    assert_eq!(runtime.state().confirmed.len(), 1);

    let duplicate = runtime
        .prepare_pull_response(response)
        .expect("duplicate pull should be a no-op");
    install(&mut runtime, duplicate);
    assert_eq!(runtime.state().revision, revision);
}

#[test]
fn malformed_or_incomplete_pull_never_advances_checkpoint() {
    let runtime = ClientRuntime::open(empty_state()).expect("empty state is valid");
    let end = checkpoint("cp-1", 1, 1);
    let incomplete = PullResponse::Batch(PullBatch {
        frames: vec![
            PullFrame::Start {
                checkpoint: end.clone(),
            },
            PullFrame::Commit {
                checkpoint: end.token.clone(),
                commit: PullCommit {
                    sequence: 1,
                    source: None,
                    records: Vec::new(),
                },
            },
        ],
    });
    assert!(matches!(
        runtime.prepare_pull_response(incomplete),
        Err(ClientError::InvalidPullFraming)
    ));
    assert!(runtime.state().checkpoint.is_none());

    let gap = pull_batch(
        checkpoint("cp-2", 1, 2),
        vec![PullCommit {
            sequence: 2,
            source: None,
            records: Vec::new(),
        }],
    );
    assert!(matches!(
        runtime.prepare_pull_response(gap),
        Err(ClientError::CommitSequenceGap)
    ));
    assert!(runtime.state().checkpoint.is_none());
}

#[test]
fn reset_replaces_confirmed_state_and_replays_pending_intent() {
    let mut state = empty_state();
    state.confirmed.insert(
        record_id("old"),
        RecordState::Present {
            value: "old".to_owned(),
            version: 1,
            reference: None,
        },
    );
    let mut runtime = ClientRuntime::open(state).expect("state is valid");
    let prepared = runtime
        .prepare_enqueue(commit("pending", vec![upsert("local", "pending", None)]))
        .expect("enqueue should prepare");
    install(&mut runtime, prepared);
    let reset = PullResponse::Reset(ResetSnapshot {
        reason: ResetReason::CheckpointExpired,
        checkpoint: checkpoint("reset", 2, 7),
        records: vec![SnapshotRecord {
            record_id: record_id("fresh"),
            state: RecordState::Present {
                value: "fresh".to_owned(),
                version: 4,
                reference: None,
            },
        }],
    });
    let prepared = runtime
        .prepare_pull_response(reset)
        .expect("reset should prepare");
    install(&mut runtime, prepared);

    assert!(!runtime.state().confirmed.contains_key(&record_id("old")));
    assert!(runtime.state().confirmed.contains_key(&record_id("fresh")));
    assert!(runtime.optimistic().contains_key(&record_id("local")));
    assert_eq!(runtime.state().outbox.len(), 1);
}

#[test]
fn accepted_id_mapping_rewrites_every_durable_reference() {
    let local = record_id("temp");
    let canonical = record_id("server");
    let mut state = empty_state();
    state.confirmed.insert(
        local.clone(),
        RecordState::Present {
            value: "temp-value".to_owned(),
            version: 1,
            reference: None,
        },
    );
    state.confirmed.insert(
        record_id("parent"),
        RecordState::Present {
            value: "parent".to_owned(),
            version: 1,
            reference: Some(local.clone()),
        },
    );
    state.outcomes.push(ResolvedCommit {
        local_commit: commit("prior", vec![upsert("prior", "prior", Some("temp"))]),
        outcome: DurableOutcome::Rejected {
            identity: identity("prior"),
            reason: RejectReason::InvalidOperation,
        },
    });
    let mut runtime = ClientRuntime::open(state).expect("state is valid");
    let pending = commit("map", vec![upsert("temp", "new", Some("parent"))]);
    let prepared = runtime
        .prepare_enqueue(pending.clone())
        .expect("enqueue should prepare");
    install(&mut runtime, prepared);
    let mapping = IdMapping {
        local_id: local.clone(),
        canonical_id: canonical.clone(),
    };
    let response = push_response(DurableOutcome::Accepted {
        identity: pending.identity,
        sequence: 2,
        id_mappings: vec![mapping],
    });
    let prepared = runtime
        .prepare_push_response(response)
        .expect("mapping should prepare");
    install(&mut runtime, prepared);

    assert!(!runtime.state().confirmed.contains_key(&local));
    assert!(runtime.state().confirmed.contains_key(&canonical));
    assert_eq!(runtime.state().id_map[&local], canonical);
    assert_eq!(
        runtime.state().confirmed[&record_id("parent")],
        RecordState::Present {
            value: "parent".to_owned(),
            version: 1,
            reference: Some(record_id("server")),
        }
    );
    assert_eq!(
        runtime.state().outcomes[0].local_commit.operations[0],
        upsert("prior", "prior", Some("server"))
    );
    assert_eq!(
        runtime.state().outcomes[1].local_commit.operations[0].record_id(),
        &record_id("server")
    );
}

#[test]
fn corrupt_persisted_state_is_rejected_before_restart_replay() {
    fn assert_persistable<T: Serialize + for<'de> Deserialize<'de>>() {}
    assert_persistable::<DurableClientState<Value>>();

    let mut state = empty_state();
    state.outbox = vec![
        commit("duplicate", vec![upsert("a", "one", None)]),
        commit("duplicate", vec![upsert("b", "two", None)]),
    ];
    assert!(matches!(
        ClientRuntime::open(state),
        Err(ClientError::DuplicateCommitId(_))
    ));

    let mut invalid_map = empty_state();
    invalid_map.id_map = BTreeMap::from([(record_id("a"), record_id("a"))]);
    assert!(matches!(
        ClientRuntime::open(invalid_map),
        Err(ClientError::InvalidIdMapping(_))
    ));

    let mut exhausted = empty_state();
    exhausted.revision = u64::MAX;
    let runtime = ClientRuntime::open(exhausted).expect("maximum revision is loadable");
    assert!(matches!(
        runtime.prepare_enqueue(commit("next", vec![upsert("a", "one", None)])),
        Err(ClientError::RevisionExhausted)
    ));
}

#[test]
fn client_error_display_does_not_expose_record_identifiers() {
    let error = ClientError::IdMappingCollision(record_id("private-record-id"));
    let displayed = error.to_string();
    assert_eq!(displayed, "record ID mapping collides with canonical state");
    assert!(!displayed.contains("private-record-id"));
}
