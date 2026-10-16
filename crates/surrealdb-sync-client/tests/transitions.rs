use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use surrealdb_sync_client::{
    AutomaticConflictPolicy, ClientError, ClientRuntime, ConflictResolution,
    ConflictResolutionRequest, DurableClientState, OptimisticRecord, ResolvedCommit,
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

#[test]
fn prefer_server_policy_leaves_delete_batch_and_tombstone_unresolved() {
    let stale_upsert = Operation::Upsert {
        record_id: record_id("a"),
        base_version: BaseVersion::Exact(1),
        value: "local".to_owned(),
        reference: None,
    };
    let cases = [
        (
            vec![Operation::Delete {
                record_id: record_id("a"),
                base_version: 1,
            }],
            RecordState::Present {
                value: "server".to_owned(),
                version: 2,
                reference: None,
            },
        ),
        (
            vec![stale_upsert.clone(), upsert("b", "other", None)],
            RecordState::Present {
                value: "server".to_owned(),
                version: 2,
                reference: None,
            },
        ),
        (vec![stale_upsert], RecordState::Tombstone { version: 2 }),
    ];
    for (index, (operations, authoritative)) in cases.into_iter().enumerate() {
        let local = commit(&format!("conflict-{index}"), operations);
        let mut state = empty_state();
        state.outbox.push(local.clone());
        let mut runtime = ClientRuntime::open(state).unwrap();
        let prepared = runtime
            .prepare_push_response_with_policy(
                push_response(DurableOutcome::Conflict {
                    identity: local.identity.clone(),
                    record_id: record_id("a"),
                    authoritative,
                }),
                AutomaticConflictPolicy::PreferServerV1,
            )
            .unwrap();
        install(&mut runtime, prepared);
        assert_eq!(runtime.state().outcomes[0].resolution, None);
        assert_eq!(runtime.state().outcomes[0].local_commit, local);
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
            resolution: None,
            covered_by_reset: false,
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

fn runtime_with_conflict(
    local: ClientCommit<Value>,
    authoritative: RecordState<Value>,
) -> ClientRuntime<Value> {
    let mut state = empty_state();
    if authoritative != RecordState::Absent {
        state
            .confirmed
            .insert(record_id("a"), authoritative.clone());
    }
    state.outcomes.push(ResolvedCommit {
        outcome: DurableOutcome::Conflict {
            identity: local.identity.clone(),
            record_id: record_id("a"),
            authoritative,
        },
        local_commit: local,
        resolution: None,
        covered_by_reset: false,
    });
    ClientRuntime::open(state).expect("conflict state is valid")
}

#[test]
fn keep_server_is_durable_and_preserves_the_original_conflict() {
    let local = commit(
        "conflict",
        vec![Operation::Upsert {
            record_id: record_id("a"),
            base_version: BaseVersion::Exact(1),
            value: "local".to_owned(),
            reference: None,
        }],
    );
    let mut runtime = runtime_with_conflict(
        local.clone(),
        RecordState::Present {
            value: "server".to_owned(),
            version: 2,
            reference: None,
        },
    );
    let prepared = runtime
        .prepare_resolve_conflict(
            &local.identity.client_commit_id,
            ConflictResolutionRequest::KeepServer,
        )
        .expect("keep-server should prepare");
    let persisted = install(&mut runtime, prepared);

    assert!(runtime.state().outbox.is_empty());
    assert_eq!(runtime.state().outcomes[0].local_commit, local);
    assert_eq!(
        runtime.state().outcomes[0].resolution,
        Some(ConflictResolution::KeepServer)
    );
    assert_eq!(
        ClientRuntime::open(persisted)
            .expect("resolution should reopen")
            .state()
            .outcomes[0]
            .resolution,
        Some(ConflictResolution::KeepServer)
    );
    runtime
        .prepare_resolve_conflict(
            &local.identity.client_commit_id,
            ConflictResolutionRequest::KeepServer,
        )
        .expect("retrying the same keep-server decision is idempotent");
}

#[test]
fn keep_local_retries_the_complete_intent_with_a_fresh_identity() {
    let local = commit(
        "conflict",
        vec![
            Operation::Upsert {
                record_id: record_id("a"),
                base_version: BaseVersion::Exact(1),
                value: "local".to_owned(),
                reference: None,
            },
            upsert("b", "second", None),
        ],
    );
    let mut runtime = runtime_with_conflict(
        local.clone(),
        RecordState::Present {
            value: "server".to_owned(),
            version: 2,
            reference: None,
        },
    );
    let replacement = commit(
        "retry",
        vec![
            Operation::Upsert {
                record_id: record_id("a"),
                base_version: BaseVersion::Exact(2),
                value: "local".to_owned(),
                reference: None,
            },
            upsert("b", "second", None),
        ],
    );
    let prepared = runtime
        .prepare_resolve_conflict(
            &local.identity.client_commit_id,
            ConflictResolutionRequest::KeepLocal {
                replacement: replacement.clone(),
            },
        )
        .expect("keep-local should prepare");
    let persisted = install(&mut runtime, prepared);

    assert_eq!(runtime.state().outbox, vec![replacement.clone()]);
    assert_eq!(runtime.state().outcomes[0].local_commit, local);
    assert_eq!(
        runtime.state().outcomes[0].resolution,
        Some(ConflictResolution::KeepLocal {
            replacement: replacement.identity.clone(),
        })
    );
    assert_eq!(
        ClientRuntime::open(persisted)
            .expect("retry should survive restart")
            .state()
            .outbox,
        vec![replacement]
    );
    runtime
        .prepare_resolve_conflict(
            &local.identity.client_commit_id,
            ConflictResolutionRequest::KeepLocal {
                replacement: runtime.state().outbox[0].clone(),
            },
        )
        .expect("retrying the same keep-local decision is idempotent");
}

#[test]
fn merge_requires_the_conflict_record_at_the_authoritative_base() {
    let local = commit(
        "conflict",
        vec![Operation::Upsert {
            record_id: record_id("a"),
            base_version: BaseVersion::Exact(1),
            value: "local".to_owned(),
            reference: None,
        }],
    );
    let mut runtime = runtime_with_conflict(local.clone(), RecordState::Tombstone { version: 3 });
    let stale = commit("stale-merge", vec![upsert("a", "merged", None)]);
    assert_eq!(
        runtime.prepare_resolve_conflict(
            &local.identity.client_commit_id,
            ConflictResolutionRequest::Merge { replacement: stale },
        ),
        Err(ClientError::InvalidConflictResolution)
    );

    let replacement = commit(
        "merge",
        vec![Operation::Upsert {
            record_id: record_id("a"),
            base_version: BaseVersion::Exact(3),
            value: "merged".to_owned(),
            reference: None,
        }],
    );
    let prepared = runtime
        .prepare_resolve_conflict(
            &local.identity.client_commit_id,
            ConflictResolutionRequest::Merge {
                replacement: replacement.clone(),
            },
        )
        .expect("fresh tombstone merge should prepare");
    install(&mut runtime, prepared);

    assert_eq!(runtime.state().outbox, vec![replacement.clone()]);
    assert_eq!(
        runtime.state().outcomes[0].resolution,
        Some(ConflictResolution::Merge {
            replacement: replacement.identity,
        })
    );
    assert!(matches!(
        runtime.prepare_resolve_conflict(
            &local.identity.client_commit_id,
            ConflictResolutionRequest::KeepServer,
        ),
        Err(ClientError::ConflictAlreadyResolved(_))
    ));
}

#[test]
fn keep_local_never_relabels_changed_intent_or_invalid_delete() {
    let local = commit(
        "conflict",
        vec![Operation::Upsert {
            record_id: record_id("a"),
            base_version: BaseVersion::Exact(1),
            value: "local".to_owned(),
            reference: None,
        }],
    );
    let runtime = runtime_with_conflict(local.clone(), RecordState::Absent);
    let changed = commit("changed", vec![upsert("a", "different", None)]);
    assert_eq!(
        runtime.prepare_resolve_conflict(
            &local.identity.client_commit_id,
            ConflictResolutionRequest::KeepLocal {
                replacement: changed,
            },
        ),
        Err(ClientError::InvalidConflictResolution)
    );

    let delete = commit(
        "delete-conflict",
        vec![Operation::Delete {
            record_id: record_id("a"),
            base_version: 1,
        }],
    );
    let runtime = runtime_with_conflict(delete.clone(), RecordState::Tombstone { version: 2 });
    let replacement = commit(
        "delete-retry",
        vec![Operation::Delete {
            record_id: record_id("a"),
            base_version: 2,
        }],
    );
    assert_eq!(
        runtime.prepare_resolve_conflict(
            &delete.identity.client_commit_id,
            ConflictResolutionRequest::KeepLocal { replacement },
        ),
        Err(ClientError::InvalidConflictResolution)
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
fn empty_pull_rotates_checkpoint_token_without_advancing_cursor() {
    let mut runtime = ClientRuntime::open(empty_state()).expect("empty state is valid");
    let first = checkpoint("cp-1", 1, 1);
    let prepared = runtime
        .prepare_pull_response(pull_batch(
            first.clone(),
            vec![PullCommit {
                sequence: 1,
                source: None,
                records: Vec::new(),
            }],
        ))
        .expect("first complete pull should prepare");
    install(&mut runtime, prepared);

    let refreshed = checkpoint("cp-2", 1, 1);
    let empty_response = pull_batch(refreshed.clone(), Vec::new());
    let prepared = runtime
        .prepare_pull_response(empty_response.clone())
        .expect("empty pull should rotate the consumed token");
    assert_eq!(prepared.state().checkpoint, Some(refreshed.clone()));
    let persisted = install(&mut runtime, prepared);
    assert_eq!(runtime.state().checkpoint, Some(refreshed.clone()));
    let revision = runtime.state().revision;

    let duplicate = runtime
        .prepare_pull_response(empty_response)
        .expect("exact duplicate remains idempotent");
    install(&mut runtime, duplicate);
    assert_eq!(runtime.state().revision, revision);

    let restarted = ClientRuntime::open(persisted).expect("rotated checkpoint survives restart");
    assert_eq!(restarted.state().checkpoint, Some(refreshed));
    let next = checkpoint("cp-3", 1, 2);
    let prepared = runtime
        .prepare_pull_response(pull_batch(
            next.clone(),
            vec![PullCommit {
                sequence: 2,
                source: None,
                records: Vec::new(),
            }],
        ))
        .expect("new commit should apply after token rotation");
    install(&mut runtime, prepared);
    assert_eq!(runtime.state().checkpoint, Some(next));
}

#[test]
fn same_cursor_new_token_cannot_reapply_commit_frames() {
    let mut runtime = ClientRuntime::open(empty_state()).expect("empty state is valid");
    let first = checkpoint("cp-1", 1, 1);
    let first_commit = PullCommit {
        sequence: 1,
        source: None,
        records: Vec::new(),
    };
    let prepared = runtime
        .prepare_pull_response(pull_batch(first.clone(), vec![first_commit.clone()]))
        .expect("first pull should apply");
    install(&mut runtime, prepared);

    let replay = pull_batch(checkpoint("cp-2", 1, 1), vec![first_commit]);
    assert!(matches!(
        runtime.prepare_pull_response(replay),
        Err(ClientError::InvalidPullFraming)
    ));
    assert_eq!(runtime.state().checkpoint, Some(first));
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
        resolution: None,
        covered_by_reset: false,
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

#[test]
fn delayed_reset_cannot_roll_back_epoch_sequence_or_authorization() {
    let mut state = empty_state();
    let mut current = checkpoint("current", 3, 8);
    current.scope.authorization_revision = 5;
    state.checkpoint = Some(current);
    state.confirmed.insert(
        record_id("a"),
        RecordState::Present {
            value: "current".into(),
            version: 2,
            reference: None,
        },
    );
    let client = ClientRuntime::open(state.clone()).unwrap();
    for (epoch, sequence, authorization, expected) in [
        (2, 8, 5, ClientError::CursorWouldMoveBackward),
        (3, 7, 5, ClientError::CursorWouldMoveBackward),
        (3, 8, 4, ClientError::ScopeMismatch),
    ] {
        let mut old = checkpoint("delayed", epoch, sequence);
        old.scope.authorization_revision = authorization;
        let result = client.prepare_pull_response(PullResponse::Reset(ResetSnapshot {
            reason: ResetReason::EpochChanged,
            checkpoint: old,
            records: vec![],
        }));
        assert_eq!(result.unwrap_err(), expected);
        assert_eq!(client.state(), &state);
    }
    // A newer authority epoch may restart its sequence; a newer authorization
    // revision may legitimately replace the visible records at the same cursor.
    for (epoch, sequence, authorization) in [(4, 0, 5), (3, 8, 6)] {
        let mut fresh = checkpoint("fresh", epoch, sequence);
        fresh.scope.authorization_revision = authorization;
        assert!(
            client
                .prepare_pull_response(PullResponse::Reset(ResetSnapshot {
                    reason: ResetReason::AuthorizationChanged,
                    checkpoint: fresh,
                    records: vec![],
                }))
                .is_ok()
        );
    }
}

#[test]
fn read_snapshot_and_batch_only_change_at_install() {
    use surrealdb_sync_client::{ReadConfirmedAbsenceV1, ReadCoverageV1, ReadReadinessV1};
    let mut runtime = ClientRuntime::open(empty_state()).unwrap();
    let before = runtime.read_snapshot();
    assert_eq!(before.metadata.coverage, ReadCoverageV1::Unknown);
    assert_eq!(
        before.row(&record_id("missing")).confirmed_absence,
        ReadConfirmedAbsenceV1::Unknown
    );
    let prepared = runtime
        .prepare_enqueue(commit(
            "two",
            vec![upsert("a", "A", None), upsert("b", "B", None)],
        ))
        .unwrap();
    let stale = prepared.clone();
    assert_eq!(runtime.read_snapshot(), before);
    // A failed persistence attempt simply drops the prepared value.
    drop(
        runtime
            .prepare_enqueue(commit("discard", vec![upsert("c", "C", None)]))
            .unwrap(),
    );
    assert_eq!(runtime.read_snapshot(), before);
    let batch = runtime.install_with_read_batch(prepared).unwrap().unwrap();
    assert_eq!(batch.before_revision, before.metadata.revision);
    assert_eq!(batch.changes.len(), 2);
    assert_eq!(batch.revision, runtime.read_snapshot().metadata.revision);
    let installed = runtime.read_snapshot();
    assert!(matches!(
        runtime.install_with_read_batch(stale),
        Err(ClientError::StaleTransition { .. })
    ));
    assert_eq!(runtime.read_snapshot(), installed);
    assert_eq!(
        installed.metadata.readiness,
        ReadReadinessV1::LocalAvailableAwaitingCoverage
    );
}

#[test]
fn authorized_reset_coverage_survives_reopen_but_checkpoint_alone_is_unknown() {
    use surrealdb_sync_client::{ReadConfirmedAbsenceV1, ReadCoverageV1, ReadReadinessV1};
    let mut runtime = ClientRuntime::open(empty_state()).unwrap();
    let prepared = runtime
        .prepare_pull_response(pull_batch(checkpoint("opaque-secret", 1, 0), vec![]))
        .unwrap();
    install(&mut runtime, prepared);
    assert_eq!(
        runtime.read_snapshot().metadata.coverage,
        ReadCoverageV1::Unknown
    );
    let prepared = runtime
        .prepare_pull_response(PullResponse::Reset(ResetSnapshot {
            reason: ResetReason::EpochChanged,
            checkpoint: checkpoint("reset-secret", 2, 0),
            records: vec![],
        }))
        .unwrap();
    let persisted = install(&mut runtime, prepared);
    let reopened = ClientRuntime::open(persisted).unwrap();
    let snapshot = reopened.read_snapshot();
    assert_eq!(
        snapshot.metadata.readiness,
        ReadReadinessV1::AuthorizedScopeAsOf
    );
    assert_eq!(
        snapshot.row(&record_id("missing")).confirmed_absence,
        ReadConfirmedAbsenceV1::AbsentWithinAuthorizedScope
    );
    assert!(!format!("{:?}", snapshot.metadata).contains("secret"));
    for invalid in 0..4 {
        let mut state = reopened.state().clone();
        match invalid {
            0 => state.checkpoint = None,
            1 => state.reset_coverage.as_mut().unwrap().cursor.epoch = 1,
            2 => state.reset_coverage.as_mut().unwrap().cursor.sequence = 1,
            3 => {
                state
                    .reset_coverage
                    .as_mut()
                    .unwrap()
                    .scope
                    .authorization_revision = 99
            }
            _ => unreachable!(),
        }
        assert!(matches!(
            ClientRuntime::open(state),
            Err(ClientError::InvalidReadCoverage)
        ));
    }
}

#[test]
fn same_cursor_fresh_token_is_metadata_only_but_cannot_reapply_frames() {
    let mut runtime = ClientRuntime::open(empty_state()).unwrap();
    let prepared = runtime
        .prepare_pull_response(pull_batch(
            checkpoint("first", 1, 1),
            vec![PullCommit {
                sequence: 1,
                source: None,
                records: vec![],
            }],
        ))
        .unwrap();
    install(&mut runtime, prepared);
    let prepared = runtime
        .prepare_pull_response(pull_batch(checkpoint("fresh", 1, 1), vec![]))
        .unwrap();
    let batch = runtime.install_with_read_batch(prepared).unwrap().unwrap();
    assert!(batch.changes.is_empty());
    let prepared = runtime
        .prepare_pull_response(pull_batch(checkpoint("fresh", 1, 1), vec![]))
        .unwrap();
    assert!(runtime.install_with_read_batch(prepared).unwrap().is_none());
    let response = pull_batch(
        checkpoint("another", 1, 1),
        vec![PullCommit {
            sequence: 1,
            source: None,
            records: vec![],
        }],
    );
    assert!(matches!(
        runtime.prepare_pull_response(response),
        Err(ClientError::InvalidPullFraming)
    ));
}

#[test]
fn auth_reset_with_pending_or_outcome_history_preserves_old_journal() {
    use surrealdb_sync_client::ScopeTransitionDecisionV1;
    let mut runtime = ClientRuntime::open(empty_state()).unwrap();
    let prepared = runtime
        .prepare_pull_response(pull_batch(checkpoint("old", 1, 0), vec![]))
        .unwrap();
    install(&mut runtime, prepared);
    let local = commit("local", vec![upsert("private", "old", None)]);
    let prepared = runtime.prepare_enqueue(local.clone()).unwrap();
    install(&mut runtime, prepared);
    let mut incoming = checkpoint("new-auth", 1, 1);
    incoming.scope.authorization_revision = 2;
    let reset = PullResponse::Reset(ResetSnapshot {
        reason: ResetReason::EpochChanged,
        checkpoint: incoming,
        records: vec![],
    });
    let before = runtime.state().clone();
    assert!(matches!(
        runtime.prepare_pull_response(reset.clone()),
        Err(ClientError::PendingQuarantineRequired)
    ));
    assert_eq!(runtime.state(), &before);
    assert_eq!(
        runtime.scope_transition_decision(&ScopeIdentity("other".into()), 1),
        ScopeTransitionDecisionV1::PendingQuarantineRequired
    );
    let prepared = runtime
        .prepare_push_response(push_response(DurableOutcome::Accepted {
            identity: local.identity,
            sequence: 1,
            id_mappings: vec![],
        }))
        .unwrap();
    install(&mut runtime, prepared);
    let before = runtime.state().clone();
    assert!(matches!(
        runtime.prepare_pull_response(reset),
        Err(ClientError::NeedsNewScopeJournal)
    ));
    assert_eq!(runtime.state(), &before);
    assert_eq!(
        runtime.scope_transition_decision(&ScopeIdentity("other".into()), 1),
        ScopeTransitionDecisionV1::NeedsNewScopeJournal
    );
}

#[test]
fn late_ack_after_reset_has_unknown_epoch_and_never_becomes_confirmed() {
    use surrealdb_sync_client::{ReadAcceptedStatusV1, ReadConfirmedAbsenceV1};
    let mut runtime = ClientRuntime::open(empty_state()).unwrap();
    let local = commit("late", vec![upsert("a", "intent", None)]);
    let prepared = runtime.prepare_enqueue(local.clone()).unwrap();
    install(&mut runtime, prepared);
    let prepared = runtime
        .prepare_pull_response(PullResponse::Reset(ResetSnapshot {
            reason: ResetReason::EpochChanged,
            checkpoint: checkpoint("new", 2, 0),
            records: vec![],
        }))
        .unwrap();
    install(&mut runtime, prepared);
    let prepared = runtime
        .prepare_push_response(push_response(DurableOutcome::Accepted {
            identity: local.identity,
            sequence: 9,
            id_mappings: vec![],
        }))
        .unwrap();
    install(&mut runtime, prepared);
    let row = runtime.read_snapshot().row(&record_id("a"));
    assert_eq!(row.confirmed, RecordState::Absent);
    assert_eq!(
        row.confirmed_absence,
        ReadConfirmedAbsenceV1::AbsentWithinAuthorizedScope
    );
    assert!(row.optimistic.is_some());
    assert_eq!(
        row.accepted[0].status,
        ReadAcceptedStatusV1::AwaitingPullEpochUnknown
    );
}

#[test]
fn covered_reset_marker_is_accepted_only_and_keeps_original_receipt() {
    use surrealdb_sync_client::ReadAcceptedStatusV1;
    let mut runtime = ClientRuntime::open(empty_state()).unwrap();
    let local = commit("accepted", vec![upsert("a", "old", None)]);
    let prepared = runtime.prepare_enqueue(local.clone()).unwrap();
    install(&mut runtime, prepared);
    let receipt = DurableOutcome::Accepted {
        identity: local.identity.clone(),
        sequence: u64::MAX,
        id_mappings: vec![],
    };
    let prepared = runtime
        .prepare_push_response(push_response(receipt.clone()))
        .unwrap();
    install(&mut runtime, prepared);
    let prepared = runtime
        .prepare_pull_response(PullResponse::Reset(ResetSnapshot {
            reason: ResetReason::EpochChanged,
            checkpoint: checkpoint("new", 2, 0),
            records: vec![],
        }))
        .unwrap();
    let durable = install(&mut runtime, prepared);
    assert_eq!(durable.outcomes[0].outcome, receipt);
    assert_eq!(durable.outcomes[0].local_commit, local);
    let reopened = ClientRuntime::open(durable.clone()).unwrap();
    let row = reopened.read_snapshot().row(&record_id("a"));
    assert!(row.optimistic.is_none());
    assert_eq!(row.accepted[0].sequence, u64::MAX);
    assert_eq!(
        row.accepted[0].status,
        ReadAcceptedStatusV1::CoveredByAuthorizedReset
    );
    let mut invalid = durable;
    invalid.outcomes[0].outcome = DurableOutcome::Rejected {
        identity: local.identity,
        reason: RejectReason::Unauthorized,
    };
    assert!(matches!(
        ClientRuntime::open(invalid),
        Err(ClientError::InvalidReadCoverage)
    ));
}

#[test]
fn outcome_only_conflict_is_visible_even_without_projection_row() {
    let local = commit(
        "delete-conflict",
        vec![Operation::Delete {
            record_id: record_id("a"),
            base_version: 1,
        }],
    );
    let mut runtime = runtime_with_conflict(local.clone(), RecordState::Absent);
    let row = runtime.read_snapshot().row(&record_id("a"));
    assert!(row.optimistic.is_none());
    assert_eq!(row.conflicts.len(), 1);
    assert!(row.conflicts[0].resolution.is_none());
    let prepared = runtime
        .prepare_resolve_conflict(
            &local.identity.client_commit_id,
            ConflictResolutionRequest::KeepServer,
        )
        .unwrap();
    let batch = runtime.install_with_read_batch(prepared).unwrap().unwrap();
    assert_eq!(batch.changes.len(), 1);
    assert_eq!(
        runtime.read_snapshot().row(&record_id("a")).conflicts[0].resolution,
        Some(ConflictResolution::KeepServer)
    );
}
