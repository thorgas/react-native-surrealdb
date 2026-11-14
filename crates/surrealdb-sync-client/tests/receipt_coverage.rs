use surrealdb_sync_client::*;
use surrealdb_sync_protocol::*;
fn context() -> ReceiptContext {
    ReceiptContext {
        authority_incarnation: "authority-1".into(),
        dataset_incarnation: "dataset-1".into(),
        partition_id: PartitionId("p".into()),
        client_id: ClientId("c".into()),
        scope: ScopeSnapshot {
            identity: ScopeIdentity("s".into()),
            authorization_revision: 1,
            subscription_revision: 1,
        },
    }
}
fn commit(id: &str, value: &str) -> ClientCommit<String> {
    ClientCommit {
        identity: CommitIdentity {
            client_commit_id: ClientCommitId(id.into()),
            fingerprint: Fingerprint(format!("fp-{id}")),
        },
        operations: vec![Operation::Upsert {
            record_id: RecordId("recipe:one".into()),
            base_version: BaseVersion::Absent,
            value: value.into(),
            reference: None,
        }],
    }
}
fn ack(c: &ClientCommit<String>, epoch: u64) -> PushResponseV2<String> {
    PushResponseV2 {
        response: PushResponse {
            schema_version: SchemaVersion::V1,
            partition_id: context().partition_id,
            client_id: context().client_id,
            outcome: DurableOutcome::Accepted {
                identity: c.identity.clone(),
                sequence: 1,
                id_mappings: vec![],
            },
        },
        receipt: Some(AcceptanceReceipt {
            context: context(),
            identity: c.identity.clone(),
            cursor: Cursor { epoch, sequence: 1 },
        }),
    }
}
fn reset(epoch: u64, seq: u64) -> PullResponseV2<String> {
    PullResponseV2 {
        context: context(),
        response: PullResponse::Reset(ResetSnapshot {
            reason: ResetReason::CheckpointExpired,
            checkpoint: Checkpoint {
                token: OpaqueCheckpoint(format!("cp-{epoch}-{seq}")),
                cursor: Cursor {
                    epoch,
                    sequence: seq,
                },
                scope: context().scope,
            },
            records: vec![SnapshotRecord {
                record_id: RecordId("recipe:one".into()),
                state: RecordState::Present {
                    value: "server-newer".into(),
                    version: 2,
                    reference: None,
                },
            }],
        }),
    }
}
fn runtime() -> ClientRuntime<String> {
    ClientRuntime::open(DurableClientState::empty(
        context().partition_id,
        context().client_id,
        context().scope.identity,
        1,
    ))
    .unwrap()
}
fn restart(r: ClientRuntime<String>) -> ClientRuntime<String> {
    ClientRuntime::open(r.state().clone()).unwrap()
}
#[test]
fn ack_pull_and_pull_before_ack_survive_every_install_and_do_not_overwrite_later_edits() {
    for pull_first in [true, false] {
        let c = commit("one", "old-intent");
        let mut r = runtime();
        r.install(r.prepare_enqueue(c.clone()).unwrap()).unwrap();
        r = restart(r);
        if pull_first {
            r.install(r.prepare_pull_response_v2(reset(1, 2)).unwrap())
                .unwrap();
            r = restart(r);
        }
        r.install(r.prepare_push_response_v2(ack(&c, 1)).unwrap())
            .unwrap();
        r = restart(r);
        if !pull_first {
            r.install(r.prepare_pull_response_v2(reset(1, 2)).unwrap())
                .unwrap();
            r = restart(r);
        }
        assert!(covered(r.state(), &c.identity));
        assert_eq!(r.read_snapshot_v2().protocol_version, 2);
        assert_eq!(
            r.read_snapshot_v2().rows[&RecordId("recipe:one".into())].accepted[0].status,
            ReadAcceptedStatusV1::CoveredByVerifiedPull
        );
        assert_ne!(
            r.read_snapshot().rows[&RecordId("recipe:one".into())].accepted[0].status,
            ReadAcceptedStatusV1::CoveredByVerifiedPull
        );
        let next = commit("two", "new-offline");
        r.install(r.prepare_enqueue(next).unwrap()).unwrap();
        assert_eq!(r.state().outbox.len(), 1);
        assert!(
            optimistic_operations(r.state())
                .all(|op| matches!(op, Operation::Upsert { value, .. } if value == "new-offline"))
        );
        let revision = r.state().revision;
        r.install(r.prepare_push_response_v2(ack(&c, 1)).unwrap())
            .unwrap();
        assert_eq!(revision, r.state().revision);
    }
}
#[test]
fn late_ack_never_borrows_epoch_scope_or_incarnation_from_a_higher_pull() {
    let c = commit("one", "intent");
    for epoch in [2, 3] {
        let mut r = runtime();
        r.install(r.prepare_enqueue(c.clone()).unwrap()).unwrap();
        r.install(r.prepare_pull_response_v2(reset(epoch, 100)).unwrap())
            .unwrap();
        r.install(r.prepare_push_response_v2(ack(&c, 1)).unwrap())
            .unwrap();
        assert!(!covered(r.state(), &c.identity));
    }
    for field in [
        "authority",
        "dataset",
        "scope",
        "client",
        "partition",
        "auth",
        "sub",
        "fingerprint",
    ] {
        let mut r = runtime();
        r.install(r.prepare_enqueue(c.clone()).unwrap()).unwrap();
        r.install(r.prepare_pull_response_v2(reset(1, 2)).unwrap())
            .unwrap();
        let mut a = ack(&c, 1);
        let receipt = a.receipt.as_mut().unwrap();
        match field {
            "authority" => receipt.context.authority_incarnation = "other".into(),
            "dataset" => receipt.context.dataset_incarnation = "other".into(),
            "scope" => receipt.context.scope.identity.0 = "other".into(),
            "client" => receipt.context.client_id.0 = "other".into(),
            "partition" => receipt.context.partition_id.0 = "other".into(),
            "auth" => receipt.context.scope.authorization_revision += 1,
            "sub" => receipt.context.scope.subscription_revision += 1,
            _ => receipt.identity.fingerprint.0 = "other".into(),
        }
        if let Ok(p) = r.prepare_push_response_v2(a) {
            r.install(p).unwrap();
            assert!(!covered(r.state(), &c.identity));
        }
    }
}
#[test]
fn incomplete_batch_and_legacy_ack_cannot_prove_coverage() {
    let c = commit("one", "intent");
    let mut r = runtime();
    r.install(r.prepare_enqueue(c.clone()).unwrap()).unwrap();
    let a = ack(&c, 1);
    r.install(r.prepare_push_response(a.response).unwrap())
        .unwrap();
    let mut p = reset(1, 2);
    p.response = PullResponse::Batch(PullBatch { frames: vec![] });
    assert!(r.prepare_pull_response_v2(p).is_err());
    r.install(r.prepare_pull_response_v2(reset(1, 2)).unwrap())
        .unwrap();
    assert!(!covered(r.state(), &c.identity));
}
#[test]
fn ordinary_complete_pull_installs_proof_atomically_and_rejects_partial_or_reordered_frames() {
    let c = commit("one", "intent");
    let mut r = runtime();
    r.install(r.prepare_pull_response_v2(reset(1, 0)).unwrap())
        .unwrap();
    r.install(r.prepare_enqueue(c.clone()).unwrap()).unwrap();
    r.install(r.prepare_push_response_v2(ack(&c, 1)).unwrap())
        .unwrap();
    let cp = Checkpoint {
        token: OpaqueCheckpoint("next".into()),
        cursor: Cursor {
            epoch: 1,
            sequence: 1,
        },
        scope: context().scope,
    };
    let frames = vec![
        PullFrame::Start {
            checkpoint: cp.clone(),
        },
        PullFrame::Commit {
            checkpoint: cp.token.clone(),
            commit: PullCommit {
                sequence: 1,
                source: None,
                records: vec![AppliedRecord {
                    record_id: RecordId("recipe:one".into()),
                    state: RecordState::Present {
                        value: "pulled".into(),
                        version: 2,
                        reference: None,
                    },
                }],
            },
        },
        PullFrame::End { checkpoint: cp },
    ];
    for bad in [
        frames[..2].to_vec(),
        vec![frames[1].clone(), frames[0].clone(), frames[2].clone()],
    ] {
        assert!(
            r.prepare_pull_response_v2(PullResponseV2 {
                context: context(),
                response: PullResponse::Batch(PullBatch { frames: bad })
            })
            .is_err()
        );
        assert!(!covered(r.state(), &c.identity));
    }
    let transition = r
        .prepare_pull_response_v2(PullResponseV2 {
            context: context(),
            response: PullResponse::Batch(PullBatch { frames }),
        })
        .unwrap();
    assert!(!covered(r.state(), &c.identity));
    let recovered = ClientRuntime::open(transition.state().clone()).unwrap();
    assert!(covered(recovered.state(), &c.identity));
    r.install(transition).unwrap();
    assert!(covered(r.state(), &c.identity));
}
fn fingerprint(
    _: &DurableClientState<String>,
    c: &ClientCommit<String>,
) -> Result<Fingerprint, ClientError> {
    Ok(Fingerprint(format!("fp-{:?}", c.operations)))
}
#[test]
fn two_and_three_offline_edits_coalesce_durably_before_transmission() {
    let mut r = runtime();
    for (id, value) in [("one", "first"), ("two", "second"), ("three", "third")] {
        let p = r
            .prepare_enqueue_latest(commit(id, value), fingerprint)
            .unwrap();
        r.install(p).unwrap();
        r = restart(r);
        assert_eq!(r.state().outbox.len(), 1);
        assert!(r.state().command_queue.deferred.is_empty());
        assert_eq!(r.state().outbox[0].identity.client_commit_id.0, "one");
        assert!(
            matches!(&r.state().outbox[0].operations[0], Operation::Upsert { value: v, .. } if v == value)
        );
    }
}
#[test]
fn lost_ack_preserves_fifo_successors_and_rebases_only_after_verified_coverage() {
    let mut r = runtime();
    let c = commit("one", "first");
    r.install(r.prepare_enqueue_latest(c, fingerprint).unwrap())
        .unwrap();
    let transmitted = r.state().outbox[0].clone();
    r.install(r.prepare_mark_transmitted(&transmitted.identity).unwrap())
        .unwrap();
    r = restart(r);
    for (id, v) in [("two", "second"), ("three", "third")] {
        r.install(
            r.prepare_enqueue_latest(commit(id, v), fingerprint)
                .unwrap(),
        )
        .unwrap();
        r = restart(r);
    }
    assert_eq!(r.state().command_queue.deferred.len(), 2);
    assert!(
        matches!(optimistic_operations(r.state()).last().unwrap(), Operation::Upsert { value, .. } if value == "third")
    );
    // Lost ACK: pull first cannot authorize changing the transmitted predecessor bytes.
    r.install(r.prepare_pull_response_v2(reset(1, 2)).unwrap())
        .unwrap();
    let p = r.prepare_admit_successors(fingerprint).unwrap();
    r.install(p).unwrap();
    assert_eq!(r.state().outbox[0], transmitted);
    let p = r.prepare_push_response_v2(ack(&transmitted, 1)).unwrap();
    let p = r.prepare_with_successors(p, fingerprint).unwrap();
    r.install(p).unwrap();
    r = restart(r);
    assert_eq!(r.state().outbox[0].identity.client_commit_id.0, "two");
    assert_eq!(
        r.state().command_queue.deferred[0]
            .identity
            .client_commit_id
            .0,
        "three"
    );
    assert!(matches!(
        &r.state().outbox[0].operations[0],
        Operation::Upsert {
            base_version: BaseVersion::Exact(2),
            ..
        }
    ));
    let old = r.state().clone();
    r.install(r.prepare_push_response_v2(ack(&transmitted, 1)).unwrap())
        .unwrap();
    assert_eq!(r.state(), &old);
}
#[test]
fn predecessor_conflict_surfaces_without_discarding_or_sending_successor() {
    let mut r = runtime();
    let c = commit("one", "first");
    r.install(r.prepare_enqueue_latest(c, fingerprint).unwrap())
        .unwrap();
    let c = r.state().outbox[0].clone();
    r.install(r.prepare_mark_transmitted(&c.identity).unwrap())
        .unwrap();
    r.install(
        r.prepare_enqueue_latest(commit("two", "latest"), fingerprint)
            .unwrap(),
    )
    .unwrap();
    let response = PushResponse {
        schema_version: SchemaVersion::V1,
        partition_id: context().partition_id,
        client_id: context().client_id,
        outcome: DurableOutcome::Conflict {
            identity: c.identity,
            record_id: RecordId("recipe:one".into()),
            authoritative: RecordState::Present {
                value: "external".into(),
                version: 4,
                reference: None,
            },
        },
    };
    let p = r.prepare_push_response(response).unwrap();
    let p = r.prepare_with_successors(p, fingerprint).unwrap();
    r.install(p).unwrap();
    r = restart(r);
    assert!(r.state().outbox.is_empty());
    assert_eq!(r.state().command_queue.deferred.len(), 1);
    let row = &r.read_snapshot_v2().rows[&RecordId("recipe:one".into())];
    assert_eq!(row.conflicts.len(), 1);
    assert_eq!(row.pending.len(), 1);
    assert!(
        matches!(row.optimistic.as_ref().unwrap(), OptimisticRecord::Present { value, .. } if value == "latest")
    );
}
