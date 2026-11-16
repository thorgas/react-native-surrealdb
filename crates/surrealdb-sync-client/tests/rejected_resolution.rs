//! Crash boundaries are before persistence, after persistence/before install, and after install.
use surrealdb_sync_client::*;
use surrealdb_sync_protocol::*;

fn id(i: usize) -> RecordId {
    RecordId(format!("item:{i}"))
}
fn commit(name: &str, records: &[usize]) -> ClientCommit<String> {
    ClientCommit {
        identity: CommitIdentity {
            client_commit_id: ClientCommitId(name.into()),
            fingerprint: Fingerprint(name.into()),
        },
        operations: records
            .iter()
            .map(|i| Operation::Upsert {
                record_id: id(*i),
                base_version: BaseVersion::Exact(1),
                value: name.into(),
                reference: None,
            })
            .collect(),
    }
}
fn runtime() -> ClientRuntime<String> {
    let mut state = DurableClientState::empty(
        PartitionId("p".into()),
        ClientId("c".into()),
        ScopeIdentity("s".into()),
        1,
    );
    for i in 0..4 {
        state.confirmed.insert(
            id(i),
            RecordState::Present {
                value: "authority".into(),
                version: 7,
                reference: None,
            },
        );
    }
    ClientRuntime::open(state).unwrap()
}
fn fingerprint(
    _: &DurableClientState<String>,
    c: &ClientCommit<String>,
) -> Result<Fingerprint, ClientError> {
    Ok(Fingerprint(format!("rebased:{:?}", c.operations)))
}
fn apply(r: &mut ClientRuntime<String>, t: PreparedTransition<String>) {
    // Crash before persistence leaves the exact old journal recoverable.
    assert_eq!(
        ClientRuntime::open(r.state().clone()).unwrap().state(),
        r.state()
    );
    // Crash after persistence but before install recovers the complete new journal.
    let persisted = ClientRuntime::open(t.state().clone()).unwrap();
    r.install(t).unwrap();
    assert_eq!(r.state(), persisted.state());
    *r = persisted;
}
fn enqueue(r: &mut ClientRuntime<String>, c: ClientCommit<String>) {
    let t = r.prepare_enqueue_latest(c, fingerprint).unwrap();
    apply(r, t);
}
fn reject(r: &mut ClientRuntime<String>, c: &ClientCommit<String>, v2: bool) {
    let response = PushResponse {
        schema_version: SchemaVersion::V1,
        partition_id: PartitionId("p".into()),
        client_id: ClientId("c".into()),
        outcome: DurableOutcome::Rejected {
            identity: c.identity.clone(),
            reason: RejectReason::Unauthorized,
        },
    };
    let t = if v2 {
        r.prepare_push_response_v2(PushResponseV2 {
            response,
            receipt: None,
        })
    } else {
        r.prepare_push_response(response)
    }
    .unwrap();
    let t = r.prepare_with_successors(t, fingerprint).unwrap();
    apply(r, t);
}
fn resolve(r: &ClientRuntime<String>, c: &ClientCommit<String>) -> PreparedTransition<String> {
    r.prepare_with_successors(
        r.prepare_resolve_rejected_keep_server(&c.identity.client_commit_id)
            .unwrap(),
        fingerprint,
    )
    .unwrap()
}
fn pull(r: &ClientRuntime<String>, version: u64) -> PreparedTransition<String> {
    r.prepare_pull_response(PullResponse::Reset(ResetSnapshot {
        reason: ResetReason::CheckpointExpired,
        checkpoint: Checkpoint {
            token: OpaqueCheckpoint(format!("cp{version}")),
            cursor: Cursor {
                epoch: 1,
                sequence: version,
            },
            scope: ScopeSnapshot {
                identity: ScopeIdentity("s".into()),
                authorization_revision: 1,
                subscription_revision: 1,
            },
        },
        records: (0..4)
            .map(|i| SnapshotRecord {
                record_id: id(i),
                state: RecordState::Present {
                    value: "new-authority".into(),
                    version,
                    reference: None,
                },
            })
            .collect(),
    }))
    .unwrap()
}
#[test]
fn rejected_legacy_journal_closes_idempotently_and_rolls_back_without_successors() {
    for v2 in [false, true] {
        let mut r = runtime();
        enqueue(&mut r, commit("refused", &[0]));
        let c = r.state().outbox[0].clone();
        reject(&mut r, &c, v2);
        assert!(r.state().outcomes[0].resolution.is_none()); // exact pre-upgrade shape
        r = ClientRuntime::open(r.state().clone()).unwrap();
        let t = resolve(&r, &c);
        apply(&mut r, t);
        assert!(matches!(
            r.state().outcomes[0].resolution,
            Some(ConflictResolution::KeepServer)
        ));
        assert_eq!(r.state().outcomes[0].local_commit, c);
        assert!(
            matches!(&r.optimistic()[&id(0)], OptimisticRecord::Present { value, .. } if value == "authority")
        );
        let before = r.state().clone();
        let t = resolve(&r, &c);
        apply(&mut r, t);
        assert_eq!(r.state(), &before);
        reject(&mut r, &c, v2);
        assert_eq!(r.state(), &before); // replay cannot reopen
    }
}
#[test]
fn aggregate_resolution_promotes_whole_fifo_and_successor_rejection_blocks_again() {
    for v2 in [false, true] {
        let mut r = runtime();
        enqueue(&mut r, commit("first", &[0, 1]));
        let first = r.state().outbox[0].clone();
        let t = r.prepare_mark_transmitted(&first.identity).unwrap();
        apply(&mut r, t);
        enqueue(&mut r, commit("second", &[0, 1]));
        enqueue(&mut r, commit("third", &[1, 2]));
        let deferred = r.state().command_queue.deferred.clone();
        reject(&mut r, &first, v2);
        assert!(r.state().outbox.is_empty());
        assert_eq!(r.state().command_queue.deferred, deferred);
        let t = resolve(&r, &first);
        apply(&mut r, t);
        assert_eq!(r.state().outbox.len(), 1);
        let second = r.state().outbox[0].clone();
        assert_eq!(second.identity.client_commit_id.0, "second");
        for op in &second.operations {
            assert!(matches!(
                op,
                Operation::Upsert {
                    base_version: BaseVersion::Exact(7),
                    ..
                }
            ));
        }
        assert_ne!(
            second.identity.fingerprint,
            deferred[0].identity.fingerprint
        );
        assert_eq!(r.state().command_queue.deferred, deferred[1..]);
        reject(&mut r, &second, v2);
        let t = resolve(&r, &first);
        apply(&mut r, t);
        assert!(r.state().outbox.is_empty());
        let t = resolve(&r, &second);
        apply(&mut r, t);
        assert_eq!(r.state().outbox[0].identity.client_commit_id.0, "third");
        assert!(r.state().command_queue.deferred.is_empty());
    }
}
#[test]
fn bounded_chains_keep_all_64_successors_and_promote_only_one_per_record() {
    let mut r = runtime();
    let mut firsts = vec![];
    for i in 0..4 {
        enqueue(&mut r, commit(&format!("first{i}"), &[i]));
        let first = r
            .state()
            .outbox
            .iter()
            .find(|c| c.identity.client_commit_id.0 == format!("first{i}"))
            .unwrap()
            .clone();
        let t = r.prepare_mark_transmitted(&first.identity).unwrap();
        apply(&mut r, t);
        for n in 0..16 {
            enqueue(&mut r, commit(&format!("next{i}-{n}"), &[i]));
        }
        firsts.push(first);
    }
    assert_eq!(r.state().command_queue.deferred.len(), 64);
    let before = r.state().clone();
    assert!(
        r.prepare_enqueue_latest(commit("overflow", &[0]), fingerprint)
            .is_err()
    );
    assert_eq!(r.state(), &before);
    for c in &firsts {
        reject(&mut r, c, false);
        let t = resolve(&r, c);
        apply(&mut r, t);
    }
    assert_eq!(r.state().outbox.len(), 4);
    assert_eq!(r.state().command_queue.deferred.len(), 60);
    for i in 0..4 {
        assert!(
            r.state()
                .outbox
                .iter()
                .any(|c| c.identity.client_commit_id.0 == format!("next{i}-0"))
        );
    }
    for i in 0..4 {
        for n in 0..16 {
            let active = r
                .state()
                .outbox
                .iter()
                .find(|c| c.identity.client_commit_id.0 == format!("next{i}-{n}"))
                .unwrap()
                .clone();
            reject(&mut r, &active, n % 2 == 0);
            let t = resolve(&r, &active);
            apply(&mut r, t);
        }
    }
    assert!(r.state().outbox.is_empty());
    assert!(r.state().command_queue.deferred.is_empty());
    assert_eq!(r.state().outcomes.len(), 68);
}
#[test]
fn pull_race_never_installs_a_stale_prepared_resolution() {
    for pull_first in [true, false] {
        let mut r = runtime();
        enqueue(&mut r, commit("first", &[0]));
        let first = r.state().outbox[0].clone();
        let t = r.prepare_mark_transmitted(&first.identity).unwrap();
        apply(&mut r, t);
        enqueue(&mut r, commit("second", &[0]));
        reject(&mut r, &first, true);
        let stale_resolution = resolve(&r, &first);
        let stale_pull = pull(&r, 9);
        if pull_first {
            apply(&mut r, stale_pull);
            assert!(r.install(stale_resolution).is_err());
            let t = resolve(&r, &first);
            apply(&mut r, t);
            assert!(matches!(
                r.state().outbox[0].operations[0],
                Operation::Upsert {
                    base_version: BaseVersion::Exact(9),
                    ..
                }
            ));
        } else {
            apply(&mut r, stale_resolution);
            let sent = r.state().outbox[0].clone();
            assert!(r.install(stale_pull).is_err());
            let t = pull(&r, 9);
            apply(&mut r, t);
            assert_eq!(r.state().outbox[0], sent); // never rewrite a promoted identity
        }
    }
}
#[test]
fn failed_fingerprint_and_invalid_resolution_cannot_partially_close_a_rejection() {
    let mut r = runtime();
    enqueue(&mut r, commit("first", &[0]));
    let c = r.state().outbox[0].clone();
    assert!(
        r.prepare_resolve_rejected_keep_server(&c.identity.client_commit_id)
            .is_err()
    );
    let t = r.prepare_mark_transmitted(&c.identity).unwrap();
    apply(&mut r, t);
    enqueue(&mut r, commit("second", &[0]));
    reject(&mut r, &c, false);
    let before = r.state().clone();
    let closed = r
        .prepare_resolve_rejected_keep_server(&c.identity.client_commit_id)
        .unwrap();
    assert!(
        r.prepare_with_successors(closed, |_, _| Err(ClientError::InvalidReadCoverage))
            .is_err()
    );
    assert_eq!(r.state(), &before);
    assert!(
        r.prepare_resolve_conflict(
            &c.identity.client_commit_id,
            ConflictResolutionRequest::KeepServer
        )
        .is_err()
    );
    for invalid in [
        ConflictResolution::PreferServerPolicyV1,
        ConflictResolution::KeepLocal {
            replacement: c.identity.clone(),
        },
        ConflictResolution::Merge {
            replacement: c.identity.clone(),
        },
    ] {
        let mut state = before.clone();
        state.outcomes[0].resolution = Some(invalid);
        assert!(ClientRuntime::open(state).is_err());
    }
}
