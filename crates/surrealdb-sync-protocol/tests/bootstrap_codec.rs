use ciborium::value::Value as Cbor;
use std::collections::BTreeMap;
use surrealdb_sync_protocol::{bootstrap::*, bootstrap_codec::*, *};

fn record(id: &str, value: CanonicalValue) -> SnapshotRecord<CanonicalValue> {
    SnapshotRecord {
        record_id: RecordId(id.into()),
        state: RecordState::Present {
            value,
            version: 1,
            reference: None,
        },
    }
}
fn start() -> BootstrapStartRequest {
    BootstrapStartRequest {
        partition_id: PartitionId("h".into()),
        client_id: ClientId("c".into()),
        request_id: "r".into(),
        requested_scope: ScopeIdentity("s".into()),
        subscription_revision: 1,
        journal_generation: "g".into(),
    }
}
fn manifest_for(pages: &mut [BootstrapPage]) -> BootstrapManifest {
    let request = start();
    let mut content = BootstrapContentHasher::new("z".into()).unwrap();
    for page in pages.iter() {
        content.push(page).unwrap();
    }
    let s = content.summary();
    let manifest = BootstrapManifest {
        session_id: "z".into(),
        start_request_id: "r".into(),
        start_fingerprint: bootstrap_start_fingerprint(&request).unwrap(),
        context: BootstrapContext {
            authority_id: "A".into(),
            principal_id: "P".into(),
            partition_id: request.partition_id,
            client_id: request.client_id,
            journal_generation: request.journal_generation,
            scope: ScopeSnapshot {
                identity: request.requested_scope,
                authorization_revision: 1,
                subscription_revision: 1,
            },
        },
        cursor: Cursor {
            epoch: 1,
            sequence: 2,
        },
        limits: BootstrapLimits::default(),
        page_count: s.page_count,
        record_count: s.record_count,
        content_bytes: s.content_bytes,
        root: s.root,
        expires_at: 99,
    };
    let digest = bootstrap_manifest_digest(&manifest).unwrap();
    for page in pages {
        page.manifest_digest = digest;
    }
    manifest
}
fn page(index: u64, records: Vec<SnapshotRecord<CanonicalValue>>) -> BootstrapPage {
    BootstrapPage {
        session_id: "z".into(),
        manifest_digest: BootstrapDigest([0; 32]),
        index,
        records,
    }
}
fn fixture() -> (
    BootstrapStartRequest,
    BootstrapManifest,
    BootstrapPage,
    BootstrapPageRequest,
    BootstrapCompleteRequest,
    BootstrapReceipt,
) {
    let mut pages = [page(0, vec![record("a", CanonicalValue::Int(7))])];
    let manifest = manifest_for(&mut pages);
    let hash = bootstrap_manifest_digest(&manifest).unwrap();
    let request = BootstrapPageRequest {
        partition_id: PartitionId("h".into()),
        client_id: ClientId("c".into()),
        session_id: "z".into(),
        index: 0,
    };
    let complete = BootstrapCompleteRequest {
        partition_id: request.partition_id.clone(),
        client_id: request.client_id.clone(),
        session_id: "z".into(),
        manifest_digest: hash,
    };
    let receipt = BootstrapReceipt {
        session_id: "z".into(),
        manifest_digest: hash,
        checkpoint: Checkpoint {
            token: OpaqueCheckpoint("k".into()),
            cursor: manifest.cursor,
            scope: manifest.context.scope.clone(),
        },
        expires_at: 99,
    };
    (
        start(),
        manifest,
        pages[0].clone(),
        request,
        complete,
        receipt,
    )
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn raw(value: Cbor) -> Vec<u8> {
    let mut bytes = vec![];
    ciborium::ser::into_writer(&value, &mut bytes).unwrap();
    bytes
}
fn parsed(bytes: &[u8]) -> Cbor {
    ciborium::de::from_reader(bytes).unwrap()
}
fn with_envelope(bytes: &[u8], change: impl FnOnce(&mut Vec<Cbor>)) -> Vec<u8> {
    let Cbor::Array(mut values) = parsed(bytes) else {
        panic!()
    };
    change(&mut values);
    raw(Cbor::Array(values))
}

#[test]
fn fixed_six_message_goldens_and_domain_hashes() {
    let (s, m, p, q, c, r) = fixture();
    let bytes = [
        encode_bootstrap_start(&s).unwrap(),
        encode_bootstrap_manifest(&m).unwrap(),
        encode_bootstrap_page_request(&q).unwrap(),
        encode_bootstrap_page(&p).unwrap(),
        encode_bootstrap_complete(&c).unwrap(),
        encode_bootstrap_receipt(&r).unwrap(),
    ];
    let expected = GOLDENS;
    for (actual, expected) in bytes.iter().zip(expected) {
        assert_eq!(hex(actual), expected);
    }
    assert_eq!(decode_bootstrap_start(&bytes[0]).unwrap(), s);
    assert_eq!(decode_bootstrap_manifest(&bytes[1]).unwrap(), m);
    assert_eq!(decode_bootstrap_page_request(&bytes[2]).unwrap(), q);
    assert_eq!(decode_bootstrap_page(&bytes[3]).unwrap(), p);
    assert_eq!(decode_bootstrap_complete(&bytes[4]).unwrap(), c);
    assert_eq!(decode_bootstrap_receipt(&bytes[5]).unwrap(), r);
    assert_eq!(
        hex(&bootstrap_start_fingerprint(&s).unwrap().0),
        "9930c35bbb387db1cad0eceb2969a01053869bb21f67671d9b1827cda664a7bc"
    );
    assert_eq!(
        hex(&bootstrap_page_content_digest(&p).unwrap().0),
        "77fa55a0a2a6b8bece022e09c4089dfc2947dbdd2f5fbaaab9b0f04973a8e85d"
    );
    assert_eq!(
        hex(&m.root.0),
        "ba7101cc689f328eaa9b25984c70e78685c69d8097a42552096c2e38f2b8f0ad"
    );
    assert_eq!(
        hex(&bootstrap_manifest_digest(&m).unwrap().0),
        "960e7d1d2496949fd0510056508552a7b71935311e97d0a0db9bbd6f2e116fe1"
    );
    assert_eq!(m.content_bytes, 11);
    validate_bootstrap_manifest_context(&m, &s, &m.context).unwrap();
    validate_bootstrap_receipt(&m, &r).unwrap();
}
#[test]
fn full_u64_fields_and_existing_canonical_values_roundtrip() {
    let values = vec![
        CanonicalValue::None,
        CanonicalValue::Null,
        CanonicalValue::Bool(true),
        CanonicalValue::Int(i64::MIN),
        CanonicalValue::Float(CanonicalFloat::new(-0.0).unwrap()),
        CanonicalValue::Float(CanonicalFloat::new(1.25).unwrap()),
        CanonicalValue::String("ä🥣".into()),
        CanonicalValue::Bytes(vec![0, 255]),
        CanonicalValue::Array(vec![CanonicalValue::Null]),
        CanonicalValue::Object(BTreeMap::from([
            ("é".into(), CanonicalValue::Int(7)),
            ("a".into(), CanonicalValue::None),
        ])),
        CanonicalValue::RecordId {
            table: "t".into(),
            key: Box::new(CanonicalValue::String("x".into())),
        },
    ];
    let mut rows = values
        .into_iter()
        .enumerate()
        .map(|(i, v)| record(&format!("r{i:03}"), v))
        .collect::<Vec<_>>();
    rows.push(SnapshotRecord {
        record_id: RecordId("z".into()),
        state: RecordState::Tombstone { version: u64::MAX },
    });
    if let RecordState::Present {
        version, reference, ..
    } = &mut rows[0].state
    {
        *version = u64::MAX;
        *reference = Some(RecordId("r001".into()));
    }
    let mut pages = [page(0, rows)];
    let mut manifest = manifest_for(&mut pages);
    let mut request = start();
    request.subscription_revision = u64::MAX;
    manifest.context.scope.subscription_revision = u64::MAX;
    manifest.context.scope.authorization_revision = u64::MAX;
    manifest.start_fingerprint = bootstrap_start_fingerprint(&request).unwrap();
    manifest.cursor = Cursor {
        epoch: u64::MAX,
        sequence: u64::MAX,
    };
    manifest.expires_at = u64::MAX;
    pages[0].manifest_digest = bootstrap_manifest_digest(&manifest).unwrap();
    assert_eq!(
        decode_bootstrap_start(&encode_bootstrap_start(&request).unwrap()).unwrap(),
        request
    );
    assert_eq!(
        decode_bootstrap_manifest(&encode_bootstrap_manifest(&manifest).unwrap()).unwrap(),
        manifest
    );
    assert_eq!(
        decode_bootstrap_page(&encode_bootstrap_page(&pages[0]).unwrap()).unwrap(),
        pages[0]
    );
    let receipt = BootstrapReceipt {
        session_id: manifest.session_id.clone(),
        manifest_digest: bootstrap_manifest_digest(&manifest).unwrap(),
        checkpoint: Checkpoint {
            token: OpaqueCheckpoint("k".into()),
            cursor: manifest.cursor,
            scope: manifest.context.scope.clone(),
        },
        expires_at: u64::MAX,
    };
    assert_eq!(
        decode_bootstrap_receipt(&encode_bootstrap_receipt(&receipt).unwrap()).unwrap(),
        receipt
    );
    let mut verifier = BootstrapVerifier::new(manifest).unwrap();
    verifier.push(&pages[0]).unwrap();
    verifier.finish().unwrap();
}
#[test]
fn empty_snapshot_root_has_fixed_golden_and_requires_exact_zero_counts() {
    let empty = BootstrapContentHasher::new("z".into()).unwrap().summary();
    assert_eq!(
        hex(&empty.root.0),
        "6fe78b05363a986225f66df2bd9ff735711df25aec702aa78cc7bb3e39d24c8b"
    );
    let manifest = manifest_for(&mut []);
    assert_eq!(
        BootstrapVerifier::new(manifest.clone())
            .unwrap()
            .finish()
            .unwrap(),
        empty
    );
    let mut bad = manifest.clone();
    bad.root.0[0] ^= 1;
    assert!(BootstrapVerifier::new(bad).unwrap().finish().is_err());
    for field in ["records", "bytes"] {
        let mut bad = manifest.clone();
        if field == "records" {
            bad.record_count = 1;
        } else {
            bad.content_bytes = 1;
        }
        assert!(encode_bootstrap_manifest(&bad).is_err());
    }
    assert!(encode_bootstrap_page(&page(0, vec![])).is_err());
}
#[test]
fn context_start_and_receipt_binding_are_explicit_not_authorization() {
    let (s, m, _, _, _, r) = fixture();
    for field in 0..8 {
        let mut different = m.context.clone();
        match field {
            0 => different.authority_id.push('x'),
            1 => different.principal_id.push('x'),
            2 => different.partition_id.0.push('x'),
            3 => different.client_id.0.push('x'),
            4 => different.journal_generation.push('x'),
            5 => different.scope.identity.0.push('x'),
            6 => different.scope.authorization_revision += 1,
            _ => different.scope.subscription_revision += 1,
        };
        assert!(validate_bootstrap_manifest_context(&m, &s, &different).is_err());
        let mut altered = m.clone();
        altered.context = different;
        assert_ne!(
            bootstrap_manifest_digest(&altered).unwrap(),
            bootstrap_manifest_digest(&m).unwrap()
        );
    }
    let mut wrong_start = s.clone();
    wrong_start.request_id.push('x');
    assert!(validate_bootstrap_manifest_context(&m, &wrong_start, &m.context).is_err());
    for field in 0..6 {
        let mut wrong = r.clone();
        match field {
            0 => wrong.session_id.push('x'),
            1 => wrong.manifest_digest.0[0] ^= 1,
            2 => wrong.checkpoint.cursor.sequence += 1,
            3 => wrong.checkpoint.scope.authorization_revision += 1,
            4 => wrong.expires_at += 1,
            _ => wrong.checkpoint.scope.identity.0.push('x'),
        };
        assert!(validate_bootstrap_receipt(&m, &wrong).is_err());
    }
}
#[test]
fn header_hash_has_no_cycle_and_forged_self_consistent_page_only_fails_final_root() {
    let (_, manifest, honest, _, _, _) = fixture();
    let mut forged = honest.clone();
    if let RecordState::Present { value, .. } = &mut forged.records[0].state {
        *value = CanonicalValue::Int(8);
    }
    assert_ne!(
        bootstrap_page_content_digest(&honest).unwrap(),
        bootstrap_page_content_digest(&forged).unwrap()
    );
    let mut verifier = BootstrapVerifier::new(manifest).unwrap();
    verifier.push(&forged).unwrap();
    assert!(verifier.finish().is_err());
    let mut other_header = honest.clone();
    other_header.manifest_digest.0[0] ^= 1;
    assert_eq!(
        bootstrap_page_content_digest(&honest).unwrap(),
        bootstrap_page_content_digest(&other_header).unwrap()
    );
    let mut content = BootstrapContentHasher::new("z".into()).unwrap();
    content.push(&other_header).unwrap();
    assert_eq!(content.summary().root, root_for_page(&honest));
}
fn root_for_page(page: &BootstrapPage) -> BootstrapDigest {
    let mut hasher = BootstrapContentHasher::new(page.session_id.clone()).unwrap();
    hasher.push(page).unwrap();
    hasher.summary().root
}

#[test]
fn two_individually_bounded_pages_verify_more_than_one_body_item_budget() {
    let mut pages = (0..2)
        .map(|i| {
            page(
                i,
                (0..300)
                    .map(|j| record(&format!("r{:04}", i * 300 + j), CanonicalValue::Int(7)))
                    .collect(),
            )
        })
        .collect::<Vec<_>>();
    let manifest = manifest_for(&mut pages);
    let mut verifier = BootstrapVerifier::new(manifest.clone()).unwrap();
    for p in &pages {
        let bytes = encode_bootstrap_page(p).unwrap();
        assert_eq!(&decode_bootstrap_page(&bytes).unwrap(), p);
        verifier.push(p).unwrap();
    }
    assert_eq!(verifier.finish().unwrap().record_count, 600);
    let reset = PullResponse::Reset(ResetSnapshot {
        reason: ResetReason::EpochChanged,
        checkpoint: Checkpoint {
            token: OpaqueCheckpoint("k".into()),
            cursor: manifest.cursor,
            scope: manifest.context.scope,
        },
        records: pages.into_iter().flat_map(|p| p.records).collect(),
    });
    assert_eq!(
        encode_pull_response(&reset),
        Err(CodecError::ContainerLimit)
    );
}
#[test]
fn duplicate_reordered_cross_session_and_bad_counter_pages_leave_stream_unchanged() {
    let mut pages = vec![
        page(0, vec![record("a", CanonicalValue::Int(7))]),
        page(1, vec![record("b", CanonicalValue::Int(7))]),
    ];
    let manifest = manifest_for(&mut pages);
    let mut v = BootstrapVerifier::new(manifest.clone()).unwrap();
    let initial = v.summary();
    assert!(v.push(&pages[1]).is_err());
    assert_eq!(v.summary(), initial);
    v.push(&pages[0]).unwrap();
    let before = v.summary();
    let mut cases = vec![pages[0].clone()];
    let mut wrong = pages[1].clone();
    wrong.session_id = "foreign".into();
    cases.push(wrong);
    let mut wrong = pages[1].clone();
    wrong.manifest_digest.0[0] ^= 1;
    cases.push(wrong);
    let mut wrong = pages[1].clone();
    wrong.records[0].record_id = RecordId("a".into());
    cases.push(wrong);
    let mut wrong = pages[1].clone();
    wrong.index = u64::MAX;
    cases.push(wrong);
    let mut wrong = pages[1].clone();
    wrong.records[0].state = RecordState::Absent;
    cases.push(wrong);
    let mut wrong = pages[1].clone();
    wrong.records[0].state = RecordState::Present {
        value: CanonicalValue::Bytes(vec![0; MAX_STRING_BYTES + 1]),
        version: 1,
        reference: None,
    };
    cases.push(wrong);
    for wrong in cases {
        assert!(v.push(&wrong).is_err());
        assert_eq!(v.summary(), before);
    }
    v.push(&pages[1]).unwrap();
    v.finish().unwrap();
    let mut wrong_manifest = manifest;
    wrong_manifest.record_count = 1;
    assert!(encode_bootstrap_manifest(&wrong_manifest).is_err());
}
#[test]
fn declared_aggregate_limits_are_checked_without_advancing_candidate_hash_state() {
    let mut pages = [page(0, vec![record("a", CanonicalValue::Int(7))])];
    let mut m = manifest_for(&mut pages);
    m.content_bytes -= 1;
    pages[0].manifest_digest = bootstrap_manifest_digest(&m).unwrap();
    let mut v = BootstrapVerifier::new(m).unwrap();
    let before = v.summary();
    assert!(v.push(&pages[0]).is_err());
    assert_eq!(v.summary(), before);
    let (_, m, _, _, _, _) = fixture();
    for field in 0..6 {
        let mut bad = m.clone();
        match field {
            0 => bad.record_count = u64::MAX,
            1 => bad.page_count = u64::MAX,
            2 => bad.content_bytes = u64::MAX,
            3 => bad.limits.max_content_bytes = MAX_BOOTSTRAP_CONTENT_BYTES + 1,
            4 => bad.limits.max_records = 0,
            _ => bad.limits.max_pages = MAX_BOOTSTRAP_PAGES + 1,
        };
        assert!(encode_bootstrap_manifest(&bad).is_err());
    }
}
#[test]
fn encoder_preflights_row_count_depth_and_one_oversized_record_before_payload_clones() {
    let oversized = record("a", CanonicalValue::Bytes(vec![0; MAX_STRING_BYTES + 1]));
    let mut excessive_rows = (0..=MAX_CONTAINER_ITEMS)
        .map(|i| record(&format!("r{i:04}"), CanonicalValue::Null))
        .collect::<Vec<_>>();
    excessive_rows[0] = oversized.clone();
    let p = page(0, excessive_rows);
    assert_eq!(encode_bootstrap_page(&p), Err(CodecError::ContainerLimit));
    assert_eq!(
        encode_bootstrap_page(&page(0, vec![oversized])),
        Err(CodecError::StringLimit)
    );
    let mut value = CanonicalValue::Null;
    for _ in 0..MAX_DEPTH {
        value = CanonicalValue::Array(vec![value]);
    }
    assert!(canonical_cbor(&value).is_ok());
    assert_eq!(
        encode_bootstrap_page(&page(0, vec![record("a", value)])),
        Err(CodecError::DepthLimit)
    );
    let composite = CanonicalValue::Array(vec![CanonicalValue::Bytes(vec![1; 4092]); 1024]);
    assert!(
        canonical_cbor(&composite).is_ok(),
        "record value alone fits existing canonical byte limit"
    );
    let full_record = record(&"r".repeat(MAX_IDENTITY_BYTES), composite);
    assert_eq!(
        encode_bootstrap_page(&page(0, vec![full_record])),
        Err(CodecError::EncodedLimit),
        "one record exceeding full page bytes must fail explicitly"
    );
    for state in [RecordState::Absent, RecordState::Tombstone { version: 0 }] {
        assert!(
            encode_bootstrap_page(&page(
                0,
                vec![SnapshotRecord {
                    record_id: RecordId("a".into()),
                    state
                }]
            ))
            .is_err()
        );
    }
}
#[test]
fn hostile_envelopes_lengths_and_noncanonical_forms_are_rejected() {
    let (s, _, p, _, _, _) = fixture();
    let start = encode_bootstrap_start(&s).unwrap();
    let cases = vec![
        with_envelope(&start, |v| v[0] = Cbor::Text(V1_NAME.into())),
        with_envelope(&start, |v| v[1] = Cbor::Integer(1.into())),
        with_envelope(&start, |v| v[2] = Cbor::Integer(99.into())),
        with_envelope(&start, |v| v.push(Cbor::Null)),
        with_envelope(&start, |v| {
            let Cbor::Array(p) = &mut v[3] else { panic!() };
            p.push(Cbor::Null);
        }),
    ];
    for bytes in cases {
        assert!(decode_bootstrap_start(&bytes).is_err());
    }
    for len in 0..start.len() {
        assert!(decode_bootstrap_start(&start[..len]).is_err());
    }
    let mut trailing = start.clone();
    trailing.push(0);
    assert!(decode_bootstrap_start(&trailing).is_err());
    let mut indefinite = start.clone();
    indefinite[0] = 0x9f;
    indefinite.push(0xff);
    assert!(decode_bootstrap_start(&indefinite).is_err());
    let schema_offset = 1 + 2 + BOOTSTRAP_NAME.len();
    let mut nonminimal = start.clone();
    nonminimal.splice(schema_offset..schema_offset + 1, [0x18, 0]);
    assert!(decode_bootstrap_start(&nonminimal).is_err());
    for head in [0x9b, 0x5b, 0x7b] {
        let mut bytes = vec![head];
        bytes.extend([0xff; 8]);
        assert!(decode_bootstrap_start(&bytes).is_err());
    }
    let mut deep = vec![0x81; MAX_DEPTH + 2];
    deep.push(0);
    assert!(decode_bootstrap_start(&deep).is_err());
    assert!(decode_bootstrap_start(&vec![0; MAX_ENCODED_BYTES + 1]).is_err());
    let page = encode_bootstrap_page(&p).unwrap();
    assert!(decode_bootstrap_start(&page).is_err());
    for len in [31, 33] {
        let bad = with_envelope(&page, |v| {
            let Cbor::Array(p) = &mut v[3] else { panic!() };
            p[1] = Cbor::Bytes(vec![0; len]);
        });
        assert!(decode_bootstrap_page(&bad).is_err());
    }
    let invalid = with_envelope(&page, |v| {
        let Cbor::Array(p) = &mut v[3] else { panic!() };
        p[0] = Cbor::Text("x".repeat(MAX_IDENTITY_BYTES + 1));
    });
    assert!(decode_bootstrap_page(&invalid).is_err());
}
#[test]
fn uncanonical_or_duplicate_maps_and_unsupported_tags_reuse_existing_value_decoder() {
    let (_, _, p, _, _, _) = fixture();
    let bytes = encode_bootstrap_page(&p).unwrap();
    for value in [
        Cbor::Map(vec![
            (Cbor::Text("b".into()), Cbor::Null),
            (Cbor::Text("a".into()), Cbor::Null),
        ]),
        Cbor::Map(vec![
            (Cbor::Text("a".into()), Cbor::Null),
            (Cbor::Text("a".into()), Cbor::Null),
        ]),
        Cbor::Tag(999, Box::new(Cbor::Null)),
    ] {
        let bad = with_envelope(&bytes, |v| {
            let Cbor::Array(p) = &mut v[3] else { panic!() };
            let Cbor::Array(records) = &mut p[3] else {
                panic!()
            };
            let Cbor::Array(record) = &mut records[0] else {
                panic!()
            };
            let Cbor::Array(state) = &mut record[1] else {
                panic!()
            };
            state[1] = value;
        });
        assert!(decode_bootstrap_page(&bad).is_err());
    }
}

const GOLDENS: [&str; 6] = [
    "84781a7375727265616c64622d73796e632d626f6f7473747261702f310000866168616361726173016167",
    "84781a7375727265616c64622d73796e632d626f6f7473747261702f3100018b617a617258209930c35bbb387db1cad0eceb2969a01053869bb21f67671d9b1827cda664a7bc86614161506168616361678361730101820102831940001940001a0400000001010b5820ba7101cc689f328eaa9b25984c70e78685c69d8097a42552096c2e38f2b8f0ad1863",
    "84781a7375727265616c64622d73796e632d626f6f7473747261702f3100028461686163617a00",
    "84781a7375727265616c64622d73796e632d626f6f7473747261702f31000384617a5820960e7d1d2496949fd0510056508552a7b71935311e97d0a0db9bbd6f2e116fe1008182616184010701f6",
    "84781a7375727265616c64622d73796e632d626f6f7473747261702f3100048461686163617a5820960e7d1d2496949fd0510056508552a7b71935311e97d0a0db9bbd6f2e116fe1",
    "84781a7375727265616c64622d73796e632d626f6f7473747261702f31000584617a5820960e7d1d2496949fd0510056508552a7b71935311e97d0a0db9bbd6f2e116fe183616b82010283617301011863",
];
