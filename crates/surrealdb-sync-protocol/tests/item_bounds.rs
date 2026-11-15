//! ADR 0017: one record or commit may use `MAX_TOTAL_ITEMS`; an envelope that packs several
//! stays within `PACKED_TOTAL_ITEMS`, the bound decoders before ADR 0017 enforce.
use std::collections::BTreeMap;

use surrealdb_sync_protocol::bootstrap::{BootstrapDigest, BootstrapPage};
use surrealdb_sync_protocol::bootstrap_codec::{decode_bootstrap_page, encode_bootstrap_page};
use surrealdb_sync_protocol::{
    AppliedRecord, CanonicalValue, Checkpoint, CodecError, Cursor, MAX_TOTAL_ITEMS,
    OpaqueCheckpoint, PACKED_TOTAL_ITEMS, PullBatch, PullCommit, PullFrame, PullResponse, RecordId,
    RecordState, ResetReason, ResetSnapshot, ScopeIdentity, ScopeSnapshot, SnapshotRecord,
    canonical_cbor, canonical_cbor_packed, decode_canonical_cbor, decode_pull_response,
    encode_pull_response,
};

type Value = CanonicalValue;

fn text(value: &str) -> Value {
    Value::String(value.into())
}

fn object(entries: Vec<(&str, Value)>) -> Value {
    Value::Object(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect::<BTreeMap<_, _>>(),
    )
}

/// One referenced nutrient reading in the product's shape: 23 codec items.
fn reading(index: usize) -> Value {
    object(vec![
        ("nutrientId", text(&format!("nutrient-{index}"))),
        ("value", Value::Int(index as i64)),
        ("unit", text("g")),
        ("comparator", text("exact")),
        ("source", text("database")),
        ("confidence", Value::Int(1)),
        (
            "reference",
            object(vec![
                ("source", text("bls")),
                ("recordId", text("B123")),
                ("datasetVersion", text("4.0")),
                ("attribution", text("Bundeslebensmittelschluessel")),
            ]),
        ),
    ])
}

/// A recipe whose ingredients each carry 43 referenced readings, like the production outliers.
fn recipe(ingredients: usize) -> Value {
    let ingredient = |index: usize| {
        object(vec![
            ("name", text(&format!("ingredient-{index}"))),
            ("quantity", text("1")),
            (
                "nutrition",
                object(vec![
                    (
                        "basis",
                        object(vec![
                            ("amount", Value::Int(100)),
                            ("unit", text("g")),
                            ("preparation", text("as-sold")),
                        ]),
                    ),
                    ("values", Value::Array((0..43).map(reading).collect())),
                ]),
            ),
        ])
    };
    object(vec![
        ("recipeName", text("fixture")),
        (
            "ingredients",
            Value::Array((0..ingredients).map(ingredient).collect()),
        ),
    ])
}

/// Codec items as the canonical byte preflight counts them.
fn items(value: &Value) -> usize {
    match value {
        Value::Array(values) => 1 + values.iter().map(items).sum::<usize>(),
        Value::Object(values) => 1 + values.values().map(|value| 1 + items(value)).sum::<usize>(),
        _ => 1,
    }
}

fn checkpoint() -> Checkpoint {
    Checkpoint {
        token: OpaqueCheckpoint("token".into()),
        cursor: Cursor {
            epoch: 1,
            sequence: 2,
        },
        scope: ScopeSnapshot {
            identity: ScopeIdentity("scope".into()),
            authorization_revision: 1,
            subscription_revision: 1,
        },
    }
}

fn present(value: Value) -> RecordState<Value> {
    RecordState::Present {
        value,
        version: 1,
        reference: None,
    }
}

fn batch(commits: Vec<Value>) -> PullResponse<Value> {
    let mut frames = vec![PullFrame::Start {
        checkpoint: checkpoint(),
    }];
    for (index, value) in commits.into_iter().enumerate() {
        frames.push(PullFrame::Commit {
            checkpoint: OpaqueCheckpoint("token".into()),
            commit: PullCommit {
                sequence: index as u64 + 1,
                source: None,
                records: vec![AppliedRecord {
                    record_id: RecordId(format!("recipe:{index}")),
                    state: present(value),
                }],
            },
        });
    }
    frames.push(PullFrame::End {
        checkpoint: checkpoint(),
    });
    PullResponse::Batch(PullBatch { frames })
}

fn reset(values: Vec<Value>) -> PullResponse<Value> {
    PullResponse::Reset(ResetSnapshot {
        reason: ResetReason::EpochChanged,
        checkpoint: checkpoint(),
        records: values
            .into_iter()
            .enumerate()
            .map(|(index, value)| SnapshotRecord {
                record_id: RecordId(format!("recipe:{index:04}")),
                state: present(value),
            })
            .collect(),
    })
}

fn page(values: Vec<Value>) -> BootstrapPage {
    BootstrapPage {
        session_id: "session".into(),
        manifest_digest: BootstrapDigest([7; 32]),
        index: 0,
        records: values
            .into_iter()
            .enumerate()
            .map(|(index, value)| SnapshotRecord {
                record_id: RecordId(format!("recipe:{index:04}")),
                state: present(value),
            })
            .collect(),
    }
}

#[test]
fn bounds_are_the_documented_values() {
    assert_eq!(PACKED_TOTAL_ITEMS, 4_096);
    assert_eq!(MAX_TOTAL_ITEMS, 65_536);
    // The fixture reading must match the product shape the limit is derived from.
    assert_eq!(items(&reading(0)), 23);
}

#[test]
fn a_recipe_over_the_packed_bound_round_trips_as_one_value() {
    let value = recipe(8);
    assert!(
        items(&value) > PACKED_TOTAL_ITEMS,
        "fixture must exceed the old bound"
    );
    let bytes = canonical_cbor(&value).expect("one record may use the full bound");
    assert_eq!(decode_canonical_cbor(&bytes), Ok(value));
}

#[test]
fn the_packed_value_encoder_keeps_the_legacy_bound() {
    let value = recipe(8);
    assert_eq!(
        canonical_cbor_packed(&value),
        Err(CodecError::ContainerLimit)
    );
    let small = recipe(1);
    let bytes = canonical_cbor_packed(&small).expect("small values fit the packed bound");
    assert_eq!(Ok(bytes), canonical_cbor(&small));
}

#[test]
fn the_largest_admitted_recipe_fits_and_one_more_ingredient_does_not() {
    // 54 ingredients of this fixture shape stay below 65,536 items; the bound is explicit.
    let fits = recipe(54);
    assert!(items(&fits) <= MAX_TOTAL_ITEMS);
    assert!(canonical_cbor(&fits).is_ok());
    let mut too_large = recipe(54);
    let Value::Object(fields) = &mut too_large else {
        unreachable!()
    };
    let Some(Value::Array(ingredients)) = fields.get_mut("ingredients") else {
        unreachable!()
    };
    while items(&Value::Array(ingredients.clone())) <= MAX_TOTAL_ITEMS {
        ingredients.push(ingredients[0].clone());
    }
    assert_eq!(canonical_cbor(&too_large), Err(CodecError::ContainerLimit));
}

#[test]
fn a_single_commit_batch_may_carry_an_oversized_record() {
    let response = batch(vec![recipe(8)]);
    let bytes = encode_pull_response(&response).expect("one commit uses the full bound");
    assert_eq!(decode_pull_response(&bytes), Ok(response));
}

#[test]
fn a_multi_commit_batch_stays_within_the_packed_bound() {
    // Together over 4,096 items: the authority must split these into two batches.
    let response = batch(vec![recipe(2), recipe(2)]);
    assert_eq!(
        encode_pull_response(&response),
        Err(CodecError::ContainerLimit)
    );
    let small = batch(vec![recipe(1), recipe(1)]);
    let bytes = encode_pull_response(&small).expect("small commits still pack together");
    assert_eq!(decode_pull_response(&bytes), Ok(small));
}

#[test]
fn a_packed_envelope_at_the_bound_still_decodes_under_the_legacy_limit() {
    // Grow a multi-record reset one record at a time; the largest accepted envelope is
    // exactly what a pre-ADR-0017 decoder accepts, and one more record is refused.
    let mut values = vec![Value::Int(0), Value::Int(1)];
    let mut accepted = None;
    loop {
        match encode_pull_response(&reset(values.clone())) {
            Ok(bytes) => accepted = Some(bytes),
            Err(error) => {
                assert_eq!(error, CodecError::ContainerLimit);
                break;
            }
        }
        values.push(Value::Int(values.len() as i64));
    }
    let bytes = accepted.expect("a small reset packs");
    assert!(legacy_preflight_items(&bytes) <= PACKED_TOTAL_ITEMS);
    assert!(decode_pull_response(&bytes).is_ok());
}

#[test]
fn a_single_record_reset_may_carry_an_oversized_record() {
    let response = reset(vec![recipe(8)]);
    let bytes = encode_pull_response(&response).expect("one record uses the full bound");
    assert_eq!(decode_pull_response(&bytes), Ok(response));
    assert_eq!(
        encode_pull_response(&reset(vec![recipe(8), Value::Int(1)])),
        Err(CodecError::ContainerLimit)
    );
}

#[test]
fn bootstrap_pages_pack_small_records_and_isolate_an_oversized_one() {
    let alone = page(vec![recipe(8)]);
    let bytes = encode_bootstrap_page(&alone).expect("a lone record uses the full bound");
    assert_eq!(decode_bootstrap_page(&bytes), Ok(alone));
    assert_eq!(
        encode_bootstrap_page(&page(vec![recipe(8), Value::Int(1)])),
        Err(CodecError::ContainerLimit)
    );
    let packed = page(vec![recipe(1), recipe(1)]);
    let bytes = encode_bootstrap_page(&packed).expect("small records pack together");
    assert!(legacy_preflight_items(&bytes) <= PACKED_TOTAL_ITEMS);
}

/// Counts CBOR data items the way the pre-ADR-0017 byte preflight does.
fn legacy_preflight_items(bytes: &[u8]) -> usize {
    fn scan(bytes: &[u8], cursor: &mut usize) -> usize {
        let initial = bytes[*cursor];
        *cursor += 1;
        let (major, additional) = (initial >> 5, initial & 0x1f);
        let argument = match additional {
            0..=23 => u64::from(additional),
            24..=27 => {
                let width = 1usize << (additional - 24);
                let mut value = 0u64;
                for byte in &bytes[*cursor..*cursor + width] {
                    value = (value << 8) | u64::from(*byte);
                }
                *cursor += width;
                value
            }
            _ => panic!("indefinite lengths are not canonical"),
        };
        match major {
            2 | 3 => {
                *cursor += argument as usize;
                1
            }
            4 => 1 + (0..argument).map(|_| scan(bytes, cursor)).sum::<usize>(),
            5 => {
                1 + (0..argument * 2)
                    .map(|_| scan(bytes, cursor))
                    .sum::<usize>()
            }
            6 => 1 + scan(bytes, cursor),
            _ => 1,
        }
    }
    let mut cursor = 0;
    let count = scan(bytes, &mut cursor);
    assert_eq!(cursor, bytes.len());
    count
}
