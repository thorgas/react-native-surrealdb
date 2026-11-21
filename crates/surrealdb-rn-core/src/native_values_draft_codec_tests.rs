use std::{env, fs};

use super::{decode_value, encode_value, CodecError};
use serde::Deserialize;
use serde_json::json;
use surrealdb::types::{
    Array, Datetime, Number, Object, RecordId, RecordIdKey, Set, Uuid, Value as SurrealValue,
};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DecodeVectorFixture {
    valid: Vec<ValidDecodeVector>,
    rejected: Vec<RejectedDecodeVector>,
    oversized_encoded_input: OversizedInputVector,
    native_set_cardinality_case: NativeSetCardinalityCase,
    resource_boundary_cases: ResourceBoundaryCases,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ValidDecodeVector {
    id: String,
    cbor_hex: String,
    expected: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RejectedDecodeVector {
    id: String,
    cbor_hex: String,
    error: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NativeSetCardinalityCase {
    id: String,
    cbor_hex: String,
    rust_error: String,
    typescript_disposition: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResourceBoundaryCases {
    max_total_items: usize,
    over_total_items: usize,
    max_aggregate_string_bytes: usize,
    over_aggregate_string_bytes: usize,
    max_encoded_bytes: usize,
    record_identity_table_bytes: usize,
    record_identity_key_bytes: usize,
    empty_record_string_key_bytes: usize,
    over_encoded_output_bytes: usize,
    native_set_equality_checked_in_typescript: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct InteropRequest {
    cases: Vec<InteropCase>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct InteropCase {
    id: String,
    cbor_hex: String,
    #[serde(default)]
    expected: Option<String>,
    #[serde(default)]
    expect_error: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OversizedInputVector {
    length: usize,
    error: String,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn decimal(literal: &str) -> SurrealValue {
    let value = literal
        .parse()
        .expect("the inferred pinned Decimal parser accepts this literal");
    SurrealValue::Number(Number::from_decimal(value))
}

#[test]
fn scalar_golden_vectors_match_the_draft_profile_bytes() {
    let vectors = [
        (SurrealValue::None, "d9ea608200f6"),
        (SurrealValue::Null, "d9ea608201f6"),
        (SurrealValue::Number(Number::Int(1)), "d9ea60820301"),
        (SurrealValue::String("1".into()), "d9ea6082056131"),
        (
            SurrealValue::Number(Number::Float(-0.0)),
            "d9ea608204488000000000000000",
        ),
        (
            decimal("1.00"),
            "d9ea60820a83f44c00000000000000000000006402",
        ),
        (
            SurrealValue::RecordId(RecordId::new("note", RecordIdKey::Number(7))),
            "d9ea60820d82646e6f7465820007",
        ),
    ];

    for (value, expected) in vectors {
        let encoded = encode_value(&value).expect("value is in the draft profile");
        assert_eq!(hex(&encoded), expected);
        let decoded = decode_value(&encoded).expect("golden is canonical");
        assert_eq!(encode_value(&decoded).unwrap(), encoded);
    }
}

#[test]
fn independently_authored_literal_vectors_decode_to_pinned_native_values() {
    let fixture: DecodeVectorFixture = serde_json::from_str(include_str!(
        "../../../packages/react-native-surrealdb/test/fixtures/native-values-decode-vectors.json"
    ))
    .expect("shared hand-authored decode fixture is valid JSON");

    for vector in fixture.valid {
        let bytes = hex_to_bytes(&vector.cbor_hex);
        let decoded = decode_value(&bytes)
            .unwrap_or_else(|error| panic!("{} should decode, got {error:?}", vector.id));
        assert_eq!(
            decoded,
            expected_native_value(&vector.expected),
            "{}",
            vector.id
        );
        // This second assertion catches representation loss for Decimal scale,
        // Float signed zero, and the retained member type inside a Set.
        assert_eq!(encode_value(&decoded).unwrap(), bytes, "{}", vector.id);
    }

    for vector in fixture.rejected {
        let bytes = hex_to_bytes(&vector.cbor_hex);
        let error = decode_value(&bytes).expect_err(&vector.id);
        assert_eq!(format!("{error:?}"), vector.error, "{}", vector.id);
    }

    assert_eq!(fixture.oversized_encoded_input.error, "EncodedLimit");
    assert_eq!(
        decode_value(&vec![0; fixture.oversized_encoded_input.length]),
        Err(CodecError::EncodedLimit)
    );

    assert_eq!(
        fixture.native_set_cardinality_case.rust_error,
        "SetCardinality"
    );
    assert_eq!(
        fixture.native_set_cardinality_case.typescript_disposition,
        "representation-only-accepts-both-members"
    );
    let collapsed = decode_value(&hex_to_bytes(&fixture.native_set_cardinality_case.cbor_hex));
    assert_eq!(
        collapsed,
        Err(CodecError::SetCardinality),
        "{}",
        fixture.native_set_cardinality_case.id
    );
}

#[test]
fn aggregate_resource_limits_and_record_key_boundaries_match_shared_fixture() {
    let fixture: DecodeVectorFixture = serde_json::from_str(include_str!(
        "../../../packages/react-native-surrealdb/test/fixtures/native-values-decode-vectors.json"
    ))
    .expect("shared hand-authored decode fixture is valid JSON");
    let limits = fixture.resource_boundary_cases;
    assert_eq!(limits.max_total_items, 4_096);
    assert_eq!(limits.over_total_items, limits.max_total_items + 1);
    assert_eq!(limits.max_aggregate_string_bytes, 1_048_576);
    assert_eq!(
        limits.over_aggregate_string_bytes,
        limits.max_aggregate_string_bytes + 1
    );
    assert_eq!(limits.max_encoded_bytes, 4_194_304);
    assert_eq!(limits.record_identity_table_bytes, 1_025);
    assert_eq!(limits.record_identity_key_bytes, 1_025);
    assert_eq!(limits.empty_record_string_key_bytes, 0);
    assert_eq!(
        limits.over_encoded_output_bytes,
        limits.max_encoded_bytes + 1
    );
    assert!(!limits.native_set_equality_checked_in_typescript);

    let item_tree = |extra_leaf: bool| {
        let branches = (0..64).map(|index| {
            let leaf_count = if index < 63 {
                63
            } else {
                62 + usize::from(extra_leaf)
            };
            SurrealValue::Array(Array::from(vec![SurrealValue::Null; leaf_count]))
        });
        SurrealValue::Array(Array::from(branches.collect::<Vec<_>>()))
    };
    let at_item_limit = encode_value(&item_tree(false)).expect("4096 value nodes fit");
    assert!(at_item_limit.len() <= limits.max_encoded_bytes);
    assert_eq!(decode_value(&at_item_limit), Ok(item_tree(false)));
    assert_eq!(
        encode_value(&item_tree(true)),
        Err(CodecError::TotalItemsLimit)
    );

    let half = limits.max_aggregate_string_bytes / 2;
    let string_tree = |second_len: usize| {
        SurrealValue::Array(Array::from(vec![
            SurrealValue::String("a".repeat(half)),
            SurrealValue::Bytes(vec![0; second_len].into()),
        ]))
    };
    let at_string_limit = encode_value(&string_tree(half)).expect("aggregate string bytes fit");
    assert!(at_string_limit.len() <= limits.max_encoded_bytes);
    assert_eq!(decode_value(&at_string_limit), Ok(string_tree(half)));
    assert_eq!(
        encode_value(&string_tree(half + 1)),
        Err(CodecError::StringLimit)
    );
    assert_eq!(
        encode_value(&SurrealValue::Bytes(
            vec![0; limits.over_encoded_output_bytes].into()
        )),
        Err(CodecError::StringLimit),
        "the stricter payload budget rejects this before the 4 MiB output cap"
    );

    let long_table = "t".repeat(limits.record_identity_table_bytes);
    let long_key = "k".repeat(limits.record_identity_key_bytes);
    let empty_string_key = SurrealValue::RecordId(RecordId::new(
        long_table.clone(),
        RecordIdKey::String(long_key),
    ));
    let encoded =
        encode_value(&empty_string_key).expect("native RID has no arbitrary table/key byte cap");
    assert_eq!(decode_value(&encoded), Ok(empty_string_key));
    let empty_string_key =
        SurrealValue::RecordId(RecordId::new("note", RecordIdKey::String(String::new())));
    let encoded = encode_value(&empty_string_key).expect("native RID permits an empty string key");
    assert_eq!(decode_value(&encoded), Ok(empty_string_key));
    assert_eq!(
        encode_value(&SurrealValue::RecordId(RecordId::new(
            "",
            RecordIdKey::String(String::new())
        ))),
        Err(CodecError::InvalidRecordId),
    );
}

#[test]
#[ignore = "driven by the opt-in TypeScript interop test harness"]
fn typescript_bytes_cross_rust_decoder() {
    let (Ok(request_path), Ok(response_path)) = (
        env::var("NATIVE_VALUES_DRAFT_INTEROP_REQUEST"),
        env::var("NATIVE_VALUES_DRAFT_INTEROP_RESPONSE"),
    ) else {
        return;
    };
    let request: InteropRequest =
        serde_json::from_slice(&fs::read(request_path).expect("interop request fixture exists"))
            .expect("interop request fixture is valid JSON");
    let mut results = Vec::with_capacity(request.cases.len());
    for case in request.cases {
        let bytes = hex_to_bytes(&case.cbor_hex);
        if let Some(expected_error) = case.expect_error {
            let error = decode_value(&bytes).expect_err(&case.id);
            assert_eq!(format!("{error:?}"), expected_error, "{}", case.id);
            results.push(json!({
                "id": case.id,
                "status": "rejected",
                "error": format!("{error:?}")
            }));
            continue;
        }

        let expected_id = case
            .expected
            .expect("accepted interop case names native expectation");
        let decoded = decode_value(&bytes)
            .unwrap_or_else(|error| panic!("{} expected acceptance: {error:?}", case.id));
        assert_eq!(decoded, expected_native_value(&expected_id), "{}", case.id);
        let encoded = encode_value(&decoded).expect("native-decoded value can re-encode");
        results.push(json!({
            "id": case.id,
            "status": "accepted",
            "cborHex": hex(&encoded)
        }));
    }
    fs::write(
        response_path,
        serde_json::to_vec(&json!({ "results": results })).expect("interop response serializes"),
    )
    .expect("interop response is written");
}

fn expected_native_value(id: &str) -> SurrealValue {
    match id {
        "none" => SurrealValue::None,
        "null" => SurrealValue::Null,
        "int-one" => SurrealValue::Number(Number::Int(1)),
        "int64-min" => SurrealValue::Number(Number::Int(i64::MIN)),
        "int64-max" => SurrealValue::Number(Number::Int(i64::MAX)),
        "string-one" => SurrealValue::String("1".into()),
        "float-negative-zero" => SurrealValue::Number(Number::Float(-0.0)),
        "decimal-max-coefficient-scale-zero" => decimal("79228162514264337593543950335"),
        "decimal-max-coefficient-scale-28" => decimal("7.9228162514264337593543950335"),
        "datetime-min-utc" => SurrealValue::Datetime(Datetime::MIN_UTC),
        "datetime-max-utc" => SurrealValue::Datetime(Datetime::MAX_UTC),
        "datetime-fractional-nanos" => {
            SurrealValue::Datetime(Datetime::from_timestamp(1, 999_999_999).unwrap())
        }
        "uuid-00112233" => SurrealValue::Uuid(
            Uuid::try_from("00112233-4455-6677-8899-aabbccddeeff".to_owned()).unwrap(),
        ),
        "record-key-integer-seven" => {
            SurrealValue::RecordId(RecordId::new("note", RecordIdKey::Number(7)))
        }
        "record-key-string-seven" => {
            SurrealValue::RecordId(RecordId::new("note", RecordIdKey::String("7".into())))
        }
        "record-key-empty-string" => {
            SurrealValue::RecordId(RecordId::new("note", RecordIdKey::String(String::new())))
        }
        "record-key-uuid" => SurrealValue::RecordId(RecordId::new(
            "note",
            RecordIdKey::Uuid(
                Uuid::try_from("00112233-4455-6677-8899-aabbccddeeff".to_owned()).unwrap(),
            ),
        )),
        "set-retained-float-one" => {
            SurrealValue::Set(Set::from(vec![SurrealValue::Number(Number::Float(1.0))]))
        }
        "set-retained-decimal-scale-two" => SurrealValue::Set(Set::from(vec![decimal("1.00")])),
        "set-wire-order-differs-from-native-order" => SurrealValue::Set(Set::from(vec![
            SurrealValue::Number(Number::Int(1)),
            decimal("0.00"),
        ])),
        "ordinary-map-with-tag-lookalike-keys" => {
            let mut object = Object::new();
            object.insert("$surreal", "tag:60000");
            object.insert("surrealdb-sync/native-values/1", "native-values/1");
            SurrealValue::Object(object)
        }
        "depth-32-array-chain" => {
            let mut value = SurrealValue::Number(Number::Int(1));
            for _ in 0..31 {
                value = SurrealValue::Array(Array::from(vec![value]));
            }
            value
        }
        _ => panic!("unknown expected native-value vector: {id}"),
    }
}

#[test]
fn finite_float_representation_preserves_negative_zero() {
    let value = SurrealValue::Number(Number::Float(-0.0));
    let bytes = encode_value(&value).unwrap();
    let decoded = decode_value(&bytes).unwrap();
    let SurrealValue::Number(Number::Float(decoded)) = decoded else {
        panic!("float tag must decode as a float");
    };
    assert_eq!(decoded.to_bits(), (-0.0_f64).to_bits());
    assert_eq!(
        bytes,
        encode_value(&SurrealValue::Number(Number::Float(decoded))).unwrap()
    );
}

#[test]
fn non_finite_float_values_fail_closed() {
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert_eq!(
            encode_value(&SurrealValue::Number(Number::Float(value))),
            Err(CodecError::InvalidFloat)
        );
    }

    let nan = hex_to_bytes("d9ea608204487ff8000000000001");
    assert_eq!(decode_value(&nan), Err(CodecError::InvalidFloat));
}

#[test]
fn decimal_scale_survives_decode_and_encode() {
    let value = decimal("1.00");
    let encoded = encode_value(&value).unwrap();
    assert_eq!(hex(&encoded), "d9ea60820a83f44c00000000000000000000006402");
    assert_eq!(
        encode_value(&decode_value(&encoded).unwrap()).unwrap(),
        encoded
    );
}

#[test]
fn set_encoding_preserves_the_native_representative_and_sorts_wire_bytes() {
    let int_first = Set::from(vec![
        SurrealValue::Number(Number::Int(1)),
        SurrealValue::Number(Number::Float(1.0)),
    ]);
    let float_first = Set::from(vec![
        SurrealValue::Number(Number::Float(1.0)),
        SurrealValue::Number(Number::Int(1)),
    ]);
    assert_eq!(int_first.len(), 1);
    assert_eq!(float_first.len(), 1);
    let int_bytes = encode_value(&SurrealValue::Set(int_first)).unwrap();
    let float_bytes = encode_value(&SurrealValue::Set(float_first)).unwrap();
    assert_eq!(hex(&int_bytes), "d9ea60820c81d9ea608204483ff0000000000000");
    assert_eq!(hex(&float_bytes), "d9ea60820c81d9ea60820301");
    assert_ne!(int_bytes, float_bytes);

    let decimal_scale_one = Set::from(vec![decimal("1.00"), decimal("1.0")]);
    let decimal_scale_two = Set::from(vec![decimal("1.0"), decimal("1.00")]);
    assert_eq!(decimal_scale_one.len(), 1);
    assert_eq!(decimal_scale_two.len(), 1);
    assert_eq!(
        hex(&encode_value(&SurrealValue::Set(decimal_scale_one)).unwrap()),
        "d9ea60820c81d9ea60820a83f44c00000000000000000000000a01"
    );
    assert_eq!(
        hex(&encode_value(&SurrealValue::Set(decimal_scale_two)).unwrap()),
        "d9ea60820c81d9ea60820a83f44c00000000000000000000006402"
    );

    let positive_zero = Set::from(vec![
        SurrealValue::Number(Number::Float(-0.0)),
        SurrealValue::Number(Number::Float(0.0)),
    ]);
    let negative_zero = Set::from(vec![
        SurrealValue::Number(Number::Float(0.0)),
        SurrealValue::Number(Number::Float(-0.0)),
    ]);
    assert_eq!(
        hex(&encode_value(&SurrealValue::Set(positive_zero)).unwrap()),
        "d9ea60820c81d9ea608204480000000000000000"
    );
    assert_eq!(
        hex(&encode_value(&SurrealValue::Set(negative_zero)).unwrap()),
        "d9ea60820c81d9ea608204488000000000000000"
    );

    let wire_order = SurrealValue::Set(Set::from(vec![
        decimal("0.00"),
        SurrealValue::Number(Number::Int(1)),
    ]));
    let encoded = encode_value(&wire_order).unwrap();
    assert_eq!(
        encode_value(&decode_value(&encoded).unwrap()).unwrap(),
        encoded
    );
    let int_member = encoded
        .windows(6)
        .position(|window| window == [0xd9, 0xea, 0x60, 0x82, 0x03, 0x01])
        .unwrap();
    let decimal_member = encoded
        .windows(6)
        .position(|window| window == [0xd9, 0xea, 0x60, 0x82, 0x0a, 0x83])
        .unwrap();
    assert!(int_member < decimal_member);
}

#[test]
fn decoder_rejects_distinct_encodings_that_collapse_in_a_native_set() {
    for bytes in [
        "d9ea60820c82d9ea60820301d9ea608204483ff0000000000000",
        "d9ea60820c82d9ea60820a83f44c00000000000000000000000a01d9ea60820a83f44c00000000000000000000006402",
        "d9ea60820c82d9ea608204480000000000000000d9ea608204488000000000000000",
    ] {
        assert_eq!(
            decode_value(&hex_to_bytes(bytes)),
            Err(CodecError::SetCardinality)
        );
    }
}

#[test]
fn decoder_rejects_set_members_out_of_byte_order() {
    let bytes = hex_to_bytes("d9ea60820c82d9ea608204483ff0000000000000d9ea60820301");
    assert_eq!(decode_value(&bytes), Err(CodecError::SetOrder));
}

#[test]
fn typed_record_keys_keep_integer_and_string_identity_distinct() {
    let integer = SurrealValue::RecordId(RecordId::new("note", RecordIdKey::Number(7)));
    let string = SurrealValue::RecordId(RecordId::new("note", RecordIdKey::String("7".into())));
    let integer_bytes = encode_value(&integer).unwrap();
    let string_bytes = encode_value(&string).unwrap();
    assert_ne!(integer_bytes, string_bytes);
    assert_eq!(
        encode_value(&decode_value(&integer_bytes).unwrap()).unwrap(),
        integer_bytes
    );
    assert_eq!(
        encode_value(&decode_value(&string_bytes).unwrap()).unwrap(),
        string_bytes
    );
}

#[test]
fn ordinary_objects_with_tag_like_keys_remain_objects() {
    let mut object = surrealdb::types::Object::new();
    object.insert("$surreal", "native-values/1");
    object.insert("surrealdb-sync/native-values/1", "tag:60000");
    let value = SurrealValue::Object(object);
    let bytes = encode_value(&value).unwrap();
    let decoded = decode_value(&bytes).unwrap();
    assert_eq!(decoded, value);
    assert_eq!(encode_value(&decoded).unwrap(), bytes);
}

#[test]
fn untagged_maps_and_noncanonical_integer_encodings_are_rejected() {
    assert_eq!(decode_value(&[0xa0]), Err(CodecError::Decode));
    assert_eq!(
        decode_value(&hex_to_bytes("d9ea6082031801")),
        Err(CodecError::NonCanonical)
    );
    assert_eq!(
        decode_value(&hex_to_bytes("d9ea60820301f6")),
        Err(CodecError::TrailingBytes)
    );
}

fn hex_to_bytes(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16).unwrap();
            let low = (pair[1] as char).to_digit(16).unwrap();
            ((high << 4) | low) as u8
        })
        .collect()
}
