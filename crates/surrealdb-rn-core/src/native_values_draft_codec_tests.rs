use super::{CodecError, decode_value, encode_value};
use surrealdb::types::{Number, RecordId, RecordIdKey, Set, Value as SurrealValue};

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
