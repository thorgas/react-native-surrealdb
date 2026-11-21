//! Isolated value codec spike for the proposed native-values profile.
//!
//! This module is deliberately private and unwired from sync, transport,
//! storage, and UniFFI. It is a draft implementation/test surface pending
//! engine contract adoption. Do not use it for durable or network data.

use std::cmp::Ordering;

use surrealdb::types::{
    Array, Datetime, Number, Object, RecordId, RecordIdKey, Uuid, Value as SurrealValue,
};

const VALUE_TAG: u64 = 60_000;
const MAX_DEPTH: usize = 32;
const MAX_CONTAINER_ITEMS: usize = 1_024;
const MAX_TOTAL_ITEMS: usize = 4_096;
const MAX_STRING_BYTES: usize = 1_048_576;
const MAX_ENCODED_BYTES: usize = 4_194_304;
const MAX_DECIMAL_COEFFICIENT: u128 = 79_228_162_514_264_337_593_543_950_335;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CodecError {
    Decode,
    TrailingBytes,
    NonCanonical,
    UnsupportedValue,
    InvalidFloat,
    InvalidDecimal,
    InvalidDatetime,
    InvalidUuid,
    InvalidRecordId,
    DuplicateObjectKey,
    SetOrder,
    SetCardinality,
    DepthLimit,
    ContainerLimit,
    TotalItemsLimit,
    StringLimit,
    EncodedLimit,
}

/// Encode one SurrealDB value as the internal tag-60000 draft form.
fn encode_value(value: &SurrealValue) -> Result<Vec<u8>, CodecError> {
    let mut writer = Writer::default();
    let mut budget = Budget::default();
    encode_node(value, 1, &mut budget, &mut writer)?;
    Ok(writer.bytes)
}

/// Decode one complete draft value. The result is exposed only after exact
/// canonical re-encoding succeeds.
fn decode_value(bytes: &[u8]) -> Result<SurrealValue, CodecError> {
    if bytes.len() > MAX_ENCODED_BYTES {
        return Err(CodecError::EncodedLimit);
    }
    let mut reader = Reader::new(bytes);
    let mut budget = Budget::default();
    let value = decode_node(&mut reader, 1, &mut budget)?;
    if !reader.is_empty() {
        return Err(CodecError::TrailingBytes);
    }
    if encode_value(&value)?.as_slice() != bytes {
        return Err(CodecError::NonCanonical);
    }
    Ok(value)
}

#[derive(Default)]
struct Budget {
    items: usize,
    string_bytes: usize,
}

impl Budget {
    fn item(&mut self, depth: usize) -> Result<(), CodecError> {
        if depth > MAX_DEPTH {
            return Err(CodecError::DepthLimit);
        }
        self.items += 1;
        if self.items > MAX_TOTAL_ITEMS {
            return Err(CodecError::TotalItemsLimit);
        }
        Ok(())
    }

    fn string(&mut self, bytes: usize) -> Result<(), CodecError> {
        if bytes > MAX_STRING_BYTES {
            return Err(CodecError::StringLimit);
        }
        self.string_bytes = self
            .string_bytes
            .checked_add(bytes)
            .ok_or(CodecError::EncodedLimit)?;
        if self.string_bytes > MAX_STRING_BYTES {
            return Err(CodecError::StringLimit);
        }
        Ok(())
    }
}

#[derive(Default)]
struct Writer {
    bytes: Vec<u8>,
}

impl Writer {
    fn raw(&mut self, bytes: &[u8]) -> Result<(), CodecError> {
        let next = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or(CodecError::EncodedLimit)?;
        if next > MAX_ENCODED_BYTES {
            return Err(CodecError::EncodedLimit);
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn head(&mut self, major: u8, value: u64) -> Result<(), CodecError> {
        let prefix = major << 5;
        match value {
            0..=23 => self.raw(&[prefix | value as u8]),
            24..=0xff => self.raw(&[prefix | 24, value as u8]),
            0x100..=0xffff => self.raw(&[prefix | 25, (value >> 8) as u8, value as u8]),
            0x1_0000..=0xffff_ffff => self.raw(&[
                prefix | 26,
                (value >> 24) as u8,
                (value >> 16) as u8,
                (value >> 8) as u8,
                value as u8,
            ]),
            _ => self.raw(&[
                prefix | 27,
                (value >> 56) as u8,
                (value >> 48) as u8,
                (value >> 40) as u8,
                (value >> 32) as u8,
                (value >> 24) as u8,
                (value >> 16) as u8,
                (value >> 8) as u8,
                value as u8,
            ]),
        }
    }

    fn uint(&mut self, value: u64) -> Result<(), CodecError> {
        self.head(0, value)
    }

    fn int(&mut self, value: i64) -> Result<(), CodecError> {
        if value >= 0 {
            self.uint(value as u64)
        } else {
            self.head(1, (-1_i128 - i128::from(value)) as u64)
        }
    }

    fn array(&mut self, len: usize) -> Result<(), CodecError> {
        self.head(4, len as u64)
    }

    fn bytes(&mut self, value: &[u8]) -> Result<(), CodecError> {
        self.head(2, value.len() as u64)?;
        self.raw(value)
    }

    fn text(&mut self, value: &str) -> Result<(), CodecError> {
        self.head(3, value.len() as u64)?;
        self.raw(value.as_bytes())
    }

    fn tagged(&mut self, kind: u64) -> Result<(), CodecError> {
        self.head(6, VALUE_TAG)?;
        self.array(2)?;
        self.uint(kind)
    }
}

fn encode_node(
    value: &SurrealValue,
    depth: usize,
    budget: &mut Budget,
    writer: &mut Writer,
) -> Result<(), CodecError> {
    budget.item(depth)?;
    match value {
        SurrealValue::None => {
            writer.tagged(0)?;
            writer.raw(&[0xf6])
        }
        SurrealValue::Null => {
            writer.tagged(1)?;
            writer.raw(&[0xf6])
        }
        SurrealValue::Bool(value) => {
            writer.tagged(2)?;
            writer.raw(&[if *value { 0xf5 } else { 0xf4 }])
        }
        SurrealValue::Number(Number::Int(value)) => {
            writer.tagged(3)?;
            writer.int(*value)
        }
        SurrealValue::Number(Number::Float(value)) => {
            if !value.is_finite() {
                return Err(CodecError::InvalidFloat);
            }
            writer.tagged(4)?;
            writer.bytes(&value.to_bits().to_be_bytes())
        }
        SurrealValue::Number(Number::Decimal(value)) => {
            let coefficient = value.mantissa().unsigned_abs();
            if coefficient > MAX_DECIMAL_COEFFICIENT || value.scale() > 28 {
                return Err(CodecError::InvalidDecimal);
            }
            writer.tagged(10)?;
            writer.array(3)?;
            writer.raw(&[if value.is_sign_negative() { 0xf5 } else { 0xf4 }])?;
            writer.bytes(&coefficient.to_be_bytes()[4..])?;
            writer.uint(u64::from(value.scale()))
        }
        SurrealValue::String(value) => {
            budget.string(value.len())?;
            writer.tagged(5)?;
            writer.text(value)
        }
        SurrealValue::Bytes(value) => {
            budget.string(value.len())?;
            writer.tagged(6)?;
            writer.bytes(value.as_ref())
        }
        SurrealValue::Array(value) => {
            if value.len() > MAX_CONTAINER_ITEMS {
                return Err(CodecError::ContainerLimit);
            }
            writer.tagged(7)?;
            writer.array(value.len())?;
            for child in value.iter() {
                encode_node(child, depth + 1, budget, writer)?;
            }
            Ok(())
        }
        SurrealValue::Object(value) => {
            if value.len() > MAX_CONTAINER_ITEMS {
                return Err(CodecError::ContainerLimit);
            }
            writer.tagged(8)?;
            writer.array(value.len())?;
            let mut entries = value.iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));
            for (key, child) in entries {
                budget.string(key.len())?;
                writer.array(2)?;
                writer.text(key)?;
                encode_node(child, depth + 1, budget, writer)?;
            }
            Ok(())
        }
        SurrealValue::Datetime(value) => {
            let timestamp = value.into_inner();
            writer.tagged(9)?;
            writer.array(2)?;
            writer.int(timestamp.timestamp())?;
            writer.uint(u64::from(timestamp.timestamp_subsec_nanos()))
        }
        SurrealValue::Uuid(value) => {
            writer.tagged(11)?;
            writer.bytes(value.into_inner().as_bytes())
        }
        SurrealValue::RecordId(value) => {
            writer.tagged(13)?;
            writer.array(2)?;
            let table = value.table.as_str();
            if table.is_empty() {
                return Err(CodecError::InvalidRecordId);
            }
            budget.string(table.len())?;
            writer.text(table)?;
            match &value.key {
                RecordIdKey::Number(key) => {
                    writer.array(2)?;
                    writer.uint(0)?;
                    writer.int(*key)
                }
                RecordIdKey::String(key) => {
                    budget.string(key.len())?;
                    writer.array(2)?;
                    writer.uint(1)?;
                    writer.text(key)
                }
                RecordIdKey::Uuid(key) => {
                    writer.array(2)?;
                    writer.uint(2)?;
                    writer.bytes(key.into_inner().as_bytes())
                }
                RecordIdKey::Array(_) | RecordIdKey::Object(_) | RecordIdKey::Range(_) => {
                    return Err(CodecError::InvalidRecordId);
                }
            }
        }
        SurrealValue::Set(value) => {
            if value.len() > MAX_CONTAINER_ITEMS {
                return Err(CodecError::ContainerLimit);
            }
            let mut members = Vec::with_capacity(value.len());
            for member in value.iter() {
                let mut encoded = Writer::default();
                encode_node(member, depth + 1, budget, &mut encoded)?;
                members.push(encoded.bytes);
            }
            members.sort();
            if members.windows(2).any(|pair| pair[0] >= pair[1]) {
                return Err(CodecError::SetOrder);
            }
            writer.tagged(12)?;
            writer.array(members.len())?;
            for member in members {
                writer.raw(&member)?;
            }
            Ok(())
        }
        SurrealValue::Duration(_)
        | SurrealValue::Geometry(_)
        | SurrealValue::Table(_)
        | SurrealValue::File(_)
        | SurrealValue::Range(_)
        | SurrealValue::Regex(_) => Err(CodecError::UnsupportedValue),
    }
}

fn decode_node(
    reader: &mut Reader<'_>,
    depth: usize,
    budget: &mut Budget,
) -> Result<SurrealValue, CodecError> {
    budget.item(depth)?;
    reader.expect_head(6, VALUE_TAG)?;
    if reader.array_len()? != 2 {
        return Err(CodecError::Decode);
    }
    let kind = reader.uint()?;
    match kind {
        0 => {
            reader.expect_null()?;
            Ok(SurrealValue::None)
        }
        1 => {
            reader.expect_null()?;
            Ok(SurrealValue::Null)
        }
        2 => Ok(SurrealValue::Bool(reader.boolean()?)),
        3 => Ok(SurrealValue::Number(Number::Int(reader.int()?))),
        4 => {
            let bytes = reader.bytes()?;
            let bits = <[u8; 8]>::try_from(bytes).map_err(|_| CodecError::InvalidFloat)?;
            let value = f64::from_bits(u64::from_be_bytes(bits));
            if !value.is_finite() {
                return Err(CodecError::InvalidFloat);
            }
            Ok(SurrealValue::Number(Number::Float(value)))
        }
        5 => {
            let value = reader.text()?;
            budget.string(value.len())?;
            Ok(SurrealValue::String(value.to_owned()))
        }
        6 => {
            let value = reader.bytes()?.to_vec();
            budget.string(value.len())?;
            Ok(SurrealValue::Bytes(value.into()))
        }
        7 => {
            let len = reader.container_len()?;
            let mut values = Vec::with_capacity(len);
            for _ in 0..len {
                values.push(decode_node(reader, depth + 1, budget)?);
            }
            Ok(SurrealValue::Array(Array::from(values)))
        }
        8 => {
            let len = reader.container_len()?;
            let mut values = Object::new();
            let mut previous: Option<String> = None;
            for _ in 0..len {
                if reader.array_len()? != 2 {
                    return Err(CodecError::Decode);
                }
                let key_text = reader.text()?;
                budget.string(key_text.len())?;
                let key = key_text.to_owned();
                if let Some(last) = previous.as_ref() {
                    match last.as_bytes().cmp(key.as_bytes()) {
                        Ordering::Less => {}
                        Ordering::Equal => return Err(CodecError::DuplicateObjectKey),
                        Ordering::Greater => return Err(CodecError::NonCanonical),
                    }
                }
                previous = Some(key.clone());
                values.insert(key, decode_node(reader, depth + 1, budget)?);
            }
            Ok(SurrealValue::Object(values))
        }
        9 => {
            if reader.array_len()? != 2 {
                return Err(CodecError::InvalidDatetime);
            }
            let seconds = reader.int()?;
            let nanos = u32::try_from(reader.uint()?).map_err(|_| CodecError::InvalidDatetime)?;
            Datetime::from_timestamp(seconds, nanos)
                .map(SurrealValue::Datetime)
                .ok_or(CodecError::InvalidDatetime)
        }
        10 => decode_decimal(reader),
        11 => {
            let bytes = reader.bytes()?;
            if bytes.len() != 16 {
                return Err(CodecError::InvalidUuid);
            }
            let mut hex = String::with_capacity(32);
            for byte in bytes {
                use std::fmt::Write as _;
                write!(&mut hex, "{byte:02x}").map_err(|_| CodecError::InvalidUuid)?;
            }
            let uuid = Uuid::try_from(hex).map_err(|_| CodecError::InvalidUuid)?;
            Ok(SurrealValue::Uuid(uuid))
        }
        12 => decode_set(reader, depth, budget),
        13 => decode_record_id(reader, budget),
        _ => Err(CodecError::UnsupportedValue),
    }
}

fn decode_decimal(reader: &mut Reader<'_>) -> Result<SurrealValue, CodecError> {
    if reader.array_len()? != 3 {
        return Err(CodecError::InvalidDecimal);
    }
    let negative = reader.boolean()?;
    let bytes = reader.bytes()?;
    if bytes.len() != 12 {
        return Err(CodecError::InvalidDecimal);
    }
    let coefficient = bytes
        .iter()
        .fold(0_u128, |value, byte| (value << 8) | u128::from(*byte));
    let scale = u32::try_from(reader.uint()?).map_err(|_| CodecError::InvalidDecimal)?;
    if coefficient > MAX_DECIMAL_COEFFICIENT || scale > 28 {
        return Err(CodecError::InvalidDecimal);
    }
    let mut digits = coefficient.to_string();
    if scale > 0 {
        let scale = scale as usize;
        if digits.len() <= scale {
            digits.insert_str(0, &"0".repeat(scale + 1 - digits.len()));
        }
        digits.insert(digits.len() - scale, '.');
    }
    if negative {
        digits.insert(0, '-');
    }
    let decimal = digits.parse().map_err(|_| CodecError::InvalidDecimal)?;
    Ok(SurrealValue::Number(Number::from_decimal(decimal)))
}

fn decode_set(
    reader: &mut Reader<'_>,
    depth: usize,
    budget: &mut Budget,
) -> Result<SurrealValue, CodecError> {
    let len = reader.container_len()?;
    let mut members = Vec::with_capacity(len);
    let mut previous: Option<Vec<u8>> = None;
    for _ in 0..len {
        let start = reader.offset;
        let member = decode_node(reader, depth + 1, budget)?;
        let encoded = reader.bytes[start..reader.offset].to_vec();
        if previous
            .as_ref()
            .is_some_and(|prior| prior.as_slice().cmp(&encoded) != Ordering::Less)
        {
            return Err(CodecError::SetOrder);
        }
        previous = Some(encoded);
        members.push(member);
    }
    let set = surrealdb::types::Set::from(members.clone());
    if set.len() != members.len() {
        return Err(CodecError::SetCardinality);
    }
    Ok(SurrealValue::Set(set))
}

fn decode_record_id(
    reader: &mut Reader<'_>,
    budget: &mut Budget,
) -> Result<SurrealValue, CodecError> {
    if reader.array_len()? != 2 {
        return Err(CodecError::InvalidRecordId);
    }
    let table_text = reader.text()?;
    budget.string(table_text.len())?;
    let table = table_text.to_owned();
    if table.is_empty() || reader.array_len()? != 2 {
        return Err(CodecError::InvalidRecordId);
    }
    let key_kind = reader.uint()?;
    let key = match key_kind {
        0 => RecordIdKey::Number(reader.int()?),
        1 => {
            let key_text = reader.text()?;
            budget.string(key_text.len())?;
            let text = key_text.to_owned();
            RecordIdKey::String(text)
        }
        2 => {
            let bytes = reader.bytes()?;
            if bytes.len() != 16 {
                return Err(CodecError::InvalidRecordId);
            }
            let mut hex = String::with_capacity(32);
            for byte in bytes {
                use std::fmt::Write as _;
                write!(&mut hex, "{byte:02x}").map_err(|_| CodecError::InvalidRecordId)?;
            }
            RecordIdKey::Uuid(Uuid::try_from(hex).map_err(|_| CodecError::InvalidRecordId)?)
        }
        _ => return Err(CodecError::InvalidRecordId),
    };
    Ok(SurrealValue::RecordId(RecordId::new(table, key)))
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn byte(&mut self) -> Result<u8, CodecError> {
        let byte = *self.bytes.get(self.offset).ok_or(CodecError::Decode)?;
        self.offset += 1;
        Ok(byte)
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], CodecError> {
        let end = self.offset.checked_add(len).ok_or(CodecError::Decode)?;
        let bytes = self.bytes.get(self.offset..end).ok_or(CodecError::Decode)?;
        self.offset = end;
        Ok(bytes)
    }

    fn head(&mut self) -> Result<(u8, u64), CodecError> {
        let initial = self.byte()?;
        let major = initial >> 5;
        let additional = initial & 0x1f;
        let value = match additional {
            0..=23 => u64::from(additional),
            24 => {
                let value = u64::from(self.byte()?);
                if value < 24 {
                    return Err(CodecError::NonCanonical);
                }
                value
            }
            25 => {
                let raw = self.take(2)?;
                let value = u64::from(u16::from_be_bytes([raw[0], raw[1]]));
                if value <= 0xff {
                    return Err(CodecError::NonCanonical);
                }
                value
            }
            26 => {
                let raw = self.take(4)?;
                let value = u64::from(u32::from_be_bytes(raw.try_into().unwrap()));
                if value <= 0xffff {
                    return Err(CodecError::NonCanonical);
                }
                value
            }
            27 => {
                let raw = self.take(8)?;
                let value = u64::from_be_bytes(raw.try_into().unwrap());
                if value <= 0xffff_ffff {
                    return Err(CodecError::NonCanonical);
                }
                value
            }
            _ => return Err(CodecError::Decode),
        };
        Ok((major, value))
    }

    fn expect_head(&mut self, major: u8, value: u64) -> Result<(), CodecError> {
        if self.head()? == (major, value) {
            Ok(())
        } else {
            Err(CodecError::Decode)
        }
    }

    fn uint(&mut self) -> Result<u64, CodecError> {
        let (major, value) = self.head()?;
        if major != 0 {
            return Err(CodecError::Decode);
        }
        Ok(value)
    }

    fn int(&mut self) -> Result<i64, CodecError> {
        let (major, value) = self.head()?;
        let int = match major {
            0 => i64::try_from(value).map_err(|_| CodecError::Decode)?,
            1 => i64::try_from(-1_i128 - i128::from(value)).map_err(|_| CodecError::Decode)?,
            _ => return Err(CodecError::Decode),
        };
        Ok(int)
    }

    fn array_len(&mut self) -> Result<usize, CodecError> {
        let (major, value) = self.head()?;
        if major != 4 {
            return Err(CodecError::Decode);
        }
        usize::try_from(value).map_err(|_| CodecError::ContainerLimit)
    }

    fn container_len(&mut self) -> Result<usize, CodecError> {
        let len = self.array_len()?;
        if len > MAX_CONTAINER_ITEMS {
            return Err(CodecError::ContainerLimit);
        }
        Ok(len)
    }

    fn bytes(&mut self) -> Result<&'a [u8], CodecError> {
        let (major, len) = self.head()?;
        if major != 2 {
            return Err(CodecError::Decode);
        }
        let len = usize::try_from(len).map_err(|_| CodecError::StringLimit)?;
        if len > MAX_STRING_BYTES {
            return Err(CodecError::StringLimit);
        }
        self.take(len)
    }

    fn text(&mut self) -> Result<&'a str, CodecError> {
        let (major, len) = self.head()?;
        if major != 3 {
            return Err(CodecError::Decode);
        }
        let len = usize::try_from(len).map_err(|_| CodecError::StringLimit)?;
        if len > MAX_STRING_BYTES {
            return Err(CodecError::StringLimit);
        }
        std::str::from_utf8(self.take(len)?).map_err(|_| CodecError::Decode)
    }

    fn boolean(&mut self) -> Result<bool, CodecError> {
        match self.byte()? {
            0xf4 => Ok(false),
            0xf5 => Ok(true),
            _ => Err(CodecError::Decode),
        }
    }

    fn expect_null(&mut self) -> Result<(), CodecError> {
        if self.byte()? == 0xf6 {
            Ok(())
        } else {
            Err(CodecError::Decode)
        }
    }
}

#[cfg(test)]
#[path = "native_values_draft_codec_tests.rs"]
mod tests;
