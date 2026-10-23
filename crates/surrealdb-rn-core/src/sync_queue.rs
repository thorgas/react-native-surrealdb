//! Bounded borrowed FIFO observation; no durable state or protocol changes.
use crate::sync_client::NativeSyncError;
use serde::Serialize;
use std::io::{self, Write};

pub(crate) const MAX_COMMITS: u32 = 64;
pub(crate) const MAX_BYTES: u32 = 4 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, uniffi::Record)]
pub struct NativePendingWindow {
    pub revision: u64,
    pub commits: Vec<String>,
    /// UTF-8 bytes of JSON-encoded `commits`, including escapes, quotes and brackets.
    pub bytes: u32,
    pub remaining_count: u32,
    pub first_commit_too_large: bool,
}

pub(crate) fn limits(count: u32, bytes: u32) -> Result<(), NativeSyncError> {
    if count == 0 || count > MAX_COMMITS || !(2..=MAX_BYTES).contains(&bytes) {
        return Err(NativeSyncError::InvalidInput);
    }
    Ok(())
}

struct BudgetWriter {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for BudgetWriter {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        if input.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("pending byte budget"));
        }
        self.bytes.extend_from_slice(input);
        Ok(input.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn bounded_json<T: Serialize>(value: &T, limit: usize) -> Option<Vec<u8>> {
    let mut writer = BudgetWriter {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut writer, value).ok()?;
    Some(writer.bytes)
}

pub(crate) fn window<'a, T: Serialize + 'a>(
    revision: u64,
    items: impl ExactSizeIterator<Item = &'a T>,
    count: u32,
    max_bytes: u32,
) -> Result<NativePendingWindow, NativeSyncError> {
    limits(count, max_bytes)?;
    let total = items.len();
    let mut result = NativePendingWindow {
        revision,
        commits: Vec::new(),
        bytes: 2,
        remaining_count: 0,
        first_commit_too_large: false,
    };
    for value in items.take(count as usize) {
        let available = max_bytes as usize - result.bytes as usize;
        let Some(raw) = bounded_json(value, available) else {
            result.first_commit_too_large = result.commits.is_empty();
            break;
        };
        let raw = String::from_utf8(raw).map_err(|_| NativeSyncError::Internal)?;
        let comma = usize::from(!result.commits.is_empty());
        let Some(quoted) = bounded_json(&raw, available.saturating_sub(comma)) else {
            result.first_commit_too_large = result.commits.is_empty();
            break;
        };
        result.bytes += (quoted.len() + comma) as u32;
        result.commits.push(raw);
    }
    result.remaining_count =
        u32::try_from(total - result.commits.len()).map_err(|_| NativeSyncError::Internal)?;
    Ok(result)
}

/// Same array-of-protocol-strings accounting as window, without aggregate cloning.
pub(crate) fn admits<'a, T: Serialize + 'a>(
    items: impl ExactSizeIterator<Item = &'a T>,
    candidate: &'a T,
    count: u32,
    bytes: u32,
) -> Result<(), NativeSyncError> {
    limits(count, bytes)?;
    if items.len() >= count as usize {
        return Err(NativeSyncError::QueueCapacity);
    }
    let mut charged = 2usize;
    let mut seen = false;
    for value in items.chain(std::iter::once(candidate)) {
        let available = bytes as usize - charged;
        let raw = bounded_json(value, available).ok_or(NativeSyncError::QueueCapacity)?;
        let raw = String::from_utf8(raw).map_err(|_| NativeSyncError::Internal)?;
        let comma = usize::from(seen);
        let quoted = bounded_json(&raw, available.saturating_sub(comma))
            .ok_or(NativeSyncError::QueueCapacity)?;
        charged += quoted.len() + comma;
        seen = true;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_utf8_escaped_array_budget_and_fifo() {
        let values = ["é\n\"", "second"];
        let raw = values
            .iter()
            .map(|v| serde_json::to_string(v).unwrap())
            .collect::<Vec<_>>();
        let exact = serde_json::to_vec(&raw).unwrap().len() as u32;
        let full = window(9, values.iter(), 2, exact).unwrap();
        assert_eq!(full.commits, raw);
        assert_eq!(full.bytes, exact);
        assert_eq!(full.remaining_count, 0);
        let short = window(9, values.iter(), 2, exact - 1).unwrap();
        assert_eq!(short.commits.len(), 1);
        assert_eq!(short.remaining_count, 1);
        assert!(!short.first_commit_too_large);
        let first = window(9, values.iter(), 2, 2).unwrap();
        assert!(first.first_commit_too_large);
        assert!(first.commits.is_empty());
        assert_eq!(first.bytes, 2);
        assert!(admits(values[..1].iter(), &values[1], 2, exact).is_ok());
        assert!(matches!(
            admits(values[..1].iter(), &values[1], 2, exact - 1),
            Err(NativeSyncError::QueueCapacity)
        ));
        assert!(matches!(
            admits(values.iter(), &"third", 2, exact),
            Err(NativeSyncError::QueueCapacity)
        ));
    }
}
