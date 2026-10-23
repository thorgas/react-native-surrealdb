//! Stateless, bounded native read replay. All access is serialized by the runtime mutex.
use std::collections::VecDeque;

use crate::sync_client::NativeSyncError;
use serde::Deserialize;
use serde_json::{Value, json};
use surrealdb_sync_client::{ClientRuntime, CommittedReadBatchV1};

pub(crate) const MAX_BATCHES: usize = 128;
pub(crate) const MAX_JOURNAL_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Cursor {
    process_epoch: String,
    revision: String,
}
struct Batch {
    before: u64,
    revision: u64,
    json: String,
}
#[derive(Default)]
pub(crate) struct ReadJournal {
    epoch: JournalEpoch,
    batches: VecDeque<Batch>,
    bytes: usize,
    view_cursors: VecDeque<String>,
}

pub(crate) fn capabilities() -> String {
    json!({"protocolVersion":1,"maxJournalBatches":MAX_BATCHES,"maxJournalBytes":MAX_JOURNAL_BYTES,"maxOutputBytes":MAX_OUTPUT_BYTES,"requiresMatchingNativeAbi":true,"predicateSubscriptions":false}).to_string()
}

/// Random prefix identifies a fresh cell/process; a checked counter guarantees
/// no epoch reuse within the lifetime of this journal, including recovery.
pub(crate) struct JournalEpoch {
    prefix: String,
    generation: u64,
}
impl Default for JournalEpoch {
    fn default() -> Self {
        Self {
            prefix: surrealdb::types::Uuid::new_v4().to_string(),
            generation: 0,
        }
    }
}
impl JournalEpoch {
    pub(crate) fn value(&self) -> String {
        format!("{}:{}", self.prefix, self.generation)
    }
    #[cfg(test)]
    pub(crate) fn exhaust_for_test(&mut self) {
        self.generation = u64::MAX;
    }
    pub(crate) fn advance(&mut self) -> Result<(), NativeSyncError> {
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or(NativeSyncError::RecoveryRequired)?;
        Ok(())
    }
}
impl ReadJournal {
    pub(crate) fn rotate(&mut self) -> Result<(), NativeSyncError> {
        self.epoch.advance()?;
        self.batches.clear();
        self.view_cursors.clear();
        self.bytes = 0;
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn exhaust_epoch_for_test(&mut self) {
        self.epoch.generation = u64::MAX;
    }
    fn cursor(&self, revision: u64) -> Value {
        json!({"processEpoch":self.epoch.value(),"revision":revision.to_string()})
    }
    pub(crate) fn view_epoch(&self) -> String {
        self.epoch.value()
    }
    pub(crate) fn issued_view_cursor(&self, cursor: &str) -> bool {
        self.view_cursors.iter().any(|issued| issued == cursor)
    }
    pub(crate) fn remember_view_cursor(&mut self, cursor: String) {
        while self.view_cursors.len() >= 64
            || self.view_cursors.iter().map(String::len).sum::<usize>() + cursor.len() > 256 * 1024
        {
            if self.view_cursors.pop_front().is_none() {
                return;
            }
        }
        self.view_cursors.push_back(cursor);
    }
    pub fn bootstrap(&self, runtime: &ClientRuntime<Value>) -> Result<Value, NativeSyncError> {
        let mut snapshot =
            serde_json::to_value(runtime.read_snapshot()).map_err(|_| NativeSyncError::Internal)?;
        snapshot_wire(&mut snapshot)?;
        Ok(json!({"snapshot":snapshot,"cursor":self.cursor(runtime.state().revision)}))
    }
    pub fn record(&mut self, batch: Option<CommittedReadBatchV1<Value>>) {
        let Some(batch) = batch else {
            return;
        };
        let encoded = (|| {
            let mut value = serde_json::to_value(&batch).ok()?;
            decimal(&mut value["beforeRevision"]).ok()?;
            decimal(&mut value["revision"]).ok()?;
            metadata_wire(&mut value["metadata"]).ok()?;
            for change in value["changes"].as_array_mut()? {
                if change["kind"] == "upsert" {
                    row_wire(&mut change["row"]).ok()?;
                }
            }
            serde_json::to_string(&value).ok()
        })();
        let Some(encoded) = encoded.filter(|s| s.len() <= MAX_JOURNAL_BYTES) else {
            self.batches.clear();
            self.bytes = 0;
            return;
        };
        while self.batches.len() >= MAX_BATCHES || self.bytes + encoded.len() > MAX_JOURNAL_BYTES {
            if let Some(old) = self.batches.pop_front() {
                self.bytes -= old.json.len();
            }
        }
        self.bytes += encoded.len();
        self.batches.push_back(Batch {
            before: batch.before_revision,
            revision: batch.revision,
            json: encoded,
        });
    }
    pub fn changes(
        &self,
        runtime: &ClientRuntime<Value>,
        cursor: &str,
        limit: u32,
    ) -> Result<Value, NativeSyncError> {
        if !(1..=MAX_BATCHES as u32).contains(&limit) || cursor.len() > 4096 {
            return Err(NativeSyncError::InvalidInput);
        }
        let cursor: Cursor =
            serde_json::from_str(cursor).map_err(|_| NativeSyncError::InvalidInput)?;
        let revision = cursor
            .revision
            .parse::<u64>()
            .map_err(|_| NativeSyncError::InvalidInput)?;
        if revision.to_string() != cursor.revision {
            return Err(NativeSyncError::InvalidInput);
        }
        let current = runtime.state().revision;
        let first = self
            .batches
            .iter()
            .position(|batch| batch.before == revision);
        if cursor.process_epoch != self.epoch.value()
            || revision > current
            || (revision < current && first.is_none())
        {
            let mut reset = self.bootstrap(runtime)?;
            reset["kind"] = json!("reset_required");
            return Ok(reset);
        }
        let mut batches = Vec::new();
        let mut after = revision;
        if revision < current {
            for batch in self
                .batches
                .iter()
                .skip(first.unwrap_or(0))
                .take(limit as usize)
            {
                if batch.before != after {
                    return Err(NativeSyncError::Internal);
                }
                batches.push(
                    serde_json::from_str::<Value>(&batch.json)
                        .map_err(|_| NativeSyncError::Internal)?,
                );
                after = batch.revision;
            }
        }
        Ok(json!({"kind":"batches","batches":batches,"cursor":self.cursor(after)}))
    }
}
pub(crate) fn output(value: &Value) -> Result<String, NativeSyncError> {
    let json = serde_json::to_string(value).map_err(|_| NativeSyncError::Internal)?;
    if json.len() > MAX_OUTPUT_BYTES {
        return Err(NativeSyncError::ReadOutputTooLarge);
    }
    Ok(json)
}
fn decimal(value: &mut Value) -> Result<(), NativeSyncError> {
    *value = Value::String(value.as_u64().ok_or(NativeSyncError::Internal)?.to_string());
    Ok(())
}
fn frontier(value: &mut Value) -> Result<(), NativeSyncError> {
    decimal(&mut value["epoch"])?;
    decimal(&mut value["sequence"])
}
pub(crate) fn metadata_wire(value: &mut Value) -> Result<(), NativeSyncError> {
    decimal(&mut value["revision"])?;
    decimal(&mut value["scope"]["subscriptionRevision"])?;
    if !value["scope"]["authorizationRevision"].is_null() {
        decimal(&mut value["scope"]["authorizationRevision"])?;
    }
    if !value["asOf"].is_null() {
        frontier(&mut value["asOf"])?;
    }
    if value["coverage"]["kind"] == "authorized_reset" {
        frontier(&mut value["coverage"]["cursor"])?;
    }
    Ok(())
}
fn row_wire(value: &mut Value) -> Result<(), NativeSyncError> {
    if !value["confirmed"]["version"].is_null() {
        decimal(&mut value["confirmed"]["version"])?;
    }
    match value["optimistic"]["state"].as_str() {
        Some("present") if value["optimistic"]["baseVersion"].is_object() => {
            decimal(&mut value["optimistic"]["baseVersion"]["exact"])?
        }
        Some("deleted") => decimal(&mut value["optimistic"]["baseVersion"])?,
        _ => {}
    }
    for accepted in value["accepted"]
        .as_array_mut()
        .ok_or(NativeSyncError::Internal)?
    {
        decimal(&mut accepted["sequence"])?;
    }
    Ok(())
}
fn snapshot_wire(value: &mut Value) -> Result<(), NativeSyncError> {
    metadata_wire(&mut value["metadata"])?;
    for row in value["rows"]
        .as_object_mut()
        .ok_or(NativeSyncError::Internal)?
        .values_mut()
    {
        row_wire(row)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use surrealdb_sync_client::DurableClientState;
    use surrealdb_sync_protocol::{
        BaseVersion, ClientCommit, ClientCommitId, ClientId, CommitIdentity, Fingerprint,
        Operation, PartitionId, RecordId, ScopeIdentity,
    };
    fn runtime() -> ClientRuntime<Value> {
        ClientRuntime::open(DurableClientState::empty(
            PartitionId("p".into()),
            ClientId("c".into()),
            ScopeIdentity("s".into()),
            1,
        ))
        .unwrap()
    }
    fn enqueue(
        runtime: &mut ClientRuntime<Value>,
        journal: &mut ReadJournal,
        id: usize,
        value: Value,
    ) {
        let prepared = runtime
            .prepare_enqueue(ClientCommit {
                identity: CommitIdentity {
                    client_commit_id: ClientCommitId(id.to_string()),
                    fingerprint: Fingerprint(format!("f{id}")),
                },
                operations: vec![Operation::Upsert {
                    record_id: RecordId(format!("person:{id}")),
                    base_version: BaseVersion::Absent,
                    value,
                    reference: None,
                }],
            })
            .unwrap();
        journal.record(runtime.install_with_read_batch(prepared).unwrap());
    }
    #[test]
    fn counters_preserve_u64_and_never_rewrite_user_payload_numbers() {
        let mut value = json!({"metadata":{"revision":u64::MAX,"scope":{"subscriptionRevision":u64::MAX,"authorizationRevision":u64::MAX},"asOf":{"epoch":u64::MAX,"sequence":u64::MAX},"coverage":{"kind":"authorized_reset","cursor":{"epoch":u64::MAX,"sequence":u64::MAX}}},"rows":{"a":{"confirmed":{"state":"present","version":u64::MAX,"value":{"version":42,"epoch":7}},"optimistic":{"state":"present","baseVersion":{"exact":u64::MAX},"value":{"revision":99}},"accepted":[{"sequence":u64::MAX}]},"b":{"confirmed":{"state":"tombstone","version":u64::MAX},"optimistic":{"state":"deleted","baseVersion":u64::MAX},"accepted":[]}}});
        snapshot_wire(&mut value).unwrap();
        assert_eq!(value["metadata"]["revision"], u64::MAX.to_string());
        assert_eq!(
            value["metadata"]["coverage"]["cursor"]["epoch"],
            u64::MAX.to_string()
        );
        assert_eq!(value["rows"]["a"]["confirmed"]["value"]["version"], 42);
        assert_eq!(value["rows"]["a"]["optimistic"]["value"]["revision"], 99);
        assert_eq!(
            value["rows"]["a"]["optimistic"]["baseVersion"]["exact"],
            u64::MAX.to_string()
        );
        assert_eq!(
            value["rows"]["b"]["optimistic"]["baseVersion"],
            u64::MAX.to_string()
        );
        assert_eq!(
            value["rows"]["a"]["accepted"][0]["sequence"],
            u64::MAX.to_string()
        );
    }
    #[test]
    fn bounded_replay_reports_overflow_other_handles_and_bad_cursors() {
        let mut runtime = runtime();
        let mut journal = ReadJournal::default();
        let start = journal.bootstrap(&runtime).unwrap()["cursor"].to_string();
        for id in 0..MAX_BATCHES + 1 {
            enqueue(&mut runtime, &mut journal, id, json!({"value":id}));
        }
        assert_eq!(journal.batches.len(), MAX_BATCHES);
        assert!(journal.bytes <= MAX_JOURNAL_BYTES);
        assert_eq!(
            journal.changes(&runtime, &start, 128).unwrap()["kind"],
            "reset_required"
        );
        let current = journal.bootstrap(&runtime).unwrap()["cursor"].to_string();
        assert!(
            journal.changes(&runtime, &current, 1).unwrap()["batches"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        let other = ReadJournal::default();
        assert_eq!(
            other.changes(&runtime, &current, 1).unwrap()["kind"],
            "reset_required"
        );
        assert!(matches!(
            journal.changes(&runtime, &current, 0),
            Err(NativeSyncError::InvalidInput)
        ));
        assert!(matches!(
            journal.changes(&runtime, &current, 129),
            Err(NativeSyncError::InvalidInput)
        ));
        assert!(matches!(
            journal.changes(&runtime, "{}", 1),
            Err(NativeSyncError::InvalidInput)
        ));
        enqueue(
            &mut runtime,
            &mut journal,
            999,
            json!({"large":"x".repeat(MAX_JOURNAL_BYTES)}),
        );
        assert!(journal.batches.is_empty());
        assert_eq!(journal.bytes, 0);
        assert_eq!(
            journal.changes(&runtime, &current, 1).unwrap()["kind"],
            "reset_required"
        );
        assert!(matches!(
            output(&json!({"large":"x".repeat(MAX_OUTPUT_BYTES)})),
            Err(NativeSyncError::ReadOutputTooLarge)
        ));
    }
}
