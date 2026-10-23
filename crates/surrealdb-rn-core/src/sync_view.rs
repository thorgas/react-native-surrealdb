//! Bounded local-view reads. No storage query, network, or full snapshot allocation.
use crate::{
    sync_client::NativeSyncError,
    sync_read::{ReadJournal, metadata_wire},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{self, Write},
};
use surrealdb_sync_client::{ClientRuntime, ConflictResolution, optimistic_operations};
use surrealdb_sync_protocol::{DurableOutcome, Operation, RecordId, RecordState};

const MAX_INPUT: usize = 16 * 1024;
const MAX_OUTPUT: usize = 256 * 1024;
const MAX_VISITS: u32 = 16_384;
const MAX_NODES: u32 = 65_536;
const MAX_VALUE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Selector {
    All,
    Eq { path: Vec<String>, value: Value },
    And { terms: Vec<Selector> },
}
#[derive(Deserialize, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    Count {
        #[serde(rename = "protocolVersion")]
        protocol_version: u8,
        selector: Selector,
    },
    Page {
        #[serde(rename = "protocolVersion")]
        protocol_version: u8,
        selector: Selector,
        projection: Vec<Vec<String>>,
        limit: u32,
        cursor: Option<String>,
    },
}
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Cursor {
    token: String,
    process_epoch: String,
    revision: String,
    scope: Value,
    query: String,
    last_record_id: String,
}
#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct Work {
    records_visited: u32,
    operations_visited: u32,
    value_nodes_visited: u32,
    value_bytes_visited: usize,
}
fn invalid<T>() -> Result<T, NativeSyncError> {
    Err(NativeSyncError::InvalidInput)
}
fn budget<T>() -> Result<T, NativeSyncError> {
    Err(NativeSyncError::ReadOutputTooLarge)
}
impl Work {
    fn visit(&mut self, operation: bool) -> Result<(), NativeSyncError> {
        if operation {
            self.operations_visited += 1;
        } else {
            self.records_visited += 1;
        }
        if self.records_visited + self.operations_visited > MAX_VISITS {
            return budget();
        }
        Ok(())
    }
    fn bytes(&mut self, n: usize) -> Result<(), NativeSyncError> {
        self.value_bytes_visited = self
            .value_bytes_visited
            .checked_add(n)
            .ok_or(NativeSyncError::ReadOutputTooLarge)?;
        if self.value_bytes_visited > MAX_VALUE_BYTES {
            return budget();
        }
        Ok(())
    }
    fn value(&mut self, value: &Value, depth: u32) -> Result<(), NativeSyncError> {
        self.value_nodes_visited += 1;
        if self.value_nodes_visited > MAX_NODES || depth > 64 {
            return budget();
        }
        match value {
            Value::String(s) => self.bytes(s.len())?,
            Value::Array(a) => {
                for v in a {
                    self.value(v, depth + 1)?;
                }
            }
            Value::Object(o) => {
                for (k, v) in o {
                    self.bytes(k.len())?;
                    self.value(v, depth + 1)?;
                }
            }
            _ => self.bytes(24)?,
        }
        Ok(())
    }
}
fn path_valid(path: &[String]) -> bool {
    !path.is_empty()
        && path.len() <= 8
        && path.iter().all(|s| {
            !s.is_empty()
                && s.len() <= 128
                && !matches!(
                    s.as_str(),
                    "__proto__" | "constructor" | "prototype" | "$surreal"
                )
                && !s.chars().any(char::is_control)
        })
}
fn validate_selector(selector: &Selector) -> Result<(), NativeSyncError> {
    match selector {
        Selector::All => Ok(()),
        Selector::Eq { path, value }
            if path_valid(path)
                && (value.is_null()
                    || value.is_boolean()
                    || value.as_str().is_some_and(|s| s.len() <= 1024)) =>
        {
            Ok(())
        }
        Selector::And { terms } if !terms.is_empty() && terms.len() <= 8 => {
            for term in terms {
                if !matches!(term, Selector::Eq { .. }) {
                    return invalid();
                }
                validate_selector(term)?;
            }
            Ok(())
        }
        _ => invalid(),
    }
}
fn field<'a>(
    mut value: &'a Value,
    path: &[String],
    work: &mut Work,
) -> Result<Option<&'a Value>, NativeSyncError> {
    for key in path {
        // Tagged lossless values are canonical atoms, never traversable domain objects.
        if value
            .as_object()
            .is_some_and(|o| o.contains_key("$surreal"))
        {
            return Ok(None);
        }
        work.value_nodes_visited += 1;
        if work.value_nodes_visited > MAX_NODES {
            return budget();
        }
        let Some(next) = value.as_object().and_then(|o| o.get(key)) else {
            return Ok(None);
        };
        value = next;
    }
    Ok(Some(value))
}
fn matches(selector: &Selector, value: &Value, work: &mut Work) -> Result<bool, NativeSyncError> {
    match selector {
        Selector::All => Ok(true),
        Selector::Eq {
            path,
            value: wanted,
        } => {
            let got = field(value, path, work)?;
            // A scalar comparison never recursively compares a large composite value.
            Ok(
                got.is_some_and(|v| {
                    (v.is_null() || v.is_boolean() || v.is_string()) && v == wanted
                }),
            )
        }
        Selector::And { terms } => {
            for term in terms {
                if !matches(term, value, work)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
    }
}
fn effective<'a>(
    runtime: &'a ClientRuntime<Value>,
    work: &mut Work,
) -> Result<BTreeMap<&'a RecordId, &'a Value>, NativeSyncError> {
    let state = runtime.state();
    let mut rows = BTreeMap::new();
    for (id, row) in &state.confirmed {
        work.visit(false)?;
        if id.0.len() > 4096 {
            return budget();
        }
        work.bytes(id.0.len())?;
        if let RecordState::Present { value, .. } = row {
            work.value(value, 0)?;
            rows.insert(id, value);
        }
    }
    // Every historical operation is charged, including ones excluded by overlay rules.
    for commit in state
        .outcomes
        .iter()
        .map(|o| &o.local_commit)
        .chain(&state.outbox)
    {
        work.visit(true)?; // Empty commits/history also cost work.
        work.bytes(commit.identity.client_commit_id.0.len())?;
        work.bytes(commit.identity.fingerprint.0.len())?;
        for operation in &commit.operations {
            work.visit(true)?;
            work.bytes(operation.record_id().0.len())?;
            if let Operation::Upsert { value, .. } = operation {
                work.value(value, 0)?;
            }
        }
    }
    for _ in state
        .outcomes
        .iter()
        .map(|_| ())
        .chain(state.outbox.iter().map(|_| ()))
    {
        work.visit(true)?;
    }
    for operation in optimistic_operations(state) {
        work.visit(true)?;
        match operation {
            Operation::Upsert {
                record_id, value, ..
            } => {
                rows.insert(record_id, value);
            }
            Operation::Delete { record_id, .. } => {
                rows.remove(record_id);
            }
        }
    }
    Ok(rows)
}
fn summaries(
    runtime: &ClientRuntime<Value>,
    ids: &BTreeSet<&RecordId>,
    work: &mut Work,
) -> Result<Value, NativeSyncError> {
    let mut rows = serde_json::Map::new();
    for id in ids {
        rows.insert(
            id.0.clone(),
            json!({"pending":[],"conflicts":[],"accepted":[]}),
        );
    }
    for commit in &runtime.state().outbox {
        work.visit(true)?;
        for op in &commit.operations {
            work.visit(true)?;
            if ids.contains(op.record_id()) {
                work.bytes(
                    commit.identity.client_commit_id.0.len() + commit.identity.fingerprint.0.len(),
                )?;
                rows[&op.record_id().0]["pending"]
                    .as_array_mut()
                    .ok_or(NativeSyncError::Internal)?
                    .push(
                        serde_json::to_value(&commit.identity)
                            .map_err(|_| NativeSyncError::Internal)?,
                    );
            }
        }
    }
    for resolved in &runtime.state().outcomes {
        work.visit(true)?;
        for op in &resolved.local_commit.operations {
            work.visit(true)?;
            if !ids.contains(op.record_id()) {
                continue;
            }
            work.bytes(
                resolved.local_commit.identity.client_commit_id.0.len()
                    + resolved.local_commit.identity.fingerprint.0.len(),
            )?;
            if let Some(
                ConflictResolution::KeepLocal { replacement }
                | ConflictResolution::Merge { replacement },
            ) = &resolved.resolution
            {
                work.bytes(replacement.client_commit_id.0.len() + replacement.fingerprint.0.len())?;
            }
            let row = rows
                .get_mut(&op.record_id().0)
                .ok_or(NativeSyncError::Internal)?;
            match &resolved.outcome {
                DurableOutcome::Accepted { sequence,.. } => row["accepted"].as_array_mut().ok_or(NativeSyncError::Internal)?.push(json!({"identity":resolved.local_commit.identity,"sequence":sequence.to_string(),"status":if resolved.covered_by_reset {"covered_by_authorized_reset"} else {"awaiting_pull_epoch_unknown"}})),
                DurableOutcome::Conflict { .. } => row["conflicts"].as_array_mut().ok_or(NativeSyncError::Internal)?.push(json!({"identity":resolved.local_commit.identity,"resolution":resolved.resolution})),
                DurableOutcome::Rejected { .. } => {}
            }
        }
    }
    Ok(Value::Object(rows))
}
/// This is a local observed count, not a domain-validity or backend-freshness claim.
pub(crate) fn read(
    runtime: &ClientRuntime<Value>,
    journal: &ReadJournal,
    input: &str,
) -> Result<Value, NativeSyncError> {
    if input.len() > MAX_INPUT {
        return invalid();
    }
    let request: Request =
        serde_json::from_str(input).map_err(|_| NativeSyncError::InvalidInput)?;
    let (version, selector) = match &request {
        Request::Count {
            protocol_version,
            selector,
        }
        | Request::Page {
            protocol_version,
            selector,
            ..
        } => (*protocol_version, selector),
    };
    if version != 1 {
        return invalid();
    }
    validate_selector(selector)?;
    let selector = selector.clone();
    for text in [
        &runtime.state().partition_id.0,
        &runtime.state().requested_scope.0,
    ] {
        if text.len() > 4096 {
            return budget();
        }
    }
    let mut metadata =
        serde_json::to_value(runtime.read_metadata()).map_err(|_| NativeSyncError::Internal)?;
    metadata_wire(&mut metadata)?;
    let query = match &request {
        Request::Count { .. } => String::new(),
        Request::Page {
            selector,
            projection,
            limit,
            ..
        } => {
            if !(1..=64).contains(limit)
                || projection.is_empty()
                || projection.len() > 16
                || projection.iter().any(|p| !path_valid(p))
            {
                return invalid();
            }
            let unique: BTreeSet<_> = projection.iter().collect();
            if unique.len() != projection.len() {
                return invalid();
            }
            serde_json::to_string(&(selector, projection, limit))
                .map_err(|_| NativeSyncError::Internal)?
        }
    };
    if query.len() > 4096 {
        return invalid();
    }
    let mut after = None;
    if let Request::Page {
        cursor: Some(raw), ..
    } = &request
    {
        if raw.len() > 8192 {
            return invalid();
        }
        let cursor: Cursor =
            serde_json::from_str(raw).map_err(|_| NativeSyncError::InvalidInput)?;
        if serde_json::to_string(&cursor).map_err(|_| NativeSyncError::Internal)? != *raw {
            return invalid();
        }
        if cursor.process_epoch != journal.view_epoch()
            || cursor.revision != runtime.state().revision.to_string()
            || cursor.scope != metadata["scope"]
            || !journal.issued_view_cursor(raw)
        {
            return Ok(json!({"protocolVersion":1,"kind":"stale_cursor","metadata":metadata}));
        }
        if cursor.query != query {
            return invalid();
        }
        after = Some(cursor.last_record_id);
    }
    let mut work = Work::default();
    let effective = effective(runtime, &mut work)?;
    match request {
        Request::Count { .. } => {
            let mut count = 0u64;
            for value in effective.values() {
                work.visit(false)?;
                if matches(&selector, value, &mut work)? {
                    count += 1;
                }
            }
            Ok(
                json!({"protocolVersion":1,"kind":"count","observedCount":count.to_string(),"metadata":metadata,"work":work}),
            )
        }
        Request::Page {
            projection, limit, ..
        } => {
            let mut selected = Vec::new();
            let mut has_more = false;
            for (id, value) in effective {
                work.visit(false)?;
                if after.as_ref().is_some_and(|last| id.0 <= *last)
                    || !matches(&selector, value, &mut work)?
                {
                    continue;
                }
                if selected.len() == limit as usize {
                    has_more = true;
                    break;
                }
                selected.push((id, value));
            }
            let ids = selected.iter().map(|(id, _)| *id).collect();
            let summary = summaries(runtime, &ids, &mut work)?;
            let mut rows = Vec::new();
            for (id, value) in &selected {
                let mut fields = Vec::new();
                for path in &projection {
                    let item = field(value, path, &mut work)?;
                    if let Some(v) = item {
                        work.value(v, 0)?;
                    }
                    fields.push(json!({"path":path,"present":item.is_some(),"value":item}));
                }
                rows.push(json!({"recordId":id.0,"fields":fields,"pending":summary[&id.0]["pending"],"conflicts":summary[&id.0]["conflicts"],"accepted":summary[&id.0]["accepted"]}));
            }
            let cursor = if has_more {
                Some(
                    serde_json::to_string(&Cursor {
                        token: surrealdb::types::Uuid::new_v4().to_string(),
                        process_epoch: journal.view_epoch(),
                        revision: runtime.state().revision.to_string(),
                        scope: metadata["scope"].clone(),
                        query,
                        last_record_id: selected
                            .last()
                            .ok_or(NativeSyncError::Internal)?
                            .0
                            .0
                            .clone(),
                    })
                    .map_err(|_| NativeSyncError::Internal)?,
                )
            } else {
                None
            };
            if cursor.as_ref().is_some_and(|s| s.len() > 8192) {
                return budget();
            }
            Ok(
                json!({"protocolVersion":1,"kind":"page","rows":rows,"nextCursor":cursor,"metadata":metadata,"work":work}),
            )
        }
    }
}
struct BoundedWriter(Vec<u8>);
impl Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.0.len() + bytes.len() > MAX_OUTPUT {
            return Err(io::Error::other("bounded view output"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub(crate) fn output(value: &Value) -> Result<String, NativeSyncError> {
    let mut writer = BoundedWriter(Vec::new());
    serde_json::to_writer(&mut writer, value).map_err(|_| NativeSyncError::ReadOutputTooLarge)?;
    String::from_utf8(writer.0).map_err(|_| NativeSyncError::Internal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use surrealdb_sync_client::DurableClientState;
    use surrealdb_sync_protocol::{
        BaseVersion, ClientCommit, ClientCommitId, ClientId, CommitIdentity, Fingerprint,
        PartitionId, ScopeIdentity,
    };
    fn runtime(n: usize) -> ClientRuntime<Value> {
        let mut state = DurableClientState::empty(
            PartitionId("p".into()),
            ClientId("c".into()),
            ScopeIdentity("s".into()),
            1,
        );
        for i in 0..n {
            state.confirmed.insert(RecordId(format!("item:{i:05}")),RecordState::Present{value:json!({"category":"a","name":format!("name {i}"),"large":"x".repeat(1024)}),version:1,reference:None});
        }
        ClientRuntime::open(state).unwrap()
    }
    fn count() -> String {
        json!({"protocolVersion":1,"operation":"count","selector":{"kind":"eq","path":["category"],"value":"a"}}).to_string()
    }
    fn page(cursor: Option<&str>) -> String {
        json!({"protocolVersion":1,"operation":"page","selector":{"kind":"all"},"projection":[["name"],["missing"]],"limit":2,"cursor":cursor}).to_string()
    }
    #[test]
    fn projected_page_is_bounded_and_unknown_coverage_is_explicit() {
        let runtime = runtime(1000);
        let journal = ReadJournal::default();
        let result = read(&runtime, &journal, &page(None)).unwrap();
        assert_eq!(result["rows"].as_array().unwrap().len(), 2);
        assert_eq!(result["rows"][0]["fields"][0]["value"], "name 0");
        assert_eq!(result["rows"][0]["fields"][1]["present"], false);
        assert_eq!(result["metadata"]["coverage"]["kind"], "unknown");
        let encoded = output(&result).unwrap();
        assert!(encoded.len() < 4096);
        let snapshot_bytes = serde_json::to_vec(&runtime.read_snapshot()).unwrap().len();
        assert!(snapshot_bytes > 1_000_000);
        eprintln!(
            "synthetic_local_view_transfer source_records=1000 snapshot_bytes={snapshot_bytes} page_bytes={} work={}",
            encoded.len(),
            result["work"]
        );
        assert_eq!(
            read(&runtime, &journal, &count()).unwrap()["observedCount"],
            "1000"
        );
    }
    #[test]
    fn issued_cursors_bind_query_revision_process_and_are_revocable() {
        let mut runtime = runtime(3);
        let mut journal = ReadJournal::default();
        let first = read(&runtime, &journal, &page(None)).unwrap();
        let cursor = first["nextCursor"].as_str().unwrap();
        assert_eq!(
            read(&runtime, &journal, &page(Some(cursor))).unwrap()["kind"],
            "stale_cursor"
        );
        journal.remember_view_cursor(cursor.into());
        let next = read(&runtime, &journal, &page(Some(cursor))).unwrap();
        assert_eq!(next["rows"][0]["recordId"], "item:00002");
        assert_eq!(next["nextCursor"], Value::Null);
        let mut changed: Value = serde_json::from_str(&page(Some(cursor))).unwrap();
        changed["limit"] = json!(1);
        assert!(read(&runtime, &journal, &changed.to_string()).is_err());
        let mut forged: Cursor = serde_json::from_str(cursor).unwrap();
        forged.last_record_id = "item:00001x".into();
        let rejected = read(
            &runtime,
            &journal,
            &page(Some(&serde_json::to_string(&forged).unwrap())),
        )
        .unwrap();
        assert_eq!(rejected["kind"], "stale_cursor");
        assert!(rejected.get("rows").is_none());
        assert_eq!(
            read(&runtime, &ReadJournal::default(), &page(Some(cursor))).unwrap()["kind"],
            "stale_cursor"
        );
        enqueue(
            &mut runtime,
            Operation::Upsert {
                record_id: RecordId("item:00003".into()),
                base_version: BaseVersion::Absent,
                value: json!({"category":"a"}),
                reference: None,
            },
        );
        assert_eq!(
            read(&runtime, &journal, &page(Some(cursor))).unwrap()["kind"],
            "stale_cursor"
        );
        journal.rotate().unwrap();
        assert!(!journal.issued_view_cursor(cursor));
    }
    fn enqueue(runtime: &mut ClientRuntime<Value>, operation: Operation<Value>) {
        let prepared = runtime
            .prepare_enqueue(ClientCommit {
                identity: CommitIdentity {
                    client_commit_id: ClientCommitId("local".into()),
                    fingerprint: Fingerprint("f".into()),
                },
                operations: vec![operation],
            })
            .unwrap();
        runtime.install(prepared).unwrap();
    }
    #[test]
    fn overlay_and_pending_match_existing_snapshot_semantics() {
        let mut runtime = runtime(2);
        enqueue(
            &mut runtime,
            Operation::Delete {
                record_id: RecordId("item:00000".into()),
                base_version: 1,
            },
        );
        let journal = ReadJournal::default();
        let result = read(&runtime, &journal, &count()).unwrap();
        assert_eq!(result["observedCount"], "1");
        let mut work = Work::default();
        let borrowed = effective(&runtime, &mut work).unwrap();
        let snapshot = runtime.read_snapshot();
        for (id, value) in borrowed {
            match snapshot.rows[id].optimistic.as_ref().unwrap() {
                surrealdb_sync_client::OptimisticRecord::Present {
                    value: expected, ..
                } => assert_eq!(value, expected),
                _ => panic!("effective deleted row"),
            }
        }
    }
    #[test]
    fn accepted_and_conflict_provenance_stay_explicit() {
        use surrealdb_sync_protocol::{PushResponse, SchemaVersion};
        let journal = ReadJournal::default();
        let mut accepted = runtime(0);
        enqueue(
            &mut accepted,
            Operation::Upsert {
                record_id: RecordId("new".into()),
                base_version: BaseVersion::Absent,
                value: json!({"category":"a","name":"local"}),
                reference: None,
            },
        );
        let pending_page = read(&accepted, &journal, &page(None)).unwrap();
        assert_eq!(
            pending_page["rows"][0]["pending"].as_array().unwrap().len(),
            1
        );
        assert_eq!(pending_page["rows"][0]["accepted"], json!([]));
        let identity = accepted.state().outbox[0].identity.clone();
        let prepared = accepted
            .prepare_push_response(PushResponse {
                schema_version: SchemaVersion::V1,
                partition_id: PartitionId("p".into()),
                client_id: ClientId("c".into()),
                outcome: DurableOutcome::Accepted {
                    identity,
                    sequence: 1,
                    id_mappings: vec![],
                },
            })
            .unwrap();
        accepted.install(prepared).unwrap();
        assert_eq!(
            read(&accepted, &journal, &count()).unwrap()["observedCount"],
            "1"
        );
        let response = read(&accepted, &journal, &page(None)).unwrap();
        assert_eq!(response["rows"][0]["pending"], json!([]));
        assert_eq!(
            response["rows"][0]["accepted"][0]["status"],
            "awaiting_pull_epoch_unknown"
        );
        let mut conflicted = runtime(1);
        enqueue(
            &mut conflicted,
            Operation::Upsert {
                record_id: RecordId("item:00000".into()),
                base_version: BaseVersion::Exact(1),
                value: json!({"category":"b","name":"pending"}),
                reference: None,
            },
        );
        assert_eq!(
            read(&conflicted, &journal, &count()).unwrap()["observedCount"],
            "0"
        );
        let identity = conflicted.state().outbox[0].identity.clone();
        let prepared = conflicted
            .prepare_push_response(PushResponse {
                schema_version: SchemaVersion::V1,
                partition_id: PartitionId("p".into()),
                client_id: ClientId("c".into()),
                outcome: DurableOutcome::Conflict {
                    identity,
                    record_id: RecordId("item:00000".into()),
                    authoritative: RecordState::Present {
                        value: json!({"category":"a","name":"server"}),
                        version: 2,
                        reference: None,
                    },
                },
            })
            .unwrap();
        conflicted.install(prepared).unwrap();
        assert_eq!(
            read(&conflicted, &journal, &count()).unwrap()["observedCount"],
            "1"
        );
        let response = read(&conflicted, &journal, &page(None)).unwrap();
        assert_eq!(response["rows"][0]["fields"][0]["value"], "name 0");
        // The existing confirmed projection is retained; the conflict receipt is separate audit metadata.
        assert_eq!(
            response["rows"][0]["conflicts"][0]["resolution"],
            Value::Null
        );
    }

    #[test]
    fn canonical_atoms_cannot_be_queried_through_wire_tag_internals() {
        use surrealdb_sync_protocol::CanonicalValue;
        let canonical = CanonicalValue::Object(BTreeMap::from([
            ("amount".into(), CanonicalValue::Int(i64::MAX)),
            ("bytes".into(), CanonicalValue::Bytes(vec![0, 1, 255])),
            (
                "ordinary".into(),
                CanonicalValue::Object(BTreeMap::from([(
                    "value".into(),
                    CanonicalValue::String("42".into()),
                )])),
            ),
        ]));
        let value = crate::sync_http_codec::canonical_to_json(canonical).unwrap();
        let mut state = runtime(0).state().clone();
        state.confirmed.insert(
            RecordId("atom".into()),
            RecordState::Present {
                value: value.clone(),
                version: 1,
                reference: None,
            },
        );
        let runtime = ClientRuntime::open(state).unwrap();
        let journal = ReadJournal::default();
        for (path, wanted) in [
            (vec!["amount", "value"], i64::MAX.to_string()),
            (vec!["bytes", "base64"], "AAH/".into()),
        ] {
            let query = json!({"protocolVersion":1,"operation":"count","selector":{"kind":"eq","path":path,"value":wanted}});
            assert_eq!(
                read(&runtime, &journal, &query.to_string()).unwrap()["observedCount"],
                "0"
            );
        }
        let query = json!({"protocolVersion":1,"operation":"count","selector":{"kind":"eq","path":["ordinary","value"],"value":"42"}});
        assert_eq!(
            read(&runtime, &journal, &query.to_string()).unwrap()["observedCount"],
            "1"
        );
        let query = json!({"protocolVersion":1,"operation":"page","selector":{"kind":"all"},"projection":[["amount"],["bytes"],["ordinary","value"],["amount","value"]],"limit":1});
        let response = read(&runtime, &journal, &query.to_string()).unwrap();
        assert_eq!(response["rows"][0]["fields"][0]["value"], value["amount"]);
        assert_eq!(response["rows"][0]["fields"][1]["value"], value["bytes"]);
        assert_eq!(response["rows"][0]["fields"][2]["value"], "42");
        assert_eq!(response["rows"][0]["fields"][3]["present"], false);
    }

    #[test]
    fn malformed_and_unbounded_work_fail_without_snapshot_fallback() {
        let runtime = runtime(1);
        let journal = ReadJournal::default();
        for selector in [
            json!({"kind":"eq","path":["__proto__"],"value":"a"}),
            json!({"kind":"eq","path":["category"],"value":9007199254740993u64}),
            json!({"kind":"and","terms":[{"kind":"all"}]}),
        ] {
            assert!(matches!(
                read(
                    &runtime,
                    &journal,
                    &json!({"protocolVersion":1,"operation":"count","selector":selector})
                        .to_string()
                ),
                Err(NativeSyncError::InvalidInput)
            ));
        }
        let mut state = runtime.state().clone();
        state.confirmed.insert(
            RecordId("huge".into()),
            RecordState::Present {
                value: json!({"blob":"z".repeat(MAX_VALUE_BYTES+1)}),
                version: 1,
                reference: None,
            },
        );
        let big = ClientRuntime::open(state).unwrap();
        assert!(matches!(
            read(&big, &journal, &count()),
            Err(NativeSyncError::ReadOutputTooLarge)
        ));
        assert!(matches!(
            read(&super::tests::runtime(9000), &journal, &count()),
            Err(NativeSyncError::ReadOutputTooLarge)
        ));
        let mut state = super::tests::runtime(0).state().clone();
        for i in 0..9000 {
            state.confirmed.insert(
                RecordId(format!("empty:{i}")),
                RecordState::Present {
                    value: json!({}),
                    version: 1,
                    reference: None,
                },
            );
        }
        assert!(matches!(
            read(&ClientRuntime::open(state).unwrap(), &journal, &count()),
            Err(NativeSyncError::ReadOutputTooLarge)
        ));
        assert!(matches!(
            output(&json!({"large":"x".repeat(MAX_OUTPUT)})),
            Err(NativeSyncError::ReadOutputTooLarge)
        ));
    }
    #[test]
    fn cursor_retention_is_bounded_and_rotation_does_not_change_old_read_protocol() {
        let mut journal = ReadJournal::default();
        for i in 0..65 {
            journal.remember_view_cursor(i.to_string());
        }
        assert!(!journal.issued_view_cursor("0"));
        assert!(journal.issued_view_cursor("64"));
        for i in 0..64 {
            journal.remember_view_cursor(format!("{i}{}", "x".repeat(8192)));
        }
        assert!(!journal.issued_view_cursor("64"));
        journal.rotate().unwrap();
        assert!(!journal.issued_view_cursor("63"));
    }
}
