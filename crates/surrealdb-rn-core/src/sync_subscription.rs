//! Separate authorized-window wire and bounded read replay. Call only under RuntimeState lock.
use crate::sync_client::NativeSyncError;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use surrealdb_sync_client::ClientRuntime;
use surrealdb_sync_protocol::subscription::{
    MembershipChange, SubscriptionRequest, SubscriptionResponse,
};

pub(crate) const MAX_QUERY_BYTES: usize = 1024 * 1024;
const MAX_TOTAL_BYTES: usize = 16 * 1024 * 1024;
const MAX_BATCHES: usize = 128;
const MAX_OUTPUT: usize = 16 * 1024 * 1024;

pub(crate) fn capabilities() -> String {
    json!({"protocolVersion":1,"maxSubscriptions":32,"maxWindowRows":32,"maxJournalBatches":MAX_BATCHES,"maxJournalBytes":MAX_QUERY_BYTES,"maxAggregateJournalBytes":MAX_TOTAL_BYTES,"maxOutputBytes":MAX_OUTPUT,"requiresMatchingNativeAbi":true}).to_string()
}
fn unsigned(value: &mut Value, input: bool) -> Result<(), NativeSyncError> {
    if input {
        let text = value.as_str().ok_or(NativeSyncError::InvalidInput)?;
        let number = text
            .parse::<u64>()
            .map_err(|_| NativeSyncError::InvalidInput)?;
        if number.to_string() != text {
            return Err(NativeSyncError::InvalidInput);
        }
        *value = json!(number);
    } else {
        *value = json!(value.as_u64().ok_or(NativeSyncError::Internal)?.to_string());
    }
    Ok(())
}
fn scalar(value: &mut Value, input: bool) -> Result<(), NativeSyncError> {
    if value["kind"] == "int" {
        if input {
            let text = value["value"]
                .as_str()
                .ok_or(NativeSyncError::InvalidInput)?;
            let number = text
                .parse::<i64>()
                .map_err(|_| NativeSyncError::InvalidInput)?;
            if number.to_string() != text {
                return Err(NativeSyncError::InvalidInput);
            }
            value["value"] = json!(number);
        } else {
            value["value"] = json!(
                value["value"]
                    .as_i64()
                    .ok_or(NativeSyncError::Internal)?
                    .to_string()
            );
        }
    }
    Ok(())
}
fn scope(value: &mut Value, input: bool) -> Result<(), NativeSyncError> {
    unsigned(&mut value["authorizationRevision"], input)?;
    unsigned(&mut value["subscriptionRevision"], input)
}
fn frontier(value: &mut Value, input: bool) -> Result<(), NativeSyncError> {
    unsigned(&mut value["epoch"], input)?;
    unsigned(&mut value["sequence"], input)
}
pub(crate) fn request_wire(value: &mut Value, input: bool) -> Result<(), NativeSyncError> {
    unsigned(&mut value["subscriptionRevision"], input)?;
    unsigned(&mut value["command"]["generation"], input)?;
    if value["command"]["kind"] == "start" {
        for scalar_value in value["command"]["parameters"]
            .as_object_mut()
            .ok_or(NativeSyncError::InvalidInput)?
            .values_mut()
        {
            scalar(scalar_value, input)?;
        }
    }
    Ok(())
}
fn row_wire(value: &mut Value, input: bool) -> Result<(), NativeSyncError> {
    unsigned(&mut value["version"], input)?;
    scalar(&mut value["sortKey"], input)
}
pub(crate) fn response_wire(value: &mut Value, input: bool) -> Result<(), NativeSyncError> {
    unsigned(&mut value["generation"], input)?;
    unsigned(&mut value["viewRevision"], input)?;
    scope(&mut value["scope"], input)?;
    frontier(&mut value["frontier"], input)?;
    if value["response"]["kind"] == "delta" {
        unsigned(&mut value["response"]["beforeRevision"], input)?;
    }
    for change in value["changes"]
        .as_array_mut()
        .ok_or(NativeSyncError::InvalidInput)?
    {
        if change["kind"] == "upsert" {
            row_wire(&mut change["row"], input)?;
        } else if change["kind"] == "delete" {
            unsigned(&mut change["version"], input)?;
        }
    }
    Ok(())
}
pub(crate) fn parse_request(text: &str) -> Result<SubscriptionRequest, NativeSyncError> {
    let mut value = input(text)?;
    request_wire(&mut value, true)?;
    serde_json::from_value(value).map_err(|_| NativeSyncError::InvalidInput)
}
pub(crate) fn parse_response(text: &str) -> Result<SubscriptionResponse<Value>, NativeSyncError> {
    let mut value = input(text)?;
    response_wire(&mut value, true)?;
    let response: SubscriptionResponse<Value> =
        serde_json::from_value(value).map_err(|_| NativeSyncError::InvalidInput)?;
    let canonical = map_response(response.clone(), |value| {
        crate::sync_codec::canonical_json(&value)
    })?;
    surrealdb_sync_protocol::subscription_codec::encode_subscription_response(&canonical)
        .map_err(|_| NativeSyncError::InvalidInput)?;
    Ok(response)
}
pub(crate) fn input(text: &str) -> Result<Value, NativeSyncError> {
    if text.len() > crate::sync_state::MAX_SYNC_STATE_BYTES {
        return Err(NativeSyncError::InvalidInput);
    }
    serde_json::from_str(text).map_err(|_| NativeSyncError::InvalidInput)
}
pub(crate) fn output(value: &Value) -> Result<String, NativeSyncError> {
    let text = serde_json::to_string(value).map_err(|_| NativeSyncError::Internal)?;
    if text.len() > MAX_OUTPUT {
        return Err(NativeSyncError::ReadOutputTooLarge);
    }
    Ok(text)
}
pub(crate) fn request(runtime: &ClientRuntime<Value>, id: &str) -> Result<Value, NativeSyncError> {
    if !surrealdb_sync_protocol::subscription::valid_subscription_text(id) {
        return Err(NativeSyncError::InvalidInput);
    }
    let Some(request) = runtime.subscription_request(id) else {
        return Ok(Value::Null);
    };
    let mut value = serde_json::to_value(request).map_err(|_| NativeSyncError::Internal)?;
    request_wire(&mut value, false)?;
    Ok(value)
}
fn frame(runtime: &ClientRuntime<Value>, id: &str) -> Result<Value, NativeSyncError> {
    let snapshot = if let Some(snapshot) = runtime.subscription_snapshot(id) {
        let mut value = serde_json::to_value(snapshot).map_err(|_| NativeSyncError::Internal)?;
        for field in ["revision", "generation", "viewRevision"] {
            unsigned(&mut value["metadata"][field], false)?;
        }
        scope(&mut value["metadata"]["scope"], false)?;
        frontier(&mut value["metadata"]["frontier"], false)?;
        for row in value["rows"]
            .as_array_mut()
            .ok_or(NativeSyncError::Internal)?
        {
            row_wire(row, false)?;
        }
        value
    } else {
        Value::Null
    };
    let cancelled = runtime
        .state()
        .subscriptions
        .as_ref()
        .and_then(|state| state.cancelled.get(id))
        .map(u64::to_string);
    Ok(json!({"snapshot":snapshot,"request":request(runtime,id)?,"cancelledThrough":cancelled}))
}
struct Batch {
    before: u64,
    revision: u64,
    json: String,
}
#[derive(Default)]
struct History {
    batches: VecDeque<Batch>,
    bytes: usize,
}
pub(crate) struct SubscriptionJournal {
    epoch: String,
    histories: BTreeMap<String, History>,
}
impl Default for SubscriptionJournal {
    fn default() -> Self {
        Self {
            epoch: surrealdb::types::Uuid::new_v4().to_string(),
            histories: BTreeMap::new(),
        }
    }
}
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Cursor {
    process_epoch: String,
    subscription_id: String,
    revision: String,
}
impl SubscriptionJournal {
    fn cursor(&self, id: &str, revision: u64) -> Value {
        json!({"processEpoch":self.epoch,"subscriptionId":id,"revision":revision.to_string()})
    }
    pub(crate) fn bootstrap(
        &self,
        runtime: &ClientRuntime<Value>,
        id: &str,
    ) -> Result<Value, NativeSyncError> {
        if !surrealdb_sync_protocol::subscription::valid_subscription_text(id) {
            return Err(NativeSyncError::InvalidInput);
        }
        let mut value = frame(runtime, id)?;
        value["cursor"] = self.cursor(id, runtime.state().revision);
        Ok(value)
    }
    pub(crate) fn record(&mut self, runtime: &ClientRuntime<Value>, before: u64) {
        if before == runtime.state().revision {
            return;
        }
        let Some(state) = &runtime.state().subscriptions else {
            return;
        };
        let ids = state
            .requests
            .keys()
            .chain(state.cancelled.keys())
            .chain(state.slots.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        for id in ids {
            let history = self.histories.entry(id.clone()).or_default();
            let encoded = frame(runtime, &id).and_then(|mut value| {
                value["beforeRevision"] = json!(before.to_string());
                value["revision"] = json!(runtime.state().revision.to_string());
                output(&value)
            });
            let Ok(encoded) = encoded else {
                history.batches.clear();
                history.bytes = 0;
                continue;
            };
            if encoded.len() > MAX_QUERY_BYTES {
                history.batches.clear();
                history.bytes = 0;
                continue;
            }
            while history.batches.len() >= MAX_BATCHES
                || history.bytes + encoded.len() > MAX_QUERY_BYTES
            {
                if let Some(old) = history.batches.pop_front() {
                    history.bytes -= old.json.len();
                }
            }
            history.bytes += encoded.len();
            history.batches.push_back(Batch {
                before,
                revision: runtime.state().revision,
                json: encoded,
            });
        }
        while self.histories.values().map(|h| h.bytes).sum::<usize>() > MAX_TOTAL_BYTES {
            let oldest = self
                .histories
                .iter()
                .filter_map(|(id, h)| h.batches.front().map(|b| (id.clone(), b.revision)))
                .min_by_key(|(_, r)| *r);
            let Some((id, _)) = oldest else {
                break;
            };
            if let Some(history) = self.histories.get_mut(&id)
                && let Some(old) = history.batches.pop_front()
            {
                history.bytes -= old.json.len();
            }
        }
    }
    pub(crate) fn changes(
        &self,
        runtime: &ClientRuntime<Value>,
        id: &str,
        cursor: &str,
        limit: u32,
    ) -> Result<Value, NativeSyncError> {
        if !surrealdb_sync_protocol::subscription::valid_subscription_text(id)
            || !(1..=MAX_BATCHES as u32).contains(&limit)
            || cursor.len() > 4096
        {
            return Err(NativeSyncError::InvalidInput);
        }
        let cursor: Cursor =
            serde_json::from_str(cursor).map_err(|_| NativeSyncError::InvalidInput)?;
        let revision = cursor
            .revision
            .parse::<u64>()
            .map_err(|_| NativeSyncError::InvalidInput)?;
        if revision.to_string() != cursor.revision || cursor.subscription_id != id {
            return Err(NativeSyncError::InvalidInput);
        }
        let current = runtime.state().revision;
        let history = self.histories.get(id);
        if cursor.process_epoch != self.epoch
            || revision > current
            || (revision < current
                && history
                    .and_then(|h| h.batches.front())
                    .is_none_or(|b| revision < b.before))
        {
            let mut reset = self.bootstrap(runtime, id)?;
            reset["kind"] = json!("reset_required");
            return Ok(reset);
        }
        let batches = history
            .into_iter()
            .flat_map(|h| h.batches.iter())
            .filter(|b| b.revision > revision)
            .take(limit as usize)
            .map(|b| serde_json::from_str::<Value>(&b.json).map_err(|_| NativeSyncError::Internal))
            .collect::<Result<Vec<_>, _>>()?;
        let after = batches
            .last()
            .and_then(|b| b["revision"].as_str())
            .and_then(|r| r.parse::<u64>().ok())
            .unwrap_or(current);
        Ok(json!({"kind":"batches","batches":batches,"cursor":self.cursor(id,after)}))
    }
}

fn map_response<A, B>(
    response: SubscriptionResponse<A>,
    mut convert: impl FnMut(A) -> Result<B, NativeSyncError>,
) -> Result<SubscriptionResponse<B>, NativeSyncError> {
    let changes = response
        .changes
        .into_iter()
        .map(|change| {
            Ok(match change {
                MembershipChange::Upsert { row } => MembershipChange::Upsert {
                    row: surrealdb_sync_protocol::subscription::SubscriptionRow {
                        record_id: row.record_id,
                        value: convert(row.value)?,
                        version: row.version,
                        sort_key: row.sort_key,
                    },
                },
                MembershipChange::Leave { record_id } => MembershipChange::Leave { record_id },
                MembershipChange::Delete { record_id, version } => {
                    MembershipChange::Delete { record_id, version }
                }
            })
        })
        .collect::<Result<_, NativeSyncError>>()?;
    Ok(SubscriptionResponse {
        partition_id: response.partition_id,
        client_id: response.client_id,
        subscription_id: response.subscription_id,
        generation: response.generation,
        plan_fingerprint: response.plan_fingerprint,
        scope: response.scope,
        frontier: response.frontier,
        view_revision: response.view_revision,
        coverage: response.coverage,
        response: response.response,
        token: response.token,
        ordered_ids: response.ordered_ids,
        changes,
    })
}

#[uniffi::export(async_runtime = "tokio")]
pub async fn encode_subscription_request_json(
    request_json: String,
) -> Result<Vec<u8>, NativeSyncError> {
    surrealdb_sync_protocol::subscription_codec::encode_subscription_request(&parse_request(
        &request_json,
    )?)
    .map_err(|_| NativeSyncError::InvalidInput)
}
#[uniffi::export(async_runtime = "tokio")]
pub async fn decode_subscription_response(bytes: Vec<u8>) -> Result<String, NativeSyncError> {
    let response =
        surrealdb_sync_protocol::subscription_codec::decode_subscription_response(&bytes)
            .map_err(|_| NativeSyncError::Protocol)?;
    let response = map_response(response, crate::sync_http_codec::canonical_to_json)?;
    let mut value = serde_json::to_value(response).map_err(|_| NativeSyncError::Internal)?;
    response_wire(&mut value, false)?;
    output(&value)
}

#[cfg(test)]
pub(crate) fn test_request() -> Value {
    json!({"partitionId":"partition","clientId":"client","requestedScope":"all","subscriptionRevision":"1","command":{"kind":"start","subscriptionId":"window","generation":"1","definitionId":"ranked","parameters":{"score":{"kind":"int","value":i64::MIN.to_string()}},"limit":2}})
}
#[cfg(test)]
pub(crate) fn test_response() -> Value {
    json!({"partitionId":"partition","clientId":"client","subscriptionId":"window","generation":"1","planFingerprint":"plan","scope":{"identity":"all","subscriptionRevision":"1","authorizationRevision":"1"},"frontier":{"epoch":"1","sequence":"0"},"viewRevision":"1","coverage":{"definitionId":"ranked","limit":2,"orderField":"score","orderKind":"int","direction":"ascending"},"response":{"kind":"snapshot","reason":"initial"},"token":"resume-token","orderedIds":["probe:a","probe:b"],"changes":[{"kind":"upsert","row":{"recordId":"probe:a","value":{"name":"a","counter":{"$surreal":"int","value":"9007199254740993"}},"version":"18446744073709551615","sortKey":{"kind":"int","value":i64::MIN.to_string()}}},{"kind":"upsert","row":{"recordId":"probe:b","value":{"name":"b"},"version":"2","sortKey":{"kind":"int","value":"0"}}}]})
}
#[cfg(test)]
mod tests {
    use super::*;
    use surrealdb_sync_client::DurableClientState;
    use surrealdb_sync_protocol::{ClientId, PartitionId, ScopeIdentity};
    fn runtime() -> ClientRuntime<Value> {
        ClientRuntime::open(DurableClientState::empty(
            PartitionId("partition".into()),
            ClientId("client".into()),
            ScopeIdentity("all".into()),
            1,
        ))
        .unwrap()
    }
    fn install(
        runtime: &mut ClientRuntime<Value>,
        journal: &mut SubscriptionJournal,
        prepared: surrealdb_sync_client::PreparedTransition<Value>,
    ) {
        let before = runtime.state().revision;
        runtime.install_with_read_batch(prepared).unwrap();
        journal.record(runtime, before);
    }
    #[tokio::test]
    async fn subscription_codec_preserves_structural_i64_u64_and_payload_wire_tags() {
        let request = test_request();
        let encoded = encode_subscription_request_json(request.to_string())
            .await
            .unwrap();
        if std::env::var_os("SURREALDB_EXPORT_WINDOW_FIXTURES").is_some() {
            println!(
                "WINDOW_REQUEST_CBOR_HEX={}",
                encoded
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            );
        }
        let decoded =
            surrealdb_sync_protocol::subscription_codec::decode_subscription_request(&encoded)
                .unwrap();
        assert_eq!(decoded, parse_request(&request.to_string()).unwrap());
        let response = parse_response(&test_response().to_string()).unwrap();
        let canonical =
            map_response(response, |value| crate::sync_codec::canonical_json(&value)).unwrap();
        let encoded =
            surrealdb_sync_protocol::subscription_codec::encode_subscription_response(&canonical)
                .unwrap();
        if std::env::var_os("SURREALDB_EXPORT_WINDOW_FIXTURES").is_some() {
            println!(
                "WINDOW_RESPONSE_CBOR_HEX={}",
                encoded
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            );
        }
        let decoded: Value =
            serde_json::from_str(&decode_subscription_response(encoded).await.unwrap()).unwrap();
        assert_eq!(decoded, test_response());
        let mut invalid = request;
        invalid["command"]["parameters"]["score"]["value"] = json!("-0");
        assert!(parse_request(&invalid.to_string()).is_err());
        let mut oversized = test_response();
        oversized["changes"][1]["row"]["value"] = json!({"large":"x".repeat(surrealdb_sync_protocol::subscription::MAX_SUBSCRIPTION_BYTES)});
        assert!(parse_response(&oversized.to_string()).is_err());
    }
    #[test]
    fn aggregate_query_replay_budget_evicts_without_dropping_durable_windows() {
        let mut runtime = runtime();
        let mut journal = SubscriptionJournal::default();
        let mut cursors = Vec::new();
        for n in 0..32 {
            let id = format!("window-{n}");
            let mut request = test_request();
            request["command"]["subscriptionId"] = json!(id);
            cursors.push((
                id.clone(),
                journal.bootstrap(&runtime, &id).unwrap()["cursor"].to_string(),
            ));
            let prepared = runtime
                .prepare_start_subscription(parse_request(&request.to_string()).unwrap())
                .unwrap();
            install(&mut runtime, &mut journal, prepared);
            let mut response = test_response();
            response["subscriptionId"] = json!(id);
            response["changes"][1]["row"]["value"] = json!({"large":"x".repeat(100_000)});
            let prepared = runtime
                .prepare_subscription_response(parse_response(&response.to_string()).unwrap())
                .unwrap();
            install(&mut runtime, &mut journal, prepared);
        }
        assert!(journal.histories.values().map(|h| h.bytes).sum::<usize>() <= MAX_TOTAL_BYTES);
        assert!(
            journal
                .histories
                .values()
                .all(|h| h.bytes <= MAX_QUERY_BYTES && h.batches.len() <= MAX_BATCHES)
        );
        assert_eq!(
            runtime.state().subscriptions.as_ref().unwrap().slots.len(),
            32
        );
        assert!(cursors.iter().any(|(id, cursor)| {
            journal.changes(&runtime, id, cursor, 128).unwrap()["kind"] == "reset_required"
        }));
    }

    #[test]
    fn subscription_journal_bounds_bytes_count_identity_and_cancellation() {
        let mut runtime = runtime();
        let mut journal = SubscriptionJournal::default();
        let initial = journal.bootstrap(&runtime, "window").unwrap()["cursor"].to_string();
        let prepared = runtime
            .prepare_start_subscription(parse_request(&test_request().to_string()).unwrap())
            .unwrap();
        install(&mut runtime, &mut journal, prepared);
        let prepared = runtime
            .prepare_subscription_response(parse_response(&test_response().to_string()).unwrap())
            .unwrap();
        install(&mut runtime, &mut journal, prepared);
        let current = journal.bootstrap(&runtime, "window").unwrap();
        assert_eq!(
            current["snapshot"]["rows"][0]["version"],
            u64::MAX.to_string()
        );
        assert_eq!(
            current["snapshot"]["rows"][0]["value"]["counter"]["value"],
            "9007199254740993"
        );
        assert!(current["snapshot"]["metadata"].get("token").is_none());
        assert_eq!(current["request"]["command"]["kind"], "resume");
        assert_eq!(
            journal.changes(&runtime, "window", &initial, 1).unwrap()["batches"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        for n in 2..=140 {
            let mut response = test_response();
            response["viewRevision"] = json!(n.to_string());
            let prepared = runtime
                .prepare_subscription_response(parse_response(&response.to_string()).unwrap())
                .unwrap();
            install(&mut runtime, &mut journal, prepared);
        }
        assert!(journal.histories["window"].batches.len() <= MAX_BATCHES);
        assert!(journal.histories["window"].bytes <= MAX_QUERY_BYTES);
        assert_eq!(
            journal.changes(&runtime, "window", &initial, 128).unwrap()["kind"],
            "reset_required"
        );
        let old = current["cursor"].to_string();
        assert_eq!(
            SubscriptionJournal::default()
                .changes(&runtime, "window", &old, 1)
                .unwrap()["kind"],
            "reset_required"
        );
        assert!(journal.changes(&runtime, "other", &old, 1).is_err());
        assert!(journal.changes(&runtime, "window", &old, 0).is_err());
        let mut response = parse_response(&test_response().to_string()).unwrap();
        response.view_revision = 141;
        if let MembershipChange::Upsert { row } = &mut response.changes[1] {
            row.version = 3;
            row.value = json!({"large":"x".repeat(MAX_QUERY_BYTES)});
        }
        let prepared = runtime.prepare_subscription_response(response).unwrap();
        install(&mut runtime, &mut journal, prepared);
        assert!(journal.histories["window"].batches.is_empty());
        assert_eq!(journal.histories["window"].bytes, 0);
        let prepared = runtime.prepare_cancel_subscription("window", 1).unwrap();
        install(&mut runtime, &mut journal, prepared);
        let cancelled = journal.bootstrap(&runtime, "window").unwrap();
        assert!(cancelled["snapshot"].is_null());
        assert_eq!(cancelled["cancelledThrough"], "1");
    }
}
