//! Dormant private bootstrap storage. Pages never become confirmed/readable here.
//! Encoded reservations are deliberately conservative; they do not bound RSS or DB scans.
use std::collections::{BTreeMap, BTreeSet};

use surrealdb::engine::any::Any;
use surrealdb::method::Transaction;
use surrealdb::types::{Array, Bytes, RecordId, SurrealValue, Value};
use surrealdb_sync_protocol::RecordState;
use surrealdb_sync_protocol::bootstrap::{BootstrapContext, BootstrapManifest, BootstrapPage};
use surrealdb_sync_protocol::bootstrap_codec::{
    bootstrap_manifest_digest, decode_bootstrap_manifest, decode_bootstrap_page,
    decode_bootstrap_start, validate_bootstrap_manifest_context,
};

use crate::SurrealDatabase;
use crate::sync_state::{
    MAX_AGGREGATE_STATE_BYTES, MAX_COMPONENT_ITEMS, MAX_SYNC_STATE_BYTES, SyncStateError,
    SyncStateKey, encode_tagged, finish_transaction, meta_record_id, state_budget,
    transaction_state,
};

const HEADER: &str = "_sync_bootstrap_header";
const PAGE: &str = "_sync_bootstrap_page";
// Final metadata and the future consumed marker each have the existing per-component cap.
// Old state is reserved again for retained pending/outcomes. This can reject early safely.
const FINAL_OVERHEAD: usize = 3 * MAX_SYNC_STATE_BYTES;
const SCHEMA: &str = r#"
DEFINE FIELD IF NOT EXISTS bootstrap_fence ON _sync_client_meta TYPE option<string>;
DEFINE TABLE IF NOT EXISTS _sync_bootstrap_header SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD IF NOT EXISTS format_version ON _sync_bootstrap_header TYPE int;
DEFINE FIELD IF NOT EXISTS start ON _sync_bootstrap_header TYPE bytes;
DEFINE FIELD IF NOT EXISTS manifest ON _sync_bootstrap_header TYPE bytes;
DEFINE FIELD IF NOT EXISTS cancelled ON _sync_bootstrap_header TYPE bool;
DEFINE FIELD IF NOT EXISTS incarnation ON _sync_bootstrap_header TYPE string;
DEFINE TABLE IF NOT EXISTS _sync_bootstrap_page SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD IF NOT EXISTS format_version ON _sync_bootstrap_page TYPE int;
DEFINE FIELD IF NOT EXISTS partition_id ON _sync_bootstrap_page TYPE string;
DEFINE FIELD IF NOT EXISTS client_id ON _sync_bootstrap_page TYPE string;
DEFINE FIELD IF NOT EXISTS index ON _sync_bootstrap_page TYPE string;
DEFINE FIELD IF NOT EXISTS payload ON _sync_bootstrap_page TYPE bytes;
DEFINE INDEX IF NOT EXISTS by_client ON _sync_bootstrap_page FIELDS partition_id, client_id;
"#;

#[derive(SurrealValue)]
struct Header {
    format_version: i64,
    cancelled: bool,
    incarnation: String,
    start: Bytes,
    manifest: Bytes,
}
#[derive(SurrealValue)]
struct PageRow {
    format_version: i64,
    partition_id: String,
    client_id: String,
    index: String,
    payload: Bytes,
}
#[derive(SurrealValue)]
struct Fence {
    bootstrap_fence: Option<String>,
}
#[derive(SurrealValue)]
struct Schema {
    tables: BTreeMap<String, String>,
}
#[derive(SurrealValue)]
struct Lengths {
    bytes: u64,
    key_bytes: u64,
    canonical: bool,
}
struct Stage {
    header: Header,
    manifest: BootstrapManifest,
    pages: BTreeMap<u64, Vec<u8>>,
}

pub(crate) struct BootstrapStore<'a> {
    database: &'a SurrealDatabase,
}

impl<'a> BootstrapStore<'a> {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Dormant private staging until reviewed native composition"
        )
    )]
    pub(crate) fn new(database: &'a SurrealDatabase) -> Self {
        Self { database }
    }

    /// Only this explicit entry installs stage schema/the optional metadata fence.
    /// Expected context is independently supplied by authenticated composition, not the body.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Dormant private staging until reviewed native composition"
        )
    )]
    pub(crate) async fn begin(
        &self,
        start: &[u8],
        manifest: &[u8],
        expected: &BootstrapContext,
    ) -> Result<u64, SyncStateError> {
        let key = SyncStateKey::new(&expected.partition_id, &expected.client_id);
        crate::sync_state::validate_key(key)?;
        let decoded =
            decode_bootstrap_manifest(manifest).map_err(|_| SyncStateError::CorruptState)?;
        validate_header(start, &decoded, expected)?;
        if start
            .len()
            .checked_add(manifest.len())
            .is_none_or(|n| n > MAX_SYNC_STATE_BYTES)
        {
            return Err(SyncStateError::StateTooLarge);
        }
        let client = self.database.client().await.map_err(SyncStateError::Core)?;
        let transaction = client.begin().await.map_err(SyncStateError::Database)?;
        let operation = async {
            transaction
                .query(SCHEMA)
                .await
                .map_err(SyncStateError::Database)?
                .check()
                .map_err(SyncStateError::Database)?;
            // Combined persisted stage/current scalar admission MUST precede any
            // current-state payload decoding, including on exact begin retry.
            let existing = load_stage(&transaction, key).await?;
            if existing.is_none() {
                let current =
                    crate::sync_state::transaction_state_inventory(&transaction, key).await?;
                let incoming = start.len() + manifest.len();
                if current
                    .0
                    .checked_mul(2)
                    .and_then(|n| n.checked_add(FINAL_OVERHEAD))
                    .and_then(|n| n.checked_add(incoming))
                    .is_none_or(|n| n > MAX_AGGREGATE_STATE_BYTES)
                    || current
                        .1
                        .checked_mul(2)
                        .and_then(|n| n.checked_add(4))
                        .is_none_or(|n| n > MAX_COMPONENT_ITEMS)
                {
                    return Err(SyncStateError::StateTooLarge);
                }
            }
            let state = transaction_state(&transaction, key).await?;
            check_barrier(&transaction, key).await?;
            if read_fence(&transaction, key).await?.is_none() {
                fence_first_barrier(&transaction, key, &decoded.context).await?;
            }
            if state.requested_scope != expected.scope.identity
                || state.subscription_revision != expected.scope.subscription_revision
            {
                return Err(SyncStateError::CorruptState);
            }
            if let Some(existing) = existing {
                check_budget(key, state_budget(&state)?, &existing)?;
                if existing.header.cancelled {
                    if &existing.header.manifest[..] == manifest {
                        return Err(SyncStateError::RevisionConflict);
                    }
                } else {
                    validate_header(&existing.header.start, &existing.manifest, expected)?;
                    return if &existing.header.start[..] == start
                        && &existing.header.manifest[..] == manifest
                    {
                        parse_fence(&existing.header.incarnation)
                    } else {
                        Err(SyncStateError::RevisionConflict)
                    };
                }
            }
            let next = next_fence(read_fence(&transaction, key).await?)?;
            let incarnation = parse_metadata_fence(&next)?.0;
            let next = format!("{incarnation}:a");
            let stage = Stage {
                header: Header {
                    format_version: 1,
                    cancelled: false,
                    incarnation: incarnation.to_string(),
                    start: start.to_vec().into(),
                    manifest: manifest.to_vec().into(),
                },
                manifest: decoded,
                pages: BTreeMap::new(),
            };
            check_budget(key, state_budget(&state)?, &stage)?;
            let _: Option<Value> = transaction
                .upsert(header_id(key))
                .content(stage.header)
                .await
                .map_err(SyncStateError::Database)?;
            write_fence(&transaction, key, next).await?;
            Ok(incarnation)
        }
        .await;
        finish_transaction(transaction, operation).await
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Dormant private staging until reviewed native composition"
        )
    )]
    pub(crate) async fn stage_page(
        &self,
        raw: &[u8],
        expected: &BootstrapContext,
        incarnation: u64,
    ) -> Result<(), SyncStateError> {
        let key = SyncStateKey::new(&expected.partition_id, &expected.client_id);
        crate::sync_state::validate_key(key)?;
        let page = decode_bootstrap_page(raw).map_err(|_| SyncStateError::CorruptState)?;
        // Validate native admissibility/encoded reservation before any persistence.
        page_reservation(&page)?;
        let client = self.database.client().await.map_err(SyncStateError::Core)?;
        let transaction = client.begin().await.map_err(SyncStateError::Database)?;
        let operation = async {
            let fence = read_fence(&transaction, key)
                .await?
                .ok_or(SyncStateError::CorruptState)?;
            let mut stage = load_stage(&transaction, key)
                .await?
                .ok_or(SyncStateError::CorruptState)?;
            validate_header(&stage.header.start, &stage.manifest, expected)?;
            if parse_fence(&stage.header.incarnation)? != incarnation {
                return Err(SyncStateError::RevisionConflict);
            }
            if stage.header.cancelled {
                return Err(SyncStateError::RevisionConflict);
            }
            check_barrier(&transaction, key).await?;
            validate_page(&stage.manifest, &page)?;
            if let Some(old) = stage.pages.get(&page.index) {
                return if old == raw {
                    Ok(())
                } else {
                    Err(SyncStateError::RevisionConflict)
                };
            }
            stage.pages.insert(page.index, raw.to_vec());
            let state = transaction_state(&transaction, key).await?;
            check_budget(key, state_budget(&state)?, &stage)?;
            let row = PageRow {
                format_version: 1,
                partition_id: key.partition_id.0.clone(),
                client_id: key.client_id.0.clone(),
                index: page.index.to_string(),
                payload: raw.to_vec().into(),
            };
            let _: Option<Value> = transaction
                .upsert(page_id(key, page.index))
                .content(row)
                .await
                .map_err(SyncStateError::Database)?;
            write_fence(&transaction, key, next_fence(Some(fence))?).await
        }
        .await;
        finish_transaction(transaction, operation).await
    }

    /// Progress grants no coverage/read revision or completion. It performs bounded
    /// full stage/current integrity validation, NOT a metadata-only/constant-time read.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Dormant private staging until reviewed native composition"
        )
    )]
    pub(crate) async fn progress(
        &self,
        expected: &BootstrapContext,
        incarnation: u64,
    ) -> Result<Option<Vec<u64>>, SyncStateError> {
        let key = SyncStateKey::new(&expected.partition_id, &expected.client_id);
        crate::sync_state::validate_key(key)?;
        let client = self.database.client().await.map_err(SyncStateError::Core)?;
        let transaction = client.begin().await.map_err(SyncStateError::Database)?;
        let operation = async {
            if read_fence(&transaction, key).await?.is_none() {
                return Ok(None);
            }
            let Some(stage) = load_stage(&transaction, key).await? else {
                return Ok(None);
            };
            validate_header(&stage.header.start, &stage.manifest, expected)?;
            if parse_fence(&stage.header.incarnation)? != incarnation {
                return Err(SyncStateError::RevisionConflict);
            }
            if stage.header.cancelled {
                return Ok(None);
            }
            let state = transaction_state(&transaction, key).await?;
            check_budget(key, state_budget(&state)?, &stage)?;
            Ok(Some(stage.pages.keys().copied().collect()))
        }
        .await;
        transaction
            .cancel()
            .await
            .map_err(SyncStateError::Database)?;
        operation
    }

    /// Full context AND manifest bytes bind cancellation; delayed callers cannot erase a successor.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Dormant private staging until reviewed native composition"
        )
    )]
    pub(crate) async fn cancel(
        &self,
        manifest: &[u8],
        expected: &BootstrapContext,
        incarnation: u64,
    ) -> Result<(), SyncStateError> {
        let key = SyncStateKey::new(&expected.partition_id, &expected.client_id);
        crate::sync_state::validate_key(key)?;
        let client = self.database.client().await.map_err(SyncStateError::Core)?;
        let transaction = client.begin().await.map_err(SyncStateError::Database)?;
        let operation = async {
            let Some(fence) = read_fence(&transaction, key).await? else {
                return Ok(());
            };
            let Some(stage) = load_stage(&transaction, key).await? else {
                return Ok(());
            };
            validate_header(&stage.header.start, &stage.manifest, expected)?;
            if parse_fence(&stage.header.incarnation)? != incarnation {
                return Err(SyncStateError::RevisionConflict);
            }
            if &stage.header.manifest[..] != manifest {
                return Err(SyncStateError::RevisionConflict);
            }
            for index in stage.pages.keys() {
                let _: Option<Value> = transaction
                    .delete(page_id(key, *index))
                    .await
                    .map_err(SyncStateError::Database)?;
            }
            let _: Option<Value> = transaction
                .upsert(header_id(key))
                .content(Header {
                    cancelled: true,
                    ..stage.header
                })
                .await
                .map_err(SyncStateError::Database)?;
            let next = next_fence(Some(fence))?;
            write_fence(
                &transaction,
                key,
                format!("{}:i", parse_metadata_fence(&next)?.0),
            )
            .await
        }
        .await;
        finish_transaction(transaction, operation).await
    }
}

fn validate_header(
    start: &[u8],
    manifest: &BootstrapManifest,
    expected: &BootstrapContext,
) -> Result<(), SyncStateError> {
    let start = decode_bootstrap_start(start).map_err(|_| SyncStateError::CorruptState)?;
    validate_bootstrap_manifest_context(manifest, &start, expected)
        .map_err(|_| SyncStateError::CorruptState)
}
fn validate_page(manifest: &BootstrapManifest, page: &BootstrapPage) -> Result<(), SyncStateError> {
    if page.session_id != manifest.session_id
        || page.manifest_digest
            != bootstrap_manifest_digest(manifest).map_err(|_| SyncStateError::CorruptState)?
        || page.index >= manifest.page_count
    {
        return Err(SyncStateError::CorruptState);
    }
    Ok(())
}
fn page_reservation(page: &BootstrapPage) -> Result<usize, SyncStateError> {
    page.records.iter().try_fold(0usize, |sum, record| {
        let native_id = crate::sync_state::native_record_id(&record.record_id)?;
        if native_id.table.as_str().starts_with("_sync_") {
            return Err(SyncStateError::UnsupportedRecordId);
        }
        let state = match &record.state {
            RecordState::Present {
                value,
                version,
                reference,
            } => {
                if !matches!(value, surrealdb_sync_protocol::CanonicalValue::Object(_)) {
                    return Err(SyncStateError::RecordValueMustBeObject);
                }
                let value = crate::sync_http_codec::canonical_to_json(value.clone())
                    .map_err(|_| SyncStateError::CorruptState)?;
                if !matches!(
                    crate::wire::from_wire_value(value.clone())
                        .map_err(SyncStateError::ValueCodec)?,
                    Value::Object(_)
                ) {
                    return Err(SyncStateError::RecordValueMustBeObject);
                }
                RecordState::Present {
                    value,
                    version: *version,
                    reference: reference.clone(),
                }
            }
            RecordState::Tombstone { version } => RecordState::Tombstone { version: *version },
            RecordState::Absent => return Err(SyncStateError::CorruptState),
        };
        // Both the actual tagged payload and its final metadata key are charged.
        let bytes = encode_tagged(&state)?
            .len()
            .checked_add(encode_tagged(&record.record_id)?.len())
            .ok_or(SyncStateError::StateTooLarge)?;
        sum.checked_add(bytes).ok_or(SyncStateError::StateTooLarge)
    })
}
fn check_budget(
    key: SyncStateKey<'_>,
    current: (usize, usize),
    stage: &Stage,
) -> Result<(), SyncStateError> {
    if stage.header.cancelled {
        if !stage.pages.is_empty() {
            return Err(SyncStateError::CorruptState);
        }
        return check_inactive_budget(current);
    }
    let mut bytes = current
        .0
        .checked_mul(2)
        .and_then(|n| n.checked_add(FINAL_OVERHEAD))
        .and_then(|n| n.checked_add(stage.header.start.len() + stage.header.manifest.len()))
        .ok_or(SyncStateError::StateTooLarge)?;
    let mut count = current
        .1
        .checked_mul(2)
        .and_then(|n| n.checked_add(4))
        .ok_or(SyncStateError::StateTooLarge)?;
    let mut records = 0u64;
    let mut ids = BTreeSet::new();
    let mut last_id = None;
    for (index, raw) in &stage.pages {
        let page = decode_bootstrap_page(raw).map_err(|_| SyncStateError::CorruptState)?;
        validate_page(&stage.manifest, &page)?;
        if page.index != *index {
            return Err(SyncStateError::CorruptState);
        }
        for record in &page.records {
            if !ids.insert(record.record_id.clone())
                || last_id
                    .as_ref()
                    .is_some_and(|last| last >= &record.record_id)
            {
                return Err(SyncStateError::CorruptState);
            }
            last_id = Some(record.record_id.clone());
        }
        records = records
            .checked_add(page.records.len() as u64)
            .ok_or(SyncStateError::StateTooLarge)?;
        let key_bytes = encode_tagged(&(
            key.partition_id.0.as_str(),
            key.client_id.0.as_str(),
            index.to_string(),
        ))?
        .len()
        .checked_mul(2)
        .and_then(|n| n.checked_add(128))
        .ok_or(SyncStateError::StateTooLarge)?;
        let reservation = page_reservation(&page)?;
        bytes = bytes
            .checked_add(raw.len())
            .and_then(|n| n.checked_add(reservation))
            .and_then(|n| n.checked_add(key_bytes))
            .ok_or(SyncStateError::StateTooLarge)?;
        count = count
            .checked_add(page.records.len() + 1)
            .ok_or(SyncStateError::StateTooLarge)?;
    }
    // Header key and rotating fence are real encoded overhead too.
    bytes = bytes
        .checked_add(
            encode_tagged(&(
                key.partition_id.0.as_str(),
                key.client_id.0.as_str(),
                u64::MAX.to_string(),
            ))?
            .len(),
        )
        .ok_or(SyncStateError::StateTooLarge)?;
    if bytes > MAX_AGGREGATE_STATE_BYTES
        || count > MAX_COMPONENT_ITEMS
        || records > stage.manifest.record_count
    {
        return Err(SyncStateError::StateTooLarge);
    }
    Ok(())
}

async fn read_fence(
    transaction: &Transaction<Any>,
    key: SyncStateKey<'_>,
) -> Result<Option<String>, SyncStateError> {
    // Meta exists on every initialized v1/v2 journal. This never touches stage schema.
    let mut response = transaction
        .query("SELECT IF bootstrap_fence = NONE THEN NONE ELSE string::slice(bootstrap_fence,0,24) END AS bootstrap_fence FROM $id")
        .bind(("id", meta_record_id(key)))
        .await
        .map_err(SyncStateError::Database)?
        .check()
        .map_err(SyncStateError::Database)?;
    let rows: Vec<Fence> = response.take(0).map_err(SyncStateError::Database)?;
    let fence = rows.into_iter().next().and_then(|r| r.bootstrap_fence);
    if let Some(value) = &fence {
        parse_metadata_fence(value)?;
    }
    Ok(fence)
}
fn parse_fence(raw: &str) -> Result<u64, SyncStateError> {
    let number = raw
        .parse::<u64>()
        .map_err(|_| SyncStateError::CorruptState)?;
    if number.to_string() != raw {
        return Err(SyncStateError::CorruptState);
    }
    Ok(number)
}
fn parse_metadata_fence(raw: &str) -> Result<(u64, bool), SyncStateError> {
    let (counter, active) = raw.split_once(':').ok_or(SyncStateError::CorruptState)?;
    let active = match active {
        "a" => true,
        "i" => false,
        _ => return Err(SyncStateError::CorruptState),
    };
    Ok((parse_fence(counter)?, active))
}
fn next_fence(raw: Option<String>) -> Result<String, SyncStateError> {
    let (counter, active) = raw.map_or(Ok((0, true)), |v| parse_metadata_fence(&v))?;
    let next = counter
        .checked_add(1)
        .ok_or(SyncStateError::StateTooLarge)?;
    Ok(format!("{next}:{}", if active { "a" } else { "i" }))
}
fn check_inactive_budget(budget: (usize, usize)) -> Result<(), SyncStateError> {
    // One bounded closed marker remains; no download/final-state reservation survives cancel.
    if budget
        .0
        .checked_add(MAX_SYNC_STATE_BYTES + 65_536)
        .is_none_or(|n| n > MAX_AGGREGATE_STATE_BYTES)
        || budget
            .1
            .checked_add(4)
            .is_none_or(|n| n > MAX_COMPONENT_ITEMS)
    {
        return Err(SyncStateError::StateTooLarge);
    }
    Ok(())
}
pub(crate) async fn write_fence(
    transaction: &Transaction<Any>,
    key: SyncStateKey<'_>,
    fence: String,
) -> Result<(), SyncStateError> {
    transaction
        .query("UPDATE $id SET bootstrap_fence = $fence")
        .bind(("id", meta_record_id(key)))
        .bind(("fence", fence))
        .await
        .map_err(SyncStateError::Database)?
        .check()
        .map_err(SyncStateError::Database)?;
    Ok(())
}
pub(crate) async fn guard_state_write(
    transaction: &Transaction<Any>,
    key: SyncStateKey<'_>,
    budget: (usize, usize),
) -> Result<Option<String>, SyncStateError> {
    let Some(fence) = read_fence(transaction, key).await? else {
        return Ok(None);
    };
    if parse_metadata_fence(&fence)?.1 {
        let stage = load_stage(transaction, key)
            .await?
            .ok_or(SyncStateError::CorruptState)?;
        if stage.header.cancelled {
            return Err(SyncStateError::CorruptState);
        }
        check_budget(key, budget, &stage)?;
    } else {
        let stage = load_stage(transaction, key)
            .await?
            .ok_or(SyncStateError::CorruptState)?;
        if !stage.header.cancelled {
            return Err(SyncStateError::CorruptState);
        }
        check_inactive_budget(budget)?;
    }
    Ok(Some(next_fence(Some(fence))?))
}
pub(crate) async fn guard_barrier_write(
    transaction: &Transaction<Any>,
    key: SyncStateKey<'_>,
) -> Result<(), SyncStateError> {
    let Some(fence) = read_fence(transaction, key).await? else {
        // Bootstrap-only barrier-key fencing remains under review. No speculative
        // mutation of legacy metadata is permitted in this pre-bootstrap path.
        return Ok(());
    };
    let stage = load_stage(transaction, key)
        .await?
        .ok_or(SyncStateError::CorruptState)?;
    let state = transaction_state(transaction, key).await?;
    if parse_metadata_fence(&fence)?.1 {
        check_budget(key, state_budget(&state)?, &stage)?;
    } else {
        if !stage.header.cancelled {
            return Err(SyncStateError::CorruptState);
        }
        check_inactive_budget(state_budget(&state)?)?;
    }
    write_fence(transaction, key, next_fence(Some(fence))?).await
}
/// A first begin shares a real write intent with an otherwise metadata-independent
/// barrier writer. The valid temporary row is removed before commit; no barrier
/// schema/decoder changes or ordinary v1 metadata rewrites are needed.
/// Pinned memory AND SurrealKV conflict/absence tests are an upgrade prerequisite.
async fn fence_first_barrier(
    transaction: &Transaction<Any>,
    key: SyncStateKey<'_>,
    context: &BootstrapContext,
) -> Result<(), SyncStateError> {
    use crate::sync_state::ScopeBarrier;
    check_barrier(transaction, key).await?;
    let id = barrier_id(key);
    let temporary = ScopeBarrier {
        reason: "needs_new_scope_journal".into(),
        required_scope: context.scope.identity.0.clone(),
        required_subscription_revision: context.scope.subscription_revision.to_string(),
        required_authorization_revision: context.scope.authorization_revision.to_string(),
    };
    let created: Option<ScopeBarrier> = transaction
        .create(id.clone())
        .content(temporary.clone())
        .await
        .map_err(SyncStateError::Database)?;
    let deleted: Option<ScopeBarrier> = transaction
        .delete(id.clone())
        .await
        .map_err(SyncStateError::Database)?;
    let absent: Option<ScopeBarrier> = transaction
        .select(id)
        .await
        .map_err(SyncStateError::Database)?;
    let matches = |row: &ScopeBarrier| {
        row.reason == temporary.reason
            && row.required_scope == temporary.required_scope
            && row.required_subscription_revision == temporary.required_subscription_revision
            && row.required_authorization_revision == temporary.required_authorization_revision
    };
    if !created.as_ref().is_some_and(matches)
        || !deleted.as_ref().is_some_and(matches)
        || absent.is_some()
    {
        return Err(SyncStateError::CorruptState);
    }
    Ok(())
}
fn barrier_id(key: SyncStateKey<'_>) -> RecordId {
    RecordId::new(
        "_sync_client_scope_barrier",
        Array::from(vec![key.partition_id.0.clone(), key.client_id.0.clone()]),
    )
}
async fn check_barrier(
    transaction: &Transaction<Any>,
    key: SyncStateKey<'_>,
) -> Result<(), SyncStateError> {
    let id = barrier_id(key);
    let barrier: Option<Value> = transaction
        .select(id)
        .await
        .map_err(SyncStateError::Database)?;
    if barrier.is_some() {
        return Err(SyncStateError::RevisionConflict);
    }
    Ok(())
}
fn header_id(key: SyncStateKey<'_>) -> RecordId {
    RecordId::new(
        HEADER,
        Array::from(vec![key.partition_id.0.clone(), key.client_id.0.clone()]),
    )
}
fn page_id(key: SyncStateKey<'_>, index: u64) -> RecordId {
    RecordId::new(
        PAGE,
        Array::from(vec![
            key.partition_id.0.clone(),
            key.client_id.0.clone(),
            index.to_string(),
        ]),
    )
}

async fn load_stage(
    transaction: &Transaction<Any>,
    key: SyncStateKey<'_>,
) -> Result<Option<Stage>, SyncStateError> {
    let mut response = transaction
        .query("INFO FOR DB")
        .await
        .map_err(SyncStateError::Database)?
        .check()
        .map_err(SyncStateError::Database)?;
    let schema: Option<Schema> = response.take(0).map_err(SyncStateError::Database)?;
    let schema = schema.ok_or(SyncStateError::CorruptState)?;
    if !schema.tables.contains_key(HEADER) || !schema.tables.contains_key(PAGE) {
        return Err(SyncStateError::CorruptState);
    }
    // Scalars first, including header bytes. No unbounded blob is fetched before this guard.
    let mut lengths = transaction.query("SELECT bytes::len(start) + bytes::len(manifest) AS bytes, bytes::len(<bytes>incarnation) AS key_bytes, format_version = 1 AS canonical FROM $header; SELECT bytes::len(payload) AS bytes, bytes::len(<bytes>encoding::json::encode([partition_id,client_id,index])) AS key_bytes, (format_version = 1 AND record::id(id) = [partition_id,client_id,index]) AS canonical FROM _sync_bootstrap_page WHERE partition_id = $partition AND client_id = $client LIMIT 16385")
        .bind(("header", header_id(key))).bind(("partition", key.partition_id.0.clone())).bind(("client", key.client_id.0.clone())).await.map_err(SyncStateError::Database)?.check().map_err(SyncStateError::Database)?;
    let headers: Vec<Lengths> = lengths.take(0).map_err(SyncStateError::Database)?;
    let pages: Vec<Lengths> = lengths.take(1).map_err(SyncStateError::Database)?;
    let current = crate::sync_state::preflight_inspection(
        transaction,
        &schema.tables.into_keys().collect(),
        key,
    )
    .await?;
    let mut aggregate = 0usize;
    for row in headers.iter().chain(&pages) {
        if !row.canonical {
            return Err(SyncStateError::CorruptState);
        }
        if row.bytes > MAX_SYNC_STATE_BYTES as u64 || row.key_bytes > (12 * 512 + 128) {
            return Err(SyncStateError::StateTooLarge);
        }
        aggregate = aggregate
            .checked_add(row.bytes as usize)
            .and_then(|n| n.checked_add(row.key_bytes as usize))
            .ok_or(SyncStateError::StateTooLarge)?;
    }
    if aggregate
        .checked_add(current.0)
        .is_none_or(|n| n > MAX_AGGREGATE_STATE_BYTES)
        || pages.len() + headers.len() + current.1 > MAX_COMPONENT_ITEMS
    {
        return Err(SyncStateError::StateTooLarge);
    }
    let header: Option<Header> = transaction
        .select(header_id(key))
        .await
        .map_err(SyncStateError::Database)?;
    let Some(header) = header else {
        return if pages.is_empty() {
            Ok(None)
        } else {
            Err(SyncStateError::CorruptState)
        };
    };
    let incarnation = parse_fence(&header.incarnation)?;
    let fence = read_fence(transaction, key)
        .await?
        .ok_or(SyncStateError::CorruptState)?;
    let (counter, active) = parse_metadata_fence(&fence)?;
    if header.cancelled == active || incarnation > counter {
        return Err(SyncStateError::CorruptState);
    }
    let manifest =
        decode_bootstrap_manifest(&header.manifest).map_err(|_| SyncStateError::CorruptState)?;
    validate_header(&header.start, &manifest, &manifest.context)?;
    if manifest.context.partition_id != *key.partition_id
        || manifest.context.client_id != *key.client_id
        || (header.cancelled && !pages.is_empty())
    {
        return Err(SyncStateError::CorruptState);
    }
    let mut response = transaction.query("SELECT * FROM _sync_bootstrap_page WHERE partition_id = $partition AND client_id = $client LIMIT 16385").bind(("partition", key.partition_id.0.clone())).bind(("client", key.client_id.0.clone())).await.map_err(SyncStateError::Database)?.check().map_err(SyncStateError::Database)?;
    let rows: Vec<PageRow> = response.take(0).map_err(SyncStateError::Database)?;
    let mut pages = BTreeMap::new();
    for row in rows {
        let index = parse_fence(&row.index)?;
        if row.partition_id != key.partition_id.0
            || row.client_id != key.client_id.0
            || pages.insert(index, row.payload.to_vec()).is_some()
        {
            return Err(SyncStateError::CorruptState);
        }
    }
    Ok(Some(Stage {
        header,
        manifest,
        pages,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync_state::{ScopeBarrier, StoredSyncState, SyncStateStore};
    use crate::{ConnectOptions, connect};
    use surrealdb_sync_client::ClientRuntime;
    use surrealdb_sync_protocol::bootstrap::{
        BootstrapDigest, BootstrapLimits, BootstrapStartRequest,
    };
    use surrealdb_sync_protocol::bootstrap_codec::{
        BootstrapContentHasher, bootstrap_start_fingerprint, encode_bootstrap_manifest,
        encode_bootstrap_page, encode_bootstrap_start,
    };
    use surrealdb_sync_protocol::{
        CanonicalValue, ClientCommit, ClientCommitId, ClientId, CommitIdentity, Cursor,
        Fingerprint, Operation, PartitionId, RecordId as ProtocolId, ScopeIdentity, ScopeSnapshot,
        SnapshotRecord,
    };

    async fn database() -> std::sync::Arc<SurrealDatabase> {
        connect(ConnectOptions {
            endpoint: "mem://".into(),
            namespace: Some("bootstrap".into()),
            database: Some("bootstrap".into()),
        })
        .await
        .unwrap()
    }
    fn state() -> StoredSyncState {
        StoredSyncState::empty(
            PartitionId("partition".into()),
            ClientId("client".into()),
            ScopeIdentity("all".into()),
            1,
        )
    }
    async fn initialized(database: &SurrealDatabase) -> StoredSyncState {
        let state = state();
        database
            .client()
            .await
            .unwrap()
            .query("DEFINE TABLE item SCHEMALESS")
            .await
            .unwrap()
            .check()
            .unwrap();
        let store = SyncStateStore::new(database);
        store.define_schema().await.unwrap();
        store.save_atomic(None, &state).await.unwrap();
        state
    }
    fn pending(state: StoredSyncState, id: &str) -> StoredSyncState {
        ClientRuntime::open(state)
            .unwrap()
            .prepare_enqueue(ClientCommit {
                identity: CommitIdentity {
                    client_commit_id: ClientCommitId(id.into()),
                    fingerprint: Fingerprint(id.into()),
                },
                operations: vec![Operation::Delete {
                    record_id: ProtocolId("item:pending".into()),
                    base_version: 0,
                }],
            })
            .unwrap()
            .into_state()
    }
    fn wire(session: &str) -> (Vec<u8>, Vec<u8>, BootstrapContext, Vec<Vec<u8>>) {
        let state = state();
        let context = BootstrapContext {
            authority_id: "authority".into(),
            principal_id: "owner".into(),
            partition_id: state.partition_id,
            client_id: state.client_id,
            journal_generation: "generation".into(),
            scope: ScopeSnapshot {
                identity: state.requested_scope,
                authorization_revision: 1,
                subscription_revision: 1,
            },
        };
        let start = BootstrapStartRequest {
            partition_id: context.partition_id.clone(),
            client_id: context.client_id.clone(),
            request_id: format!("request-{session}"),
            requested_scope: context.scope.identity.clone(),
            subscription_revision: 1,
            journal_generation: context.journal_generation.clone(),
        };
        let mut pages = (0..2)
            .map(|index| BootstrapPage {
                session_id: session.into(),
                manifest_digest: BootstrapDigest([0; 32]),
                index,
                records: vec![SnapshotRecord {
                    record_id: ProtocolId(format!("item:{index}")),
                    state: RecordState::Present {
                        value: CanonicalValue::Object(BTreeMap::new()),
                        version: 1,
                        reference: None,
                    },
                }],
            })
            .collect::<Vec<_>>();
        let mut hasher = BootstrapContentHasher::new(session.into()).unwrap();
        for page in &pages {
            hasher.push(page).unwrap();
        }
        let content = hasher.summary();
        let manifest = BootstrapManifest {
            session_id: session.into(),
            start_request_id: start.request_id.clone(),
            start_fingerprint: bootstrap_start_fingerprint(&start).unwrap(),
            context: context.clone(),
            cursor: Cursor {
                epoch: 1,
                sequence: 1,
            },
            limits: BootstrapLimits::default(),
            page_count: content.page_count,
            record_count: content.record_count,
            content_bytes: content.content_bytes,
            root: content.root,
            expires_at: u64::MAX,
        };
        let digest = bootstrap_manifest_digest(&manifest).unwrap();
        for page in &mut pages {
            page.manifest_digest = digest;
        }
        (
            encode_bootstrap_start(&start).unwrap(),
            encode_bootstrap_manifest(&manifest).unwrap(),
            context,
            pages
                .iter()
                .map(|p| encode_bootstrap_page(p).unwrap())
                .collect(),
        )
    }
    async fn schema(database: &SurrealDatabase) -> BTreeSet<String> {
        let client = database.client().await.unwrap();
        let mut response = client.query("INFO FOR DB").await.unwrap();
        response
            .take::<Option<Schema>>(0)
            .unwrap()
            .unwrap()
            .tables
            .into_keys()
            .collect()
    }
    fn barrier() -> ScopeBarrier {
        ScopeBarrier {
            reason: "needs_new_scope_journal".into(),
            required_scope: "new".into(),
            required_subscription_revision: "2".into(),
            required_authorization_revision: "2".into(),
        }
    }

    #[tokio::test]
    async fn no_stage_writes_leave_stage_schema_and_fence_absent() {
        let db = database().await;
        let state = initialized(&db).await;
        let store = SyncStateStore::new(&db);
        let before = schema(&db).await;
        let next = pending(state.clone(), "pending");
        store.apply_transition_atomic(&state, &next).await.unwrap();
        assert_eq!(
            store
                .load(SyncStateKey::new(&next.partition_id, &next.client_id))
                .await
                .unwrap(),
            Some(next.clone())
        );
        let raw_before: Option<Value> = db
            .client()
            .await
            .unwrap()
            .select(meta_record_id(SyncStateKey::new(
                &next.partition_id,
                &next.client_id,
            )))
            .await
            .unwrap();
        store
            .save_scope_barrier(
                SyncStateKey::new(&next.partition_id, &next.client_id),
                barrier(),
            )
            .await
            .unwrap();
        let raw_after: Option<Value> = db
            .client()
            .await
            .unwrap()
            .select(meta_record_id(SyncStateKey::new(
                &next.partition_id,
                &next.client_id,
            )))
            .await
            .unwrap();
        if let (Some(Value::Object(before)), Some(Value::Object(after))) = (&raw_before, &raw_after)
        {
            let changed = before
                .iter()
                .filter(|(key, value)| after.get(*key) != Some(*value))
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            assert!(
                raw_before == raw_after,
                "barrier changed metadata fields: {changed:?}"
            );
        } else {
            assert!(
                raw_before == raw_after,
                "metadata presence/type changed before={:?} after={:?}",
                raw_before.as_ref().map(std::mem::discriminant),
                raw_after.as_ref().map(std::mem::discriminant)
            );
        }
        assert_eq!(schema(&db).await, before);
        assert!(!before.contains(HEADER));
        let client = db.client().await.unwrap();
        let transaction = client.begin().await.unwrap();
        assert_eq!(
            read_fence(
                &transaction,
                SyncStateKey::new(&next.partition_id, &next.client_id)
            )
            .await
            .unwrap(),
            None
        );
        transaction.cancel().await.unwrap();
        assert_eq!(
            store
                .load(SyncStateKey::new(&next.partition_id, &next.client_id))
                .await
                .unwrap(),
            Some(next)
        );
    }

    #[tokio::test]
    async fn pages_are_separate_idempotent_out_of_order_and_survive_store_reopen() {
        let db = database().await;
        let initial = initialized(&db).await;
        let initial = pending(initial, "pending");
        SyncStateStore::new(&db)
            .save_atomic(Some(0), &initial)
            .await
            .unwrap();
        let (start, manifest, context, pages) = wire("session");
        let store = BootstrapStore::new(&db);
        let handle = store.begin(&start, &manifest, &context).await.unwrap();
        assert_eq!(
            store.begin(&start, &manifest, &context).await.unwrap(),
            handle
        );
        store.stage_page(&pages[1], &context, handle).await.unwrap();
        store.stage_page(&pages[1], &context, handle).await.unwrap();
        let reopened = BootstrapStore::new(&db);
        assert_eq!(
            reopened.progress(&context, handle).await.unwrap(),
            Some(vec![1])
        );
        reopened
            .stage_page(&pages[0], &context, handle)
            .await
            .unwrap();
        assert_eq!(
            reopened.progress(&context, handle).await.unwrap(),
            Some(vec![0, 1])
        );
        assert_eq!(
            SyncStateStore::new(&db)
                .load(SyncStateKey::new(&initial.partition_id, &initial.client_id))
                .await
                .unwrap(),
            Some(initial)
        );
        let mut foreign = context.clone();
        foreign.principal_id = "other".into();
        assert!(reopened.cancel(&manifest, &foreign, handle).await.is_err());
        let mut altered = decode_bootstrap_page(&pages[0]).unwrap();
        altered.records[0].state = RecordState::Tombstone { version: 2 };
        assert!(
            reopened
                .stage_page(&encode_bootstrap_page(&altered).unwrap(), &context, handle)
                .await
                .is_err()
        );
        assert_eq!(
            reopened.progress(&context, handle).await.unwrap(),
            Some(vec![0, 1])
        );
    }

    #[tokio::test]
    async fn cancelled_incarnation_never_repeats_even_after_aba_and_reopen() {
        let db = database().await;
        initialized(&db).await;
        let (start_a, manifest_a, context, pages) = wire("a");
        let (start_b, manifest_b, _, _) = wire("b");
        let store = BootstrapStore::new(&db);
        let a = store.begin(&start_a, &manifest_a, &context).await.unwrap();
        store.cancel(&manifest_a, &context, a).await.unwrap();
        assert!(store.begin(&start_a, &manifest_a, &context).await.is_err());
        let b = store.begin(&start_b, &manifest_b, &context).await.unwrap();
        assert!(store.cancel(&manifest_a, &context, a).await.is_err());
        store.cancel(&manifest_b, &context, b).await.unwrap();
        let reopened = BootstrapStore::new(&db);
        let a2 = reopened
            .begin(&start_a, &manifest_a, &context)
            .await
            .unwrap();
        assert!(a2 > b && b > a);
        assert!(reopened.cancel(&manifest_a, &context, a).await.is_err());
        assert!(reopened.stage_page(&pages[0], &context, a).await.is_err());
        assert_eq!(reopened.progress(&context, a2).await.unwrap(), Some(vec![]));
    }

    #[tokio::test]
    async fn barrier_blocks_pages_but_cancel_keeps_barrier_and_pending() {
        let db = database().await;
        let state = initialized(&db).await;
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        let (start, manifest, context, pages) = wire("session");
        let store = BootstrapStore::new(&db);
        let handle = store.begin(&start, &manifest, &context).await.unwrap();
        store.stage_page(&pages[0], &context, handle).await.unwrap();
        SyncStateStore::new(&db)
            .save_scope_barrier(key, barrier())
            .await
            .unwrap();
        assert!(store.stage_page(&pages[1], &context, handle).await.is_err());
        store.cancel(&manifest, &context, handle).await.unwrap();
        assert!(
            SyncStateStore::new(&db)
                .load_scope_barrier(key)
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(
            SyncStateStore::new(&db).load(key).await.unwrap(),
            Some(state)
        );
    }

    #[tokio::test]
    async fn scalar_preflight_rejects_invalid_oversize_blob_before_codec() {
        let db = database().await;
        let state = initialized(&db).await;
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        let (start, manifest, context, _) = wire("session");
        let store = BootstrapStore::new(&db);
        let handle = store.begin(&start, &manifest, &context).await.unwrap();
        let client = db.client().await.unwrap();
        let _: Option<Value> = client
            .upsert(page_id(key, 0))
            .content(PageRow {
                format_version: 1,
                partition_id: key.partition_id.0.clone(),
                client_id: key.client_id.0.clone(),
                index: "0".into(),
                payload: vec![0xff; MAX_SYNC_STATE_BYTES + 1].into(),
            })
            .await
            .unwrap();
        assert!(matches!(
            store.progress(&context, handle).await,
            Err(SyncStateError::StateTooLarge)
        ));
        assert_eq!(
            SyncStateStore::new(&db).load(key).await.unwrap(),
            Some(state)
        );
    }

    #[test]
    fn reservations_use_actual_tagged_encoding_and_reject_scalar_projection() {
        let (_, _, _, pages) = wire("session");
        let mut page = decode_bootstrap_page(&pages[0]).unwrap();
        let initial = page_reservation(&page).unwrap();
        page.records[0].state = RecordState::Present {
            value: CanonicalValue::Object(BTreeMap::from([(
                "bytes".into(),
                CanonicalValue::Bytes(vec![0xff; 4096]),
            )])),
            version: 1,
            reference: None,
        };
        let encoded = page_reservation(&page).unwrap();
        assert!(encoded > initial + 4096); // native base64 + tagged JSON expansion, not CBOR estimate
        page.records[0].state = RecordState::Present {
            value: CanonicalValue::String("scalar".into()),
            version: 1,
            reference: None,
        };
        assert!(matches!(
            page_reservation(&page),
            Err(SyncStateError::RecordValueMustBeObject)
        ));
    }

    // Explicit begin/barrier and ordinary writers MUST conflict on the same metadata
    // record even when their snapshots predate stage schema. No sleeps/scheduling oracle.
    #[tokio::test]
    async fn metadata_fence_conflicts_in_both_orders_and_unrelated_keys_do_not() {
        for persistent in [false, true] {
            for stage_first in [false, true] {
                let directory = temp_dir::TempDir::new().unwrap();
                let db = if persistent {
                    connect(ConnectOptions {
                        endpoint: format!("surrealkv://{}", directory.path().display()),
                        namespace: Some("bootstrap".into()),
                        database: Some("bootstrap".into()),
                    })
                    .await
                    .unwrap()
                } else {
                    database().await
                };

                let state = initialized(&db).await;
                let key = SyncStateKey::new(&state.partition_id, &state.client_id);
                let raw_before: Option<Value> = db
                    .client()
                    .await
                    .unwrap()
                    .select(meta_record_id(key))
                    .await
                    .unwrap();
                let client = db.client().await.unwrap();
                let a = client.clone().begin().await.unwrap();
                let b = client.begin().await.unwrap();
                assert_eq!(read_fence(&a, key).await.unwrap(), None);
                assert_eq!(read_fence(&b, key).await.unwrap(), None);
                // Snapshot/absence is captured BEFORE explicit begin defines stage schema.
                a.query(SCHEMA).await.unwrap().check().unwrap();
                let (_, _, context, _) = wire("fence");
                fence_first_barrier(&a, key, &context).await.unwrap();
                write_fence(&a, key, "1:a".into()).await.unwrap();
                guard_barrier_write(&b, key).await.unwrap();
                let _: Option<ScopeBarrier> =
                    b.upsert(barrier_id(key)).content(barrier()).await.unwrap();
                if stage_first {
                    a.commit().await.unwrap();
                    assert!(b.commit().await.is_err());
                } else {
                    b.commit().await.unwrap();
                    assert!(a.commit().await.is_err());
                }
                let actual_barrier = SyncStateStore::new(&db)
                    .load_scope_barrier(key)
                    .await
                    .unwrap();
                assert_eq!(actual_barrier.is_none(), stage_first);
                assert_eq!(
                    SyncStateStore::new(&db).load(key).await.unwrap(),
                    Some(state.clone())
                );
                let raw_after: Option<Value> = db
                    .client()
                    .await
                    .unwrap()
                    .select(meta_record_id(key))
                    .await
                    .unwrap();
                if let (Some(Value::Object(before)), Some(Value::Object(mut after))) =
                    (raw_before, raw_after)
                {
                    after.remove("bootstrap_fence");
                    assert!(
                        before == after,
                        "begin/barrier changed preexisting metadata fields/bytes"
                    );
                } else {
                    panic!("metadata presence/type changed");
                }
                db.close().await.unwrap();
            }
        }
        let db = database().await;
        let state = initialized(&db).await;
        let mut other = state.clone();
        other.client_id = ClientId("unrelated".into());
        SyncStateStore::new(&db)
            .save_atomic(None, &other)
            .await
            .unwrap();
        let client = db.client().await.unwrap();
        let a = client.clone().begin().await.unwrap();
        let b = client.begin().await.unwrap();
        guard_barrier_write(&a, SyncStateKey::new(&state.partition_id, &state.client_id))
            .await
            .unwrap();
        guard_barrier_write(&b, SyncStateKey::new(&other.partition_id, &other.client_id))
            .await
            .unwrap();
        let _: Option<ScopeBarrier> = a
            .upsert(barrier_id(SyncStateKey::new(
                &state.partition_id,
                &state.client_id,
            )))
            .content(barrier())
            .await
            .unwrap();
        let _: Option<ScopeBarrier> = b
            .upsert(barrier_id(SyncStateKey::new(
                &other.partition_id,
                &other.client_id,
            )))
            .content(barrier())
            .await
            .unwrap();
        a.commit().await.unwrap();
        b.commit().await.unwrap();
    }
    #[tokio::test]
    async fn journal_growth_capacity_failure_rolls_back_every_component() {
        let db = database().await;
        let initial = initialized(&db).await;
        let (start, manifest, context, _) = wire("capacity");
        let store = BootstrapStore::new(&db);
        let handle = store.begin(&start, &manifest, &context).await.unwrap();
        let mut next = initial.clone();
        next.revision = 1;
        for index in 0..26 {
            next.confirmed.insert(
                ProtocolId(format!("item:large{index}")),
                RecordState::Present {
                    value: serde_json::json!({"value": "x".repeat(1024*1024)}),
                    version: 1,
                    reference: None,
                },
            );
        }
        // Current-only state is valid. Combined current+stage+final reservation is not.
        assert!(state_budget(&next).unwrap().0 < MAX_AGGREGATE_STATE_BYTES);
        assert!(matches!(
            SyncStateStore::new(&db).save_atomic(Some(0), &next).await,
            Err(SyncStateError::StateTooLarge)
        ));
        assert_eq!(
            SyncStateStore::new(&db)
                .load(SyncStateKey::new(&initial.partition_id, &initial.client_id))
                .await
                .unwrap(),
            Some(initial)
        );
        assert_eq!(
            store.progress(&context, handle).await.unwrap(),
            Some(vec![])
        );
    }

    #[tokio::test]
    async fn cancelled_page_transaction_does_not_change_stage_or_accounting() {
        let db = database().await;
        let state = initialized(&db).await;
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        let (start, manifest, context, pages) = wire("rollback");
        let store = BootstrapStore::new(&db);
        let handle = store.begin(&start, &manifest, &context).await.unwrap();
        let client = db.client().await.unwrap();
        let transaction = client.begin().await.unwrap();
        let _: Option<Value> = transaction
            .upsert(page_id(key, 0))
            .content(PageRow {
                format_version: 1,
                partition_id: key.partition_id.0.clone(),
                client_id: key.client_id.0.clone(),
                index: "0".into(),
                payload: pages[0].clone().into(),
            })
            .await
            .unwrap();
        write_fence(&transaction, key, "999:a".into())
            .await
            .unwrap();
        transaction.cancel().await.unwrap();
        assert_eq!(
            store.progress(&context, handle).await.unwrap(),
            Some(vec![])
        );
        store.stage_page(&pages[0], &context, handle).await.unwrap();
        // Treat this successful commit's response as lost: reopening/exact retry cannot double charge.
        let reopened = BootstrapStore::new(&db);
        reopened
            .stage_page(&pages[0], &context, handle)
            .await
            .unwrap();
        assert_eq!(
            reopened.progress(&context, handle).await.unwrap(),
            Some(vec![0])
        );
    }
    #[tokio::test]
    async fn surrealkv_restart_preserves_stage_counter_and_old_pending_without_migration() {
        let directory = temp_dir::TempDir::new().unwrap();
        let options = || ConnectOptions {
            endpoint: format!("surrealkv://{}", directory.path().display()),
            namespace: Some("bootstrap".into()),
            database: Some("bootstrap".into()),
        };
        let db = connect(options()).await.unwrap();
        let initial = initialized(&db).await;
        let pending = pending(initial, "preserved");
        let key = SyncStateKey::new(&pending.partition_id, &pending.client_id);
        SyncStateStore::new(&db)
            .save_atomic(Some(0), &pending)
            .await
            .unwrap();
        let (start, manifest, context, pages) = wire("durable");
        let handle = BootstrapStore::new(&db)
            .begin(&start, &manifest, &context)
            .await
            .unwrap();
        BootstrapStore::new(&db)
            .stage_page(&pages[1], &context, handle)
            .await
            .unwrap();
        db.close().await.unwrap();
        drop(db);
        let db = connect(options()).await.unwrap();
        assert_eq!(
            BootstrapStore::new(&db)
                .progress(&context, handle)
                .await
                .unwrap(),
            Some(vec![1])
        );
        assert_eq!(
            SyncStateStore::new(&db).load(key).await.unwrap(),
            Some(pending.clone())
        );
        BootstrapStore::new(&db)
            .cancel(&manifest, &context, handle)
            .await
            .unwrap();
        db.close().await.unwrap();
        drop(db);
        let db = connect(options()).await.unwrap();
        assert_eq!(
            BootstrapStore::new(&db)
                .progress(&context, handle)
                .await
                .unwrap(),
            None
        );
        assert!(
            BootstrapStore::new(&db)
                .begin(&start, &manifest, &context)
                .await
                .is_err()
        );
        assert_eq!(
            SyncStateStore::new(&db).load(key).await.unwrap(),
            Some(pending)
        );
        db.close().await.unwrap();
    }
    #[tokio::test]
    async fn normal_growth_preserves_stage_then_cancel_keeps_counter_monotonic() {
        let db = database().await;
        let state = initialized(&db).await;
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        let (start, manifest, context, pages) = wire("growth");
        let stage = BootstrapStore::new(&db);
        let handle = stage.begin(&start, &manifest, &context).await.unwrap();
        stage.stage_page(&pages[0], &context, handle).await.unwrap();
        let next = pending(state.clone(), "native-pending");
        SyncStateStore::new(&db)
            .apply_transition_atomic(&state, &next)
            .await
            .unwrap();
        assert_eq!(
            stage.progress(&context, handle).await.unwrap(),
            Some(vec![0])
        );
        stage.cancel(&manifest, &context, handle).await.unwrap();
        let second = pending(next.clone(), "inactive-pending");
        SyncStateStore::new(&db)
            .apply_transition_atomic(&next, &second)
            .await
            .unwrap();
        let client = db.client().await.unwrap();
        let transaction = client.begin().await.unwrap();
        let fence = read_fence(&transaction, key).await.unwrap().unwrap();
        let (counter, active) = parse_metadata_fence(&fence).unwrap();
        assert!(counter > handle);
        assert!(!active);
        transaction.cancel().await.unwrap();
        assert_eq!(
            SyncStateStore::new(&db).load(key).await.unwrap(),
            Some(second)
        );
    }

    #[tokio::test]
    async fn missing_stage_table_and_malformed_counter_never_recreate_or_zero_inventory() {
        let db = database().await;
        let state = initialized(&db).await;
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        let (start, manifest, context, _) = wire("corrupt");
        let stage = BootstrapStore::new(&db);
        let handle = stage.begin(&start, &manifest, &context).await.unwrap();
        let client = db.client().await.unwrap();
        client
            .query("REMOVE TABLE _sync_bootstrap_page")
            .await
            .unwrap()
            .check()
            .unwrap();
        assert!(matches!(
            stage.progress(&context, handle).await,
            Err(SyncStateError::CorruptState)
        ));
        let next = pending(state.clone(), "no-recreate");
        assert!(
            SyncStateStore::new(&db)
                .save_atomic(Some(0), &next)
                .await
                .is_err()
        );
        assert!(!schema(&db).await.contains(PAGE));
        client
            .query("UPDATE $id SET bootstrap_fence='01:a'")
            .bind(("id", meta_record_id(key)))
            .await
            .unwrap()
            .check()
            .unwrap();
        assert!(matches!(
            stage.progress(&context, handle).await,
            Err(SyncStateError::CorruptState)
        ));
        assert_eq!(
            SyncStateStore::new(&db).load(key).await.unwrap(),
            Some(state)
        );
    }
    #[tokio::test]
    async fn existing_blocked_barrier_is_never_removed_by_first_begin() {
        let db = database().await;
        let state = initialized(&db).await;
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        SyncStateStore::new(&db)
            .save_scope_barrier(key, barrier())
            .await
            .unwrap();
        let before: Option<Value> = db
            .client()
            .await
            .unwrap()
            .select(barrier_id(key))
            .await
            .unwrap();
        let before_schema = schema(&db).await;
        let (start, manifest, context, _) = wire("blocked");
        assert!(
            BootstrapStore::new(&db)
                .begin(&start, &manifest, &context)
                .await
                .is_err()
        );
        let after: Option<Value> = db
            .client()
            .await
            .unwrap()
            .select(barrier_id(key))
            .await
            .unwrap();
        assert!(before == after, "existing barrier changed");
        assert_eq!(schema(&db).await, before_schema);
        assert_eq!(
            SyncStateStore::new(&db).load(key).await.unwrap(),
            Some(state)
        );
    }
    #[tokio::test]
    async fn page_reservation_and_real_journal_transition_conflict_before_either_can_overwrite() {
        for persistent in [false, true] {
            for page_first in [false, true] {
                let directory = temp_dir::TempDir::new().unwrap();
                let db = if persistent {
                    connect(ConnectOptions {
                        endpoint: format!("surrealkv://{}", directory.path().display()),
                        namespace: Some("bootstrap".into()),
                        database: Some("bootstrap".into()),
                    })
                    .await
                    .unwrap()
                } else {
                    database().await
                };
                let state = initialized(&db).await;
                let key = SyncStateKey::new(&state.partition_id, &state.client_id);
                let (start, manifest, context, pages) = wire("race");
                let stage = BootstrapStore::new(&db);
                let handle = stage.begin(&start, &manifest, &context).await.unwrap();
                let client = db.client().await.unwrap();
                let a = client.clone().begin().await.unwrap();
                let b = client.begin().await.unwrap();
                let old = read_fence(&a, key).await.unwrap();
                assert_eq!(read_fence(&b, key).await.unwrap(), old);
                let next = pending(state.clone(), "racing-pending");
                crate::sync_state::test_transition_in_transaction(&b, &state, &next)
                    .await
                    .unwrap();
                let _: Option<PageRow> = a
                    .upsert(page_id(key, 0))
                    .content(PageRow {
                        format_version: 1,
                        partition_id: key.partition_id.0.clone(),
                        client_id: key.client_id.0.clone(),
                        index: "0".into(),
                        payload: pages[0].clone().into(),
                    })
                    .await
                    .unwrap();
                write_fence(&a, key, next_fence(old).unwrap())
                    .await
                    .unwrap();
                if page_first {
                    a.commit().await.unwrap();
                    assert!(b.commit().await.is_err());
                } else {
                    b.commit().await.unwrap();
                    assert!(a.commit().await.is_err());
                }
                assert_eq!(
                    stage.progress(&context, handle).await.unwrap(),
                    Some(if page_first { vec![0] } else { vec![] })
                );
                assert_eq!(
                    SyncStateStore::new(&db).load(key).await.unwrap(),
                    Some(if page_first { state.clone() } else { next })
                );
                db.close().await.unwrap();
            }
        }
    }
    #[tokio::test]
    async fn exhausted_counter_rolls_back_page_cancel_and_journal_growth() {
        let db = database().await;
        let state = initialized(&db).await;
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        let (start, manifest, context, pages) = wire("overflow");
        let stage = BootstrapStore::new(&db);
        let handle = stage.begin(&start, &manifest, &context).await.unwrap();
        db.client()
            .await
            .unwrap()
            .query("UPDATE $id SET bootstrap_fence = $fence")
            .bind(("id", meta_record_id(key)))
            .bind(("fence", format!("{}:a", u64::MAX)))
            .await
            .unwrap()
            .check()
            .unwrap();
        assert!(matches!(
            stage.stage_page(&pages[0], &context, handle).await,
            Err(SyncStateError::StateTooLarge)
        ));
        assert!(matches!(
            stage.cancel(&manifest, &context, handle).await,
            Err(SyncStateError::StateTooLarge)
        ));
        assert!(matches!(
            SyncStateStore::new(&db)
                .save_atomic(Some(0), &pending(state.clone(), "overflow-pending"))
                .await,
            Err(SyncStateError::StateTooLarge)
        ));
        assert_eq!(
            stage.progress(&context, handle).await.unwrap(),
            Some(vec![])
        );
        assert_eq!(
            SyncStateStore::new(&db).load(key).await.unwrap(),
            Some(state)
        );
    }
    #[tokio::test]
    async fn combined_scalar_rejection_precedes_current_decode_in_begin_and_barrier() {
        let db = database().await;
        let state = initialized(&db).await;
        let key = SyncStateKey::new(&state.partition_id, &state.client_id);
        let (start, manifest, context, _) = wire("predecode");
        let stage = BootstrapStore::new(&db);
        stage.begin(&start, &manifest, &context).await.unwrap();
        let client = db.client().await.unwrap();
        // Malformed JSON of a valid small length would produce Decode if the
        // current-state decoder is entered. Aggregate stage inventory must win.
        client
            .query("UPDATE $id SET payload = $payload")
            .bind(("id", meta_record_id(key)))
            .bind(("payload", Bytes::from(vec![0xff])))
            .await
            .unwrap()
            .check()
            .unwrap();
        for index in 0..16 {
            let _: Option<PageRow> = client
                .upsert(page_id(key, index))
                .content(PageRow {
                    format_version: 1,
                    partition_id: key.partition_id.0.clone(),
                    client_id: key.client_id.0.clone(),
                    index: index.to_string(),
                    payload: vec![0xff; MAX_SYNC_STATE_BYTES].into(),
                })
                .await
                .unwrap();
        }
        assert!(
            matches!(
                stage.begin(&start, &manifest, &context).await,
                Err(SyncStateError::StateTooLarge)
            ),
            "current decoder ran before combined scalar guard"
        );
        assert!(
            matches!(
                SyncStateStore::new(&db)
                    .save_scope_barrier(key, barrier())
                    .await,
                Err(SyncStateError::StateTooLarge)
            ),
            "barrier current decoder ran before combined scalar guard"
        );
        let existing: Option<ScopeBarrier> = client.select(barrier_id(key)).await.unwrap();
        assert!(existing.is_none());
        db.close().await.unwrap();
    }
    #[tokio::test]
    async fn native_key_boundary_is_enforced_at_every_entry_before_database_access() {
        let db = database().await;
        let (start, manifest, context, pages) = wire("key-limit");
        db.close().await.unwrap();
        for partition in [false, true] {
            let mut oversized = context.clone();
            if partition {
                oversized.partition_id = PartitionId("é".repeat(257));
            } else {
                oversized.client_id = ClientId("c".repeat(513));
            }
            let store = BootstrapStore::new(&db);
            assert!(matches!(
                store.begin(&start, &manifest, &oversized).await,
                Err(SyncStateError::IdentityTooLarge)
            ));
            assert!(matches!(
                store.stage_page(&pages[0], &oversized, 1).await,
                Err(SyncStateError::IdentityTooLarge)
            ));
            assert!(matches!(
                store.progress(&oversized, 1).await,
                Err(SyncStateError::IdentityTooLarge)
            ));
            assert!(matches!(
                store.cancel(&manifest, &oversized, 1).await,
                Err(SyncStateError::IdentityTooLarge)
            ));
        }
    }

    #[tokio::test]
    async fn exact_512_utf8_byte_keys_support_begin_page_progress_and_cancel() {
        let db = database().await;
        db.client()
            .await
            .unwrap()
            .query("DEFINE TABLE item SCHEMALESS")
            .await
            .unwrap()
            .check()
            .unwrap();
        let mut state = state();
        state.partition_id = PartitionId("é".repeat(256));
        state.client_id = ClientId("c".repeat(512));
        let store = SyncStateStore::new(&db);
        store.define_schema().await.unwrap();
        store.save_atomic(None, &state).await.unwrap();
        let (start, manifest, mut context, pages) = wire("exact-key");
        context.partition_id = state.partition_id.clone();
        context.client_id = state.client_id.clone();
        let mut start = decode_bootstrap_start(&start).unwrap();
        start.partition_id = context.partition_id.clone();
        start.client_id = context.client_id.clone();
        let mut manifest = decode_bootstrap_manifest(&manifest).unwrap();
        manifest.context = context.clone();
        manifest.start_fingerprint = bootstrap_start_fingerprint(&start).unwrap();
        let digest = bootstrap_manifest_digest(&manifest).unwrap();
        let manifest = encode_bootstrap_manifest(&manifest).unwrap();
        let start = encode_bootstrap_start(&start).unwrap();
        let mut page = decode_bootstrap_page(&pages[0]).unwrap();
        page.manifest_digest = digest;
        let page = encode_bootstrap_page(&page).unwrap();
        let stage = BootstrapStore::new(&db);
        let handle = stage.begin(&start, &manifest, &context).await.unwrap();
        stage.stage_page(&page, &context, handle).await.unwrap();
        assert_eq!(
            stage.progress(&context, handle).await.unwrap(),
            Some(vec![0])
        );
        stage.cancel(&manifest, &context, handle).await.unwrap();
        assert_eq!(stage.progress(&context, handle).await.unwrap(), None);
    }
}
