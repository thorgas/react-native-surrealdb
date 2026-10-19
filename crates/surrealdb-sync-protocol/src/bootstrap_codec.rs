//! Canonical arrays and streaming verification for bootstrap/1. No whole-snapshot CBOR assembly.
use crate::bootstrap::*;
use crate::canonical::{encode_cbor_value, preflight_canonical_cbor};
use crate::http_codec::{
    Decoder, Encoder, array, array_vec, decode_checkpoint, decode_family_message,
    encode_family_message, identity, integer, unsigned,
};
use crate::{
    CanonicalValue, ClientId, CodecError, Cursor, PartitionId, RecordId, RecordState,
    ScopeIdentity, ScopeSnapshot, SnapshotRecord,
};
use ciborium::value::Value as Cbor;
use sha2::{Digest, Sha256};

fn encode_message(
    kind: u64,
    payload: impl FnOnce(&mut Encoder) -> Result<Cbor, CodecError>,
) -> Result<Vec<u8>, CodecError> {
    let bytes = encode_family_message(BOOTSTRAP_NAME, kind, payload)?;
    preflight_canonical_cbor(&bytes)?;
    Ok(bytes)
}
const START_DOMAIN: &[u8] = b"surrealdb-sync-bootstrap/1/start/sha256";
const PAGE_DOMAIN: &[u8] = b"surrealdb-sync-bootstrap/1/page-content/sha256";
const ROOT_DOMAIN: &[u8] = b"surrealdb-sync-bootstrap/1/ordered-root/sha256";
const MANIFEST_DOMAIN: &[u8] = b"surrealdb-sync-bootstrap/1/manifest/sha256";
fn frame(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}
fn digest(domain: &[u8], bytes: &[u8]) -> BootstrapDigest {
    let mut h = Sha256::new();
    frame(&mut h, domain);
    frame(&mut h, bytes);
    BootstrapDigest(h.finalize().into())
}
fn decoded_digest(value: Cbor) -> Result<BootstrapDigest, CodecError> {
    let Cbor::Bytes(bytes) = value else {
        return Err(CodecError::InvalidEnvelope);
    };
    Ok(BootstrapDigest(
        bytes.try_into().map_err(|_| CodecError::InvalidEnvelope)?,
    ))
}
fn canonical(input: &[u8], encoded: Vec<u8>) -> Result<(), CodecError> {
    if input == encoded {
        Ok(())
    } else {
        Err(CodecError::NonCanonical)
    }
}
fn bounded_index(index: u64) -> Result<(), CodecError> {
    if index < MAX_BOOTSTRAP_PAGES {
        Ok(())
    } else {
        Err(CodecError::ContainerLimit)
    }
}
fn valid_limits(l: BootstrapLimits) -> bool {
    l.max_records > 0
        && l.max_records <= MAX_BOOTSTRAP_RECORDS
        && l.max_pages > 0
        && l.max_pages <= MAX_BOOTSTRAP_PAGES
        && l.max_content_bytes > 0
        && l.max_content_bytes <= MAX_BOOTSTRAP_CONTENT_BYTES
}
fn valid_manifest(m: &BootstrapManifest) -> Result<(), CodecError> {
    if !valid_limits(m.limits)
        || m.page_count > m.limits.max_pages
        || m.record_count > m.limits.max_records
        || m.content_bytes > m.limits.max_content_bytes
    {
        return Err(CodecError::ContainerLimit);
    }
    if (m.page_count == 0 && (m.record_count != 0 || m.content_bytes != 0))
        || (m.page_count > 0 && (m.record_count < m.page_count || m.content_bytes == 0))
    {
        return Err(CodecError::InvalidEnvelope);
    }
    Ok(())
}
fn scope(e: &mut Encoder, s: &ScopeSnapshot) -> Result<Cbor, CodecError> {
    e.item(3)?;
    Ok(Cbor::Array(vec![
        e.identity_text(&s.identity.0)?,
        integer(s.authorization_revision),
        integer(s.subscription_revision),
    ]))
}
fn decode_scope(v: Cbor) -> Result<ScopeSnapshot, CodecError> {
    let [id, auth, sub] = array(v)?;
    Ok(ScopeSnapshot {
        identity: ScopeIdentity(identity(id)?),
        authorization_revision: unsigned(auth)?,
        subscription_revision: unsigned(sub)?,
    })
}
fn context(e: &mut Encoder, c: &BootstrapContext) -> Result<Cbor, CodecError> {
    e.item(7)?;
    Ok(Cbor::Array(vec![
        e.identity_text(&c.authority_id)?,
        e.identity_text(&c.principal_id)?,
        e.identity_text(&c.partition_id.0)?,
        e.identity_text(&c.client_id.0)?,
        e.identity_text(&c.journal_generation)?,
        scope(e, &c.scope)?,
    ]))
}
fn decode_context(v: Cbor) -> Result<BootstrapContext, CodecError> {
    let [a, p, partition, client, g, s] = array(v)?;
    Ok(BootstrapContext {
        authority_id: identity(a)?,
        principal_id: identity(p)?,
        partition_id: PartitionId(identity(partition)?),
        client_id: ClientId(identity(client)?),
        journal_generation: identity(g)?,
        scope: decode_scope(s)?,
    })
}

pub fn encode_bootstrap_start(r: &BootstrapStartRequest) -> Result<Vec<u8>, CodecError> {
    encode_message(0, |e| {
        e.item(7)?;
        Ok(Cbor::Array(vec![
            e.identity_text(&r.partition_id.0)?,
            e.identity_text(&r.client_id.0)?,
            e.identity_text(&r.request_id)?,
            e.identity_text(&r.requested_scope.0)?,
            integer(r.subscription_revision),
            e.identity_text(&r.journal_generation)?,
        ]))
    })
}
pub fn decode_bootstrap_start(bytes: &[u8]) -> Result<BootstrapStartRequest, CodecError> {
    let [p, c, id, s, rev, g] = array(decode_family_message(bytes, BOOTSTRAP_NAME, 0)?)?;
    let r = BootstrapStartRequest {
        partition_id: PartitionId(identity(p)?),
        client_id: ClientId(identity(c)?),
        request_id: identity(id)?,
        requested_scope: ScopeIdentity(identity(s)?),
        subscription_revision: unsigned(rev)?,
        journal_generation: identity(g)?,
    };
    canonical(bytes, encode_bootstrap_start(&r)?)?;
    Ok(r)
}
pub fn bootstrap_start_fingerprint(
    r: &BootstrapStartRequest,
) -> Result<BootstrapDigest, CodecError> {
    Ok(digest(START_DOMAIN, &encode_bootstrap_start(r)?))
}

pub fn encode_bootstrap_manifest(m: &BootstrapManifest) -> Result<Vec<u8>, CodecError> {
    valid_manifest(m)?;
    encode_message(1, |e| {
        e.item(12)?;
        let c = context(e, &m.context)?;
        Ok(Cbor::Array(vec![
            e.identity_text(&m.session_id)?,
            e.identity_text(&m.start_request_id)?,
            e.digest(&m.start_fingerprint.0)?,
            c,
            Cbor::Array(vec![integer(m.cursor.epoch), integer(m.cursor.sequence)]),
            Cbor::Array(vec![
                integer(m.limits.max_records),
                integer(m.limits.max_pages),
                integer(m.limits.max_content_bytes),
            ]),
            integer(m.page_count),
            integer(m.record_count),
            integer(m.content_bytes),
            e.digest(&m.root.0)?,
            integer(m.expires_at),
        ]))
    })
}
pub fn decode_bootstrap_manifest(bytes: &[u8]) -> Result<BootstrapManifest, CodecError> {
    let [
        id,
        start,
        fp,
        ctx,
        cursor,
        limits,
        pages,
        records,
        size,
        root,
        expiry,
    ] = array(decode_family_message(bytes, BOOTSTRAP_NAME, 1)?)?;
    let [epoch, sequence] = array(cursor)?;
    let [max_records, max_pages, max_content_bytes] = array(limits)?;
    let m = BootstrapManifest {
        session_id: identity(id)?,
        start_request_id: identity(start)?,
        start_fingerprint: decoded_digest(fp)?,
        context: decode_context(ctx)?,
        cursor: Cursor {
            epoch: unsigned(epoch)?,
            sequence: unsigned(sequence)?,
        },
        limits: BootstrapLimits {
            max_records: unsigned(max_records)?,
            max_pages: unsigned(max_pages)?,
            max_content_bytes: unsigned(max_content_bytes)?,
        },
        page_count: unsigned(pages)?,
        record_count: unsigned(records)?,
        content_bytes: unsigned(size)?,
        root: decoded_digest(root)?,
        expires_at: unsigned(expiry)?,
    };
    canonical(bytes, encode_bootstrap_manifest(&m)?)?;
    Ok(m)
}
pub fn bootstrap_manifest_digest(m: &BootstrapManifest) -> Result<BootstrapDigest, CodecError> {
    Ok(digest(MANIFEST_DOMAIN, &encode_bootstrap_manifest(m)?))
}
/// Equality/fingerprint checks bind caller-supplied trusted context. They do NOT authenticate it.
pub fn validate_bootstrap_manifest_context(
    m: &BootstrapManifest,
    r: &BootstrapStartRequest,
    expected: &BootstrapContext,
) -> Result<(), CodecError> {
    encode_bootstrap_manifest(m)?;
    encode_bootstrap_start(r)?;
    if m.context != *expected
        || m.start_request_id != r.request_id
        || m.start_fingerprint != bootstrap_start_fingerprint(r)?
        || r.partition_id != expected.partition_id
        || r.client_id != expected.client_id
        || r.journal_generation != expected.journal_generation
        || r.requested_scope != expected.scope.identity
        || r.subscription_revision != expected.scope.subscription_revision
    {
        return Err(CodecError::InvalidEnvelope);
    }
    Ok(())
}

pub fn encode_bootstrap_page_request(r: &BootstrapPageRequest) -> Result<Vec<u8>, CodecError> {
    bounded_index(r.index)?;
    encode_message(2, |e| {
        e.item(5)?;
        Ok(Cbor::Array(vec![
            e.identity_text(&r.partition_id.0)?,
            e.identity_text(&r.client_id.0)?,
            e.identity_text(&r.session_id)?,
            integer(r.index),
        ]))
    })
}
pub fn decode_bootstrap_page_request(bytes: &[u8]) -> Result<BootstrapPageRequest, CodecError> {
    let [p, c, s, i] = array(decode_family_message(bytes, BOOTSTRAP_NAME, 2)?)?;
    let r = BootstrapPageRequest {
        partition_id: PartitionId(identity(p)?),
        client_id: ClientId(identity(c)?),
        session_id: identity(s)?,
        index: unsigned(i)?,
    };
    canonical(bytes, encode_bootstrap_page_request(&r)?)?;
    Ok(r)
}
fn validate_records(records: &[SnapshotRecord<CanonicalValue>]) -> Result<(), CodecError> {
    if records.is_empty() || records.len() > crate::MAX_CONTAINER_ITEMS {
        return Err(CodecError::ContainerLimit);
    }
    let mut previous: Option<&RecordId> = None;
    for record in records {
        if previous.is_some_and(|id| id >= &record.record_id) {
            return Err(CodecError::InvalidEnvelope);
        }
        match &record.state {
            RecordState::Absent => return Err(CodecError::InvalidEnvelope),
            RecordState::Present { version, .. } | RecordState::Tombstone { version }
                if *version == 0 =>
            {
                return Err(CodecError::InvalidEnvelope);
            }
            _ => {}
        }
        previous = Some(&record.record_id);
    }
    Ok(())
}
fn preflight_page(p: &BootstrapPage) -> Result<(), CodecError> {
    bounded_index(p.index)?;
    validate_records(&p.records)?;
    let mut e = Encoder::family(BOOTSTRAP_NAME)?;
    e.item(5)?;
    e.preflight_identity(&p.session_id)?;
    e.preflight_digest()?;
    e.preflight_snapshot_records(&p.records, 5)
}
pub fn encode_bootstrap_page(p: &BootstrapPage) -> Result<Vec<u8>, CodecError> {
    preflight_page(p)?;
    encode_message(3, |e| {
        e.item(5)?;
        let id = e.identity_text(&p.session_id)?;
        let hash = e.digest(&p.manifest_digest.0)?;
        e.vec(p.records.len())?;
        let records = p
            .records
            .iter()
            .map(|r| e.snapshot_record(r))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Cbor::Array(vec![
            id,
            hash,
            integer(p.index),
            Cbor::Array(records),
        ]))
    })
}
pub fn decode_bootstrap_page(bytes: &[u8]) -> Result<BootstrapPage, CodecError> {
    let [id, hash, index, records] = array(decode_family_message(bytes, BOOTSTRAP_NAME, 3)?)?;
    let records = array_vec(records)?
        .into_iter()
        .map(Decoder::snapshot_record)
        .collect::<Result<Vec<_>, _>>()?;
    let p = BootstrapPage {
        session_id: identity(id)?,
        manifest_digest: decoded_digest(hash)?,
        index: unsigned(index)?,
        records,
    };
    canonical(bytes, encode_bootstrap_page(&p)?)?;
    Ok(p)
}
fn encode_content(
    index: u64,
    records: &[SnapshotRecord<CanonicalValue>],
) -> Result<Vec<u8>, CodecError> {
    let mut e = Encoder::new();
    e.item(3)?;
    e.vec(records.len())?;
    let rows = records
        .iter()
        .map(|r| e.snapshot_record(r))
        .collect::<Result<Vec<_>, _>>()?;
    encode_cbor_value(Cbor::Array(vec![integer(index), Cbor::Array(rows)]))
}
/// Full wire budget is checked first, even though the content digest excludes response headers.
pub fn bootstrap_page_content_digest(p: &BootstrapPage) -> Result<BootstrapDigest, CodecError> {
    drop(encode_bootstrap_page(p)?);
    Ok(digest(PAGE_DOMAIN, &encode_content(p.index, &p.records)?))
}

pub fn encode_bootstrap_complete(r: &BootstrapCompleteRequest) -> Result<Vec<u8>, CodecError> {
    encode_message(4, |e| {
        e.item(5)?;
        Ok(Cbor::Array(vec![
            e.identity_text(&r.partition_id.0)?,
            e.identity_text(&r.client_id.0)?,
            e.identity_text(&r.session_id)?,
            e.digest(&r.manifest_digest.0)?,
        ]))
    })
}
pub fn decode_bootstrap_complete(bytes: &[u8]) -> Result<BootstrapCompleteRequest, CodecError> {
    let [p, c, s, h] = array(decode_family_message(bytes, BOOTSTRAP_NAME, 4)?)?;
    let r = BootstrapCompleteRequest {
        partition_id: PartitionId(identity(p)?),
        client_id: ClientId(identity(c)?),
        session_id: identity(s)?,
        manifest_digest: decoded_digest(h)?,
    };
    canonical(bytes, encode_bootstrap_complete(&r)?)?;
    Ok(r)
}
pub fn encode_bootstrap_receipt(r: &BootstrapReceipt) -> Result<Vec<u8>, CodecError> {
    encode_message(5, |e| {
        e.item(5)?;
        Ok(Cbor::Array(vec![
            e.identity_text(&r.session_id)?,
            e.digest(&r.manifest_digest.0)?,
            e.checkpoint(&r.checkpoint)?,
            integer(r.expires_at),
        ]))
    })
}
pub fn decode_bootstrap_receipt(bytes: &[u8]) -> Result<BootstrapReceipt, CodecError> {
    let [s, h, c, t] = array(decode_family_message(bytes, BOOTSTRAP_NAME, 5)?)?;
    let r = BootstrapReceipt {
        session_id: identity(s)?,
        manifest_digest: decoded_digest(h)?,
        checkpoint: decode_checkpoint(c)?,
        expires_at: unsigned(t)?,
    };
    canonical(bytes, encode_bootstrap_receipt(&r)?)?;
    Ok(r)
}
pub fn validate_bootstrap_receipt(
    m: &BootstrapManifest,
    r: &BootstrapReceipt,
) -> Result<(), CodecError> {
    encode_bootstrap_receipt(r)?;
    if r.session_id != m.session_id
        || r.manifest_digest != bootstrap_manifest_digest(m)?
        || r.checkpoint.cursor != m.cursor
        || r.checkpoint.scope != m.context.scope
        || r.expires_at != m.expires_at
    {
        return Err(CodecError::InvalidEnvelope);
    }
    Ok(())
}

/// Bounded constant-history root builder; retains no pages or whole snapshot.
#[derive(Clone)]
pub struct BootstrapContentHasher {
    session_id: String,
    next_index: u64,
    records: u64,
    bytes: u64,
    last_id: Option<RecordId>,
    root: Sha256,
}
impl BootstrapContentHasher {
    pub fn new(session_id: String) -> Result<Self, CodecError> {
        if session_id.is_empty() || session_id.len() > crate::MAX_IDENTITY_BYTES {
            return Err(CodecError::InvalidIdentifier);
        }
        let mut root = Sha256::new();
        frame(&mut root, ROOT_DOMAIN);
        Ok(Self {
            session_id,
            next_index: 0,
            records: 0,
            bytes: 0,
            last_id: None,
            root,
        })
    }
    /// An error leaves hash state, counters and last ID unchanged.
    pub fn push(&mut self, p: &BootstrapPage) -> Result<(), CodecError> {
        if p.session_id != self.session_id || p.index != self.next_index {
            return Err(CodecError::InvalidEnvelope);
        }
        drop(encode_bootstrap_page(p)?);
        if self
            .last_id
            .as_ref()
            .is_some_and(|id| id >= &p.records[0].record_id)
        {
            return Err(CodecError::InvalidEnvelope);
        }
        let content = encode_content(p.index, &p.records)?;
        let records = self
            .records
            .checked_add(p.records.len() as u64)
            .ok_or(CodecError::ContainerLimit)?;
        let bytes = self
            .bytes
            .checked_add(content.len() as u64)
            .ok_or(CodecError::EncodedLimit)?;
        let next = self
            .next_index
            .checked_add(1)
            .ok_or(CodecError::ContainerLimit)?;
        if records > MAX_BOOTSTRAP_RECORDS
            || next > MAX_BOOTSTRAP_PAGES
            || bytes > MAX_BOOTSTRAP_CONTENT_BYTES
        {
            return Err(CodecError::ContainerLimit);
        }
        let hash = digest(PAGE_DOMAIN, &content);
        let mut root = self.root.clone();
        frame(&mut root, &p.index.to_be_bytes());
        frame(&mut root, &hash.0);
        self.root = root;
        self.records = records;
        self.bytes = bytes;
        self.next_index = next;
        self.last_id = p.records.last().map(|r| r.record_id.clone());
        Ok(())
    }
    pub fn summary(&self) -> BootstrapContentSummary {
        BootstrapContentSummary {
            page_count: self.next_index,
            record_count: self.records,
            content_bytes: self.bytes,
            root: BootstrapDigest(self.root.clone().finalize().into()),
        }
    }
}
/// Staged pages may be untrusted. Only finish authenticates the complete root/counts.
/// Success is NOT an installation or authorization capability.
#[derive(Clone)]
pub struct BootstrapVerifier {
    manifest: BootstrapManifest,
    manifest_digest: BootstrapDigest,
    content: BootstrapContentHasher,
}
impl BootstrapVerifier {
    pub fn new(manifest: BootstrapManifest) -> Result<Self, CodecError> {
        let manifest_digest = bootstrap_manifest_digest(&manifest)?;
        let content = BootstrapContentHasher::new(manifest.session_id.clone())?;
        Ok(Self {
            manifest,
            manifest_digest,
            content,
        })
    }
    pub fn push(&mut self, page: &BootstrapPage) -> Result<(), CodecError> {
        if page.manifest_digest != self.manifest_digest {
            return Err(CodecError::InvalidEnvelope);
        }
        let mut candidate = self.content.clone();
        candidate.push(page)?;
        let s = candidate.summary();
        if s.page_count > self.manifest.page_count
            || s.record_count > self.manifest.record_count
            || s.content_bytes > self.manifest.content_bytes
        {
            return Err(CodecError::InvalidEnvelope);
        }
        self.content = candidate;
        Ok(())
    }
    pub fn summary(&self) -> BootstrapContentSummary {
        self.content.summary()
    }
    pub fn finish(&self) -> Result<BootstrapContentSummary, CodecError> {
        let s = self.content.summary();
        if s.page_count != self.manifest.page_count
            || s.record_count != self.manifest.record_count
            || s.content_bytes != self.manifest.content_bytes
            || s.root != self.manifest.root
        {
            return Err(CodecError::InvalidEnvelope);
        }
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn full_page_item_budget_counts_headers_not_only_content() {
        let records = (0..341)
            .map(|i| SnapshotRecord {
                record_id: RecordId(format!("r{i:04}")),
                state: RecordState::Present {
                    value: CanonicalValue::Int(7),
                    version: 1,
                    reference: None,
                },
            })
            .collect::<Vec<_>>();
        assert!(
            encode_content(0, &records).is_ok(),
            "content-only budget fits exactly at4096items"
        );
        let page = BootstrapPage {
            session_id: "s".into(),
            manifest_digest: BootstrapDigest([0; 32]),
            index: 0,
            records,
        };
        assert_eq!(
            encode_bootstrap_page(&page),
            Err(CodecError::ContainerLimit)
        );
        let mut hasher = BootstrapContentHasher::new("s".into()).unwrap();
        let before = hasher.summary();
        assert!(hasher.push(&page).is_err());
        assert_eq!(hasher.summary(), before);
    }
    #[test]
    fn checked_counter_failure_does_not_advance_hidden_hash_or_last_id() {
        let page = BootstrapPage {
            session_id: "s".into(),
            manifest_digest: BootstrapDigest([0; 32]),
            index: 0,
            records: vec![SnapshotRecord {
                record_id: RecordId("a".into()),
                state: RecordState::Tombstone { version: 1 },
            }],
        };
        let mut hasher = BootstrapContentHasher::new("s".into()).unwrap();
        hasher.records = u64::MAX;
        let before = hasher.summary();
        assert!(hasher.push(&page).is_err());
        assert_eq!(hasher.summary(), before);
        assert_eq!(hasher.last_id, None);
    }
}
