//! Separately versioned bounded bootstrap messages. These types grant no authorization or install capability.
use crate::{
    CanonicalValue, Checkpoint, ClientId, Cursor, PartitionId, ScopeIdentity, ScopeSnapshot,
    SnapshotRecord,
};

pub const BOOTSTRAP_NAME: &str = "surrealdb-sync-bootstrap/1";
pub const BOOTSTRAP_CONTENT_TYPE: &str = "application/vnd.surrealdb-sync-bootstrap.v1+cbor";
pub const MAX_BOOTSTRAP_RECORDS: u64 = 16_384;
pub const MAX_BOOTSTRAP_PAGES: u64 = 16_384;
pub const MAX_BOOTSTRAP_CONTENT_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BootstrapDigest(pub [u8; 32]);
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapStartRequest {
    pub partition_id: PartitionId,
    pub client_id: ClientId,
    pub request_id: String,
    pub requested_scope: ScopeIdentity,
    pub subscription_revision: u64,
    pub journal_generation: String,
}
/// Expected context must come from trusted composition/current journal state, never the received body.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapContext {
    pub authority_id: String,
    pub principal_id: String,
    pub partition_id: PartitionId,
    pub client_id: ClientId,
    pub journal_generation: String,
    pub scope: ScopeSnapshot,
}
/// Wire aggregate bounds do not replace smaller native storage or local account budgets.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BootstrapLimits {
    pub max_records: u64,
    pub max_pages: u64,
    pub max_content_bytes: u64,
}
impl Default for BootstrapLimits {
    fn default() -> Self {
        Self {
            max_records: MAX_BOOTSTRAP_RECORDS,
            max_pages: MAX_BOOTSTRAP_PAGES,
            max_content_bytes: MAX_BOOTSTRAP_CONTENT_BYTES,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapManifest {
    pub session_id: String,
    pub start_request_id: String,
    pub start_fingerprint: BootstrapDigest,
    pub context: BootstrapContext,
    pub cursor: Cursor,
    pub limits: BootstrapLimits,
    pub page_count: u64,
    pub record_count: u64,
    pub content_bytes: u64,
    pub root: BootstrapDigest,
    /// Fixed UTC Unix seconds; codecs preserve all u64 values but do not validate a clock.
    pub expires_at: u64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapPageRequest {
    pub partition_id: PartitionId,
    pub client_id: ClientId,
    pub session_id: String,
    pub index: u64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapPage {
    pub session_id: String,
    pub manifest_digest: BootstrapDigest,
    pub index: u64,
    pub records: Vec<SnapshotRecord<CanonicalValue>>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapCompleteRequest {
    pub partition_id: PartitionId,
    pub client_id: ClientId,
    pub session_id: String,
    pub manifest_digest: BootstrapDigest,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapReceipt {
    pub session_id: String,
    pub manifest_digest: BootstrapDigest,
    pub checkpoint: Checkpoint,
    pub expires_at: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BootstrapContentSummary {
    pub page_count: u64,
    pub record_count: u64,
    pub content_bytes: u64,
    pub root: BootstrapDigest,
}
