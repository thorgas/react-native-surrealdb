//! Independent authorized ordered-window protocol. No v1 wire types change.
use crate::{
    ClientId, Cursor, OpaqueCheckpoint, PartitionId, RecordId, ScopeIdentity, ScopeSnapshot,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const SUBSCRIPTION_PROTOCOL: &str = "surrealdb-subscription/1";
pub const SUBSCRIPTION_CONTENT_TYPE: &str = "application/vnd.surrealdb-subscription.v1+cbor";
pub const MAX_SUBSCRIPTIONS: usize = 32;
pub const MAX_WINDOW_ROWS: usize = 32;
pub const MAX_PARAMETERS: usize = 8;
pub const MAX_SUBSCRIPTION_TEXT: usize = 256;

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum QueryScalar {
    Bool(bool),
    Int(i64),
    Text(String),
}
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScalarKind {
    Bool,
    Int,
    Text,
}
impl QueryScalar {
    pub fn kind(&self) -> ScalarKind {
        match self {
            Self::Bool(_) => ScalarKind::Bool,
            Self::Int(_) => ScalarKind::Int,
            Self::Text(_) => ScalarKind::Text,
        }
    }
}
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    Ascending,
    Descending,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionRequest {
    pub partition_id: PartitionId,
    pub client_id: ClientId,
    pub requested_scope: ScopeIdentity,
    pub subscription_revision: u64,
    pub command: SubscriptionCommand,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum SubscriptionCommand {
    Start {
        subscription_id: String,
        generation: u64,
        definition_id: String,
        parameters: BTreeMap<String, QueryScalar>,
        limit: u16,
    },
    Resume {
        subscription_id: String,
        generation: u64,
        token: OpaqueCheckpoint,
    },
    Cancel {
        subscription_id: String,
        generation: u64,
    },
}
impl SubscriptionCommand {
    pub fn identity(&self) -> (&str, u64) {
        match self {
            Self::Start {
                subscription_id,
                generation,
                ..
            }
            | Self::Resume {
                subscription_id,
                generation,
                ..
            }
            | Self::Cancel {
                subscription_id,
                generation,
            } => (subscription_id, *generation),
        }
    }
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowCoverage {
    pub definition_id: String,
    pub limit: u16,
    pub order_field: String,
    pub order_kind: ScalarKind,
    pub direction: SortDirection,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionRow<V> {
    pub record_id: RecordId,
    pub value: V,
    pub version: u64,
    pub sort_key: QueryScalar,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum MembershipChange<V> {
    Upsert { row: SubscriptionRow<V> },
    Leave { record_id: RecordId },
    Delete { record_id: RecordId, version: u64 },
}
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WindowResetReason {
    Initial,
    Expired,
    Stale,
    EpochChanged,
    PlanChanged,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum WindowResponseKind {
    Snapshot { reason: WindowResetReason },
    Delta { before_revision: u64 },
    Cancelled,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionResponse<V> {
    pub partition_id: PartitionId,
    pub client_id: ClientId,
    pub subscription_id: String,
    pub generation: u64,
    pub plan_fingerprint: String,
    pub scope: ScopeSnapshot,
    pub frontier: Cursor,
    pub view_revision: u64,
    pub coverage: WindowCoverage,
    pub response: WindowResponseKind,
    pub token: Option<OpaqueCheckpoint>,
    pub ordered_ids: Vec<RecordId>,
    pub changes: Vec<MembershipChange<V>>,
}

pub fn valid_subscription_text(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_SUBSCRIPTION_TEXT
}
pub fn validate_subscription_request(request: &SubscriptionRequest) -> bool {
    let (id, generation) = request.command.identity();
    if !valid_subscription_text(&request.partition_id.0)
        || !valid_subscription_text(&request.client_id.0)
        || !valid_subscription_text(&request.requested_scope.0)
        || !valid_subscription_text(id)
        || generation == 0
    {
        return false;
    }
    match &request.command {
        SubscriptionCommand::Start {
            definition_id,
            parameters,
            limit,
            ..
        } => {
            valid_subscription_text(definition_id)
                && *limit > 0
                && usize::from(*limit) <= MAX_WINDOW_ROWS
                && parameters.len() <= MAX_PARAMETERS
                && parameters.iter().all(|(key, value)| {
                    valid_subscription_text(key)
                        && match value {
                            QueryScalar::Text(value) => value.len() <= MAX_SUBSCRIPTION_TEXT,
                            _ => true,
                        }
                })
        }
        SubscriptionCommand::Resume { token, .. } => valid_subscription_text(&token.0),
        SubscriptionCommand::Cancel { .. } => true,
    }
}

/// Trusted registry entry, never a client-supplied query/path/SQL fragment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EqualityPredicate {
    pub field: String,
    pub parameter: String,
    pub kind: ScalarKind,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TrustedQueryDefinition {
    pub definition_id: String,
    pub predicates: Vec<EqualityPredicate>,
    pub order_field: String,
    pub order_kind: ScalarKind,
    pub direction: SortDirection,
    pub max_limit: u16,
}

pub const MAX_SUBSCRIPTION_BYTES: usize = 256 * 1024;
pub fn validate_subscription_response<V>(response: &SubscriptionResponse<V>) -> bool {
    let change_ids = response
        .changes
        .iter()
        .map(|change| match change {
            MembershipChange::Upsert { row } => &row.record_id,
            MembershipChange::Leave { record_id } | MembershipChange::Delete { record_id, .. } => {
                record_id
            }
        })
        .collect::<std::collections::BTreeSet<_>>();
    let shape = match response.response {
        WindowResponseKind::Cancelled => {
            response.token.is_none()
                && response.ordered_ids.is_empty()
                && response.changes.is_empty()
        }
        WindowResponseKind::Snapshot { .. } => {
            response.token.is_some()
                && response
                    .changes
                    .iter()
                    .all(|change| matches!(change, MembershipChange::Upsert { .. }))
        }
        WindowResponseKind::Delta { .. } => response.token.is_some(),
    };
    shape
        && change_ids.len() == response.changes.len()
        && response
            .ordered_ids
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            == response.ordered_ids.len()
        && valid_subscription_text(&response.partition_id.0)
        && valid_subscription_text(&response.client_id.0)
        && valid_subscription_text(&response.subscription_id)
        && response.generation > 0
        && valid_subscription_text(&response.plan_fingerprint)
        && response.frontier.epoch > 0
        && response.view_revision > 0
        && valid_subscription_text(&response.scope.identity.0)
        && valid_subscription_text(&response.coverage.definition_id)
        && valid_subscription_text(&response.coverage.order_field)
        && response.coverage.limit > 0
        && usize::from(response.coverage.limit) <= MAX_WINDOW_ROWS
        && response.ordered_ids.len() <= usize::from(response.coverage.limit)
        && response
            .ordered_ids
            .iter()
            .all(|id| valid_subscription_text(&id.0))
        && response.changes.len() <= MAX_WINDOW_ROWS * 2
        && response
            .token
            .as_ref()
            .is_none_or(|token| valid_subscription_text(&token.0))
        && response.changes.iter().all(|change| match change {
            MembershipChange::Upsert { row } => {
                valid_subscription_text(&row.record_id.0)
                    && row.version > 0
                    && row.sort_key.kind() == response.coverage.order_kind
                    && match &row.sort_key {
                        QueryScalar::Text(value) => value.len() <= MAX_SUBSCRIPTION_TEXT,
                        _ => true,
                    }
            }
            MembershipChange::Leave { record_id } => valid_subscription_text(&record_id.0),
            MembershipChange::Delete { record_id, version } => {
                valid_subscription_text(&record_id.0) && *version > 0
            }
        })
}
