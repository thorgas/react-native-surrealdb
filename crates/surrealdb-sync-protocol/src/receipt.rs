//! Opt-in wire v2. The nested v1 payload and its fingerprint remain byte-for-byte stable.
use crate::http_codec::{
    Encoder, array, decode_family_message, encode_family_message, identity, integer, unsigned,
};
use crate::*;
use ciborium::value::Value as Cbor;
use serde::{Deserialize, Serialize};

pub const V2_NAME: &str = "surrealdb-sync/2";
pub const HTTP_CONTENT_TYPE_V2: &str = "application/vnd.surrealdb-sync.v2+cbor";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReceiptContext {
    pub authority_incarnation: String,
    pub dataset_incarnation: String,
    pub partition_id: PartitionId,
    pub client_id: ClientId,
    pub scope: ScopeSnapshot,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AcceptanceReceipt {
    pub context: ReceiptContext,
    pub identity: CommitIdentity,
    pub cursor: Cursor,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PushResponseV2<V> {
    pub response: PushResponse<V>,
    pub receipt: Option<AcceptanceReceipt>,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PullResponseV2<V> {
    pub response: PullResponse<V>,
    pub context: ReceiptContext,
}
fn context(e: &mut Encoder, c: &ReceiptContext) -> Result<Cbor, CodecError> {
    e.item(10)?;
    Ok(Cbor::Array(vec![
        e.identity_text(&c.authority_incarnation)?,
        e.identity_text(&c.dataset_incarnation)?,
        e.identity_text(&c.partition_id.0)?,
        e.identity_text(&c.client_id.0)?,
        Cbor::Array(vec![
            e.identity_text(&c.scope.identity.0)?,
            integer(c.scope.authorization_revision),
            integer(c.scope.subscription_revision),
        ]),
    ]))
}
fn decode_context(v: Cbor) -> Result<ReceiptContext, CodecError> {
    let [authority, dataset, partition, client, scope] = array(v)?;
    let [scope, auth, sub] = array(scope)?;
    Ok(ReceiptContext {
        authority_incarnation: identity(authority)?,
        dataset_incarnation: identity(dataset)?,
        partition_id: PartitionId(identity(partition)?),
        client_id: ClientId(identity(client)?),
        scope: ScopeSnapshot {
            identity: ScopeIdentity(identity(scope)?),
            authorization_revision: unsigned(auth)?,
            subscription_revision: unsigned(sub)?,
        },
    })
}
fn receipt(e: &mut Encoder, r: &AcceptanceReceipt) -> Result<Cbor, CodecError> {
    if r.cursor.epoch == 0 || r.cursor.sequence == 0 {
        return Err(CodecError::InvalidEnvelope);
    }
    e.item(7)?;
    Ok(Cbor::Array(vec![
        context(e, &r.context)?,
        e.identity_text(&r.identity.client_commit_id.0)?,
        e.identity_text(&r.identity.fingerprint.0)?,
        integer(r.cursor.epoch),
        integer(r.cursor.sequence),
    ]))
}
fn decode_receipt(v: Cbor) -> Result<AcceptanceReceipt, CodecError> {
    let [context, commit, fingerprint, epoch, sequence] = array(v)?;
    Ok(AcceptanceReceipt {
        context: decode_context(context)?,
        identity: CommitIdentity {
            client_commit_id: ClientCommitId(identity(commit)?),
            fingerprint: Fingerprint(identity(fingerprint)?),
        },
        cursor: Cursor {
            epoch: unsigned(epoch)?,
            sequence: unsigned(sequence)?,
        },
    })
}
fn bytes(v: Cbor) -> Result<Vec<u8>, CodecError> {
    if let Cbor::Bytes(b) = v {
        Ok(b)
    } else {
        Err(CodecError::InvalidEnvelope)
    }
}
fn canonical(input: &[u8], output: Vec<u8>) -> Result<(), CodecError> {
    if input == output {
        Ok(())
    } else {
        Err(CodecError::NonCanonical)
    }
}
pub fn encode_push_request_v2(r: &PushRequest<CanonicalValue>) -> Result<Vec<u8>, CodecError> {
    encode_family_message(V2_NAME, 0, |_| Ok(Cbor::Bytes(encode_push_request(r)?)))
}
pub fn decode_push_request_v2(b: &[u8]) -> Result<PushRequest<CanonicalValue>, CodecError> {
    let r = decode_push_request(&bytes(decode_family_message(b, V2_NAME, 0)?)?)?;
    canonical(b, encode_push_request_v2(&r)?)?;
    Ok(r)
}
pub fn encode_pull_request_v2(r: &PullRequest) -> Result<Vec<u8>, CodecError> {
    encode_family_message(V2_NAME, 2, |_| Ok(Cbor::Bytes(encode_pull_request(r)?)))
}
pub fn decode_pull_request_v2(b: &[u8]) -> Result<PullRequest, CodecError> {
    let r = decode_pull_request(&bytes(decode_family_message(b, V2_NAME, 2)?)?)?;
    canonical(b, encode_pull_request_v2(&r)?)?;
    Ok(r)
}
pub fn encode_push_response_v2(r: &PushResponseV2<CanonicalValue>) -> Result<Vec<u8>, CodecError> {
    validate_receipt(r)?;
    encode_family_message(V2_NAME, 1, |e| {
        Ok(Cbor::Array(vec![
            Cbor::Bytes(encode_push_response(&r.response)?),
            match &r.receipt {
                Some(r) => receipt(e, r)?,
                None => Cbor::Null,
            },
        ]))
    })
}
pub fn decode_push_response_v2(b: &[u8]) -> Result<PushResponseV2<CanonicalValue>, CodecError> {
    let [response, receipt] = array(decode_family_message(b, V2_NAME, 1)?)?;
    let r = PushResponseV2 {
        response: decode_push_response(&bytes(response)?)?,
        receipt: if receipt == Cbor::Null {
            None
        } else {
            Some(decode_receipt(receipt)?)
        },
    };
    canonical(b, encode_push_response_v2(&r)?)?;
    Ok(r)
}
pub fn validate_receipt<V>(r: &PushResponseV2<V>) -> Result<(), CodecError> {
    if let Some(receipt) = &r.receipt
        && (receipt.context.partition_id != r.response.partition_id
            || receipt.context.client_id != r.response.client_id
            || receipt.identity != *r.response.outcome.identity()
            || receipt.cursor.epoch == 0
            || !matches!(&r.response.outcome, DurableOutcome::Accepted { sequence, .. } if *sequence == receipt.cursor.sequence && *sequence > 0))
    {
        return Err(CodecError::InvalidEnvelope);
    }
    Ok(())
}
pub fn encode_pull_response_v2(r: &PullResponseV2<CanonicalValue>) -> Result<Vec<u8>, CodecError> {
    encode_family_message(V2_NAME, 3, |e| {
        Ok(Cbor::Array(vec![
            Cbor::Bytes(encode_pull_response(&r.response)?),
            context(e, &r.context)?,
        ]))
    })
}
pub fn decode_pull_response_v2(b: &[u8]) -> Result<PullResponseV2<CanonicalValue>, CodecError> {
    let [response, context] = array(decode_family_message(b, V2_NAME, 3)?)?;
    let r = PullResponseV2 {
        response: decode_pull_response(&bytes(response)?)?,
        context: decode_context(context)?,
    };
    canonical(b, encode_pull_response_v2(&r)?)?;
    Ok(r)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn response(n: u64) -> PushResponseV2<CanonicalValue> {
        let identity = CommitIdentity {
            client_commit_id: ClientCommitId("commit".into()),
            fingerprint: Fingerprint(format!("sha256:{}", "ab".repeat(32))),
        };
        let context = ReceiptContext {
            authority_incarnation: "authority".into(),
            dataset_incarnation: "dataset".into(),
            partition_id: PartitionId("p".into()),
            client_id: ClientId("c".into()),
            scope: ScopeSnapshot {
                identity: ScopeIdentity("s".into()),
                authorization_revision: n,
                subscription_revision: n,
            },
        };
        PushResponseV2 {
            response: PushResponse {
                schema_version: SchemaVersion::V1,
                partition_id: context.partition_id.clone(),
                client_id: context.client_id.clone(),
                outcome: DurableOutcome::Accepted {
                    identity: identity.clone(),
                    sequence: n,
                    id_mappings: vec![],
                },
            },
            receipt: Some(AcceptanceReceipt {
                context,
                identity,
                cursor: Cursor {
                    epoch: n,
                    sequence: n,
                },
            }),
        }
    }
    #[test]
    fn v2_is_lossless_and_v1_bytes_remain_stable() {
        for n in [1, (1 << 53) + 1, u64::MAX] {
            let r = response(n);
            let v1 = encode_push_response(&r.response).unwrap();
            let v2 = encode_push_response_v2(&r).unwrap();
            assert_eq!(decode_push_response_v2(&v2).unwrap(), r);
            assert_eq!(
                encode_push_response(&decode_push_response(&v1).unwrap()).unwrap(),
                v1
            );
            assert!(decode_push_response(&v2).is_err());
            assert!(decode_push_response_v2(&v1).is_err());
        }
    }
    #[test]
    fn rejects_extra_missing_wrong_kind_noncanonical_and_substituted_fields() {
        let r = response(1);
        let b = encode_push_response_v2(&r).unwrap();
        for n in 0..b.len() {
            assert!(decode_push_response_v2(&b[..n]).is_err());
        }
        let mut trailing = b.clone();
        trailing.push(0);
        assert!(decode_push_response_v2(&trailing).is_err());
        let mut wrong = r.clone();
        wrong.receipt.as_mut().unwrap().context.client_id.0 = "other".into();
        assert!(encode_push_response_v2(&wrong).is_err());
        let mut wrong = r.clone();
        wrong.receipt.as_mut().unwrap().cursor.sequence += 1;
        assert!(encode_push_response_v2(&wrong).is_err());
        let mut wrong = r;
        wrong.receipt.as_mut().unwrap().cursor.epoch = 0;
        assert!(encode_push_response_v2(&wrong).is_err());
        assert!(decode_pull_response_v2(&b).is_err());
    }
}
