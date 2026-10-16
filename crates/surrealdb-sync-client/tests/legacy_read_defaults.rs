//! Exercise serde's actual missing-field path without adding a codec dependency.
use serde::{
    Deserialize,
    de::{self, IntoDeserializer, Visitor},
};
use surrealdb_sync_client::{ClientRuntime, DurableClientState, ReadCoverageV1, ResolvedCommit};

#[derive(Clone)]
enum Legacy {
    Text(&'static str),
    Number(u64),
    Map(Vec<(&'static str, Legacy)>),
    Seq(Vec<Legacy>),
    None,
}
impl<'de> IntoDeserializer<'de, de::value::Error> for Legacy {
    type Deserializer = Self;
    fn into_deserializer(self) -> Self {
        self
    }
}
impl<'de> de::Deserializer<'de> for Legacy {
    type Error = de::value::Error;
    fn deserialize_any<T: Visitor<'de>>(self, visitor: T) -> Result<T::Value, Self::Error> {
        match self {
            Self::Text(s) => visitor.visit_borrowed_str(s),
            Self::Number(n) => visitor.visit_u64(n),
            Self::Map(entries) => {
                visitor.visit_map(de::value::MapDeserializer::new(entries.into_iter()))
            }
            Self::Seq(entries) => {
                visitor.visit_seq(de::value::SeqDeserializer::new(entries.into_iter()))
            }
            Self::None => visitor.visit_none(),
        }
    }
    fn deserialize_option<T: Visitor<'de>>(self, visitor: T) -> Result<T::Value, Self::Error> {
        match self {
            Self::None => visitor.visit_none(),
            value => visitor.visit_some(value),
        }
    }
    fn deserialize_newtype_struct<T: Visitor<'de>>(
        self,
        _: &'static str,
        visitor: T,
    ) -> Result<T::Value, Self::Error> {
        visitor.visit_newtype_struct(self)
    }
    serde::forward_to_deserialize_any! { bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes byte_buf unit unit_struct seq tuple tuple_struct map struct enum identifier ignored_any }
}
fn identity() -> Legacy {
    Legacy::Map(vec![
        ("clientCommitId", Legacy::Text("commit")),
        ("fingerprint", Legacy::Text("fingerprint")),
    ])
}

#[test]
fn old_journal_and_outcome_default_to_unknown_coverage_and_uncovered_acceptance() {
    use Legacy::{Map, None, Number, Seq, Text};
    let legacy = Map(vec![
        ("partition_id", Text("p")),
        ("client_id", Text("c")),
        ("requested_scope", Text("s")),
        ("subscription_revision", Number(1)),
        ("revision", Number(0)),
        ("confirmed", Map(vec![])),
        ("outbox", Seq(vec![])),
        ("outcomes", Seq(vec![])),
        ("checkpoint", None),
        ("id_map", Map(vec![])),
    ]);
    let state = DurableClientState::<String>::deserialize(legacy).unwrap();
    assert!(state.reset_coverage.is_none());
    assert_eq!(
        ClientRuntime::open(state)
            .unwrap()
            .read_snapshot()
            .metadata
            .coverage,
        ReadCoverageV1::Unknown
    );
    // The outcome itself need not be opened as a journal to test serde's default.
    let legacy = Map(vec![
        (
            "local_commit",
            Map(vec![("identity", identity()), ("operations", Seq(vec![]))]),
        ),
        (
            "outcome",
            Map(vec![
                ("status", Text("accepted")),
                ("identity", identity()),
                ("sequence", Number(1)),
                ("id_mappings", Seq(vec![])),
            ]),
        ),
    ]);
    let resolved = ResolvedCommit::<String>::deserialize(legacy).unwrap();
    assert!(!resolved.covered_by_reset);
    assert!(resolved.resolution.is_none());
}
