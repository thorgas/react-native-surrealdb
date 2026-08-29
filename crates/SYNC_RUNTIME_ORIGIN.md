# Temporary sync runtime copy

`surrealdb-sync-protocol` and `surrealdb-sync-client` were copied from private canonical repository
`thorgas/surrealdb-sync-engine.dev` at commit
`056374ac5430e1e09ee73ab30b4d8c20247a2f68` on 2026-08-28.
The owned `surrealdb-sync-protocol::canonical` module and its exact dependency pins were later
synchronized from private `main` (also branch `feat/canonical-codec-v1`) at
`2032066722ccb0202f2f8481f30fd5c70f4d681e`. This includes the complete bounded
`surrealdb-sync/1` push/pull CBOR envelope in `http_codec.rs`.

The canonical protocol design, Quint specification, comprehensive conformance/adversarial tests,
authority implementation, research wiki, SurrealDB compatibility oracle, and experiments remain
private and were not copied.
These two crates are Apache-2.0 licensed; the rest of this repository remains MIT licensed.

This is a temporary source arrangement for adapter prototyping. Until a single-source export or
dependency mechanism replaces it, changes must be authored in the private canonical repository and
then deliberately synchronized here. Do not make an independent protocol decision in this copy.
