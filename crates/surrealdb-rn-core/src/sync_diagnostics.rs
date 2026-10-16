//! Bounded metadata-only measurements. No payload, identity, callback, or queue is retained.
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Clone, Debug, uniffi::Record)]
pub struct NativeSyncOperationSpan {
    pub operation_ordinal: u64,
    pub runtime_lock_wait_ns: u64,
    pub runtime_lock_hold_ns: u64,
    pub storage_ns: u64,
    pub state_encoding_ns: u64,
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct NativeSyncDiagnostics {
    pub schema_version: u32,
    pub latest_completed_operation: Option<NativeSyncOperationSpan>,
    pub span_publish_drops: u64,
    pub runtime_lock_acquisitions: u64,
    pub runtime_lock_wait_ns: u64,
    pub runtime_lock_hold_ns: u64,
    pub storage_attempts: u64,
    pub storage_ns: u64,
    pub storage_errors: u64,
    pub state_encoding_attempts: u64,
    pub state_encoding_ns: u64,
}

#[derive(Default)]
pub(crate) struct Phase {
    count: AtomicU64,
    ns: AtomicU64,
    latest_ns: AtomicU64,
}
impl Phase {
    pub(crate) fn record(&self, started: Instant) {
        // Saturation makes accounting monotonic even on extremely long lived clients.
        let ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.latest_ns.store(ns, Ordering::Relaxed);
        self.count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(1))
            })
            .ok();
        self.ns
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(ns))
            })
            .ok();
    }
}

#[derive(Default)]
pub(crate) struct SyncDiagnostics {
    pub runtime_wait: Phase,
    pub runtime_hold: Phase,
    pub storage: Phase,
    pub state_encoding: Phase,
    pub storage_errors: AtomicU64,
    latest: Mutex<Option<NativeSyncOperationSpan>>,
    span_drops: AtomicU64,
}
impl SyncDiagnostics {
    pub fn begin_operation(&self) -> u64 {
        self.storage.latest_ns.store(0, Ordering::Relaxed);
        self.state_encoding.latest_ns.store(0, Ordering::Relaxed);
        self.runtime_wait.count.load(Ordering::Relaxed)
    }
    pub fn complete_operation(&self, ordinal: u64, wait_ns: u64, started: Instant) {
        let hold_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        // Snapshot readers cannot delay a runtime operation; contention drops metadata.
        if let Ok(mut latest) = self.latest.try_lock() {
            *latest = Some(NativeSyncOperationSpan {
                operation_ordinal: ordinal,
                runtime_lock_wait_ns: wait_ns,
                runtime_lock_hold_ns: hold_ns,
                storage_ns: self.storage.latest_ns.load(Ordering::Relaxed),
                state_encoding_ns: self.state_encoding.latest_ns.load(Ordering::Relaxed),
            });
        } else {
            self.span_drops.fetch_add(1, Ordering::Relaxed);
        }
    }
    // Independent relaxed reads intentionally do not acquire runtime/database locks.
    pub fn snapshot(&self) -> NativeSyncDiagnostics {
        NativeSyncDiagnostics {
            schema_version: 1,
            latest_completed_operation: self.latest.try_lock().ok().and_then(|span| span.clone()),
            span_publish_drops: self.span_drops.load(Ordering::Relaxed),
            runtime_lock_acquisitions: self.runtime_wait.count.load(Ordering::Relaxed),
            runtime_lock_wait_ns: self.runtime_wait.ns.load(Ordering::Relaxed),
            runtime_lock_hold_ns: self.runtime_hold.ns.load(Ordering::Relaxed),
            storage_attempts: self.storage.count.load(Ordering::Relaxed),
            storage_ns: self.storage.ns.load(Ordering::Relaxed),
            storage_errors: self.storage_errors.load(Ordering::Relaxed),
            state_encoding_attempts: self.state_encoding.count.load(Ordering::Relaxed),
            state_encoding_ns: self.state_encoding.ns.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn contended_span_reader_drops_metadata_instead_of_blocking() {
        let diagnostics = SyncDiagnostics::default();
        let held = diagnostics.latest.lock().unwrap();
        diagnostics.complete_operation(1, 2, Instant::now());
        assert_eq!(diagnostics.span_drops.load(Ordering::Relaxed), 1);
        assert!(diagnostics.snapshot().latest_completed_operation.is_none());
        drop(held);
        diagnostics.complete_operation(2, 3, Instant::now());
        assert_eq!(
            diagnostics
                .snapshot()
                .latest_completed_operation
                .unwrap()
                .operation_ordinal,
            2
        );
    }

    #[test]
    fn counters_are_bounded_and_saturating() {
        let diagnostics = SyncDiagnostics::default();
        diagnostics.storage.count.store(u64::MAX, Ordering::Relaxed);
        diagnostics.storage.ns.store(u64::MAX, Ordering::Relaxed);
        diagnostics.storage.record(Instant::now());
        let snapshot = diagnostics.snapshot();
        assert_eq!(snapshot.schema_version, 1);
        assert_eq!(snapshot.storage_attempts, u64::MAX);
        assert_eq!(snapshot.storage_ns, u64::MAX);
        assert!(std::mem::size_of::<SyncDiagnostics>() <= 256);
    }
}
