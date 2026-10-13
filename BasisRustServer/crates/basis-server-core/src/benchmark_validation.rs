//! Counters used only by benchmark validation. Admission state stays separate.
#[cfg(any(feature = "benchmark-validation", test))]
pub(crate) use std::sync::atomic::AtomicU64 as Counter;

/// A zero-sized replacement: normal builds perform no telemetry atomic updates.
#[cfg(not(any(feature = "benchmark-validation", test)))]
#[derive(Debug, Default, Clone)]
pub(crate) struct Counter;

#[cfg(not(any(feature = "benchmark-validation", test)))]
impl Counter {
    #[inline(always)]
    pub(crate) fn fetch_add(&self, _: u64, _: std::sync::atomic::Ordering) {}

    #[inline(always)]
    pub(crate) fn fetch_sub(&self, _: u64, _: std::sync::atomic::Ordering) {}

    #[inline(always)]
    pub(crate) fn fetch_max(&self, _: u64, _: std::sync::atomic::Ordering) {}
}
