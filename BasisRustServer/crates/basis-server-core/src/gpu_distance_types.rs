//! Configuration and health metadata shared by CPU-only and GPU builds.
use crate::gpu_policy::ReductionPolicy;
use basis_transport::PeerId;

#[cfg_attr(not(feature = "gpu"), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OffloadSettings {
    pub enabled: bool,
    pub device: String,
    pub interval_ticks: u64,
    pub policy: ReductionPolicy,
}

#[cfg_attr(not(feature = "gpu"), allow(dead_code))]
#[derive(Debug, Clone, Copy)]
pub(crate) struct DistancePeer {
    pub id: PeerId,
    pub incarnation: u64,
    pub position: [f32; 4],
}

#[derive(Debug, Clone, Default)]
pub struct GpuDistanceStats {
    pub enabled: bool,
    pub adapter: Option<String>,
    pub interval_ticks: u64,
    pub submissions: u64,
    pub swaps: u64,
    pub missed_swaps: u64,
    pub stale_fallbacks: u64,
    pub active_epoch: Option<u64>,
    pub last_error: Option<String>,
    pub computed_pairs: u64,
    pub corrected_pairs: u64,
    pub last_worker_micros: u64,
    pub max_worker_micros: u64,
}
