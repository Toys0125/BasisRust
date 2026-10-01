//! CPU-only compatibility surface. This build has no GPU backend or worker.
pub use crate::gpu_distance_types::GpuDistanceStats;
pub(crate) use crate::gpu_distance_types::{DistancePeer, OffloadSettings};
use crate::gpu_policy::ReductionDecision;
use std::{sync::Arc, thread};

// An uninhabited bucket prevents a CPU-only build from publishing GPU data.
pub(crate) enum DistanceBucket {}

impl DistanceBucket {
    pub fn sender_indices(&self, _peers: &[DistancePeer]) -> Vec<Option<usize>> {
        match *self {}
    }

    pub fn row<'a>(
        &'a self,
        _peer: &DistancePeer,
        _indices: &'a [Option<usize>],
    ) -> Option<DistanceRow<'a>> {
        match *self {}
    }
}

#[derive(Clone, Copy)]
pub(crate) struct DistanceRow<'a> {
    pub epoch: u64,
    _bucket: &'a DistanceBucket,
}

impl DistanceRow<'_> {
    pub fn get(&self, _sender_index: usize) -> Option<ReductionDecision> {
        None
    }
}

#[derive(Debug, Default)]
pub(crate) struct DistanceOffload {
    stopped: bool,
    stats: GpuDistanceStats,
}

impl DistanceOffload {
    pub fn stop(&mut self) -> Vec<thread::JoinHandle<()>> {
        self.stopped = true;
        Vec::new()
    }

    pub fn stats(&self) -> GpuDistanceStats {
        self.stats.clone()
    }

    pub fn advance(
        &mut self,
        settings: OffloadSettings,
        _peers: &[DistancePeer],
    ) -> Option<Arc<DistanceBucket>> {
        if !self.stopped {
            self.stats.interval_ticks = settings.interval_ticks.max(1);
            self.stats.last_error = settings.enabled.then(|| {
                "GPU support is not compiled in; rebuild with --features gpu to enable it".into()
            });
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gpu_policy::ReductionPolicy;

    #[test]
    fn runtime_configuration_cannot_enable_gpu_in_cpu_build() {
        let mut offload = DistanceOffload::default();
        let mut settings = OffloadSettings {
            enabled: true,
            device: "any hardware adapter".into(),
            interval_ticks: 32,
            policy: ReductionPolicy::default(),
        };
        let peers = [DistancePeer {
            id: 1,
            incarnation: 1,
            position: [0.0; 4],
        }; 2];
        for _ in 0..128 {
            assert!(offload.advance(settings.clone(), &peers).is_none());
        }
        let stats = offload.stats();
        assert!(!stats.enabled);
        assert!(stats.adapter.is_none());
        assert!(stats.active_epoch.is_none());
        assert_eq!(stats.submissions, 0);
        assert_eq!(stats.computed_pairs, 0);
        assert!(stats.last_error.unwrap().contains("--features gpu"));
        settings.enabled = false;
        assert!(offload.advance(settings, &peers).is_none());
        assert!(offload.stats().last_error.is_none());
        assert!(offload.stop().is_empty());
    }
}
