//! Two immutable distance buckets; GPU waits are confined to a worker thread.
use crate::gpu_distance_backend::GpuDistanceBackend;
use basis_transport::PeerId;
use std::{
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{mpsc, Arc},
    thread,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OffloadSettings {
    pub enabled: bool,
    pub device: String,
    pub interval_ticks: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DistancePeer {
    pub id: PeerId,
    pub incarnation: u64,
    pub position: [f32; 4],
}

#[derive(Debug)]
pub(crate) struct DistanceBucket {
    pub epoch: u64,
    submitted_tick: u64,
    peers: Vec<DistancePeer>,
    indices: Vec<Option<(u64, usize)>>,
    distances: Vec<f32>,
}

impl DistanceBucket {
    #[cfg(test)]
    pub(crate) fn for_test(epoch: u64, peers: Vec<DistancePeer>, distances: Vec<f32>) -> Self {
        Self::new(
            Job {
                bucket: 0,
                epoch,
                submitted_tick: 0,
                peers,
            },
            distances,
        )
        .unwrap()
    }
    fn new(job: Job, distances: Vec<f32>) -> Result<Self, String> {
        let count = job.peers.len();
        if count.checked_mul(count) != Some(distances.len()) {
            return Err("GPU distance matrix has the wrong size".into());
        }
        let mut indices = vec![
            None;
            job.peers
                .iter()
                .map(|p| p.id as usize + 1)
                .max()
                .unwrap_or(0)
        ];
        for (index, peer) in job.peers.iter().enumerate() {
            if indices[peer.id as usize]
                .replace((peer.incarnation, index))
                .is_some()
            {
                return Err("GPU distance roster contains a duplicate peer".into());
            }
        }
        Ok(Self {
            epoch: job.epoch,
            submitted_tick: job.submitted_tick,
            peers: job.peers,
            indices,
            distances,
        })
    }

    fn index(&self, peer: &DistancePeer) -> Option<usize> {
        self.indices
            .get(peer.id as usize)
            .copied()
            .flatten()
            .and_then(|(incarnation, index)| (incarnation == peer.incarnation).then_some(index))
    }

    pub fn sender_indices(&self, peers: &[DistancePeer]) -> Vec<Option<usize>> {
        peers.iter().map(|peer| self.index(peer)).collect()
    }

    pub fn row<'a>(
        &'a self,
        peer: &DistancePeer,
        sender_indices: &'a [Option<usize>],
    ) -> Option<DistanceRow<'a>> {
        let index = self.index(peer)?;
        let width = self.peers.len();
        Some(DistanceRow {
            epoch: self.epoch,
            distances: &self.distances[index * width..(index + 1) * width],
            sender_indices,
            receiver_position: self.peers[index].position,
            peers: &self.peers,
        })
    }
}

#[derive(Clone, Copy)]
pub(crate) struct DistanceRow<'a> {
    pub epoch: u64,
    distances: &'a [f32],
    sender_indices: &'a [Option<usize>],
    receiver_position: [f32; 4],
    peers: &'a [DistancePeer],
}

impl DistanceRow<'_> {
    pub fn get(&self, sender_index: usize) -> Option<f32> {
        let index = self.sender_indices.get(sender_index).copied().flatten()?;
        self.distances.get(index).copied()
    }

    pub fn exact_snapshot_distance(&self, sender_index: usize) -> Option<f32> {
        let index = self.sender_indices.get(sender_index).copied().flatten()?;
        let sender = self.peers.get(index)?.position;
        let dx = self.receiver_position[0] - sender[0];
        let dy = self.receiver_position[1] - sender[1];
        let dz = self.receiver_position[2] - sender[2];
        Some(dx * dx + dy * dy + dz * dz)
    }
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
}

#[derive(Debug)]
struct Job {
    bucket: usize,
    epoch: u64,
    submitted_tick: u64,
    peers: Vec<DistancePeer>,
}
#[derive(Debug)]
enum WorkerEvent {
    Initialized(String),
    Completed(usize, Result<DistanceBucket, String>),
}
#[derive(Debug)]
struct WorkerLink {
    jobs: mpsc::SyncSender<Job>,
    events: mpsc::Receiver<WorkerEvent>,
}

fn spawn_worker(device: String) -> Result<(WorkerLink, thread::JoinHandle<()>), String> {
    let (jobs, rx) = mpsc::sync_channel::<Job>(1);
    let (events, event_rx) = mpsc::channel();
    let thread = thread::Builder::new()
        .name("BSR-GPU-Distance".into())
        .spawn(move || {
            let run = || -> Result<(), String> {
                let mut backend = GpuDistanceBackend::new(&device).map_err(|e| e.to_string())?;
                events
                    .send(WorkerEvent::Initialized(backend.adapter_name().into()))
                    .map_err(|e| e.to_string())?;
                while let Ok(job) = rx.recv() {
                    let bucket = job.bucket;
                    let positions = job
                        .peers
                        .iter()
                        .map(|peer| peer.position)
                        .collect::<Vec<_>>();
                    let result = backend
                        .compute(bucket, &positions)
                        .map_err(|e| e.to_string())
                        .and_then(|distances| DistanceBucket::new(job, distances));
                    let failed = result.is_err();
                    if events.send(WorkerEvent::Completed(bucket, result)).is_err() {
                        return Ok(());
                    }
                    if failed {
                        return Ok(());
                    }
                }
                Ok(())
            };
            let result = catch_unwind(AssertUnwindSafe(run));
            let error = match result {
                Ok(Err(error)) => Some(error),
                Err(_) => Some("GPU distance worker panicked".into()),
                _ => None,
            };
            if let Some(error) = error {
                let _ = events.send(WorkerEvent::Completed(0, Err(error)));
            }
        })
        .map_err(|e| e.to_string())?;
    Ok((
        WorkerLink {
            jobs,
            events: event_rx,
        },
        thread,
    ))
}

#[derive(Debug, Default)]
pub(crate) struct DistanceOffload {
    stopped: bool,
    settings: Option<OffloadSettings>,
    worker: Option<WorkerLink>,
    threads: Vec<thread::JoinHandle<()>>,
    initialized: bool,
    failed: bool,
    tick: u64,
    next_epoch: u64,
    active: Option<usize>,
    pending: Option<(usize, u64, u64)>,
    buckets: [Option<Arc<DistanceBucket>>; 2],
    stats: GpuDistanceStats,
}

impl DistanceOffload {
    pub fn stop(&mut self) -> Vec<thread::JoinHandle<()>> {
        self.stopped = true;
        self.worker = None;
        self.active = None;
        self.pending = None;
        self.buckets = [None, None];
        self.stats.enabled = false;
        self.stats.active_epoch = None;
        std::mem::take(&mut self.threads)
    }
    pub fn stats(&self) -> GpuDistanceStats {
        self.stats.clone()
    }

    fn fail(&mut self, error: String) {
        tracing::warn!(%error, "GPU distance offload disabled; using CPU distances");
        self.stats.last_error = Some(error);
        self.stats.active_epoch = None;
        self.worker = None;
        self.failed = true;
        self.initialized = false;
        self.active = None;
        self.pending = None;
        self.buckets = [None, None];
    }

    pub fn advance(
        &mut self,
        mut settings: OffloadSettings,
        peers: &[DistancePeer],
    ) -> Option<Arc<DistanceBucket>> {
        if self.stopped {
            return None;
        }
        // Retain retiring workers until they finish; shutdown joins any still
        // releasing GPU resources before process teardown. No tick waits here.
        self.threads.retain(|thread| !thread.is_finished());
        settings.interval_ticks = settings.interval_ticks.max(1);
        if self.settings.as_ref() != Some(&settings) {
            self.worker = None; // Dropping channels never joins or waits for GPU work.
            self.initialized = false;
            self.failed = false;
            self.tick = 0;
            self.active = None;
            self.pending = None;
            self.buckets = [None, None];
            self.stats.enabled = settings.enabled;
            self.stats.interval_ticks = settings.interval_ticks;
            self.stats.adapter = None;
            self.stats.active_epoch = None;
            self.stats.last_error = None;
            self.settings = Some(settings.clone());
        }
        self.tick = self.tick.saturating_add(1);
        if !settings.enabled || self.failed {
            return None;
        }
        if self.worker.is_none() && peers.len() > 1 {
            match spawn_worker(settings.device.clone()) {
                Ok((worker, thread)) => {
                    self.worker = Some(worker);
                    self.threads.push(thread);
                }
                Err(error) => self.fail(error),
            }
        }
        loop {
            let event = self.worker.as_ref().map(|w| w.events.try_recv());
            match event {
                Some(Ok(WorkerEvent::Initialized(adapter))) => {
                    tracing::info!(%adapter, "GPU distance offload ready");
                    self.stats.adapter = Some(adapter);
                    self.initialized = true;
                }
                Some(Ok(WorkerEvent::Completed(bucket, Ok(result)))) => {
                    if self.pending == Some((bucket, result.epoch, result.submitted_tick)) {
                        self.buckets[bucket] = Some(Arc::new(result));
                    }
                }
                Some(Ok(WorkerEvent::Completed(_, Err(error)))) => {
                    self.fail(error);
                    break;
                }
                Some(Err(mpsc::TryRecvError::Disconnected)) => {
                    self.fail("GPU distance worker disconnected".into());
                    break;
                }
                _ => break,
            }
        }
        if self.failed {
            return None;
        }
        self.advance_buckets(settings.interval_ticks);
        let boundary = self.tick % settings.interval_ticks == 0;
        if self.initialized
            && self.pending.is_none()
            && peers.len() > 1
            && (boundary || self.active.is_none())
        {
            let bucket = self.active.map_or(0, |active| 1 - active);
            self.next_epoch = self.next_epoch.saturating_add(1);
            let epoch = self.next_epoch;
            let job = Job {
                bucket,
                epoch,
                submitted_tick: self.tick,
                peers: peers.to_vec(),
            };
            match self
                .worker
                .as_ref()
                .expect("initialized worker")
                .jobs
                .try_send(job)
            {
                Ok(()) => {
                    self.buckets[bucket] = None;
                    self.pending = Some((bucket, epoch, self.tick));
                    self.stats.submissions += 1;
                }
                Err(mpsc::TrySendError::Disconnected(_)) => {
                    self.fail("GPU job queue disconnected".into())
                }
                Err(mpsc::TrySendError::Full(_)) => {}
            }
        }
        self.active
            .and_then(|index| self.buckets[index].as_ref().map(Arc::clone))
    }

    fn advance_buckets(&mut self, period: u64) {
        let max_age = period.saturating_mul(2);
        if self.tick % period == 0 {
            if let Some((bucket, _, submitted)) = self.pending {
                if self.buckets[bucket].is_some() {
                    self.pending = None;
                    if self.tick.saturating_sub(submitted) <= max_age {
                        self.active = Some(bucket);
                        self.stats.active_epoch = self.buckets[bucket].as_ref().map(|b| b.epoch);
                        self.stats.swaps += 1;
                    } else {
                        self.buckets[bucket] = None;
                        self.stats.stale_fallbacks += 1;
                    }
                } else {
                    self.stats.missed_swaps += 1;
                }
            }
        }
        if let Some(index) = self.active {
            if self.buckets[index]
                .as_ref()
                .is_some_and(|bucket| self.tick.saturating_sub(bucket.submitted_tick) > max_age)
            {
                self.active = None;
                self.stats.active_epoch = None;
                self.stats.stale_fallbacks += 1;
                // Keep the allocation alive only if another tick still owns its Arc.
                self.buckets[index] = None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn peers() -> Vec<DistancePeer> {
        vec![
            DistancePeer {
                id: 1,
                incarnation: 10,
                position: [0.0; 4],
            },
            DistancePeer {
                id: 7,
                incarnation: 20,
                position: [15.0, 0.0, 0.0, 0.0],
            },
        ]
    }
    fn mock() -> (
        DistanceOffload,
        OffloadSettings,
        mpsc::Receiver<Job>,
        mpsc::Sender<WorkerEvent>,
    ) {
        let settings = OffloadSettings {
            enabled: true,
            device: "mock".into(),
            interval_ticks: 32,
        };
        let (jobs, rx) = mpsc::sync_channel(1);
        let (tx, events) = mpsc::channel();
        let offload = DistanceOffload {
            settings: Some(settings.clone()),
            worker: Some(WorkerLink { jobs, events }),
            stats: GpuDistanceStats {
                enabled: true,
                interval_ticks: 32,
                ..Default::default()
            },
            ..Default::default()
        };
        tx.send(WorkerEvent::Initialized("mock adapter".into()))
            .unwrap();
        (offload, settings, rx, tx)
    }
    fn complete(tx: &mpsc::Sender<WorkerEvent>, job: Job, value: f32) {
        let bucket = job.bucket;
        let result = DistanceBucket::new(job, vec![0.0, value, value, 0.0]);
        tx.send(WorkerEvent::Completed(bucket, result)).unwrap();
    }

    #[test]
    fn completed_inactive_bucket_is_only_published_every_32_ticks() {
        let (mut pipeline, settings, jobs, tx) = mock();
        let peers = peers();
        assert!(pipeline.advance(settings.clone(), &peers).is_none());
        let first = jobs.try_recv().unwrap();
        assert_eq!(first.bucket, 0);
        complete(&tx, first, 225.0);
        for _ in 2..32 {
            assert!(pipeline.advance(settings.clone(), &peers).is_none());
        }
        let held = pipeline.advance(settings.clone(), &peers).unwrap();
        let indices = held.sender_indices(&peers);
        assert_eq!(held.row(&peers[0], &indices).unwrap().get(1), Some(225.0));
        let second = jobs.try_recv().unwrap();
        assert_eq!(second.bucket, 1);
        complete(&tx, second, 900.0);
        for _ in 33..64 {
            assert_eq!(
                pipeline.advance(settings.clone(), &peers).unwrap().epoch,
                held.epoch
            );
        }
        let next = pipeline.advance(settings.clone(), &peers).unwrap();
        assert_ne!(next.epoch, held.epoch);
        assert_eq!(next.row(&peers[0], &indices).unwrap().get(1), Some(900.0));
        assert_eq!(held.row(&peers[0], &indices).unwrap().get(1), Some(225.0));
        let third = jobs.try_recv().unwrap();
        assert_eq!(third.bucket, 0);
        // Reusing a former active bucket must clear its old ready result.
        for _ in 65..=96 {
            pipeline.advance(settings.clone(), &peers);
        }
        assert_eq!(pipeline.stats.swaps, 2);
        assert_eq!(pipeline.pending.unwrap().1, third.epoch);
        assert_eq!(pipeline.stats.missed_swaps, 1);
    }

    #[test]
    fn late_gpu_never_blocks_tick_and_stale_snapshots_fall_back() {
        let (mut pipeline, settings, jobs, tx) = mock();
        let peers = peers();
        pipeline.advance(settings.clone(), &peers);
        complete(&tx, jobs.try_recv().unwrap(), 225.0);
        for _ in 2..=32 {
            pipeline.advance(settings.clone(), &peers);
        }
        let delayed = jobs.try_recv().unwrap();
        for _ in 33..=65 {
            assert!(pipeline.advance(settings.clone(), &peers).is_some());
        }
        assert!(pipeline.advance(settings.clone(), &peers).is_none());
        assert_eq!(pipeline.stats.stale_fallbacks, 1);
        for _ in 67..=98 {
            assert!(pipeline.advance(settings.clone(), &peers).is_none());
        }
        complete(&tx, delayed, 900.0);
        for _ in 99..=128 {
            assert!(pipeline.advance(settings.clone(), &peers).is_none());
        }
        assert_eq!(pipeline.stats.swaps, 1);
        assert!(pipeline.stats.missed_swaps >= 2);
        assert_eq!(pipeline.stats.stale_fallbacks, 2);
        assert!(jobs.try_recv().is_ok());
    }

    #[test]
    fn snapshot_identity_survives_reorder_join_and_peer_id_reuse() {
        let peers = peers();
        let bucket = DistanceBucket::new(
            Job {
                bucket: 0,
                epoch: 1,
                submitted_tick: 1,
                peers: peers.clone(),
            },
            vec![0.0, 225.0, 225.0, 0.0],
        )
        .unwrap();
        let mut current = vec![
            peers[1],
            peers[0],
            DistancePeer {
                id: 9,
                incarnation: 30,
                position: [0.0; 4],
            },
        ];
        let indices = bucket.sender_indices(&current);
        assert_eq!(indices, vec![Some(1), Some(0), None]);
        let row = bucket.row(&current[1], &indices).unwrap();
        assert_eq!(row.get(0), Some(225.0));
        assert_eq!(row.get(2), None);
        current[0].incarnation += 1;
        let indices = bucket.sender_indices(&current);
        assert_eq!(indices[0], None);
        assert!(bucket.row(&current[0], &indices).is_none());
    }

    #[test]
    fn gpu_error_and_disable_clear_active_bucket_without_joining_worker() {
        let (mut pipeline, mut settings, jobs, tx) = mock();
        let peers = peers();
        pipeline.advance(settings.clone(), &peers);
        complete(&tx, jobs.try_recv().unwrap(), 225.0);
        for _ in 2..=32 {
            pipeline.advance(settings.clone(), &peers);
        }
        tx.send(WorkerEvent::Completed(1, Err("device lost".into())))
            .unwrap();
        assert!(pipeline.advance(settings.clone(), &peers).is_none());
        assert_eq!(pipeline.stats.last_error.as_deref(), Some("device lost"));
        assert!(pipeline.failed);
        settings.enabled = false;
        assert!(pipeline.advance(settings, &peers).is_none());
        assert!(!pipeline.stats.enabled);
        assert!(pipeline.stats.last_error.is_none());
    }

    #[test]
    fn malformed_and_duplicate_rosters_are_rejected() {
        assert!(DistanceBucket::new(
            Job {
                bucket: 0,
                epoch: 1,
                submitted_tick: 0,
                peers: peers()
            },
            vec![0.0]
        )
        .is_err());
        let peer = peers()[0];
        assert!(DistanceBucket::new(
            Job {
                bucket: 0,
                epoch: 1,
                submitted_tick: 0,
                peers: vec![peer, peer]
            },
            vec![0.0; 4]
        )
        .is_err());
    }

    #[test]
    fn shutdown_drops_channels_and_cannot_restart_worker() {
        let (mut pipeline, settings, jobs, _tx) = mock();
        let (release, wait) = mpsc::channel::<()>();
        pipeline.threads.push(thread::spawn(move || {
            let _ = wait.recv();
        }));
        pipeline.advance(settings.clone(), &peers());
        let _inflight = jobs.try_recv().unwrap();
        let threads = pipeline.stop();
        assert_eq!(threads.len(), 1);
        assert!(pipeline.threads.is_empty());
        assert!(matches!(
            jobs.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
        assert!(pipeline.advance(settings, &peers()).is_none());
        assert!(pipeline.worker.is_none());
        assert!(!pipeline.stats.enabled);
        drop(release);
        threads.into_iter().next().unwrap().join().unwrap();
    }
}
