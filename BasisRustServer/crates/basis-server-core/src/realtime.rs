//! Independent bounded admission and OS threads for voice and avatar uplinks.
use super::*;
use basis_transport::PeerSession;
use std::{collections::VecDeque, thread};

const VOICE_BATCH_INTERVAL: Duration = Duration::from_millis(10);
// Allow a short scheduling burst across both bounded stages, without retaining
// seconds of audio or discarding frames still useful to a voice jitter buffer.
const VOICE_MAX_AGE: Duration = Duration::from_millis(100);
const VOICE_FRAMES_PER_SENDER: usize = 3;
const VOICE_BATCHES_PER_LANE: usize = 5;
const MAX_VOICE_PAYLOAD: usize = 4096;
const VOICE_SEND_QUANTUM: usize = 32;

struct Input {
    session: PeerSession,
    channel: u8,
    delivery: DeliveryMethod,
    payload: Bytes,
    received: Instant,
    arrival_order: u64,
}

#[derive(Default)]
struct VoiceInbox {
    pending: Mutex<HashMap<PeerId, VecDeque<Input>>>,
    received: AtomicU64,
    replaced: AtomicU64,
    expired: AtomicU64,
    handed_off: AtomicU64,
}

impl VoiceInbox {
    fn push(&self, peer: PeerId, input: Input) {
        self.received.fetch_add(1, Ordering::Relaxed);
        let mut pending = self.pending.lock();
        let queue = pending.entry(peer).or_default();
        if queue
            .front()
            .is_some_and(|old| !old.session.same_connection(&input.session))
        {
            queue.clear();
        }
        if queue.len() == VOICE_FRAMES_PER_SENDER {
            queue.pop_front();
            self.replaced.fetch_add(1, Ordering::Relaxed);
        }
        queue.push_back(input);
    }

    fn drain(&self, now: Instant) -> Vec<(PeerId, Vec<Input>)> {
        std::mem::take(&mut *self.pending.lock())
            .into_iter()
            .filter_map(|(peer, queue)| {
                let frames = queue
                    .into_iter()
                    .filter(|input| {
                        let fresh = now.saturating_duration_since(input.received) <= VOICE_MAX_AGE;
                        if !fresh {
                            self.expired.fetch_add(1, Ordering::Relaxed);
                        }
                        fresh
                    })
                    .collect::<Vec<_>>();
                (!frames.is_empty()).then_some((peer, frames))
            })
            .collect()
    }
}

#[derive(Default)]
struct AvatarPending {
    high_pose: Option<Input>,
    pose: Option<Input>,
    delta: Option<Input>,
}

impl AvatarPending {
    fn push(&mut self, input: Input) -> (u64, u64) {
        let mut coalesced = 0;
        let mut rejected = 0;
        let previous = self
            .high_pose
            .as_ref()
            .or(self.pose.as_ref())
            .or(self.delta.as_ref());
        if previous.is_some_and(|old| !old.session.same_connection(&input.session)) {
            rejected = self.len() as u64;
            *self = Self::default();
        }
        if input.channel != channels::DELTA_AVATAR {
            if channels::quality_from_channel(input.channel) == BitQuality::High as u8 {
                // Keep the delta baseline even when a lower-quality pose follows it.
                coalesced += self.high_pose.replace(input).is_some() as u64;
                coalesced += self.delta.take().is_some() as u64;
            } else {
                // Coalesce lower-quality poses independently of the HIGH baseline.
                coalesced += self.pose.replace(input).is_some() as u64;
            }
        } else {
            // Deltas reference the full keyframe, never the preceding delta.
            coalesced += self.delta.replace(input).is_some() as u64;
        }
        (coalesced, rejected)
    }

    fn len(&self) -> usize {
        self.high_pose.is_some() as usize
            + self.pose.is_some() as usize
            + self.delta.is_some() as usize
    }

    fn drain(&mut self) -> Vec<Input> {
        let mut inputs = [self.high_pose.take(), self.pose.take(), self.delta.take()]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        inputs.sort_by_key(|input| input.arrival_order);
        inputs
    }
}

struct VoiceGroup {
    peer: PeerId,
    session: PeerSession,
    shout: bool,
    targets: Vec<Vec<PeerId>>,
    frames: Vec<(u8, Bytes, Instant)>,
}

#[derive(Default)]
struct VoiceBatch {
    groups: Vec<VoiceGroup>,
    sessions: HashMap<PeerId, PeerSession>,
}

#[derive(Default)]
struct SendLane {
    pending: Mutex<VecDeque<Arc<VoiceBatch>>>,
    wake: parking_lot::Condvar,
    replaced: AtomicU64,
    datagrams: AtomicU64,
    expired_deliveries: AtomicU64,
}

impl SendLane {
    fn push(&self, batch: Arc<VoiceBatch>) {
        let mut pending = self.pending.lock();
        if pending.len() == VOICE_BATCHES_PER_LANE {
            pending.pop_front();
            self.replaced.fetch_add(1, Ordering::Relaxed);
        }
        pending.push_back(batch);
        drop(pending);
        self.wake.notify_one();
    }
}

fn voice_allowed(state: &ServerState, peer: PeerId, shout: bool) -> bool {
    state.authenticated_peers.contains_key(&peer)
        && !admin_runtime::is_voice_muted(state, peer)
        && (!shout || state.admin_runtime.is_announcing(peer))
        && (!state.global_state.read().voice_chat_locked
            || peer_has_permission(
                state,
                peer,
                basis_server_permissions::nodes::VOICE_LOCK_BYPASS,
            ))
}

fn should_relay_voice_to_recipient(shout: bool, is_offloaded: impl FnOnce() -> bool) -> bool {
    // P2P carries spatial voice; upstream announce voice is a non-spatial broadcast relayed to all.
    shout || !is_offloaded()
}

pub(super) fn start(state: &ServerState) -> Result<Vec<thread::JoinHandle<()>>> {
    let voice = Arc::new(VoiceInbox::default());
    let avatars = Arc::new(Mutex::new(HashMap::<PeerId, AvatarPending>::new()));
    let logical_processors = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let lane_count = std::env::var("BASIS_VOICE_SEND_WORKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|count| *count > 0)
        .unwrap_or_else(|| (logical_processors / 2).max(1))
        .clamp(1, logical_processors.min(16));
    let lanes = (0..lane_count)
        .map(|_| Arc::new(SendLane::default()))
        .collect::<Vec<_>>();
    let mut threads = Vec::new();
    // Retain partially started threads in the lifecycle owner if a later spawn fails.
    let started =
        (|| -> Result<()> {
            for (index, lane) in lanes.iter().enumerate() {
                let lane = lane.clone();
                let state = state.clone();
                let sender = state.transport.dedicated_unreliable_sender()?;
                threads.push(
                    thread::Builder::new()
                        .name(format!("BSR-VoiceSend-{index}"))
                        .spawn(move || {
                            send_loop(&state, &sender, &lane, index);
                        })?,
                );
            }
            let coordinator_state = state.clone();
            let coordinator_voice = voice.clone();
            threads.push(thread::Builder::new().name("BSR-VoiceBatch".into()).spawn(
                move || {
                    voice_loop(&coordinator_state, &coordinator_voice, &lanes);
                },
            )?);
            let avatar_state = state.clone();
            let avatar_input = avatars.clone();
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            threads.push(
                thread::Builder::new()
                    .name("BSR-AvatarInput".into())
                    .spawn(move || {
                        while !avatar_state.shutdown.load(Ordering::Relaxed) {
                            avatar_state.avatar_sync.poll_memory_reclaim();
                            let pending = std::mem::take(&mut *avatar_input.lock());
                            for (peer, mut pending) in pending {
                                for input in pending.drain() {
                                    let Some(_lease) = input.session.try_read_lease() else {
                                        avatar_state
                                            .statistics
                                            .avatar_rejected
                                            .fetch_add(1, Ordering::Relaxed);
                                        continue;
                                    };
                                    if !avatar_state.transport.is_current_session(&input.session)
                                        || !avatar_state.authenticated_peers.contains_key(&peer)
                                    {
                                        avatar_state
                                            .statistics
                                            .avatar_rejected
                                            .fetch_add(1, Ordering::Relaxed);
                                        continue;
                                    }
                                    if let Err(error) = runtime.block_on(handle_message(
                                        &avatar_state,
                                        peer,
                                        Some(&input.session),
                                        input.channel,
                                        input.delivery,
                                        input.payload,
                                        false,
                                        true,
                                    )) {
                                        // Handled avatar rejections return Ok; only
                                        // failures escaping processing reach this path.
                                        avatar_state
                                            .statistics
                                            .avatar_rejected
                                            .fetch_add(1, Ordering::Relaxed);
                                        warn!("avatar input failed: {error:#}");
                                    }
                                }
                            }
                            thread::sleep(Duration::from_millis(1));
                        }
                    })?,
            );
            Ok(())
        })();
    if let Err(error) = started {
        state.realtime_threads.lock().extend(threads);
        return Err(error);
    }
    let authenticated = state.authenticated_peers.clone();
    let statistics = state.statistics.clone();
    let next_arrival_order = Arc::new(AtomicU64::new(0));
    let input_arrival_order = Arc::clone(&next_arrival_order);
    state
        .transport
        .set_realtime_handler(Some(Arc::new(move |event, session| {
            let ServerEvent::Message {
                peer,
                channel,
                delivery,
                payload,
                ..
            } = event
            else {
                return false;
            };
            let is_voice = matches!(
                *channel,
                channels::VOICE | channels::VOICE_LARGE | channels::SHOUT_VOICE
            );
            if !is_voice && !is_avatar_input(event) {
                return false;
            }
            if is_voice || is_avatar_input(event) {
                statistics.inbound_packets.fetch_add(1, Ordering::Relaxed);
            }
            if is_avatar_input(event) {
                statistics.avatar_received.fetch_add(1, Ordering::Relaxed);
            }
            // Do not allocate queues for unauthenticated senders.
            if !authenticated.contains_key(peer)
                || (is_voice && (payload.is_empty() || payload.len() > MAX_VOICE_PAYLOAD))
            {
                if is_avatar_input(event) {
                    statistics.avatar_rejected.fetch_add(1, Ordering::Relaxed);
                }
                return true;
            }
            let input = Input {
                session,
                channel: *channel,
                delivery: *delivery,
                payload: payload.clone(),
                received: Instant::now(),
                arrival_order: 0,
            };
            if is_voice {
                voice.push(*peer, input);
            } else {
                let mut avatars = avatars.lock();
                let mut input = input;
                input.arrival_order = input_arrival_order.fetch_add(1, Ordering::Relaxed);
                let (coalesced, rejected) = avatars.entry(*peer).or_default().push(input);
                statistics
                    .avatar_coalesced
                    .fetch_add(coalesced, Ordering::Relaxed);
                statistics
                    .avatar_rejected
                    .fetch_add(rejected, Ordering::Relaxed);
            }
            true
        })));
    info!("realtime processing: dedicated voice batch thread, {lane_count} voice send threads, avatar input thread");
    Ok(threads)
}

fn is_avatar_input(event: &ServerEvent) -> bool {
    is_high_frequency_inline_event(event)
        || matches!(event,
        ServerEvent::Message { channel: channels::DELTA_AVATAR, payload, .. }
        if payload.first().is_none_or(|header| header & channels::DELTA_HEADER_CONTROL_BIT == 0))
}

fn voice_loop(state: &ServerState, inbox: &VoiceInbox, lanes: &[Arc<SendLane>]) {
    let _priority = set_voice_thread_priority();
    let mut csv = std::env::var_os("BASIS_VOICE_SERVER_DIAGNOSTIC_CSV")
        .and_then(|path| std::fs::File::create(path).ok())
        .map(std::io::BufWriter::new);
    if let Some(file) = &mut csv {
        let _ = writeln!(file, "unix_seconds,received,replaced,expired,handed_off,pending_frames,lane_replacements,sent_datagrams,expired_deliveries");
    }
    let mut report = Instant::now();
    while !state.shutdown.load(Ordering::Relaxed) {
        state.avatar_sync.poll_memory_reclaim();
        let started = Instant::now();
        let mut groups = Vec::new();
        let inputs = inbox.drain(started);
        // Capture recipient incarnations once per batch, rather than cloning a connection
        // for every sender/recipient pair. Send lanes validate them again at dispatch.
        let sessions = if inputs.is_empty() {
            HashMap::new()
        } else {
            state
                .authenticated_peers
                .iter()
                .filter_map(|peer| {
                    state
                        .transport
                        .peer_session(*peer.key())
                        .map(|session| (*peer.key(), session))
                })
                .collect::<HashMap<_, _>>()
        };
        for (peer, inputs) in inputs {
            // Ordinary and announcement traffic share sequence order within each kind.
            for shout in [false, true] {
                if !voice_allowed(state, peer, shout) {
                    continue;
                }
                let selected = inputs
                    .iter()
                    .filter(|i| {
                        (i.channel == channels::SHOUT_VOICE) == shout
                            && state.transport.is_current_session(&i.session)
                    })
                    .collect::<Vec<_>>();
                let Some(first) = selected.first() else {
                    continue;
                };
                let recipients = if shout {
                    state
                        .authenticated_peers
                        .iter()
                        .map(|p| *p.key())
                        .filter(|p| *p != peer)
                        .collect()
                } else {
                    state
                        .voice_recipients
                        .get(&peer)
                        .map(|p| p.clone())
                        .unwrap_or_default()
                };
                let mut targets = vec![Vec::new(); lanes.len()];
                for recipient in recipients {
                    if recipient == peer
                        || !sessions.contains_key(&recipient)
                        || !should_relay_voice_to_recipient(shout, || {
                            state.p2p_broker.is_offloaded(peer, recipient)
                        })
                    {
                        continue;
                    }
                    targets[recipient as usize % lanes.len()].push(recipient);
                }
                let large = shout || peer > u8::MAX as u16;
                let channel = if shout {
                    channels::SHOUT_VOICE
                } else if large {
                    channels::VOICE_LARGE
                } else {
                    channels::VOICE
                };
                let frames = selected
                    .iter()
                    .map(|input| {
                        let mut writer = NetWriter::new();
                        ServerAudioSegmentMessage {
                            player_id: peer,
                            audio_segment: input.payload.to_vec(),
                        }
                        .serialize_with_id_size(&mut writer, large);
                        (channel, Bytes::from(writer.into_vec()), input.received)
                    })
                    .collect::<Vec<_>>();
                inbox
                    .handed_off
                    .fetch_add(frames.len() as u64, Ordering::Relaxed);
                groups.push(VoiceGroup {
                    peer,
                    session: first.session.clone(),
                    shout,
                    targets,
                    frames,
                });
            }
        }
        if !groups.is_empty() {
            let batch = Arc::new(VoiceBatch { groups, sessions });
            for lane in lanes {
                lane.push(batch.clone());
            }
        }
        if report.elapsed() >= Duration::from_secs(1) {
            if let Some(file) = &mut csv {
                let pending = inbox
                    .pending
                    .lock()
                    .values()
                    .map(VecDeque::len)
                    .sum::<usize>();
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64();
                let replacements: u64 = lanes
                    .iter()
                    .map(|l| l.replaced.load(Ordering::Relaxed))
                    .sum();
                let datagrams: u64 = lanes
                    .iter()
                    .map(|l| l.datagrams.load(Ordering::Relaxed))
                    .sum();
                let expired_deliveries: u64 = lanes
                    .iter()
                    .map(|l| l.expired_deliveries.load(Ordering::Relaxed))
                    .sum();
                let _ = writeln!(
                    file,
                    "{now},{},{},{},{},{pending},{replacements},{datagrams},{expired_deliveries}",
                    inbox.received.load(Ordering::Relaxed),
                    inbox.replaced.load(Ordering::Relaxed),
                    inbox.expired.load(Ordering::Relaxed),
                    inbox.handed_off.load(Ordering::Relaxed)
                );
                let _ = file.flush();
            }
            report = Instant::now();
        }
        if let Some(rest) = VOICE_BATCH_INTERVAL.checked_sub(started.elapsed()) {
            thread::sleep(rest);
        }
    }
}

fn send_loop(state: &ServerState, transport: &TransportHandle, lane: &SendLane, index: usize) {
    let _priority = set_voice_thread_priority();
    // Reuse recipient buffers. One lane exclusively owns every recipient's voice order.
    let mut packets = (0..=u16::MAX)
        .map(|_| Vec::<usize>::new())
        .collect::<Vec<_>>();
    let mut sessions = HashMap::<PeerId, PeerSession>::new();
    let mut offsets = vec![0usize; u16::MAX as usize + 1];
    let mut dispatch_cursor = 0usize;
    let mut buffers_used = false;
    while !state.shutdown.load(Ordering::Relaxed) {
        state.avatar_sync.poll_memory_reclaim();
        let batches = {
            let mut pending = lane.pending.lock();
            if pending.is_empty() {
                lane.wake.wait_for(&mut pending, VOICE_BATCH_INTERVAL);
            }
            let take = pending.len().min(2);
            pending.drain(..take).collect::<Vec<_>>()
        };
        if batches.is_empty() {
            if buffers_used && state.authenticated_peers.is_empty() {
                for sends in &mut packets {
                    sends.shrink_to_fit();
                }
                buffers_used = false;
            }
            continue;
        }
        buffers_used = true;
        // The immutable batch owns each encoded payload for the entire flush.
        // Recipient lists keep indices, avoiding millions of shared refcount
        // updates and retaining one encoded copy of each source frame.
        let mut frame_refs = Vec::new();
        // Fold scheduling bursts into one MTU-packed flush instead of discarding the
        // previous batch. Both queued batches and audio age still have hard limits.
        for batch in &batches {
            for group in &batch.groups {
                let now = Instant::now();
                let frames = group
                    .frames
                    .iter()
                    .filter(|(_, _, received)| {
                        now.saturating_duration_since(*received) <= VOICE_MAX_AGE
                    })
                    .collect::<Vec<_>>();
                lane.expired_deliveries.fetch_add(
                    ((group.frames.len() - frames.len()) * group.targets[index].len()) as u64,
                    Ordering::Relaxed,
                );
                if frames.is_empty() {
                    continue;
                }
                if !state.transport.is_current_session(&group.session)
                    || !voice_allowed(state, group.peer, group.shout)
                {
                    continue;
                }
                let first_frame = frame_refs.len();
                frame_refs.extend(frames);
                for recipient in &group.targets[index] {
                    if !should_relay_voice_to_recipient(group.shout, || {
                        state.p2p_broker.is_offloaded(group.peer, *recipient)
                    }) {
                        continue;
                    }
                    let sends = &mut packets[*recipient as usize];
                    let session = &batch.sessions[recipient];
                    match sessions.entry(*recipient) {
                        std::collections::hash_map::Entry::Occupied(mut entry) => {
                            if !entry.get().same_connection(session) {
                                sends.clear();
                                entry.insert(session.clone());
                            }
                        }
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            entry.insert(session.clone());
                        }
                    }
                    sends.extend(first_frame..frame_refs.len());
                }
            }
        }
        let mut dispatch = sessions.keys().copied().collect::<Vec<_>>();
        if !dispatch.is_empty() {
            let start = dispatch_cursor % dispatch.len();
            dispatch.rotate_left(start);
            dispatch_cursor = dispatch_cursor.wrapping_add(1);
        }
        for peer in &dispatch {
            offsets[*peer as usize] = 0;
        }
        // Small round-robin turns prevent a dense fanout from using the entire
        // audio deadline on the first recipients and starving the remaining ones.
        loop {
            let mut progressed = false;
            for peer in &dispatch {
                let sends = &packets[*peer as usize];
                let offset = offsets[*peer as usize];
                if offset == sends.len() {
                    continue;
                }
                progressed = true;
                let end = (offset + VOICE_SEND_QUANTUM).min(sends.len());
                let now = Instant::now();
                let quantum = sends[offset..end]
                    .iter()
                    .filter_map(|frame| {
                        let (channel, payload, received) = frame_refs[*frame];
                        (now.saturating_duration_since(*received) <= VOICE_MAX_AGE).then_some((
                            *channel,
                            payload.as_ref(),
                            None,
                        ))
                    })
                    .collect::<Vec<_>>();
                lane.expired_deliveries
                    .fetch_add((end - offset - quantum.len()) as u64, Ordering::Relaxed);
                match transport.try_send_session_many_unreliable_packets(&sessions[peer], &quantum)
                {
                    Ok(sent) => {
                        lane.datagrams.fetch_add(sent as u64, Ordering::Relaxed);
                    }
                    Err(error) => {
                        warn!("voice send failed: {error}");
                    }
                }
                offsets[*peer as usize] = end;
            }
            if !progressed || state.shutdown.load(Ordering::Relaxed) {
                break;
            }
        }
        sessions.clear();
        for peer in dispatch {
            packets[peer as usize].clear();
        }
    }
}

#[cfg(windows)]
struct VoiceThreadPriority(windows_sys::Win32::Foundation::HANDLE);

#[cfg(windows)]
impl Drop for VoiceThreadPriority {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe {
                windows_sys::Win32::System::Threading::AvRevertMmThreadCharacteristics(self.0);
            }
        }
    }
}

#[cfg(windows)]
fn set_voice_thread_priority() -> VoiceThreadPriority {
    use windows_sys::Win32::System::Threading::{
        AvSetMmThreadCharacteristicsW, AvSetMmThreadPriority, GetCurrentThread, SetThreadPriority,
        AVRT_PRIORITY_HIGH, THREAD_PRIORITY_HIGHEST,
    };
    // MMCSS reserves scheduling time for audio without using time-critical priority.
    // Fall back when the multimedia scheduler is unavailable on a headless host.
    let mut task_index = 0;
    let task_name = [65u16, 117, 100, 105, 111, 0]; // "Audio"
    let handle = unsafe { AvSetMmThreadCharacteristicsW(task_name.as_ptr(), &mut task_index) };
    unsafe {
        if handle.is_null() {
            SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_HIGHEST);
        } else {
            AvSetMmThreadPriority(handle, AVRT_PRIORITY_HIGH);
        }
    }
    VoiceThreadPriority(handle)
}

#[cfg(not(windows))]
struct VoiceThreadPriority;

#[cfg(not(windows))]
fn set_voice_thread_priority() -> VoiceThreadPriority {
    VoiceThreadPriority
}

#[cfg(test)]
mod tests {
    use super::*;
    use basis_transport::PacketProperty;
    use tokio::net::UdpSocket;

    async fn session() -> (TransportHandle, PeerId, PeerSession) {
        let (transport, mut events) = TransportHandle::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut writer = NetWriter::new();
        writer.put_u8(PacketProperty::ConnectRequest as u8);
        writer.put_i32(basis_protocol::version::LITENETLIB_PROTOCOL_ID);
        writer.put_i64(1);
        writer.put_i32(0);
        writer.put_u8(16);
        writer.put_bytes(&[0; 16]);
        writer.put_bytes(b"test");
        socket
            .send_to(writer.as_slice(), transport.local_addr().unwrap())
            .await
            .unwrap();
        let ServerEvent::ConnectionRequest(request) =
            tokio::time::timeout(Duration::from_secs(2), events.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("expected request")
        };
        let peer = transport.accept(&request).await.unwrap();
        let session = transport.peer_session(peer).unwrap();
        transport.shutdown();
        (transport, peer, session)
    }

    fn input(session: &PeerSession, channel: u8, sequence: u8, received: Instant) -> Input {
        Input {
            session: session.clone(),
            channel,
            delivery: DeliveryMethod::Unreliable,
            payload: Bytes::from(vec![sequence, 0, 0]),
            received,
            arrival_order: sequence as u64,
        }
    }

    #[tokio::test]
    async fn voice_admission_is_bounded_and_preserves_recent_frame_order() {
        let (_, peer, session) = session().await;
        let inbox = VoiceInbox::default();
        let now = Instant::now();
        for sequence in 0..=255 {
            inbox.push(peer, input(&session, channels::VOICE, sequence, now));
        }
        assert_eq!(inbox.pending.lock()[&peer].len(), VOICE_FRAMES_PER_SENDER);
        let drained = inbox.drain(now);
        assert_eq!(
            drained[0]
                .1
                .iter()
                .map(|i| i.payload[0])
                .collect::<Vec<_>>(),
            [253, 254, 255]
        );
        assert_eq!(inbox.replaced.load(Ordering::Relaxed), 253);
        assert!(inbox.pending.lock().is_empty());
    }

    #[tokio::test]
    async fn expired_voice_is_removed_instead_of_replayed() {
        let (_, peer, session) = session().await;
        let inbox = VoiceInbox::default();
        let now = Instant::now();
        inbox.push(
            peer,
            input(
                &session,
                channels::VOICE,
                1,
                now - VOICE_MAX_AGE - Duration::from_millis(1),
            ),
        );
        inbox.push(peer, input(&session, channels::VOICE, 2, now));
        assert_eq!(inbox.drain(now)[0].1.len(), 1);
        assert_eq!(inbox.expired.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn a_new_connection_clears_old_sender_frames() {
        let (_, peer, old) = session().await;
        let (_, new_peer, new) = session().await;
        assert_eq!(peer, new_peer);
        let inbox = VoiceInbox::default();
        let now = Instant::now();
        inbox.push(peer, input(&old, channels::VOICE, 1, now));
        inbox.push(peer, input(&new, channels::VOICE, 2, now));
        let queued = inbox.drain(now);
        assert_eq!(queued[0].1.len(), 1);
        assert_eq!(queued[0].1[0].payload[0], 2);
    }

    #[tokio::test]
    async fn avatar_coalescing_retains_baseline_before_latest_delta() {
        let (_, _, session) = session().await;
        let now = Instant::now();
        let mut pending = AvatarPending::default();
        pending.push(input(&session, channels::PLAYER_AVATAR_HIGH, 1, now));
        pending.push(input(&session, channels::DELTA_AVATAR, 2, now));
        pending.push(input(&session, channels::DELTA_AVATAR, 3, now));
        assert_eq!(pending.high_pose.as_ref().unwrap().payload[0], 1);
        assert_eq!(pending.delta.as_ref().unwrap().payload[0], 3);
        pending.push(input(&session, channels::PLAYER_AVATAR_HIGH, 4, now));
        assert!(pending.delta.is_none());
    }

    #[tokio::test]
    async fn avatar_pending_counts_replacements_and_rejects_old_session_inputs() {
        let (_, _, old_session) = session().await;
        let (_, _, new_session) = session().await;
        let now = Instant::now();
        let mut pending = AvatarPending::default();

        assert_eq!(
            pending.push(input(&old_session, channels::PLAYER_AVATAR_LOW, 1, now)),
            (0, 0)
        );
        assert_eq!(
            pending.push(input(&old_session, channels::PLAYER_AVATAR_LOW, 2, now)),
            (1, 0)
        );
        assert_eq!(
            pending.push(input(&old_session, channels::PLAYER_AVATAR_HIGH, 3, now)),
            (0, 0)
        );
        assert_eq!(
            pending.push(input(&old_session, channels::DELTA_AVATAR, 4, now)),
            (0, 0)
        );
        assert_eq!(
            pending.push(input(&old_session, channels::PLAYER_AVATAR_HIGH, 5, now)),
            (2, 0)
        );
        assert_eq!(
            pending.push(input(&new_session, channels::PLAYER_AVATAR_LOW, 6, now)),
            (0, 2)
        );
        assert_eq!(pending.len(), 1);
        assert_eq!(pending.pose.as_ref().unwrap().payload[0], 6);
    }

    #[tokio::test]
    async fn avatar_pending_preserves_admission_order_when_receive_times_tie() {
        let (_, _, session) = session().await;
        let now = Instant::now();
        let mut pending = AvatarPending::default();
        pending.push(input(&session, channels::PLAYER_AVATAR_LOW, 7, now));
        pending.push(input(&session, channels::PLAYER_AVATAR_HIGH, 8, now));

        let drained = pending.drain();
        assert_eq!(
            drained.iter().map(|i| i.payload[0]).collect::<Vec<_>>(),
            [7, 8]
        );
        assert_eq!(
            drained.last().unwrap().channel,
            channels::PLAYER_AVATAR_HIGH
        );
    }

    #[tokio::test]
    async fn keyframe_control_requests_bypass_avatar_coalescing() {
        let (_transport, peer, session) = session().await;
        assert!(!is_avatar_input(&ServerEvent::Message {
            peer,
            session,
            channel: channels::DELTA_AVATAR,
            delivery: DeliveryMethod::ReliableOrdered,
            payload: Bytes::from_static(&[channels::DELTA_CONTROL_KEYFRAME_REQUEST, 2, 0]),
        }));
    }

    #[test]
    fn send_lane_keeps_a_fixed_number_of_pending_batches() {
        let lane = SendLane::default();
        for _ in 0..1000 {
            lane.push(Arc::new(VoiceBatch::default()));
        }
        assert_eq!(
            lane.replaced.load(Ordering::Relaxed),
            1000 - VOICE_BATCHES_PER_LANE as u64
        );
        assert_eq!(lane.pending.lock().len(), VOICE_BATCHES_PER_LANE);
    }

    #[test]
    fn offloaded_pair_keeps_spatial_voice_off_relay_but_relays_shout() {
        assert!(!should_relay_voice_to_recipient(false, || true));
        assert!(should_relay_voice_to_recipient(true, || panic!(
            "shout must skip P2P lookup"
        )));
    }
}
