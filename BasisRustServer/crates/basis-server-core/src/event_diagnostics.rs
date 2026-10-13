//! Opt-in measurements of event tasks waiting behind the dispatcher semaphore.
use super::*;

#[derive(Default)]
pub(super) struct EventDiagnostics {
    spawned: AtomicU64,
    completed: AtomicU64,
    waiting: AtomicU64,
    running: AtomicU64,
    voice_spawned: AtomicU64,
    voice_completed: AtomicU64,
    voice_waiting: AtomicU64,
    voice_running: AtomicU64,
    delta_spawned: AtomicU64,
    delta_completed: AtomicU64,
    delta_waiting: AtomicU64,
    delta_running: AtomicU64,
    event_queue_depth: AtomicU64,
    task_future_bytes: AtomicU64,
}

pub(super) struct EventTaskGuard {
    diagnostics: Arc<EventDiagnostics>,
    voice: bool,
    delta: bool,
    started: bool,
}

impl EventDiagnostics {
    pub(super) fn start_from_env(state: &ServerState, worker_limit: usize) -> Option<Arc<Self>> {
        let path = std::env::var_os("BASIS_EVENT_DIAGNOSTIC_CSV")?;
        let file = match std::fs::File::create(&path) {
            Ok(file) => file,
            Err(error) => {
                warn!("cannot create event diagnostic CSV: {error}");
                return None;
            }
        };
        let diagnostics = Arc::new(Self::default());
        let reporter = diagnostics.clone();
        let shutdown = state.shutdown.clone();
        let event_future_bytes = std::mem::size_of_val(&handle_event(
            state,
            ServerEvent::UnconnectedRequest {
                remote_addr: "127.0.0.1:0".parse().unwrap(),
                nonce: 0,
                payload: Bytes::new(),
            },
        ));
        let message_future_bytes = std::mem::size_of_val(&handle_message(
            state,
            0,
            None,
            channels::VOICE,
            DeliveryMethod::Unreliable,
            Bytes::new(),
            false,
            false,
            false,
        ));
        let voice_future_bytes = std::mem::size_of_val(&relay_voice_message(state, 0, &[]));
        info!("event memory diagnostics: workers={worker_limit} handle_event_future_bytes={event_future_bytes} handle_message_future_bytes={message_future_bytes} voice_relay_future_bytes={voice_future_bytes} server_state_bytes={}", std::mem::size_of::<ServerState>());
        tokio::spawn(async move {
            let mut file = std::io::BufWriter::new(file);
            let _ = writeln!(file, "unix_seconds,spawned,completed,waiting,running,voice_spawned,voice_completed,voice_waiting,voice_running,delta_spawned,delta_completed,delta_waiting,delta_running,tokio_alive_tasks,tokio_global_queue_depth,task_future_bytes,handle_event_future_bytes,handle_message_future_bytes,voice_relay_future_bytes,worker_limit,event_queue_depth");
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let runtime = tokio::runtime::Handle::current().metrics();
                let counters = [
                    &reporter.spawned,
                    &reporter.completed,
                    &reporter.waiting,
                    &reporter.running,
                    &reporter.voice_spawned,
                    &reporter.voice_completed,
                    &reporter.voice_waiting,
                    &reporter.voice_running,
                    &reporter.delta_spawned,
                    &reporter.delta_completed,
                    &reporter.delta_waiting,
                    &reporter.delta_running,
                ]
                .map(|counter| counter.load(Ordering::Relaxed).to_string())
                .join(",");
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64();
                if writeln!(file, "{now},{counters},{},{},{},{event_future_bytes},{message_future_bytes},{voice_future_bytes},{worker_limit},{}",
                    runtime.num_alive_tasks(), runtime.global_queue_depth(),
                    reporter.task_future_bytes.load(Ordering::Relaxed),
                    reporter.event_queue_depth.load(Ordering::Relaxed)).is_err()
                    || file.flush().is_err()
                { break; }
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
            }
        });
        Some(diagnostics)
    }

    pub(super) fn spawned(self: &Arc<Self>, event: &ServerEvent) -> EventTaskGuard {
        let voice = matches!(
            event,
            ServerEvent::Message {
                channel: channels::VOICE | channels::VOICE_LARGE | channels::SHOUT_VOICE,
                ..
            }
        );
        let delta = matches!(
            event,
            ServerEvent::Message {
                channel: channels::DELTA_AVATAR,
                ..
            }
        );
        self.spawned.fetch_add(1, Ordering::Relaxed);
        self.waiting.fetch_add(1, Ordering::Relaxed);
        if voice {
            self.voice_spawned.fetch_add(1, Ordering::Relaxed);
            self.voice_waiting.fetch_add(1, Ordering::Relaxed);
        }
        if delta {
            self.delta_spawned.fetch_add(1, Ordering::Relaxed);
            self.delta_waiting.fetch_add(1, Ordering::Relaxed);
        }
        EventTaskGuard {
            diagnostics: self.clone(),
            voice,
            delta,
            started: false,
        }
    }

    pub(super) fn record_task_size(&self, bytes: usize) {
        self.task_future_bytes
            .store(bytes as u64, Ordering::Relaxed);
    }

    pub(super) fn record_queue_depth(&self, depth: usize) {
        self.event_queue_depth
            .store(depth as u64, Ordering::Relaxed);
    }
}

impl EventTaskGuard {
    pub(super) fn started(&mut self) {
        self.started = true;
        self.diagnostics.waiting.fetch_sub(1, Ordering::Relaxed);
        self.diagnostics.running.fetch_add(1, Ordering::Relaxed);
        if self.voice {
            self.diagnostics
                .voice_waiting
                .fetch_sub(1, Ordering::Relaxed);
            self.diagnostics
                .voice_running
                .fetch_add(1, Ordering::Relaxed);
        }
        if self.delta {
            self.diagnostics
                .delta_waiting
                .fetch_sub(1, Ordering::Relaxed);
            self.diagnostics
                .delta_running
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Drop for EventTaskGuard {
    fn drop(&mut self) {
        let d = &self.diagnostics;
        if self.started {
            d.running.fetch_sub(1, Ordering::Relaxed);
            if self.voice {
                d.voice_running.fetch_sub(1, Ordering::Relaxed);
            }
            if self.delta {
                d.delta_running.fetch_sub(1, Ordering::Relaxed);
            }
        } else {
            d.waiting.fetch_sub(1, Ordering::Relaxed);
            if self.voice {
                d.voice_waiting.fetch_sub(1, Ordering::Relaxed);
            }
            if self.delta {
                d.delta_waiting.fetch_sub(1, Ordering::Relaxed);
            }
        }
        d.completed.fetch_add(1, Ordering::Relaxed);
        if self.voice {
            d.voice_completed.fetch_add(1, Ordering::Relaxed);
        }
        if self.delta {
            d.delta_completed.fetch_add(1, Ordering::Relaxed);
        }
    }
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
        writer.put_bytes(b"event diagnostics test");
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
            panic!("expected connection request")
        };
        let peer = transport.accept(&request).await.unwrap();
        let session = transport.peer_session(peer).unwrap();
        transport.shutdown();
        (transport, peer, session)
    }

    fn message(session: PeerSession, channel: u8) -> ServerEvent {
        ServerEvent::Message {
            peer: session.peer_id(),
            session,
            channel,
            delivery: DeliveryMethod::Unreliable,
            payload: Bytes::new(),
        }
    }

    #[tokio::test]
    async fn tracks_waiting_running_and_completion() {
        let (_transport, _, session) = session().await;
        let diagnostics = Arc::new(EventDiagnostics::default());
        let mut guard = diagnostics.spawned(&message(session, channels::VOICE));
        assert_eq!(diagnostics.voice_waiting.load(Ordering::Relaxed), 1);
        guard.started();
        assert_eq!(diagnostics.voice_waiting.load(Ordering::Relaxed), 0);
        assert_eq!(diagnostics.voice_running.load(Ordering::Relaxed), 1);
        drop(guard);
        assert_eq!(diagnostics.voice_running.load(Ordering::Relaxed), 0);
        assert_eq!(diagnostics.voice_completed.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn cancelled_waiter_drops_its_count() {
        let (_transport, _, session) = session().await;
        let diagnostics = Arc::new(EventDiagnostics::default());
        drop(diagnostics.spawned(&message(session, channels::VOICE)));
        assert_eq!(diagnostics.waiting.load(Ordering::Relaxed), 0);
        assert_eq!(diagnostics.voice_waiting.load(Ordering::Relaxed), 0);
        assert_eq!(diagnostics.completed.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn distinguishes_avatar_delta_backlog_from_voice() {
        let (_transport, _, session) = session().await;
        let diagnostics = Arc::new(EventDiagnostics::default());
        let event = message(session, channels::DELTA_AVATAR);
        let mut guard = diagnostics.spawned(&event);
        assert_eq!(diagnostics.delta_waiting.load(Ordering::Relaxed), 1);
        assert_eq!(diagnostics.voice_waiting.load(Ordering::Relaxed), 0);
        guard.started();
        assert_eq!(diagnostics.delta_running.load(Ordering::Relaxed), 1);
        drop(guard);
        assert_eq!(diagnostics.delta_running.load(Ordering::Relaxed), 0);
        assert_eq!(diagnostics.delta_completed.load(Ordering::Relaxed), 1);
    }
}
