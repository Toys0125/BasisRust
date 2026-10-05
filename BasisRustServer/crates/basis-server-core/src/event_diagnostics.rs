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
            ServerEvent::Message {
                peer: 0,
                channel: channels::VOICE,
                delivery: DeliveryMethod::Unreliable,
                payload: Bytes::new(),
            },
        ));
        let message_future_bytes = std::mem::size_of_val(&handle_message(
            state,
            0,
            channels::VOICE,
            DeliveryMethod::Unreliable,
            Bytes::new(),
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

    fn voice() -> ServerEvent {
        ServerEvent::Message {
            peer: 0,
            channel: channels::VOICE,
            delivery: DeliveryMethod::Unreliable,
            payload: Bytes::new(),
        }
    }

    #[test]
    fn tracks_waiting_running_and_completion() {
        let diagnostics = Arc::new(EventDiagnostics::default());
        let mut guard = diagnostics.spawned(&voice());
        assert_eq!(diagnostics.voice_waiting.load(Ordering::Relaxed), 1);
        guard.started();
        assert_eq!(diagnostics.voice_waiting.load(Ordering::Relaxed), 0);
        assert_eq!(diagnostics.voice_running.load(Ordering::Relaxed), 1);
        drop(guard);
        assert_eq!(diagnostics.voice_running.load(Ordering::Relaxed), 0);
        assert_eq!(diagnostics.voice_completed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn cancelled_waiter_drops_its_count() {
        let diagnostics = Arc::new(EventDiagnostics::default());
        drop(diagnostics.spawned(&voice()));
        assert_eq!(diagnostics.waiting.load(Ordering::Relaxed), 0);
        assert_eq!(diagnostics.voice_waiting.load(Ordering::Relaxed), 0);
        assert_eq!(diagnostics.completed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn distinguishes_avatar_delta_backlog_from_voice() {
        let diagnostics = Arc::new(EventDiagnostics::default());
        let event = ServerEvent::Message {
            peer: 0,
            channel: channels::DELTA_AVATAR,
            delivery: DeliveryMethod::Unreliable,
            payload: Bytes::new(),
        };
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
