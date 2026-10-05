use crate::client::BasisClient;
use crate::config::{Config, DEFAULT_VOICE_FRAME_DURATION_MS, MAX_UNITY_VOICE_FRAME_DURATION_MS};
use crate::simulation::{cadence_next, cadence_seed, jittered_duration, CadenceOptions};
use anyhow::{anyhow, Context, Result};
use basis_protocol::channels;
use basis_protocol::io::NetWriter;
use rand::Rng;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::BufReader;
use tokio::io::{AsyncBufReadExt, AsyncReadExt};
use tokio::process::Command;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinSet;
use tokio::{io, time};
use tracing::{debug, info, warn};
use uuid::Uuid;

pub(crate) const MAX_VOICE_PACKET_BYTES: usize = 1200;
#[derive(Debug, Clone)]
pub(crate) struct VoiceLibrary {
    pub(crate) clips: Vec<VoiceClip>,
}

#[derive(Debug, Clone)]
pub(crate) struct VoiceClip {
    pub(crate) path: PathBuf,
    pub(crate) packets: Arc<OggOpusPackets>,
}

impl VoiceLibrary {
    pub(crate) async fn load(
        folder: &str,
        reencode: bool,
        frame_duration_ms: u64,
        shutdown: &Arc<AtomicBool>,
    ) -> Result<Option<Self>> {
        Self::load_with_options(
            folder,
            reencode,
            frame_duration_ms,
            shutdown,
            Path::new("ffmpeg"),
            num_cpus::get().clamp(1, 8),
        )
        .await
    }

    pub(crate) async fn load_with_options(
        folder: &str,
        reencode: bool,
        frame_duration_ms: u64,
        shutdown: &Arc<AtomicBool>,
        ffmpeg: &Path,
        parallelism: usize,
    ) -> Result<Option<Self>> {
        let folder = PathBuf::from(folder);
        let folder = if folder.is_absolute() {
            folder
        } else {
            std::env::current_dir()?.join(folder)
        };
        info!(
            "scanning voice audio folder {} (ffmpeg_reencode={})",
            folder.display(),
            reencode
        );
        if !folder.exists() {
            warn!("voice audio folder does not exist: {}", folder.display());
            return Ok(None);
        }
        if !folder.is_dir() {
            warn!("voice audio path is not a folder: {}", folder.display());
            return Ok(None);
        }

        let mut paths = Vec::new();
        for entry in std::fs::read_dir(&folder)
            .with_context(|| format!("reading voice audio folder {}", folder.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() && is_ogg_opus_path(&path) {
                paths.push(path);
            }
        }
        paths.sort();
        if paths.is_empty() {
            warn!(
                "voice audio folder contains no .opus or .ogg files: {}",
                folder.display()
            );
            return Ok(None);
        }

        let total = paths.len();
        let parallelism = parallelism.max(1).min(total);
        info!("loading {total} voice audio file(s), parallel_jobs={parallelism}");
        let mut pending = paths.into_iter();
        let mut jobs = JoinSet::new();
        let mut clips = Vec::new();
        let mut completed = 0;
        loop {
            while jobs.len() < parallelism && !shutdown.load(Ordering::Relaxed) {
                let Some(path) = pending.next() else { break };
                let shutdown = shutdown.clone();
                let ffmpeg = ffmpeg.to_owned();
                jobs.spawn(async move {
                    let result = if reencode {
                        OggOpusPackets::load_reencoded(&path, frame_duration_ms, &shutdown, &ffmpeg)
                            .await
                    } else {
                        OggOpusPackets::load(&path, &shutdown).await
                    };
                    (path, result)
                });
            }
            let Some(result) = jobs.join_next().await else {
                break;
            };
            let (path, result) = result.context("voice audio loading task failed")?;
            if shutdown.load(Ordering::Relaxed) {
                // Drain all jobs so every running FFmpeg is killed and reaped before returning.
                continue;
            }
            completed += 1;
            match result {
                Ok(packets) => clips.push(VoiceClip {
                    path: path.clone(),
                    packets: Arc::new(packets),
                }),
                Err(err) => warn!(
                    "excluding unusable voice audio file {}: {err:#}",
                    path.display()
                ),
            }
            info!(
                "voice audio progress {completed}/{total}: {} ({} usable)",
                path.display(),
                clips.len()
            );
        }
        if shutdown.load(Ordering::Relaxed) {
            info!("voice audio loading cancelled; stopped all re-encoding jobs");
            return Ok(None);
        }
        clips.sort_by(|a, b| a.path.cmp(&b.path));
        if clips.is_empty() {
            warn!(
                "voice audio folder contains no usable Ogg Opus files: {}",
                folder.display()
            );
            return Ok(None);
        }

        info!(
            "voice audio folder loaded {} usable Ogg Opus file(s)",
            clips.len()
        );
        Ok(Some(Self { clips }))
    }

    pub(crate) fn random_clip(&self) -> Option<VoiceClip> {
        if self.clips.is_empty() {
            return None;
        }
        let idx = rand::thread_rng().gen_range(0..self.clips.len());
        Some(self.clips[idx].clone())
    }
}

pub(crate) fn is_ogg_opus_path(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("opus") || ext.eq_ignore_ascii_case("ogg"))
        .unwrap_or(false)
}

#[derive(Debug, Clone)]
pub(crate) struct OggOpusPackets {
    pub(crate) packets: Vec<OpusPacket>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OpusPacket {
    pub(crate) data: Vec<u8>,
    pub(crate) duration_ms: u64,
}

// Clean up output on success, conversion failure, and cancellation.
pub(crate) struct ReencodedVoiceFile(PathBuf);

impl Drop for ReencodedVoiceFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

impl OggOpusPackets {
    pub(crate) async fn load(path: &Path, shutdown: &Arc<AtomicBool>) -> Result<Self> {
        let path = path.to_owned();
        let shutdown = shutdown.clone();
        tokio::task::spawn_blocking(move || {
            if shutdown.load(Ordering::Relaxed) {
                return Err(anyhow!("voice audio loading cancelled"));
            }
            let bytes = std::fs::read(&path)
                .with_context(|| format!("reading Opus file {}", path.display()))?;
            Self::parse_cancellable(&bytes, &shutdown)
                .with_context(|| format!("parsing Ogg Opus file {}", path.display()))
        })
        .await
        .context("Opus file loading task failed")?
    }

    pub(crate) async fn load_reencoded(
        path: &Path,
        frame_duration_ms: u64,
        shutdown: &Arc<AtomicBool>,
        ffmpeg: &Path,
    ) -> Result<Self> {
        if shutdown.load(Ordering::Relaxed) {
            return Err(anyhow!("voice audio loading cancelled"));
        }
        let output_path = ReencodedVoiceFile(
            std::env::temp_dir().join(format!("basis-rust-client-voice-{}.opus", Uuid::new_v4())),
        );
        info!(
            "re-encoding voice audio {} to 48kHz mono Opus",
            path.display()
        );
        let mut child = Command::new(ffmpeg)
            .arg("-hide_banner")
            .arg("-loglevel")
            .arg("error")
            .arg("-nostdin")
            .arg("-nostats")
            .arg("-progress")
            .arg("pipe:1")
            .arg("-y")
            .arg("-i")
            .arg(path)
            .arg("-vn")
            .arg("-ac")
            .arg("1")
            .arg("-ar")
            .arg("48000")
            .arg("-c:a")
            .arg("libopus")
            .arg("-threads")
            .arg("1")
            .arg("-b:a")
            .arg("32000")
            .arg("-application")
            .arg("audio")
            .arg("-frame_duration")
            .arg(frame_duration_ms.to_string())
            .arg(&output_path.0)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| {
                format!(
                    "running ffmpeg to re-encode {} to 48kHz mono Opus",
                    path.display()
                )
            })?;

        let mut progress =
            BufReader::new(child.stdout.take().expect("piped ffmpeg stdout")).lines();
        let mut stderr = child.stderr.take().expect("piped ffmpeg stderr");
        let stderr_output = async move {
            let mut bytes = Vec::new();
            stderr.read_to_end(&mut bytes).await?;
            io::Result::Ok(bytes)
        };
        tokio::pin!(stderr_output);
        let mut stderr_bytes = None;
        let mut progress_open = true;
        let mut encoded_time = String::new();
        let mut speed = String::new();
        let mut exit_status = None;
        let mut cancellation_check = time::interval(Duration::from_millis(50));
        let status = loop {
            if !progress_open {
                if let Some(status) = exit_status {
                    break status;
                }
            }
            tokio::select! {
                biased;
                _ = cancellation_check.tick() => {
                    if shutdown.load(Ordering::Relaxed) {
                        // kill() also waits, so cleanup cannot race a still-writing FFmpeg.
                        if exit_status.is_none() {
                            child.kill().await.context("stopping voice re-encoding process")?;
                        }
                        return Err(anyhow!("voice audio loading cancelled"));
                    }
                }
                result = &mut stderr_output, if stderr_bytes.is_none() => {
                    stderr_bytes = Some(result.context("reading ffmpeg errors")?);
                }
                line = progress.next_line(), if progress_open => {
                    match line.context("reading ffmpeg progress")? {
                        Some(line) => {
                            if let Some(value) = line.strip_prefix("out_time=") {
                                encoded_time = value.to_owned();
                            } else if let Some(value) = line.strip_prefix("speed=") {
                                speed = value.to_owned();
                            } else if line.starts_with("progress=") {
                                info!("voice re-encoding progress {}: audio_time={} speed={}",
                                    path.display(), encoded_time, speed);
                            }
                        }
                        None => progress_open = false,
                    }
                }
                result = child.wait(), if exit_status.is_none() => {
                    // Drain stdout to EOF even if the process exits before its final progress
                    // lines become ready on the async pipe.
                    exit_status = Some(result.context("waiting for ffmpeg")?);
                }
            }
        };
        let stderr_bytes = match stderr_bytes {
            Some(bytes) => bytes,
            None => stderr_output.await.context("reading ffmpeg errors")?,
        };
        if !status.success() {
            let stderr = String::from_utf8_lossy(&stderr_bytes);
            return Err(anyhow!(
                "ffmpeg failed to re-encode {}: {}",
                path.display(),
                stderr.trim()
            ));
        }

        Self::load(&output_path.0, shutdown).await.with_context(|| {
            format!(
                "reading ffmpeg re-encoded Opus output for {}",
                path.display()
            )
        })
    }

    #[cfg(test)]
    pub(crate) fn parse(bytes: &[u8]) -> Result<Self> {
        Self::parse_cancellable(bytes, &AtomicBool::new(false))
    }

    pub(crate) fn parse_cancellable(bytes: &[u8], shutdown: &AtomicBool) -> Result<Self> {
        let mut pos = 0;
        let mut current_packet = Vec::new();
        let mut packets = Vec::new();

        while pos < bytes.len() {
            if shutdown.load(Ordering::Relaxed) {
                return Err(anyhow!("voice audio loading cancelled"));
            }
            if bytes.len() - pos < 27 {
                return Err(anyhow!("truncated Ogg page header at byte {pos}"));
            }
            if &bytes[pos..pos + 4] != b"OggS" {
                return Err(anyhow!("invalid Ogg capture pattern at byte {pos}"));
            }
            let page_segments = bytes[pos + 26] as usize;
            let segment_table_start = pos + 27;
            let segment_table_end = segment_table_start + page_segments;
            if segment_table_end > bytes.len() {
                return Err(anyhow!("truncated Ogg segment table at byte {pos}"));
            }
            let data_len: usize = bytes[segment_table_start..segment_table_end]
                .iter()
                .map(|segment| *segment as usize)
                .sum();
            let data_start = segment_table_end;
            let data_end = data_start + data_len;
            if data_end > bytes.len() {
                return Err(anyhow!("truncated Ogg page data at byte {pos}"));
            }

            let mut data_pos = data_start;
            for segment_len in &bytes[segment_table_start..segment_table_end] {
                let segment_len = *segment_len as usize;
                current_packet.extend_from_slice(&bytes[data_pos..data_pos + segment_len]);
                data_pos += segment_len;
                if segment_len < 255 {
                    if is_playable_opus_packet(&current_packet) {
                        let data = std::mem::take(&mut current_packet);
                        let duration_ms = opus_packet_duration_ms(&data)
                            .unwrap_or(DEFAULT_VOICE_FRAME_DURATION_MS);
                        packets.push(OpusPacket { data, duration_ms });
                    } else {
                        current_packet.clear();
                    }
                }
            }

            pos = data_end;
        }

        if !current_packet.is_empty() {
            return Err(anyhow!("truncated Ogg packet at end of stream"));
        }
        if packets.is_empty() {
            return Err(anyhow!("Ogg Opus file contains no playable Opus packets"));
        }

        Ok(Self { packets })
    }
}

pub(crate) fn is_playable_opus_packet(packet: &[u8]) -> bool {
    !packet.is_empty() && !packet.starts_with(b"OpusHead") && !packet.starts_with(b"OpusTags")
}

pub(crate) fn opus_packet_duration_ms(packet: &[u8]) -> Option<u64> {
    let toc = *packet.first()?;
    let frame_count = match toc & 0x03 {
        0 => 1,
        1 | 2 => 2,
        3 => {
            let count_byte = *packet.get(1)?;
            (count_byte & 0x3f) as u64
        }
        _ => return None,
    };
    if frame_count == 0 {
        return None;
    }
    let config = toc >> 3;
    let frame_duration_us = match config {
        0..=11 => {
            let index = config & 0x03;
            match index {
                0 => 10_000,
                1 => 20_000,
                2 => 40_000,
                3 => 60_000,
                _ => return None,
            }
        }
        12..=15 => {
            if (config & 0x01) == 0 {
                10_000
            } else {
                20_000
            }
        }
        16..=19 => {
            let index = config & 0x03;
            match index {
                0 => 2_500,
                1 => 5_000,
                2 => 10_000,
                3 => 20_000,
                _ => return None,
            }
        }
        20..=31 => {
            let index = config & 0x03;
            match index {
                0 => 2_500,
                1 => 5_000,
                2 => 10_000,
                3 => 20_000,
                _ => return None,
            }
        }
        _ => return None,
    };
    Some((frame_duration_us * frame_count).div_ceil(1000))
}

pub(crate) fn voice_speaker_target(connected_count: usize, percent: u8) -> usize {
    let percent = percent.min(100) as usize;
    if connected_count == 0 || percent == 0 {
        return 0;
    }
    (connected_count * percent).div_ceil(100)
}

pub(crate) fn choose_next_speaker(
    connected: &[usize],
    active: &HashSet<usize>,
    avoid: &HashSet<usize>,
) -> Option<usize> {
    let eligible = connected
        .iter()
        .copied()
        .filter(|idx| !active.contains(idx))
        .collect::<Vec<_>>();
    if eligible.is_empty() {
        return None;
    }
    let preferred = eligible
        .iter()
        .copied()
        .filter(|idx| !avoid.contains(idx))
        .collect::<Vec<_>>();
    let candidates = if preferred.is_empty() {
        eligible
    } else {
        preferred
    };
    Some(candidates[rand::thread_rng().gen_range(0..candidates.len())])
}

pub(crate) fn serialize_voice_recipients_small(recipients: &[u16]) -> Vec<u8> {
    let mut writer = NetWriter::with_capacity(1 + recipients.len() * 2);
    writer.put_u8(recipients.len().min(u8::MAX as usize) as u8);
    for recipient in recipients.iter().take(u8::MAX as usize) {
        writer.put_u16(*recipient);
    }
    writer.into_vec()
}

pub(crate) fn serialize_voice_recipients_large(recipients: &[u16]) -> Vec<u8> {
    let mut writer = NetWriter::with_capacity(2 + recipients.len() * 2);
    writer.put_u16(recipients.len().min(u16::MAX as usize) as u16);
    for recipient in recipients.iter().take(u16::MAX as usize) {
        writer.put_u16(*recipient);
    }
    writer.into_vec()
}

pub(crate) fn serialize_audio_segment(sequence: u8, opus_packet: &[u8]) -> Vec<u8> {
    let mut writer = NetWriter::with_capacity(2 + opus_packet.len());
    writer.put_u8(sequence);
    writer.put_u8(0);
    writer.put_bytes(opus_packet);
    writer.into_vec()
}

pub(crate) fn distance_within(a: [f32; 3], b: [f32; 3], max_distance: f32) -> bool {
    let dx = a[0] - b[0];
    let dy = a[1] - b[1];
    let dz = a[2] - b[2];
    dx * dx + dy * dy + dz * dz <= max_distance * max_distance
}

#[derive(Clone)]
pub(crate) struct VoicePlaybackContext {
    pub(crate) clients: Arc<Mutex<Vec<Arc<BasisClient>>>>,
    pub(crate) hearing_distance: f32,
    pub(crate) frame_duration: Duration,
    pub(crate) shutdown: Arc<AtomicBool>,
    pub(crate) done_tx: mpsc::UnboundedSender<usize>,
    pub(crate) cadence: CadenceOptions,
}

pub(crate) async fn voice_workers(
    clients: Arc<Mutex<Vec<Arc<BasisClient>>>>,
    config: Config,
    library: Arc<VoiceLibrary>,
    shutdown: Arc<AtomicBool>,
    cadence: CadenceOptions,
) {
    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<usize>();
    let mut active = HashSet::<usize>::new();
    let frame_duration = Duration::from_millis(config.voice_frame_duration_ms);
    let mut refill_tick = time::interval(Duration::from_millis(250));

    loop {
        refill_tick.tick().await;
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let mut recently_finished = HashSet::new();
        while let Ok(index) = done_rx.try_recv() {
            active.remove(&index);
            recently_finished.insert(index);
        }

        let snapshot = clients.lock().await.clone();
        let connected = snapshot
            .iter()
            .filter(|client| client.connected.load(Ordering::Relaxed))
            .map(|client| client.index)
            .collect::<Vec<_>>();
        let connected_set = connected.iter().copied().collect::<HashSet<_>>();
        active.retain(|index| connected_set.contains(index));

        let target = voice_speaker_target(connected.len(), config.voice_speaker_percent);
        while active.len() < target {
            let Some(index) = choose_next_speaker(&connected, &active, &recently_finished) else {
                break;
            };
            let Some(client) = snapshot
                .iter()
                .find(|client| client.index == index)
                .cloned()
            else {
                break;
            };
            let Some(clip) = library.random_clip() else {
                active.remove(&index);
                break;
            };
            active.insert(index);

            let context = VoicePlaybackContext {
                clients: clients.clone(),
                hearing_distance: config.voice_hearing_distance,
                frame_duration,
                shutdown: shutdown.clone(),
                done_tx: done_tx.clone(),
                cadence,
            };
            tokio::spawn(async move {
                voice_playback_task(client, clip, context).await;
            });
        }
    }
}

pub(crate) async fn voice_playback_task(
    client: Arc<BasisClient>,
    clip: VoiceClip,
    context: VoicePlaybackContext,
) {
    let VoicePlaybackContext {
        clients,
        hearing_distance,
        frame_duration,
        shutdown,
        done_tx,
        cadence,
    } = context;
    let index = client.index;
    let result = async {
        let non_default_packets = clip
            .packets
            .packets
            .iter()
            .filter(|packet| packet.duration_ms != frame_duration.as_millis() as u64)
            .count();
        if non_default_packets > 0 {
            debug!(
                "voice file {} contains {}/{} packets not matching configured {}ms pacing; using per-packet Opus durations",
                clip.path.display(),
                non_default_packets,
                clip.packets.packets.len(),
                frame_duration.as_millis()
            );
        }
        let mut last_recipients = Vec::<u16>::new();
        let mut last_refresh = None::<time::Instant>;
        let mut published_once = false;
        let mut logged_first_send = false;
        let mut cadence_state = cadence_seed(client.index, 0x564f_4943_455f_5458);
        let initial_phase = if cadence.sync_batching {
            Duration::ZERO
        } else {
            Duration::from_micros(
                cadence_next(&mut cadence_state)
                    % (frame_duration.as_micros().max(1) as u64),
            )
        };
        let mut next_packet_at = time::Instant::now() + initial_phase;
        let max_lag = Duration::from_millis(DEFAULT_VOICE_FRAME_DURATION_MS * 5);
        let mut packets_sent = 0usize;
        let mut packets_skipped = 0usize;
        let mut playback_started = time::Instant::now();

        for packet in clip.packets.packets.iter() {
            if shutdown.load(Ordering::Relaxed) || !client.connected.load(Ordering::Relaxed) {
                break;
            }

            let now = time::Instant::now();
            if next_packet_at > now {
                time::sleep_until(next_packet_at).await;
            } else if now.duration_since(next_packet_at) > max_lag {
                next_packet_at = now;
            }

            let should_refresh = last_refresh
                .map(|instant| instant.elapsed() >= Duration::from_secs(1))
                .unwrap_or(true);
            if !published_once || should_refresh {
                let snapshot = clients.lock().await.clone();
                let exclusions =
                    voice_exclusions_for_inverted_recipients(&client, &snapshot, hearing_distance)
                        .await;
                if !published_once || exclusions != last_recipients {
                    publish_voice_recipient_exclusions(&client, &exclusions).await?;
                    last_recipients = exclusions;
                    published_once = true;
                }
                last_refresh = Some(time::Instant::now());
            }

            if packet.data.len() > MAX_VOICE_PACKET_BYTES {
                if let Some(diagnostics) = &client.voice_diagnostics { diagnostics.skipped(); }
                packets_skipped += 1;
                warn!(
                    "skipping oversized voice packet for client {} from {}: {} bytes",
                    client.index,
                    clip.path.display(),
                    packet.data.len()
                );
            } else if packet.duration_ms > MAX_UNITY_VOICE_FRAME_DURATION_MS {
                if let Some(diagnostics) = &client.voice_diagnostics { diagnostics.skipped(); }
                packets_skipped += 1;
                warn!(
                    "skipping voice packet for client {} from {}: {}ms exceeds Unity voice max {}ms",
                    client.index,
                    clip.path.display(),
                    packet.duration_ms,
                    MAX_UNITY_VOICE_FRAME_DURATION_MS
                );
            } else {
                let sequence = client.voice_sequence.fetch_add(1, Ordering::SeqCst);
                let payload = serialize_audio_segment(sequence, &packet.data);
                let result = client.send_unreliable(channels::VOICE, &payload).await;
                if let Some(diagnostics) = &client.voice_diagnostics {
                    diagnostics.sent(packet.data.len(), result.is_ok());
                }
                result?;
                packets_sent += 1;
                if !logged_first_send {
                    logged_first_send = true;
                    playback_started = time::Instant::now();
                    debug!(
                        "client {} sent first voice packet from {} (opus_bytes={} wire_bytes={})",
                        client.index,
                        clip.path.display(),
                        packet.data.len(),
                        payload.len()
                    );
                }
            }
            let packet_duration = Duration::from_millis(packet.duration_ms.max(1));
            next_packet_at += if cadence.sync_batching {
                packet_duration
            } else {
                jittered_duration(
                    packet_duration,
                    cadence.voice_jitter_percent,
                    &mut cadence_state,
                )
            };
        }

        if packets_sent > 0 || packets_skipped > 0 {
            debug!(
                "client {} finished voice playback from {} (sent={} skipped={} elapsed_ms={})",
                client.index,
                clip.path.display(),
                packets_sent,
                packets_skipped,
                playback_started.elapsed().as_millis()
            );
        }

        Ok::<(), anyhow::Error>(())
    }
    .await;

    if let Err(err) = result {
        warn!(
            "voice playback failed for client {} using {}: {err:#}",
            index,
            clip.path.display()
        );
    }
    let _ = done_tx.send(index);
}

pub(crate) async fn voice_exclusions_for_inverted_recipients(
    speaker: &BasisClient,
    clients: &[Arc<BasisClient>],
    hearing_distance: f32,
) -> Vec<u16> {
    let speaker_position = speaker.current_position().await;
    let mut exclusions = Vec::new();
    for candidate in clients {
        if candidate.index == speaker.index || !candidate.connected.load(Ordering::Relaxed) {
            continue;
        }
        let Some(peer_id) = candidate.remote_peer_id().await else {
            continue;
        };
        let position = candidate.current_position().await;
        if !distance_within(speaker_position, position, hearing_distance) {
            exclusions.push(peer_id);
        }
    }
    exclusions.sort_unstable();
    exclusions.dedup();
    exclusions
}

pub(crate) async fn publish_voice_recipient_exclusions(
    client: &BasisClient,
    exclusions: &[u16],
) -> Result<()> {
    if exclusions.len() <= u8::MAX as usize {
        let payload = serialize_voice_recipients_small(exclusions);
        client
            .send_reliable_ordered(channels::AUDIO_RECIPIENTS_INVERTED, &payload)
            .await
    } else {
        let payload = serialize_voice_recipients_large(exclusions);
        client
            .send_reliable_ordered(channels::AUDIO_RECIPIENTS_INVERTED_LARGE, &payload)
            .await
    }
}
