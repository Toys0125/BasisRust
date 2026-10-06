use crate::avatar::{
    build_movement_packet, parse_server_avatar_metadata, shared_receive_handoff_ready,
    unity_interval_tick_due, PoseState, ServerAvatarMetadata,
};
use crate::client::BasisClient;
use crate::config::{
    resolve_relative_to_config, Config, RawConfig, DEFAULT_VOICE_AUDIO_FOLDER,
    DEFAULT_VOICE_FRAME_DURATION_MS, DEFAULT_VOICE_HEARING_DISTANCE,
};
use crate::identity::Identity;
#[cfg(any(windows, test))]
use crate::net::windows_mtu_probe_reply;
use crate::net::{any_local_addr, bind_udp_socket};
use crate::observer::AvatarObserver;
use crate::observer_session::ObserverSession;
use crate::packet_diagnostics::PacketDiagnostics;
use crate::population::{publish_client_batch, replace_client_if_current};
#[cfg(target_os = "linux")]
use crate::receiver::shared_receive_registration_matches;
#[cfg(target_os = "linux")]
use crate::receiver::shared_receiver_mark_reliable;
#[cfg(target_os = "linux")]
use crate::receiver::shared_receiver_process_merged;
#[cfg(target_os = "linux")]
use crate::receiver::shared_receiver_send_mtu_ok;
use crate::receiver::{ping_bucket_matches, shared_maintenance_loop};
use crate::simulation::{cadence_seed, jittered_duration, worker_phase_offset, SpawnLayout};
use crate::transport::{
    parse_packet, MaintenanceOptions, ReliableReceiveState, ReliableSend, PING_INTERVAL_TICKS,
};
#[cfg(target_os = "linux")]
use crate::transport::{ParsedPacket, LITENETLIB_MAX_MTU};
#[cfg(unix)]
use crate::voice::VoiceLibrary;
use crate::voice::{
    choose_next_speaker, distance_within, opus_packet_duration_ms, serialize_audio_segment,
    serialize_voice_recipients_large, serialize_voice_recipients_small, voice_speaker_target,
    OggOpusPackets,
};
use crate::wire::{avatar_change, build_connection_payload, ready_message};
use basis_protocol::application::NetworkApplication;
use basis_protocol::avatar::{
    encode_avatar_bundle, encode_avatar_network_load_with_version, AvatarBundleItem, BitQuality,
    BitQuality as ProtocolBitQuality,
};
use basis_protocol::avatar_delta::{apply_delta, build_delta};
use basis_protocol::channels;
use basis_protocol::io::{
    NetReader as ProtocolNetReader, NetWriter, NetWriter as ProtocolNetWriter,
};
use basis_protocol::messages::{
    BasisDeserialize, BasisSerialize, ClientMetaDataMessage,
    ClientMetaDataMessage as ProtocolClientMetaDataMessage,
};
use basis_protocol::version::SERVER_VERSION;
use basis_transport::{
    DeliveryMethod, PacketProperty, DEFAULT_WINDOW_SIZE, LITENETLIB_CHANNELED_HEADER_SIZE,
    LITENETLIB_FRAGMENTED_HEADER_SIZE, LITENETLIB_FRAGMENT_HEADER_SIZE, LITENETLIB_INITIAL_MTU,
    MAX_SEQUENCE, RELIABLE_FRAGMENT_PAYLOAD_SIZE,
};
use ed25519_dalek::Verifier;
use std::collections::{HashMap, HashSet, VecDeque};
#[cfg(unix)]
use std::io::Write;
use std::net::SocketAddr;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU8, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, SystemTime};
use tokio::net::UdpSocket;
use tokio::sync::{Mutex, Notify};
use tokio::time;
use uuid::Uuid;

use ed25519_dalek::Signature;
use flate2::read::DeflateDecoder;
use std::io::Read;

#[test]
fn connection_payload_propagates_oversized_login_password() {
    let config = Config {
        password: "p".repeat(65536),
        ..Config::default()
    };
    let ready = ready_message(&config, [0.0; 3]).unwrap();
    let error = build_connection_payload(&config, &ready).unwrap_err();
    assert_eq!(
        error.downcast_ref::<basis_protocol::io::NetWriteError>(),
        Some(&basis_protocol::io::NetWriteError::LengthOverflow {
            length: 65536,
            max: 65535
        })
    );
}

#[test]
fn connection_payload_propagates_oversized_avatar_buffer() {
    let config = Config::default();
    let mut ready = ready_message(&config, [0.0; 3]).unwrap();
    ready.client_avatar_change_message.byte_array = vec![0; 65536];
    let error = build_connection_payload(&config, &ready).unwrap_err();
    assert_eq!(
        error.downcast_ref::<basis_protocol::io::NetWriteError>(),
        Some(&basis_protocol::io::NetWriteError::LengthOverflow {
            length: 65536,
            max: 65535,
        })
    );
}

#[test]
fn avatar_network_load_propagates_oversized_utf8_fields() {
    let oversized = "é".repeat(32768);
    for (url, password) in [(oversized.as_str(), ""), ("", oversized.as_str())] {
        let error = encode_avatar_network_load_with_version(url, password, "").unwrap_err();
        assert_eq!(
            error.downcast_ref::<basis_protocol::io::NetWriteError>(),
            Some(&basis_protocol::io::NetWriteError::LengthOverflow {
                length: 65536,
                max: 65535
            })
        );
    }
}

#[test]
fn shared_receive_handoff_waits_for_valid_avatar_metadata() {
    // Other reliable channels may arrive before META_DATA. They must not enable the shared
    // receiver, which acknowledges but intentionally discards application payloads.
    assert!(!shared_receive_handoff_ready(false, false));
    assert!(!shared_receive_handoff_ready(true, false));

    // The metadata handler publishes readiness only after successful parsing. A malformed or
    // truncated packet therefore leaves the dedicated handler responsible for retries.
    assert!(parse_server_avatar_metadata(&[]).is_err());
    assert!(!shared_receive_handoff_ready(true, false));

    let mut writer = ProtocolNetWriter::new();
    ProtocolClientMetaDataMessage {
        player_uuid: "test-uuid".to_string(),
        player_display_name: "test".to_string(),
        player_platform: "Headless".to_string(),
    }
    .serialize(&mut writer)
    .unwrap();
    writer.put_i32(20);
    writer.put_i32(1);
    writer.put_f32(0.0);
    writer.put_f32(2.5);
    writer.put_i32(1500);
    writer.put_bytes_with_length(&[]).unwrap();
    writer.put_u16(0);
    writer.put_u8(1);
    assert!(parse_server_avatar_metadata(writer.as_slice()).is_ok());
    assert!(shared_receive_handoff_ready(true, true));
    // Repeated valid META_DATA is harmless and retains eligibility.
    assert!(shared_receive_handoff_ready(true, true));
}

#[test]
fn avatar_observer_measures_applied_full_delta_and_bundle_gaps() {
    let mut observer = AvatarObserver::new(40.0, 1, None, Duration::from_secs(1));
    let start = std::time::Instant::now();
    let observer_position = [0.0; 3];
    let mut baseline = vec![0u8; ProtocolBitQuality::High.payload_len()];
    baseline[..4].copy_from_slice(&0.0f32.to_le_bytes());
    let mut full = vec![1, 0, 1];
    full.extend_from_slice(&baseline);
    observer.observe_channel(
        channels::PLAYER_AVATAR_HIGH,
        &full,
        observer_position,
        start,
    );

    let mut changed = baseline.clone();
    changed[20] = 1;
    let delta_body = build_delta(&baseline, &changed, ProtocolBitQuality::High).unwrap();
    let mut delta = vec![3, 1, 0, 2, 1];
    delta.extend_from_slice(&delta_body);
    observer.observe_channel(
        channels::DELTA_AVATAR,
        &delta,
        observer_position,
        start + Duration::from_millis(100),
    );

    let mut bundled_full = vec![1, 0, 3];
    bundled_full.extend_from_slice(&baseline);
    let bundle = encode_avatar_bundle(&[AvatarBundleItem {
        original_channel: channels::PLAYER_AVATAR_HIGH,
        payload: bundled_full,
    }])
    .unwrap();
    observer.observe_channel(
        channels::COMPRESSED_AVATAR_BUNDLE,
        &bundle,
        observer_position,
        start + Duration::from_millis(250),
    );

    let (summary, csv) = observer.summary_and_csv(start + Duration::from_millis(300));
    assert!(
        summary.contains(
            "window_started=true window_ms=300 near_peers=1 expected=1 missing=0 stale_500ms=0"
        ),
        "{summary}"
    );
    assert!(summary.contains("applied_gaps=2 p50_ms=100.00 p95_ms=150.00"));
    assert!(csv.contains("1,3,100.00,150.00"));
    assert!(summary.contains("decode_errors=0 unapplied_deltas=0"));
}

pub(super) fn server_fanout_delta(
    peer_id: u16,
    sequence: u8,
    base_sequence: u8,
    body: &[u8],
) -> Vec<u8> {
    // BasisServerCore::pre_serialize_delta layout: flags, ID, interval, sequence, base, body.
    let large = peer_id > u8::MAX as u16;
    let mut payload = vec![
        ProtocolBitQuality::High as u8
            | if large {
                channels::DELTA_HEADER_LARGE_ID
            } else {
                0
            },
    ];
    if large {
        payload.extend_from_slice(&peer_id.to_le_bytes());
    } else {
        payload.push(peer_id as u8);
    }
    payload.extend_from_slice(&[0, sequence, base_sequence]);
    payload.extend_from_slice(body);
    payload
}

pub(super) fn observer_full_frame(peer_id: u16, sequence: u8, payload: &[u8]) -> (u8, Vec<u8>) {
    let large = peer_id > u8::MAX as u16;
    let mut full = Vec::with_capacity(payload.len() + if large { 5 } else { 4 });
    if large {
        full.extend_from_slice(&peer_id.to_le_bytes());
    } else {
        full.push(peer_id as u8);
    }
    full.push(0); // server-to-client interval byte
    full.push(sequence);
    full.extend_from_slice(payload);
    (
        if large {
            channels::PLAYER_AVATAR_HIGH_LARGE
        } else {
            channels::PLAYER_AVATAR_HIGH
        },
        full,
    )
}

#[test]
fn avatar_observer_fanout_delta_handles_large_ids_wrap_bundles_and_reordering() {
    let mut observer = AvatarObserver::new(40.0, 2, None, Duration::from_secs(1));
    let start = std::time::Instant::now();
    let observer_position = [0.0; 3];
    let baseline = vec![0u8; ProtocolBitQuality::High.payload_len()];
    let mut changed = baseline.clone();
    changed[20] = 1;
    let delta_body = build_delta(&baseline, &changed, ProtocolBitQuality::High).unwrap();

    let (channel, full) = observer_full_frame(7, u8::MAX, &baseline);
    observer.observe_channel(channel, &full, observer_position, start);
    let wrapped_delta = server_fanout_delta(7, 0, u8::MAX, &delta_body);
    let wrapped_bundle = encode_avatar_bundle(&[AvatarBundleItem {
        original_channel: channels::DELTA_AVATAR,
        payload: wrapped_delta,
    }])
    .unwrap();
    observer.observe_channel(
        channels::COMPRESSED_AVATAR_BUNDLE,
        &wrapped_bundle,
        observer_position,
        start + Duration::from_millis(100),
    );
    let old_delta = server_fanout_delta(7, 254, u8::MAX, &delta_body);
    observer.observe_channel(
        channels::DELTA_AVATAR,
        &old_delta,
        observer_position,
        start + Duration::from_millis(150),
    );

    let (channel, large_full) = observer_full_frame(300, 40, &baseline);
    observer.observe_channel(
        channel,
        &large_full,
        observer_position,
        start + Duration::from_millis(200),
    );
    let large_delta = server_fanout_delta(300, 41, 40, &delta_body);
    let large_bundle = encode_avatar_bundle(&[AvatarBundleItem {
        original_channel: channels::DELTA_AVATAR,
        payload: large_delta,
    }])
    .unwrap();
    observer.observe_channel(
        channels::COMPRESSED_AVATAR_BUNDLE,
        &large_bundle,
        observer_position,
        start + Duration::from_millis(300),
    );

    let (summary, csv) = observer.summary_and_csv(start + Duration::from_millis(400));
    assert!(
        summary.contains("near_peers=2 expected=2 missing=0"),
        "{summary}"
    );
    assert!(summary.contains("applied_gaps=2"), "{summary}");
    assert!(
        summary.contains("decode_errors=0 unapplied_deltas=0"),
        "{summary}"
    );
    assert!(csv.contains("accepted_avatar_items,4"), "{csv}");
    assert!(csv.contains("non_newer_sequences,1"), "{csv}");
    assert!(csv.contains("decoded_delta_items,3"), "{csv}");
}

#[test]
fn avatar_observer_marks_missing_baseline_then_recovers_on_keyframe() {
    let mut observer = AvatarObserver::new(40.0, 1, None, Duration::from_secs(1));
    let start = std::time::Instant::now();
    let observer_position = [0.0; 3];
    let baseline = vec![0u8; ProtocolBitQuality::High.payload_len()];
    let mut changed = baseline.clone();
    changed[20] = 1;
    let delta_body = build_delta(&baseline, &changed, ProtocolBitQuality::High).unwrap();

    let missing_delta = server_fanout_delta(9, 2, 1, &delta_body);
    observer.observe_channel(
        channels::DELTA_AVATAR,
        &missing_delta,
        observer_position,
        start,
    );
    let (channel, full) = observer_full_frame(9, 1, &baseline);
    observer.observe_channel(
        channel,
        &full,
        observer_position,
        start + Duration::from_millis(50),
    );
    let recovered_delta = server_fanout_delta(9, 2, 1, &delta_body);
    observer.observe_channel(
        channels::DELTA_AVATAR,
        &recovered_delta,
        observer_position,
        start + Duration::from_millis(100),
    );

    let (summary, csv) = observer.summary_and_csv(start + Duration::from_millis(150));
    assert!(
        summary.contains("near_peers=1 expected=1 missing=0"),
        "{summary}"
    );
    assert!(summary.contains("applied_gaps=1"), "{summary}");
    assert!(
        summary.contains("decode_errors=0 unapplied_deltas=1"),
        "{summary}"
    );
    assert!(csv.contains("accepted_avatar_items,2"), "{csv}");
}

#[test]
fn avatar_observer_recovers_across_loss_reordering_wrap_and_window_reset() {
    let mut observer = AvatarObserver::new(40.0, 1, None, Duration::from_secs(1));
    let start = std::time::Instant::now();
    let observer_position = [0.0; 3];
    let baseline = vec![0u8; ProtocolBitQuality::High.payload_len()];
    let mut changed = baseline.clone();
    changed[20] = 1;
    let delta_body = build_delta(&baseline, &changed, ProtocolBitQuality::High).unwrap();

    // A delta before any keyframe is rejected as unapplied, then startup recovers on full.
    observer.observe_channel(
        channels::DELTA_AVATAR,
        &server_fanout_delta(1481, 2, 1, &delta_body),
        observer_position,
        start,
    );
    let (channel, full) = observer_full_frame(1481, 254, &baseline);
    observer.observe_channel(
        channel,
        &full,
        observer_position,
        start + Duration::from_millis(10),
    );

    // Sequence 255 is deliberately lost. The next keyframe-relative delta wraps to 0.
    observer.observe_channel(
        channels::DELTA_AVATAR,
        &server_fanout_delta(1481, 0, 254, &delta_body),
        observer_position,
        start + Duration::from_millis(100),
    );
    observer.observe_channel(
        channels::DELTA_AVATAR,
        &server_fanout_delta(1481, 255, 254, &delta_body),
        observer_position,
        start + Duration::from_millis(150),
    );

    // Reset cadence collection while retaining decode state, as the benchmark marker does.
    observer.begin_window(observer_position, start + Duration::from_millis(200));
    observer.observe_channel(
        channels::DELTA_AVATAR,
        &server_fanout_delta(1481, 1, 254, &delta_body),
        observer_position,
        start + Duration::from_millis(250),
    );

    // A delta naming a missing keyframe is unapplied; the next full frame restores baseline.
    observer.observe_channel(
        channels::DELTA_AVATAR,
        &server_fanout_delta(1481, 2, 77, &delta_body),
        observer_position,
        start + Duration::from_millis(300),
    );
    let (channel, recovered_full) = observer_full_frame(1481, 2, &changed);
    observer.observe_channel(
        channel,
        &recovered_full,
        observer_position,
        start + Duration::from_millis(350),
    );
    observer.observe_channel(
        channels::DELTA_AVATAR,
        &server_fanout_delta(1481, 3, 2, &baseline),
        observer_position,
        start + Duration::from_millis(400),
    );

    let (summary, csv) = observer.summary_and_csv(start + Duration::from_millis(450));
    assert!(
        summary.contains("near_peers=1 expected=1 missing=0"),
        "{summary}"
    );
    assert!(
            summary.contains("applied_full=1 applied_delta=2 malformed=0 decode_errors=0 unapplied_deltas=1 non_newer_sequences=0"),
            "{summary}"
        );
    assert!(csv.contains("non_newer_sequences,0"), "{csv}");
    assert!(csv.contains("1481,3,"), "{csv}");
    assert!(csv.contains("accepted_avatar_items,3"), "{csv}");
    assert_eq!(observer.peers.get(&1481).unwrap().last_sequence, Some(3));
}

#[test]
fn rust_delta_roundtrips_the_unity_shared_csharp_codec_fixture() {
    // Golden generated by BasisNetworkCore's pure C# High payload and delta codec; see
    // BasisRustClient/testdata/unity-shared-codec-fixture.json and the ignored generator in
    // artifacts/perf-1500/csharp-fixture. This validates codec parity, not a captured Unity pose.
    let fixture = include_str!("../../../testdata/unity-shared-codec-fixture.json");
    let decode_hex = |name: &str| {
        let prefix = format!("\"{name}\": \"");
        let text = fixture
            .lines()
            .find_map(|line| {
                line.trim()
                    .strip_prefix(&prefix)
                    .and_then(|value| value.strip_suffix("\","))
            })
            .expect("fixture hex string");
        (0..text.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&text[index..index + 2], 16).unwrap())
            .collect::<Vec<_>>()
    };
    let baseline = decode_hex("keyframe_hex");
    let current = decode_hex("current_hex");
    let csharp_delta = decode_hex("delta_hex");

    let (decoded, body_len) =
        apply_delta(&baseline, &csharp_delta, ProtocolBitQuality::High).unwrap();
    assert_eq!(decoded, current);
    assert_eq!(body_len, csharp_delta.len());
    assert_eq!(
        build_delta(&baseline, &current, ProtocolBitQuality::High).unwrap(),
        csharp_delta,
        "Rust and shared C# sources should emit the same High delta bytes"
    );
}

pub(super) async fn test_client(index: usize, server_addr: SocketAddr) -> Arc<BasisClient> {
    let socket = bind_udp_socket(any_local_addr(server_addr)).unwrap();
    socket.connect(server_addr).await.unwrap();
    Arc::new(BasisClient {
        index,
        socket: Arc::new(socket),
        server_addr,
        connect_time: 0,
        connection_number: 0,
        local_peer_id: index as i32,
        remote_peer_id: Mutex::new(None),
        connected: AtomicBool::new(true),
        in_use: AtomicBool::new(true),
        intentional_reconnect: AtomicBool::new(false),
        movement_sequence: AtomicU8::new(0),
        voice_sequence: AtomicU8::new(0),
        reliable_sequences: std::array::from_fn(|_| AtomicU16::new(0)),
        fragment_id: AtomicU16::new(0),
        ping_sequence: AtomicU16::new(0),
        pending_reliable: Mutex::new(VecDeque::new()),
        pending_reliable_active: AtomicBool::new(false),
        ack_pending: AtomicBool::new(false),
        shared_receive: AtomicBool::new(false),
        shared_receive_eligible: AtomicBool::new(false),
        receive_shutdown: Notify::new(),
        received_reliable: StdMutex::new(ReliableReceiveState::default()),
        server_avatar_metadata: StdMutex::new(None),
        force_avatar_keyframe: AtomicBool::new(false),
        pose: Mutex::new(PoseState::new_at([0.0; 3])),
        avatar_observer: None,
        packet_diagnostics: PacketDiagnostics::default(),
        avatar_diagnostics: None,
        identity: Identity::random(),
    })
}

#[test]
fn connection_payload_starts_with_v55_application_auth_and_ready() {
    let config = Config::default();
    let ready = ready_message(&config, [0.0, 0.0, 0.0]).unwrap();
    let payload = build_connection_payload(&config, &ready).unwrap();
    assert_eq!(SERVER_VERSION, 55);
    assert_eq!(&payload[0..2], &SERVER_VERSION.to_le_bytes());
    assert_eq!(payload[2], 1);
    let auth_len = u16::from_le_bytes([payload[3], payload[4]]) as usize;
    assert_eq!(&payload[5..5 + auth_len], b"default_password");
    assert!(payload.len() > 5 + auth_len);
}

#[test]
fn connection_payload_supports_raw_application_names() {
    let config = Config {
        company_name: "Custom Company".to_string(),
        product_name: "Custom Product".to_string(),
        ..Config::default()
    };
    let ready = ready_message(&config, [0.0, 0.0, 0.0]).unwrap();
    let payload = build_connection_payload(&config, &ready).unwrap();

    let mut reader = ProtocolNetReader::new(&payload);
    assert_eq!(reader.get_u16().unwrap(), SERVER_VERSION);
    let application = NetworkApplication::try_read(&mut reader).unwrap();
    assert_eq!(application.company_name, "Custom Company");
    assert_eq!(application.product_name, "Custom Product");
    assert_eq!(reader.get_bytes_with_length().unwrap(), b"default_password");
}

#[test]
fn voice_config_defaults_are_disabled_and_conservative() {
    let config = Config::default();
    assert!(!config.voice_enabled);
    assert_eq!(config.voice_audio_folder, "audio");
    assert_eq!(config.voice_speaker_percent, 10);
    assert_eq!(config.voice_hearing_distance, 25.0);
    assert_eq!(config.voice_frame_duration_ms, 20);
}

#[test]
fn voice_config_parses_xml_fields() {
    let raw = quick_xml::de::from_str::<RawConfig>(
        r#"<Configuration>
                <VoiceEnabled>true</VoiceEnabled>
                <VoiceAudioFolder>samples</VoiceAudioFolder>
                <VoiceSpeakerPercent>35</VoiceSpeakerPercent>
                <VoiceHearingDistance>12.5</VoiceHearingDistance>
                <VoiceFrameDurationMs>40</VoiceFrameDurationMs>
            </Configuration>"#,
    )
    .unwrap();
    let config = Config::from_raw(raw);
    assert!(config.voice_enabled);
    assert_eq!(config.voice_audio_folder, "samples");
    assert_eq!(config.voice_speaker_percent, 35);
    assert_eq!(config.voice_hearing_distance, 12.5);
    assert_eq!(config.voice_frame_duration_ms, 40);
}

#[test]
fn invalid_voice_config_values_fall_back_or_clamp() {
    let config = Config::from_raw(RawConfig {
        voice_speaker_percent: Some("250".to_string()),
        voice_hearing_distance: Some("NaN".to_string()),
        voice_frame_duration_ms: Some("0".to_string()),
        ..RawConfig::default()
    });
    assert_eq!(config.voice_speaker_percent, 100);
    assert_eq!(
        config.voice_hearing_distance,
        DEFAULT_VOICE_HEARING_DISTANCE
    );
    assert_eq!(
        config.voice_frame_duration_ms,
        DEFAULT_VOICE_FRAME_DURATION_MS
    );
}

#[test]
fn relative_voice_audio_folder_resolves_from_config_directory() {
    let config_path = if cfg!(windows) {
        Path::new(r"C:\work\BasisRustClient\Config.xml")
    } else {
        Path::new("/work/BasisRustClient/Config.xml")
    };
    let resolved = resolve_relative_to_config(config_path, DEFAULT_VOICE_AUDIO_FOLDER);
    assert!(Path::new(&resolved).ends_with(Path::new("BasisRustClient").join("audio")));

    let voice_path = if cfg!(windows) {
        r"C:\samples\voice"
    } else {
        "/samples/voice"
    };
    let absolute = resolve_relative_to_config(config_path, voice_path);
    assert_eq!(absolute, voice_path);
}

#[test]
fn voice_recipient_serialization_uses_expected_wire_formats() {
    assert_eq!(
        serialize_voice_recipients_small(&[7, 300]),
        vec![2, 7, 0, 44, 1]
    );

    let large = serialize_voice_recipients_large(&[7, 300]);
    assert_eq!(large, vec![2, 0, 7, 0, 44, 1]);
}

#[test]
fn audio_segment_serialization_matches_unity_wire_header() {
    assert_eq!(
        serialize_audio_segment(9, &[0xaa, 0xbb, 0xcc]),
        vec![9, 0, 0xaa, 0xbb, 0xcc]
    );
}

#[test]
fn voice_distance_filter_includes_boundary() {
    assert!(distance_within([0.0, 0.0, 0.0], [25.0, 0.0, 0.0], 25.0));
    assert!(!distance_within([0.0, 0.0, 0.0], [25.01, 0.0, 0.0], 25.0));
}

#[test]
fn ogg_opus_parser_skips_headers_and_extracts_audio_packets() {
    let mut bytes = Vec::new();
    bytes.extend(build_ogg_page(&[
        b"OpusHead".as_slice(),
        b"OpusTags".as_slice(),
        b"abc".as_slice(),
    ]));
    let packets = OggOpusPackets::parse(&bytes).unwrap();
    assert_eq!(packets.packets[0].data, b"abc".to_vec());
}

#[cfg(unix)]
struct FakeVoiceEncoder {
    root: PathBuf,
    audio: PathBuf,
    executable: PathBuf,
    inputs: Vec<PathBuf>,
}

#[cfg(unix)]
impl FakeVoiceEncoder {
    fn new() -> Self {
        use std::os::unix::fs::PermissionsExt;
        let root = std::env::temp_dir().join(format!("basis-voice-test-{}", Uuid::new_v4()));
        let audio = root.join("audio");
        std::fs::create_dir_all(&audio).unwrap();
        let executable = root.join("ffmpeg");
        // Record each process and output for cancellation checks, and gate completion.
        std::fs::write(
            &executable,
            r#"#!/bin/sh
want_input=0
for argument do
    if [ "$want_input" = 1 ]; then input="$argument"; want_input=0; fi
    if [ "$argument" = '-i' ]; then want_input=1; fi
    output="$argument"
done
: > "$output"
printf '%s\n%s\n' "$$" "$output" > "$input.started"
printf 'out_time=00:00:01.000000\nspeed=1x\nprogress=continue\n'
while [ ! -f "$input.release" ]; do sleep 0.01; done
cat "$input" > "$output"
printf 'out_time=00:00:02.000000\nspeed=2x\nprogress=end\n'
"#,
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        let inputs = (0..5)
            .map(|index| {
                let input = audio.join(format!("{index}.opus"));
                std::fs::write(&input, build_ogg_page(&[&[0x08, index]])).unwrap();
                input
            })
            .collect();
        Self {
            root,
            audio,
            executable,
            inputs,
        }
    }

    fn started(&self) -> Vec<(i32, PathBuf)> {
        self.inputs
            .iter()
            .filter_map(|input| {
                let marker = std::fs::read_to_string(input.with_extension("opus.started")).ok()?;
                let mut lines = marker.lines();
                Some((lines.next()?.parse().ok()?, PathBuf::from(lines.next()?)))
            })
            .collect()
    }

    async fn wait_for_parallel_jobs(&self) {
        time::timeout(Duration::from_secs(3), async {
            while self.started().len() < 2 {
                time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("two encoders should start before either finishes");
        assert_eq!(
            self.started().len(),
            2,
            "parallel job limit must be respected"
        );
    }
}

#[cfg(unix)]
impl Drop for FakeVoiceEncoder {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[cfg(unix)]
#[tokio::test]
async fn voice_reencode_runs_in_parallel_and_loads_all_clips() {
    let encoder = FakeVoiceEncoder::new();
    let shutdown = Arc::new(AtomicBool::new(false));
    let load = VoiceLibrary::load_with_options(
        encoder.audio.to_str().unwrap(),
        true,
        20,
        &shutdown,
        &encoder.executable,
        2,
    );
    let release = async {
        encoder.wait_for_parallel_jobs().await;
        for input in &encoder.inputs {
            std::fs::write(input.with_extension("opus.release"), b"").unwrap();
        }
    };
    let (library, ()) = time::timeout(Duration::from_secs(5), async {
        tokio::join!(load, release)
    })
    .await
    .unwrap();
    let library = library.unwrap().unwrap();
    assert_eq!(library.clips.len(), encoder.inputs.len());
    for (clip, input) in library.clips.iter().zip(&encoder.inputs) {
        assert_eq!(&clip.path, input);
        assert_eq!(clip.packets.packets.len(), 1);
    }
    assert_eq!(encoder.started().len(), encoder.inputs.len());
    assert!(encoder.started().iter().all(|(_, output)| !output.exists()));
}

#[cfg(unix)]
#[tokio::test]
async fn voice_reencode_cancellation_kills_active_jobs_and_skips_queue() {
    let encoder = FakeVoiceEncoder::new();
    let shutdown = Arc::new(AtomicBool::new(false));
    let load = VoiceLibrary::load_with_options(
        encoder.audio.to_str().unwrap(),
        true,
        20,
        &shutdown,
        &encoder.executable,
        2,
    );
    let cancel = async {
        encoder.wait_for_parallel_jobs().await;
        shutdown.store(true, Ordering::SeqCst);
    };
    let (library, ()) = time::timeout(Duration::from_secs(5), async { tokio::join!(load, cancel) })
        .await
        .unwrap();
    assert!(library.unwrap().is_none());
    let started = encoder.started();
    assert_eq!(
        started.len(),
        2,
        "queued files must not start after cancellation"
    );
    for (pid, output) in started {
        assert!(!output.exists(), "cancelled output should be removed");
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1, "encoder must be reaped");
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
    }
}

#[cfg(unix)]
#[tokio::test]
async fn voice_file_loading_keeps_shutdown_responsive_on_one_worker() {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let encoder = FakeVoiceEncoder::new();
    let fifo = encoder.root.join("blocked.opus");
    let fifo_path = CString::new(fifo.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(fifo_path.as_ptr(), 0o600) }, 0);
    let shutdown = Arc::new(AtomicBool::new(false));
    let writer_shutdown = shutdown.clone();
    let ready = Arc::new(Notify::new());
    let writer_ready = ready.clone();
    let writer = std::thread::spawn(move || {
        let mut file = std::fs::OpenOptions::new().write(true).open(&fifo).unwrap();
        writer_ready.notify_one();
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while !writer_shutdown.load(Ordering::Relaxed) && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        // Always release the reader, including if a regression blocks the async worker.
        file.write_all(&build_ogg_page(&[&[0x08]])).unwrap();
    });
    let fifo = encoder.root.join("blocked.opus");
    let load = OggOpusPackets::load(&fifo, &shutdown);
    let cancel = async {
        ready.notified().await;
        shutdown.store(true, Ordering::SeqCst);
    };
    let (result, ()) = tokio::join!(load, cancel);
    writer.join().unwrap();
    assert!(format!("{:#}", result.unwrap_err()).contains("voice audio loading cancelled"));
    assert!(shutdown.load(Ordering::Relaxed));
}

#[cfg(unix)]
#[tokio::test]
async fn voice_reencode_reports_final_progress_for_short_clips() {
    // Tracing callsite registration in concurrent tests can suppress captured events.
    // Isolate this logging assertion while keeping the subscriber scoped to the test.
    const ISOLATED: &str = "BASIS_VOICE_PROGRESS_TEST_ISOLATED";
    if std::env::var_os(ISOLATED).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::voice_reencode_reports_final_progress_for_short_clips",
                "--nocapture",
            ])
            .env(ISOLATED, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    #[derive(Clone)]
    struct CapturedLog(Arc<StdMutex<Vec<u8>>>);

    impl Write for CapturedLog {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let encoder = FakeVoiceEncoder::new();
    let input = &encoder.inputs[0];
    std::fs::write(input.with_extension("opus.release"), b"").unwrap();
    let captured = CapturedLog(Arc::new(StdMutex::new(Vec::new())));
    let log_writer = captured.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_writer(move || log_writer.clone())
        .finish();
    let _subscriber = tracing::subscriber::set_default(subscriber);
    // Other tests may register these callsites without a subscriber before this test runs.
    tracing::callsite::rebuild_interest_cache();
    let shutdown = Arc::new(AtomicBool::new(false));
    let packets = OggOpusPackets::load_reencoded(input, 20, &shutdown, &encoder.executable)
        .await
        .unwrap();
    assert_eq!(packets.packets.len(), 1);
    let log = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
    let expected = format!(
        "voice re-encoding progress {}: audio_time=00:00:02.000000 speed=2x",
        input.display()
    );
    assert!(log.contains(&expected), "{log}");
}

#[test]
fn ogg_opus_parser_reconstructs_packet_across_pages() {
    let mut bytes = Vec::new();
    bytes.extend(build_ogg_page_from_segments(&[255], &[1; 255]));
    bytes.extend(build_ogg_page_from_segments(&[3], &[2, 3, 4]));
    let packets = OggOpusPackets::parse(&bytes).unwrap();
    let mut expected = vec![1; 255];
    expected.extend_from_slice(&[2, 3, 4]);
    assert_eq!(packets.packets[0].data, expected);
}

#[test]
fn ogg_opus_parser_rejects_malformed_pages() {
    assert!(OggOpusPackets::parse(b"not ogg").is_err());
}

#[test]
fn opus_packet_duration_parses_common_frame_sizes() {
    assert_eq!(opus_packet_duration_ms(&[0x08]), Some(20));
    assert_eq!(opus_packet_duration_ms(&[0x10]), Some(40));
    assert_eq!(opus_packet_duration_ms(&[0x18]), Some(60));
    assert_eq!(opus_packet_duration_ms(&[0x09]), Some(40));
    assert_eq!(opus_packet_duration_ms(&[0x0b, 0x03]), Some(60));
    assert_eq!(opus_packet_duration_ms(&[0xf8]), Some(20));
}

#[test]
fn voice_speaker_target_uses_ceil_and_clamping() {
    assert_eq!(voice_speaker_target(250, 10), 25);
    assert_eq!(voice_speaker_target(1, 10), 1);
    assert_eq!(voice_speaker_target(250, 0), 0);
    assert_eq!(voice_speaker_target(3, 250), 3);
}

#[test]
fn eof_replacement_prefers_another_peer_when_available() {
    let connected = [1, 2, 3];
    let active = HashSet::new();
    let avoid = HashSet::from([1]);
    let replacement = choose_next_speaker(&connected, &active, &avoid).unwrap();
    assert_ne!(replacement, 1);
}

#[test]
fn metadata_empty_fields_serialize_as_failure() {
    let mut writer = NetWriter::default();
    ClientMetaDataMessage {
        player_uuid: String::new(),
        player_display_name: String::new(),
        player_platform: String::new(),
    }
    .serialize(&mut writer)
    .unwrap();
    let bytes = writer.into_vec();
    let mut reader = ProtocolNetReader::new(&bytes);
    let decoded = ProtocolClientMetaDataMessage::deserialize(&mut reader).unwrap();
    assert_eq!(decoded.player_uuid, "Failure");
    assert_eq!(decoded.player_display_name, "Failure");
    assert_eq!(decoded.player_platform, "Failure");
}

#[test]
fn avatar_network_load_deflates_raw_len_strings() {
    let encoded =
        encode_avatar_network_load_with_version("http://localhost/avatar", "pw", "").unwrap();
    let mut decoder = DeflateDecoder::new(encoded.as_slice());
    let mut raw = Vec::new();
    decoder.read_to_end(&mut raw).unwrap();
    let (url, next) = read_raw_len_string(&raw, 0);
    let (pw, next) = read_raw_len_string(&raw, next);
    let (version_tag, _) = read_raw_len_string(&raw, next);
    assert_eq!(url, "http://localhost/avatar");
    assert_eq!(pw, "pw");
    assert_eq!(version_tag, "");
}

#[test]
fn default_avatar_is_basis_loading_avatar() {
    let config = Config::default();
    let message = avatar_change(&config).unwrap();
    assert_eq!(message.load_mode, 1);

    let mut decoder = DeflateDecoder::new(message.byte_array.as_slice());
    let mut raw = Vec::new();
    decoder.read_to_end(&mut raw).unwrap();
    let (url, next) = read_raw_len_string(&raw, 0);
    let (unlock_password, next) = read_raw_len_string(&raw, next);
    let (version_tag, _) = read_raw_len_string(&raw, next);
    assert_eq!(url, "LoadingAvatar");
    assert_eq!(unlock_password, "N/A");
    assert_eq!(version_tag, "");
}

#[test]
fn avatar_change_uses_avatar_password_not_login_password() {
    let config = Config {
        password: "server-login-password".to_string(),
        avatar_password: "avatar-unlock-password".to_string(),
        ..Config::default()
    };
    let message = avatar_change(&config).unwrap();
    let mut decoder = DeflateDecoder::new(message.byte_array.as_slice());
    let mut raw = Vec::new();
    decoder.read_to_end(&mut raw).unwrap();
    let (_, next) = read_raw_len_string(&raw, 0);
    let (unlock_password, _) = read_raw_len_string(&raw, next);
    assert_eq!(unlock_password, "avatar-unlock-password");
    assert_ne!(unlock_password, "server-login-password");
}

#[test]
fn unity_frame_accumulator_quantizes_20ms_to_about_50hz_at_60fps() {
    let mut accumulator = 0.0;
    let mut due_frames = Vec::new();
    for frame in 0..300 {
        if unity_interval_tick_due(&mut accumulator, 1.0 / 60.0, 0.020) {
            due_frames.push(frame);
        }
    }
    assert!(
        (249..=250).contains(&due_frames.len()),
        "{} ticks",
        due_frames.len()
    );
    assert!(due_frames
        .windows(2)
        .all(|pair| pair[1] - pair[0] == 1 || pair[1] - pair[0] == 2));
    assert!(due_frames.windows(2).any(|pair| pair[1] - pair[0] == 2));
}

#[test]
fn unity_server_metadata_parser_reads_interval_and_delta_negotiation() {
    let mut writer = ProtocolNetWriter::new();
    ProtocolClientMetaDataMessage {
        player_uuid: "test-uuid".to_string(),
        player_display_name: "test".to_string(),
        player_platform: "Headless".to_string(),
    }
    .serialize(&mut writer)
    .unwrap();
    writer.put_i32(20);
    writer.put_i32(1);
    writer.put_f32(0.0);
    writer.put_f32(2.5);
    writer.put_i32(1500);
    writer.put_bytes_with_length(&[]).unwrap();
    writer.put_u16(0);
    writer.put_u8(1);
    let metadata = parse_server_avatar_metadata(writer.as_slice()).unwrap();
    assert_eq!(metadata.sync_interval_ms, 20);
    assert_eq!(metadata.base_multiplier, 1.0);
    assert_eq!(metadata.increase_rate, 0.0);
    assert_eq!(metadata.slowest_send_rate_secs, 2.5);
    assert!(metadata.uplink_delta_enabled);
}

#[test]
fn unity_policy_emits_high_keyframe_then_decodable_delta_and_periodic_keyframe() {
    let metadata = ServerAvatarMetadata {
        sync_interval_ms: 20,
        base_multiplier: 1.0,
        increase_rate: 0.0,
        slowest_send_rate_secs: 2.5,
        uplink_delta_enabled: true,
    };
    let frame_delta = 1.0 / 60.0;
    let mut pose = PoseState::new_at([0.0; 3]);
    let mut packets = Vec::new();
    for frame in 0..60 {
        let elapsed = (frame + 1) as f64 * frame_delta;
        if let Some(datagram) = pose.write_unity_avatar_datagram(
            frame_delta,
            elapsed,
            metadata,
            false,
            20.0_f32.to_radians(),
        ) {
            packets.push((
                elapsed,
                datagram.to_vec(),
                pose.unity.current_payload.clone(),
            ));
        }
    }
    assert!(packets.len() >= 25, "sent {} packets", packets.len());
    assert_eq!(packets[0].1[1], channels::PLAYER_AVATAR_HIGH);
    assert_eq!(packets[0].1.len(), 3 + BitQuality::High.payload_len());
    let mut baseline = packets[0].1[3..].to_vec();
    let mut baseline_sequence = packets[0].1[2];
    assert!(packets
        .iter()
        .any(|(_, packet, _)| packet[1] == channels::DELTA_AVATAR));
    for (_, packet, expected) in packets.iter().skip(1) {
        if packet[1] == channels::PLAYER_AVATAR_HIGH {
            baseline = packet[3..].to_vec();
            baseline_sequence = packet[2];
            assert_eq!(&baseline, expected);
            continue;
        }
        assert_eq!(packet[1], channels::DELTA_AVATAR);
        assert_eq!(packet[2], ProtocolBitQuality::High as u8);
        assert_eq!(packet[4], baseline_sequence);
        let (reconstructed, _) =
            apply_delta(&baseline, &packet[5..], ProtocolBitQuality::High).unwrap();
        assert_eq!(&reconstructed, expected);
    }
    let later_keyframe = packets
        .iter()
        .find(|(elapsed, packet, _)| packet[1] == channels::PLAYER_AVATAR_HIGH && *elapsed >= 0.53)
        .expect("500ms keyframe deadline should emit a new keyframe");
    assert!(later_keyframe.0 >= 0.53);
}

#[test]
fn unity_policy_falls_back_to_keyframes_and_honors_forced_rekey() {
    let mut metadata = ServerAvatarMetadata {
        sync_interval_ms: 20,
        base_multiplier: 1.0,
        increase_rate: 0.0,
        slowest_send_rate_secs: 2.5,
        uplink_delta_enabled: false,
    };
    let mut pose = PoseState::new_at([0.0; 3]);
    for frame in 0..8 {
        if let Some(packet) = pose.write_unity_avatar_datagram(
            1.0 / 60.0,
            (frame + 1) as f64 / 60.0,
            metadata,
            false,
            20.0_f32.to_radians(),
        ) {
            assert_eq!(packet[1], channels::PLAYER_AVATAR_HIGH);
        }
    }
    metadata.uplink_delta_enabled = true;
    pose = PoseState::new_at([0.0; 3]);
    let mut first_keyframe = None;
    for frame in 0..4 {
        if let Some(packet) = pose.write_unity_avatar_datagram(
            1.0 / 60.0,
            (frame + 1) as f64 / 60.0,
            metadata,
            false,
            20.0_f32.to_radians(),
        ) {
            first_keyframe = Some(packet.to_vec());
            break;
        }
    }
    assert_eq!(first_keyframe.unwrap()[1], channels::PLAYER_AVATAR_HIGH);
    for frame in 4..7 {
        let packet = pose.write_unity_avatar_datagram(
            1.0 / 60.0,
            (frame + 1) as f64 / 60.0,
            metadata,
            true,
            20.0_f32.to_radians(),
        );
        if let Some(packet) = packet {
            assert_eq!(packet[1], channels::PLAYER_AVATAR_HIGH);
            return;
        }
    }
    panic!("forced rekey did not produce an eligible keyframe");
}

#[test]
fn unity_policy_suppresses_idle_pose_until_five_second_heartbeat() {
    let metadata = ServerAvatarMetadata {
        sync_interval_ms: 20,
        base_multiplier: 1.0,
        increase_rate: 0.0,
        slowest_send_rate_secs: 2.5,
        uplink_delta_enabled: true,
    };
    let mut pose = PoseState::new_at([0.0; 3]);
    let mut send_times = Vec::new();
    for frame in 0..360 {
        let elapsed = (frame + 1) as f64 / 60.0;
        if let Some(datagram) =
            pose.write_unity_avatar_datagram(1.0 / 60.0, elapsed, metadata, false, 0.0)
        {
            send_times.push(elapsed);
            assert_eq!(datagram[1], channels::PLAYER_AVATAR_HIGH);
        }
    }
    assert_eq!(send_times.len(), 2, "heartbeat sends at {:?}", send_times);
    assert!(send_times[1] >= 5.0);
    assert!(send_times[1] - send_times[0] <= 5.1);
}

#[test]
fn colocated_layout_keeps_all_spawn_positions_at_origin() {
    let layout = SpawnLayout::new(10, 100.0).with_no_spread(true);
    for index in [0, 1, 9, 10, 1499] {
        assert_eq!(layout.base_for_client(index), [0.0; 3]);
    }
}

#[test]
fn synchronized_worker_phases_are_evenly_spaced_within_the_interval() {
    let interval = Duration::from_millis(20);
    let offsets: Vec<_> = (0..4)
        .map(|worker| worker_phase_offset(interval, worker, 4))
        .collect();
    assert_eq!(
        offsets,
        [
            Duration::ZERO,
            Duration::from_millis(5),
            Duration::from_millis(10),
            Duration::from_millis(15),
        ]
    );
    assert!(offsets.iter().all(|offset| *offset < interval));
}

#[test]
fn no_spread_movement_suppresses_positional_random_walk() {
    let mut pose = PoseState::new_at([0.0; 3]);
    let start = SystemTime::now();
    for sequence in 0..100 {
        pose.write_movement_datagram(sequence, start, false);
    }
    assert_eq!(pose.position(), [0.0; 3]);
}

#[test]
fn cadence_jitter_is_bounded_and_preserves_long_run_mean() {
    let base = Duration::from_millis(50);
    let mut state = cadence_seed(42, 0x4d4f_5645_4d45_4e54);
    let mut total_us = 0u128;
    for _ in 0..100_000 {
        let interval = jittered_duration(base, 10, &mut state);
        let us = interval.as_micros();
        assert!((45_000..=55_000).contains(&us));
        total_us += us;
    }
    let mean_us = total_us as f64 / 100_000.0;
    assert!((49_900.0..=50_100.0).contains(&mean_us));
}

#[test]
fn high_quality_payload_and_movement_packet_sizes_match() {
    let mut pose = PoseState::new_random();
    let payload = pose.high_quality_payload(0.0);
    assert_eq!(payload.len(), 159);
    assert_eq!(u16::from_le_bytes([payload[103], payload[104]]), 0x4000);
    assert_eq!(&payload[112..117], &[0, 0, 0, 0, 0]);
    assert_eq!(&payload[124..159], &[0; 35]);
    let packet = build_movement_packet(7, &mut pose, SystemTime::now());
    assert_eq!(packet.len(), 160);
    assert_eq!(packet[0], 7);
}

#[test]
fn spawn_layout_offsets_groups_by_spacing() {
    let layout = SpawnLayout::new(3, 1000.0);
    let first = layout.base_for_client(0);
    let same_group = layout.base_for_client(2);
    let next_group = layout.base_for_client(3);
    let later_group = layout.base_for_client(8);

    assert!(first[0].abs() <= 0.25);
    assert!(same_group[0].abs() <= 0.25);
    assert!((999.75..=1000.25).contains(&next_group[0]));
    assert!((1999.75..=2000.25).contains(&later_group[0]));
}

#[test]
fn fixed_spawn_layout_places_exact_groups_in_distance_bands() {
    let layout = SpawnLayout::new(150, 15.0).with_fixed_positions(true);
    for group in 0..4 {
        let first = group * 150;
        assert_eq!(
            layout.base_for_client(first),
            [group as f32 * 15.0, 0.0, 0.0]
        );
        assert_eq!(
            layout.base_for_client(first + 149),
            [group as f32 * 15.0, 0.0, 0.0]
        );
    }
}

#[test]
fn initial_ready_pose_uses_spawn_base() {
    let ready = ready_message(&Config::default(), [1000.0, 2.0, -3.0]).unwrap();
    let payload = &ready.local_avatar_sync_message.array;
    let decode_axis = |bytes: &[u8]| {
        let raw = (bytes[0] as i32) | ((bytes[1] as i32) << 8) | ((bytes[2] as i32) << 16);
        ((raw << 8) >> 8) as f32 * 0.001
    };
    let x = decode_axis(&payload[0..3]);
    let y = decode_axis(&payload[3..6]);
    let z = decode_axis(&payload[6..9]);

    assert!((999.75..=1000.25).contains(&x));
    assert!((1.75..=2.25).contains(&y));
    assert!((-3.25..=-2.75).contains(&z));
}

#[test]
fn sequence_wraps_from_255_to_0() {
    let seq = AtomicU8::new(255);
    assert_eq!(seq.fetch_add(1, Ordering::SeqCst), 255);
    assert_eq!(seq.fetch_add(1, Ordering::SeqCst), 0);
}

#[test]
fn shared_ping_buckets_visit_each_population_once_per_interval() {
    for population in [1, 14, 15, 1000] {
        let mut visits = vec![0usize; population];
        let mut bucket_total = 0;
        for tick in 0..PING_INTERVAL_TICKS {
            let bucket_count = (0..population)
                .filter(|slot| ping_bucket_matches(*slot, tick))
                .count();
            assert!(bucket_count <= population.div_ceil(PING_INTERVAL_TICKS));
            bucket_total += bucket_count;
            for (slot, visits) in visits.iter_mut().enumerate() {
                if ping_bucket_matches(slot, tick) {
                    *visits += 1;
                }
            }
        }
        assert_eq!(bucket_total, population);
        assert!(visits.iter().all(|visits| *visits == 1));
    }
}

#[test]
fn reliable_receive_suppresses_duplicate_sequences() {
    let mut state = ReliableReceiveState::default();
    assert!(!state.mark_new(7, MAX_SEQUENCE));
    assert!(state.take_dirty_channels().is_empty());
    assert!(state.ack_window(7).is_none());
    assert!(state.mark_new(7, 10));
    assert!(!state.mark_new(7, 10));
    assert!(state.mark_new(7, 11));
    assert!(!state.mark_new(7, 11));
}

#[test]
fn reliable_receive_first_packet_after_lost_zero_matches_csharp_window() {
    const CHANNEL: u8 = 74;
    let hex_bytes = |text: &str| {
        (0..text.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&text[index..index + 2], 16).unwrap())
            .collect::<Vec<_>>()
    };
    let mut state = ReliableReceiveState::default();

    // The Basis C# channel begins with `_remoteWindowStart = 0`. If sequence 0 is lost,
    // the first packet at sequence 1 is still accepted and ACKed with header window 0 and
    // absolute bit 1 set. A sender whose oldest unacknowledged sequence is 0 accepts it.
    assert!(state.mark_new(CHANNEL, 1));
    let (window_start, bits) = state.ack_window(CHANNEL).unwrap();
    assert_eq!(window_start, 0);
    assert_eq!(bits[0] & 0b10, 0b10);
    let mut first_ack = vec![PacketProperty::Ack as u8, 0, 0, CHANNEL, 2];
    first_ack.resize(21, 0);
    assert_eq!(
        first_ack,
        hex_bytes("0200004a0200000000000000000000000000000000")
    );

    // Receiving the retransmission of the missing packet must preserve the same window and
    // report both sequences, so both ACK loss and data loss can recover normally.
    assert!(state.mark_new(CHANNEL, 0));
    let (window_start, bits) = state.ack_window(CHANNEL).unwrap();
    assert_eq!(window_start, 0);
    assert_eq!(bits[0] & 0b11, 0b11);
    let mut recovered_ack = vec![PacketProperty::Ack as u8, 0, 0, CHANNEL, 3];
    recovered_ack.resize(21, 0);
    assert_eq!(
        recovered_ack,
        hex_bytes("0200004a0300000000000000000000000000000000")
    );

    // A far packet advances the window by one. Sequence 128 aliases bit zero, which is
    // cleared as sequence zero leaves the window and then set again for the newcomer.
    assert!(state.mark_new(CHANNEL, 128));
    let (window_start, bits) = state.ack_window(CHANNEL).unwrap();
    assert_eq!(window_start, 1);
    assert_eq!(bits[0] & 0b11, 0b11);
}

#[test]
fn reliable_receive_accepts_32767_to_0_wrap() {
    let mut state = ReliableReceiveState::default();
    // Walk the real initial window through to the sequence-space boundary. LiteNetLib
    // starts at zero; it does not accept 32767 as an isolated first arrival.
    for sequence in 0..MAX_SEQUENCE {
        assert!(state.mark_new(7, sequence));
    }
    assert!(state.mark_new(7, 0));
    assert!(!state.mark_new(7, 0));
    assert_eq!(state.ack_window(7).unwrap().0, MAX_SEQUENCE - 127);
}

#[test]
fn ack_dirty_flag_batches_a_burst_into_one_datagram() {
    const CHANNEL: u8 = 9;
    let mut state = ReliableReceiveState::default();

    // Nothing is owed before anything arrives.
    assert!(state.take_dirty_channels().is_empty());

    for sequence in 0..40u16 {
        assert!(state.mark_new(CHANNEL, sequence));
    }
    // Forty received packets owe exactly one ACK, not forty.
    assert_eq!(state.take_dirty_channels(), vec![CHANNEL]);
    // And the flag is cleared, so a clean pass sends nothing.
    assert!(state.take_dirty_channels().is_empty());
}

#[test]
fn ack_dirty_flag_rearms_on_retransmit_but_not_on_too_old() {
    const CHANNEL: u8 = 9;
    let mut state = ReliableReceiveState::default();
    for sequence in 0..8u16 {
        state.mark_new(CHANNEL, sequence);
    }
    assert_eq!(state.take_dirty_channels(), vec![CHANNEL]);

    // A duplicate must re-arm: the sender only stops resending once it hears about that
    // packet, so swallowing the ACK here would strand it.
    assert!(!state.mark_new(CHANNEL, 3));
    assert_eq!(
        state.take_dirty_channels(),
        vec![CHANNEL],
        "a retransmit owes an ACK even though nothing new was recorded"
    );

    // A packet from beyond the window slides it, as the C# does, and is acknowledged.
    assert!(state.mark_new(CHANNEL, 200));
    assert_eq!(state.take_dirty_channels(), vec![CHANNEL]);
    assert_eq!(
        state
            .ack_window(CHANNEL)
            .expect("channel has seen packets")
            .0,
        73,
        "200 slides the window to 200 - 128 + 1"
    );

    // Now a packet below that window is too old. LiteNetLib drops it *without*
    // acknowledging, and it must not re-arm either -- acking here would alias an in-window
    // sequence and falsely release it at the sender.
    assert!(!state.mark_new(CHANNEL, 10));
    assert!(
        state.take_dirty_channels().is_empty(),
        "a too-old packet owes no ACK"
    );
    let (window_start, bits) = state.ack_window(CHANNEL).expect("channel has seen packets");
    assert_eq!(window_start, 73);
    let acked: Vec<u16> = (0..DEFAULT_WINDOW_SIZE as u16)
        .filter(|s| {
            let index = *s as usize % DEFAULT_WINDOW_SIZE;
            bits[index / 8] & (1 << (index % 8)) != 0
        })
        .collect();
    let newcomer = 200u16 % DEFAULT_WINDOW_SIZE as u16;
    assert!(
        acked.contains(&72) && acked.contains(&newcomer),
        "bits stay absolute: the newcomer is acknowledged at its own index (acked={acked:?})"
    );
    assert!(
        !acked.contains(&10),
        "the rejected too-old packet must have set no bit"
    );
}

#[test]
fn batching_does_not_change_the_bytes_on_the_wire() {
    const CHANNEL: u8 = 4;
    // What the pre-batching client sent: one full window per received packet, built from the
    // state as it stood at that moment.
    let mut eager = ReliableReceiveState::default();
    let mut eager_acks = Vec::new();
    for sequence in 0..12u16 {
        eager.mark_new(CHANNEL, sequence);
        if let Some(window) = eager.ack_window(CHANNEL) {
            eager_acks.push(window);
        }
    }

    // What the batched client sends: one window for the whole burst.
    let mut batched = ReliableReceiveState::default();
    for sequence in 0..12u16 {
        batched.mark_new(CHANNEL, sequence);
    }
    let batched_acks: Vec<_> = batched
        .take_dirty_channels()
        .into_iter()
        .filter_map(|channel| batched.ack_window(channel))
        .collect();

    assert_eq!(batched_acks.len(), 1, "a burst owes a single ACK");
    assert_eq!(
        &batched_acks[0],
        eager_acks.last().expect("at least one eager ack"),
        "batching must not change the final window; only how many datagrams carry it \
             ({} eager vs {} batched)",
        eager_acks.len(),
        batched_acks.len()
    );
}

#[tokio::test]
async fn reliable_sequences_are_independent_per_channel() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let client = test_client(0, server.local_addr().unwrap()).await;
    let first_channel = 1;
    let second_channel = 2;
    client
        .send_reliable_ordered(first_channel, b"first")
        .await
        .unwrap();
    client
        .send_reliable_ordered(second_channel, b"second")
        .await
        .unwrap();

    let mut datagrams = Vec::new();
    for _ in 0..2 {
        let mut buffer = [0u8; LITENETLIB_INITIAL_MTU];
        let len = time::timeout(Duration::from_secs(1), server.recv(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        datagrams.push(buffer[..len].to_vec());
    }
    let mut sequences = HashMap::new();
    for datagram in datagrams {
        let packet = parse_packet(&datagram).unwrap();
        sequences.insert(packet.channel_id.unwrap(), packet.sequence.unwrap());
    }
    assert_eq!(
        sequences.get(&DeliveryMethod::channel_id(
            first_channel,
            DeliveryMethod::ReliableOrdered,
        )),
        Some(&0)
    );
    assert_eq!(
        sequences.get(&DeliveryMethod::channel_id(
            second_channel,
            DeliveryMethod::ReliableOrdered,
        )),
        Some(&0)
    );
}

#[tokio::test]
async fn reliable_packets_are_enqueued_before_first_datagram() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let client = test_client(0, server.local_addr().unwrap()).await;
    let pending_guard = client.pending_reliable.lock().await;
    let sending = {
        let client = client.clone();
        tokio::spawn(async move { client.send_reliable_ordered(1, b"queued").await })
    };
    tokio::task::yield_now().await;

    let mut buffer = [0u8; LITENETLIB_INITIAL_MTU];
    assert!(
        time::timeout(Duration::from_millis(50), server.recv(&mut buffer))
            .await
            .is_err()
    );

    drop(pending_guard);
    let len = time::timeout(Duration::from_secs(1), server.recv(&mut buffer))
        .await
        .unwrap()
        .unwrap();
    sending.await.unwrap().unwrap();
    assert_eq!(parse_packet(&buffer[..len]).unwrap().payload, b"queued");
}

#[tokio::test]
async fn shared_maintenance_loop_actually_flushes_pending_acks() {
    // Regression test for a batching change that passed every unit test while being entirely
    // dead: the flush was added to the per-client `maintenance_loop`, but shared maintenance
    // defaults on, so that loop never spawns and nothing was ever sent. The symptom was a
    // server that queued 32M reliable messages with 915 peers attached and `acks_in=37`.
    //
    // This drives the loop that actually runs, so a flush added to the wrong one fails here.
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let client = test_client(0, server.local_addr().unwrap()).await;
    client.in_use.store(true, Ordering::Relaxed);

    // A received reliable packet arms its channel, exactly as `handle_channeled` does.
    assert!(client
        .received_reliable
        .lock()
        .expect("receive state mutex poisoned")
        .mark_new(9, 0));
    client.ack_pending.store(true, Ordering::Relaxed);

    let clients = Arc::new(Mutex::new(vec![Arc::clone(&client)]));
    let refresh = Arc::new(Notify::new());
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let clients = Arc::clone(&clients);
        let refresh = Arc::clone(&refresh);
        let shutdown = Arc::clone(&shutdown);
        async move { shared_maintenance_loop(clients, refresh, shutdown).await }
    });

    let mut buffer = [0u8; 2048];
    let len = time::timeout(Duration::from_secs(2), server.recv(&mut buffer))
        .await
        .expect("shared maintenance must flush the armed ACK")
        .unwrap();
    assert_eq!(
        buffer[0] & 0x1f,
        PacketProperty::Ack as u8,
        "an ACK, not something else"
    );
    assert_eq!(
        len,
        LITENETLIB_CHANNELED_HEADER_SIZE + (DEFAULT_WINDOW_SIZE - 1) / 8 + 2,
        "a full LiteNetLib ACK window: 4-byte header plus 17 bytes of bits"
    );
    assert_eq!(buffer[3], 9, "the armed channel's id");
    assert_eq!(
        u16::from_le_bytes([buffer[1], buffer[2]]),
        0,
        "window start"
    );
    assert_eq!(
        buffer[4] & 1,
        1,
        "sequence 0 acknowledged at its absolute bit"
    );

    // The flag is cleared, so an unchanged window is not re-sent. That is the whole point of
    // batching: one datagram per change, not one per tick.
    assert!(
        time::timeout(Duration::from_millis(120), server.recv(&mut buffer))
            .await
            .is_err(),
        "an unchanged ACK window must not be re-sent"
    );

    shutdown.store(true, Ordering::Relaxed);
    refresh.notify_one();
    let _ = time::timeout(Duration::from_secs(1), task).await;
}

fn mtu_probe_fixture(mtu: usize, connection_number: u8) -> Vec<u8> {
    let mut packet = vec![0u8; mtu];
    packet[0] = PacketProperty::MtuCheck as u8 | (connection_number << 5);
    packet[1..5].copy_from_slice(&(mtu as i32).to_le_bytes());
    packet[5..13].copy_from_slice(&0x0123_4567_89ab_cdefu64.to_le_bytes());
    packet[mtu - 4..].copy_from_slice(&(mtu as i32).to_le_bytes());
    packet
}

#[test]
fn windows_mtu_reply_validates_probe_and_preserves_all_bytes_except_property() {
    for mtu in [1024, 1164, 1392, 1404, 1424, 1432] {
        let probe = mtu_probe_fixture(mtu, 2);
        let mut expected = probe.clone();
        expected[0] = PacketProperty::MtuOk as u8 | (2 << 5);
        assert_eq!(windows_mtu_probe_reply(&probe, 2), Some(expected));
        assert_eq!(probe[0], PacketProperty::MtuCheck as u8 | (2 << 5));
    }
    let probe = mtu_probe_fixture(1164, 2);
    for offset in [1, 13, probe.len() - 1] {
        let mut invalid = probe.clone();
        invalid[offset] ^= 1;
        assert!(windows_mtu_probe_reply(&invalid, 2).is_none());
    }
    assert!(windows_mtu_probe_reply(&probe, 1).is_none());
    for length in [0, 1, 13, 1023, 1433] {
        assert!(windows_mtu_probe_reply(&vec![0; length], 2).is_none());
    }
    for mtu in [1023, 1433] {
        assert!(windows_mtu_probe_reply(&mtu_probe_fixture(mtu, 2), 2).is_none());
    }
    for property in [
        PacketProperty::MtuOk as u8,
        PacketProperty::MtuCheck as u8 | 0x80,
    ] {
        let mut invalid = probe.clone();
        invalid[0] = property | (2 << 5);
        assert!(windows_mtu_probe_reply(&invalid, 2).is_none());
    }
}

#[cfg(windows)]
#[tokio::test]
async fn windows_receive_handler_echoes_valid_mtu_probe_to_connected_server() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut client = test_client(1, server.local_addr().unwrap()).await;
    Arc::get_mut(&mut client).unwrap().connection_number = 2;
    let mut received = [0u8; 2048];
    let mut invalid = mtu_probe_fixture(1164, 2);
    invalid[13] = 1;
    client.handle_packet(&invalid).await.unwrap();
    client
        .handle_packet(&mtu_probe_fixture(1164, 1))
        .await
        .unwrap();
    assert!(
        time::timeout(Duration::from_millis(50), server.recv(&mut received))
            .await
            .is_err()
    );
    for mtu in [1024, 1164, 1392, 1404, 1424, 1432] {
        let probe = mtu_probe_fixture(mtu, 2);
        client.handle_packet(&probe).await.unwrap();
        let len = time::timeout(Duration::from_secs(1), server.recv(&mut received))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(len, mtu);
        assert_eq!(
            &received[..len],
            windows_mtu_probe_reply(&probe, 2).unwrap()
        );
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn shared_receiver_echoes_only_valid_mtu_probe() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut client = test_client(1, server.local_addr().unwrap()).await;
    Arc::get_mut(&mut client).unwrap().connection_number = 2;
    let fd = client.socket.as_raw_fd();
    let make_probe = |mtu: usize| {
        let mut packet = vec![0u8; mtu];
        packet[0] = PacketProperty::MtuCheck as u8 | (2 << 5);
        packet[1..5].copy_from_slice(&(mtu as i32).to_le_bytes());
        packet[5..13].copy_from_slice(&0x0123_4567_89ab_cdefu64.to_le_bytes());
        packet[mtu - 4..].copy_from_slice(&(mtu as i32).to_le_bytes());
        packet
    };
    let probe = make_probe(1164);

    let mut bad_size = probe.clone();
    bad_size[1] ^= 1;
    let mut bad_trailer = probe.clone();
    bad_trailer[probe.len() - 1] ^= 1;
    let mut bad_padding = probe.clone();
    bad_padding[13] = 1;
    let mut bad_connection = probe.clone();
    bad_connection[0] = PacketProperty::MtuCheck as u8 | (1 << 5);
    for mut invalid in [
        bad_size,
        bad_trailer,
        bad_padding,
        bad_connection,
        make_probe(LITENETLIB_INITIAL_MTU - 1),
        make_probe(LITENETLIB_MAX_MTU + 1),
    ] {
        shared_receiver_send_mtu_ok(fd, &mut invalid, 2);
    }
    let mut received = [0u8; 2048];
    assert!(
        time::timeout(Duration::from_millis(50), server.recv(&mut received))
            .await
            .is_err()
    );

    for mtu in [LITENETLIB_INITIAL_MTU, 1164, LITENETLIB_MAX_MTU] {
        let mut probe = make_probe(mtu);
        let mut expected = probe.clone();
        expected[0] = PacketProperty::MtuOk as u8 | (2 << 5);
        shared_receiver_send_mtu_ok(fd, &mut probe, 2);
        let len = time::timeout(Duration::from_secs(1), server.recv(&mut received))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(len, mtu);
        assert_eq!(&received[..len], expected);
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn shared_receive_keeps_one_reliable_window_across_merged_and_compact_packets() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let client = test_client(1, server.local_addr().unwrap()).await;
    let channel_id =
        DeliveryMethod::channel_id(channels::AUTH_IDENTITY, DeliveryMethod::ReliableOrdered);

    // The shared epoll fast path handles Merged reliably, but it must update the same
    // persistent window as CompactMerged packets that fall through to handle_packet.
    let seq1 = vec![PacketProperty::Channeled as u8, 1, 0, channel_id, 0];
    shared_receiver_mark_reliable(&client, &seq1);
    let seq2 = vec![PacketProperty::Channeled as u8, 2, 0, channel_id, 0];
    let mut merged = vec![PacketProperty::Merged as u8];
    merged.extend_from_slice(&(seq2.len() as u16).to_le_bytes());
    merged.extend_from_slice(&seq2);
    shared_receiver_process_merged(&client, client.socket.as_raw_fd(), &merged);
    assert!(client.ack_pending.load(Ordering::Relaxed));

    let clients = Arc::new(Mutex::new(vec![Arc::clone(&client)]));
    let refresh = Arc::new(Notify::new());
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let clients = Arc::clone(&clients);
        let refresh = Arc::clone(&refresh);
        let shutdown = Arc::clone(&shutdown);
        async move { shared_maintenance_loop(clients, refresh, shutdown).await }
    });

    // The old stateless path immediately emitted header window 1 here. The persistent
    // window instead flushes header 0 with bit 1 after the shared 15 ms maintenance tick.
    let mut buffer = [0u8; 2048];
    let len = time::timeout(Duration::from_secs(1), server.recv(&mut buffer))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        parse_packet(&buffer[..len]).unwrap().property,
        PacketProperty::Ack
    );
    assert_eq!(
        u16::from_le_bytes([buffer[1], buffer[2]]),
        0,
        "the C# receive window remains anchored at zero after losing sequence zero"
    );
    assert_eq!(buffer[3], channel_id);
    assert_eq!(buffer[4] & 0b110, 0b110);

    // Sequence zero arrives as a CompactMerged raw entry and must be treated as new,
    // dispatched to the auth handler, and included in the same ACK window.
    let mut challenge = NetWriter::default();
    challenge
        .put_bytes_with_length(b"csharp-challenge")
        .unwrap();
    let mut seq0 = vec![PacketProperty::Channeled as u8, 0, 0, channel_id];
    seq0.extend_from_slice(&challenge.into_vec());
    let mut compact = vec![PacketProperty::CompactMerged as u8, 0x40, seq0.len() as u8];
    compact.extend_from_slice(&seq0);
    client.handle_packet(&compact).await.unwrap();

    let mut saw_auth_response = false;
    let mut saw_recovered_ack = false;
    while !(saw_auth_response && saw_recovered_ack) {
        let len = time::timeout(Duration::from_secs(1), server.recv(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        match parse_packet(&buffer[..len]).unwrap() {
            ParsedPacket {
                property: PacketProperty::Channeled,
                channel_id: Some(id),
                payload,
                ..
            } if id == channel_id => {
                assert!(
                    payload.len() >= 68 && u16::from_le_bytes([payload[0], payload[1]]) == 64,
                    "sequence-zero auth challenge must produce a signature response"
                );
                saw_auth_response = true;
            }
            ParsedPacket {
                property: PacketProperty::Ack,
                sequence: Some(0),
                channel_id: Some(id),
                payload,
                ..
            } if id == channel_id => {
                assert_eq!(payload[0] & 0b11, 0b11);
                saw_recovered_ack = true;
            }
            _ => {}
        }
    }

    // A duplicate CompactMerged packet is ACKed from the accumulated window but does not
    // dispatch another auth response.
    client.handle_packet(&compact).await.unwrap();
    let len = time::timeout(Duration::from_secs(1), server.recv(&mut buffer))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        parse_packet(&buffer[..len]).unwrap().property,
        PacketProperty::Ack
    );
    assert!(
        time::timeout(Duration::from_millis(40), server.recv(&mut buffer))
            .await
            .is_err(),
        "a duplicate must not trigger a second auth response"
    );

    shutdown.store(true, Ordering::Relaxed);
    refresh.notify_one();
    let _ = time::timeout(Duration::from_secs(1), task).await;
}

#[tokio::test]
async fn fragmented_reliable_matches_litenetlib_wire_and_ack_lifecycle() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let client = test_client(0, server.local_addr().unwrap()).await;

    let boundary_channel = 2;
    let boundary = vec![0x5a; LITENETLIB_INITIAL_MTU - LITENETLIB_CHANNELED_HEADER_SIZE];
    client
        .send_reliable_ordered(boundary_channel, &boundary)
        .await
        .unwrap();
    let mut buffer = [0u8; LITENETLIB_INITIAL_MTU];
    let len = time::timeout(Duration::from_secs(1), server.recv(&mut buffer))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(len, LITENETLIB_INITIAL_MTU);
    assert_eq!(buffer[0] & 0x80, 0);
    assert_eq!(parse_packet(&buffer[..len]).unwrap().payload, boundary);
    let boundary_channel_id =
        DeliveryMethod::channel_id(boundary_channel, DeliveryMethod::ReliableOrdered);
    let mut boundary_ack = vec![0; (DEFAULT_WINDOW_SIZE - 1) / 8 + 2];
    boundary_ack[0] = 1;
    client
        .process_ack(boundary_channel_id, 0, &boundary_ack)
        .await
        .unwrap();

    let channel = 1;
    let channel_id = DeliveryMethod::channel_id(channel, DeliveryMethod::ReliableOrdered);
    let payload_len = RELIABLE_FRAGMENT_PAYLOAD_SIZE * 2 + 1;
    let payload = (0..payload_len)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    client
        .send_reliable_ordered(channel, &payload)
        .await
        .unwrap();

    assert_eq!(
        LITENETLIB_FRAGMENTED_HEADER_SIZE + RELIABLE_FRAGMENT_PAYLOAD_SIZE,
        LITENETLIB_INITIAL_MTU
    );
    let mut fragments = Vec::new();
    for _ in 0..3 {
        let len = time::timeout(Duration::from_secs(1), server.recv(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        assert!(len <= LITENETLIB_INITIAL_MTU);
        let packet = parse_packet(&buffer[..len]).unwrap();
        assert_eq!(packet.property, PacketProperty::Channeled);
        assert_eq!(packet.channel_id, Some(channel_id));
        assert_eq!(buffer[0] & 0x80, 0x80);
        assert!(packet.payload.len() >= LITENETLIB_FRAGMENT_HEADER_SIZE);
        let fragment_id = u16::from_le_bytes([packet.payload[0], packet.payload[1]]);
        let part = u16::from_le_bytes([packet.payload[2], packet.payload[3]]);
        let total = u16::from_le_bytes([packet.payload[4], packet.payload[5]]);
        fragments.push((
            part,
            total,
            fragment_id,
            packet.sequence.unwrap(),
            packet.payload[6..].to_vec(),
        ));
    }
    fragments.sort_by_key(|fragment| fragment.0);
    assert_eq!(fragments.len(), 3);
    let fragment_id = fragments[0].2;
    let mut reassembled = Vec::new();
    for (part, total, id, sequence, bytes) in &fragments {
        assert_eq!(*id, fragment_id);
        assert_eq!(*total, 3);
        assert_eq!(
            *part as usize,
            reassembled.len() / RELIABLE_FRAGMENT_PAYLOAD_SIZE
        );
        assert_eq!(*sequence, *part);
        reassembled.extend_from_slice(bytes);
    }
    assert_eq!(reassembled, payload);

    // ACK bits are absolute, and the window start is the oldest sequence still in the
    // window -- here 0, since nothing has been acknowledged yet. A window start ahead of
    // the sender's own would be rejected outright, exactly as LiteNetLib rejects it.
    let mut middle_ack = vec![0u8; (DEFAULT_WINDOW_SIZE - 1) / 8 + 2];
    middle_ack[0] |= 1 << 1;
    client
        .process_ack(channel_id, 0, &middle_ack)
        .await
        .unwrap();
    assert_eq!(client.pending_reliable.lock().await.len(), 2);
    assert!(client.pending_reliable_active.load(Ordering::Relaxed));

    let mut final_ack = vec![0u8; (DEFAULT_WINDOW_SIZE - 1) / 8 + 2];
    final_ack[0] |= 1;
    final_ack[0] |= 1 << 2;
    client.process_ack(channel_id, 0, &final_ack).await.unwrap();
    assert!(client.pending_reliable.lock().await.is_empty());
    assert!(!client.pending_reliable_active.load(Ordering::Relaxed));
}

#[tokio::test]
async fn batch_publish_preserves_dense_indices_and_notifies_complete_snapshot() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server_addr = server.local_addr().unwrap();
    let managed = Arc::new(Mutex::new(Vec::new()));
    let refresh = Arc::new(Notify::new());
    let maintenance = MaintenanceOptions {
        shared: true,
        refresh: refresh.clone(),
    };
    let batch = vec![
        test_client(0, server_addr).await,
        test_client(1, server_addr).await,
        test_client(2, server_addr).await,
    ];
    let notified = refresh.notified();
    publish_client_batch(&managed, 0, &batch, &maintenance)
        .await
        .unwrap();
    time::timeout(Duration::from_secs(1), notified)
        .await
        .unwrap();
    let snapshot = managed.lock().await;
    assert_eq!(
        snapshot
            .iter()
            .map(|client| client.index)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
}

#[tokio::test]
async fn stale_reconnect_cannot_replace_or_notify() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server_addr = server.local_addr().unwrap();
    let old = test_client(0, server_addr).await;
    let current = test_client(0, server_addr).await;
    let replacement = test_client(0, server_addr).await;
    let managed = Arc::new(Mutex::new(vec![current.clone()]));
    let refresh = Arc::new(Notify::new());
    let maintenance = MaintenanceOptions {
        shared: true,
        refresh: refresh.clone(),
    };
    let notified = refresh.notified();

    assert!(!replace_client_if_current(&managed, 0, &old, &replacement, &maintenance).await);
    assert!(time::timeout(Duration::from_millis(50), notified)
        .await
        .is_err());
    assert!(Arc::ptr_eq(&managed.lock().await[0], &current));
}

#[tokio::test]
async fn replacement_releases_the_old_blocked_receive_task() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server_addr = server.local_addr().unwrap();
    let old = test_client(0, server_addr).await;
    let replacement = test_client(0, server_addr).await;
    let managed = Arc::new(Mutex::new(vec![old.clone()]));
    let maintenance = MaintenanceOptions {
        shared: false,
        refresh: Arc::new(Notify::new()),
    };
    let weak = Arc::downgrade(&old);
    let receiver = tokio::spawn(old.clone().receive_loop());
    tokio::task::yield_now().await;

    assert!(replace_client_if_current(&managed, 0, &old, &replacement, &maintenance).await);
    time::timeout(Duration::from_secs(1), receiver)
        .await
        .expect("old receive task remained blocked")
        .unwrap()
        .unwrap();
    assert!(!old.in_use.load(Ordering::Relaxed));
    assert!(!old.connected.load(Ordering::Relaxed));
    assert!(Arc::ptr_eq(&managed.lock().await[0], &replacement));

    drop(old);
    assert!(weak.upgrade().is_none());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn shared_registration_replaces_reused_fd_without_retaining_old_client() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let old_client = test_client(1, server.local_addr().unwrap()).await;
    let old_weak = Arc::downgrade(&old_client);
    assert!(shared_receive_registration_matches(
        42,
        42,
        &old_weak,
        &old_client
    ));

    let replacement = test_client(1, server.local_addr().unwrap()).await;
    assert!(
        !shared_receive_registration_matches(42, 42, &old_weak, &replacement),
        "a recycled descriptor must register the new receive window"
    );
    drop(old_client);
    assert!(
        old_weak.upgrade().is_none(),
        "the epoll registry stores only Weak"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn connect_accept_filters_non_observers_only() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server_addr = server.local_addr().unwrap();
    let observer = test_client(0, server_addr).await;
    let load_sink = test_client(1, server_addr).await;
    let mut accept = vec![0u8; 15];
    accept[0] = PacketProperty::ConnectAccept as u8;
    accept[1..9].copy_from_slice(&0i64.to_le_bytes());
    accept[11..15].copy_from_slice(&1i32.to_le_bytes());
    observer.handle_packet(&accept).await.unwrap();
    load_sink.handle_packet(&accept).await.unwrap();

    let unreliable = vec![
        PacketProperty::Unreliable as u8,
        channels::PLAYER_AVATAR_HIGH,
        1,
        2,
        3,
    ];
    server
        .send_to(&unreliable, observer.socket.local_addr().unwrap())
        .await
        .unwrap();
    server
        .send_to(&unreliable, load_sink.socket.local_addr().unwrap())
        .await
        .unwrap();
    let mut buffer = [0u8; 64];
    let observer_len = time::timeout(Duration::from_secs(1), observer.socket.recv(&mut buffer))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buffer[..observer_len], unreliable);
    assert!(time::timeout(
        Duration::from_millis(50),
        load_sink.socket.recv(&mut buffer)
    )
    .await
    .is_err());

    let channel_id =
        DeliveryMethod::channel_id(channels::META_DATA, DeliveryMethod::ReliableOrdered);
    let nested = vec![PacketProperty::Channeled as u8, 0, 0, channel_id, 42];
    let mut merged = vec![PacketProperty::Merged as u8, nested.len() as u8, 0];
    merged.extend_from_slice(&nested);
    server
        .send_to(&merged, observer.socket.local_addr().unwrap())
        .await
        .unwrap();
    server
        .send_to(&merged, load_sink.socket.local_addr().unwrap())
        .await
        .unwrap();
    let observer_len = time::timeout(Duration::from_secs(1), observer.socket.recv(&mut buffer))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buffer[..observer_len], merged);
    let load_sink_len = time::timeout(Duration::from_secs(1), load_sink.socket.recv(&mut buffer))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buffer[..load_sink_len], merged);
}

#[tokio::test]
async fn pending_reliable_flag_tracks_enqueue_and_final_ack() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server_addr = server.local_addr().unwrap();
    let client = test_client(0, server_addr).await;
    let channel_id = DeliveryMethod::channel_id(1, DeliveryMethod::ReliableOrdered);

    client.send_reliable_ordered(1, b"pending").await.unwrap();
    assert!(client.pending_reliable_active.load(Ordering::Relaxed));
    assert_eq!(client.pending_reliable.lock().await.len(), 1);

    let mut short_ack = vec![0; (DEFAULT_WINDOW_SIZE - 1) / 8 + 1];
    short_ack[0] = 1;
    client.process_ack(channel_id, 0, &short_ack).await.unwrap();
    let mut long_ack = vec![0; (DEFAULT_WINDOW_SIZE - 1) / 8 + 3];
    long_ack[0] = 1;
    client.process_ack(channel_id, 0, &long_ack).await.unwrap();
    let mut invalid_sequence_ack = vec![0; (DEFAULT_WINDOW_SIZE - 1) / 8 + 2];
    invalid_sequence_ack[0] = 1;
    client
        .process_ack(channel_id, MAX_SEQUENCE, &invalid_sequence_ack)
        .await
        .unwrap();
    assert_eq!(
        client.pending_reliable.lock().await.len(),
        1,
        "reject wrong ACK sizes and sequence values outside 0..32768"
    );

    let mut ack = vec![0; (DEFAULT_WINDOW_SIZE - 1) / 8 + 2];
    ack[0] = 1;
    client.process_ack(channel_id, 0, &ack).await.unwrap();
    assert!(!client.pending_reliable_active.load(Ordering::Relaxed));
    assert!(client.pending_reliable.lock().await.is_empty());
}

#[tokio::test]
async fn reliable_window_promotion_bounds_large_queues_per_channel_and_wraps() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let client = test_client(0, server.local_addr().unwrap()).await;
    let channel = DeliveryMethod::channel_id(1, DeliveryMethod::ReliableOrdered);
    let mut pending = (0..=32_769)
        .map(|_| ReliableSend {
            channel_id: channel,
            sequence: None,
            bytes: vec![PacketProperty::Channeled as u8, 0, 0, channel],
            last_sent: None,
        })
        .collect::<VecDeque<_>>();

    let promoted = client.promote_reliable_window(&mut pending);
    assert_eq!(promoted.len(), DEFAULT_WINDOW_SIZE);
    assert_eq!(promoted.first().unwrap().sequence, Some(0));
    assert_eq!(promoted.last().unwrap().sequence, Some(127));
    assert_eq!(pending.len(), 32_770);
    assert_eq!(
        pending
            .iter()
            .filter(|item| item.sequence.is_some())
            .count(),
        DEFAULT_WINDOW_SIZE
    );
    assert_eq!(
        client.reliable_sequences[channel as usize].load(Ordering::Relaxed),
        128
    );

    let mut wrapped = VecDeque::from([
        ReliableSend {
            channel_id: channel,
            sequence: None,
            bytes: vec![PacketProperty::Channeled as u8, 0, 0, channel],
            last_sent: None,
        },
        ReliableSend {
            channel_id: channel,
            sequence: None,
            bytes: vec![PacketProperty::Channeled as u8, 0, 0, channel],
            last_sent: None,
        },
    ]);
    client.reliable_sequences[channel as usize].store(MAX_SEQUENCE - 1, Ordering::Relaxed);
    let promoted = client.promote_reliable_window(&mut wrapped);
    assert_eq!(
        promoted
            .iter()
            .map(|item| item.sequence)
            .collect::<Vec<_>>(),
        vec![Some(MAX_SEQUENCE - 1), Some(0)]
    );
    assert!(wrapped.iter().all(|item| item.sequence.is_some()));
}

#[tokio::test]
async fn reliable_sender_holds_seq128_until_missing_zero_is_acked() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let client = test_client(0, server.local_addr().unwrap()).await;
    let channel = 3;
    let channel_id = DeliveryMethod::channel_id(channel, DeliveryMethod::ReliableOrdered);
    for _ in 0..=DEFAULT_WINDOW_SIZE {
        client
            .send_reliable_ordered(channel, b"queued")
            .await
            .unwrap();
    }

    let mut received = [0u8; 64];
    for expected in 0..DEFAULT_WINDOW_SIZE as u16 {
        let len = time::timeout(Duration::from_secs(1), server.recv(&mut received))
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for initial sequence {expected}"))
            .unwrap();
        let packet = parse_packet(&received[..len]).unwrap();
        assert_eq!(packet.sequence, Some(expected));
        assert_eq!(packet.channel_id, Some(channel_id));
    }
    assert_eq!(
        client.pending_reliable.lock().await.len(),
        DEFAULT_WINDOW_SIZE + 1
    );

    let mut ack = vec![0u8; (DEFAULT_WINDOW_SIZE - 1) / 8 + 2];
    for sequence in 1..DEFAULT_WINDOW_SIZE {
        ack[sequence / 8] |= 1 << (sequence % 8);
    }
    client.process_ack(channel_id, 0, &ack).await.unwrap();
    assert_eq!(client.pending_reliable.lock().await.len(), 2);
    assert!(
        time::timeout(Duration::from_millis(40), server.recv(&mut received))
            .await
            .is_err(),
        "the missing sequence zero keeps the 128-sequence span full"
    );

    ack.fill(0);
    for sequence in 0..DEFAULT_WINDOW_SIZE {
        ack[sequence / 8] |= 1 << (sequence % 8);
    }
    client.process_ack(channel_id, 0, &ack).await.unwrap();
    let len = time::timeout(Duration::from_secs(1), server.recv(&mut received))
        .await
        .unwrap()
        .unwrap();
    let packet = parse_packet(&received[..len]).unwrap();
    assert_eq!(packet.sequence, Some(DEFAULT_WINDOW_SIZE as u16));
    assert_eq!(client.pending_reliable.lock().await.len(), 1);
}

#[tokio::test]
async fn delayed_ack_window_cannot_alias_newer_client_sequences() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let client = test_client(0, server.local_addr().unwrap()).await;
    let channel_id = DeliveryMethod::channel_id(1, DeliveryMethod::ReliableOrdered);
    {
        let mut pending = client.pending_reliable.lock().await;
        for sequence in 10..=137u16 {
            pending.push_back(ReliableSend {
                channel_id,
                sequence: Some(sequence),
                bytes: vec![PacketProperty::Channeled as u8, 0, 0, channel_id],
                last_sent: Some(SystemTime::now()),
            });
        }
    }
    let mut old_ack = vec![0u8; (DEFAULT_WINDOW_SIZE - 1) / 8 + 2];
    for sequence in 0..10 {
        old_ack[sequence / 8] |= 1 << (sequence % 8);
    }
    client.process_ack(channel_id, 0, &old_ack).await.unwrap();
    let pending = client.pending_reliable.lock().await;
    assert!((128..=137).all(|sequence| pending
        .iter()
        .any(|item| { item.channel_id == channel_id && item.sequence == Some(sequence) })));
}

#[tokio::test]
async fn reliable_resend_skips_unassigned_records_and_retries_reserved_slots() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let client = test_client(0, server.local_addr().unwrap()).await;
    let channel_id = DeliveryMethod::channel_id(1, DeliveryMethod::ReliableOrdered);
    {
        let mut pending = client.pending_reliable.lock().await;
        pending.push_back(ReliableSend {
            channel_id,
            sequence: Some(0),
            bytes: vec![PacketProperty::Channeled as u8, 0, 0, channel_id, 7],
            last_sent: None, // reserved into the window, but its first send failed
        });
        pending.push_back(ReliableSend {
            channel_id,
            sequence: None,
            bytes: vec![PacketProperty::Channeled as u8, 0, 0, channel_id, 8],
            last_sent: None, // still queued beyond the window
        });
    }

    client.resend_reliable().await.unwrap();
    let mut received = [0u8; 64];
    let len = time::timeout(Duration::from_secs(1), server.recv(&mut received))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(parse_packet(&received[..len]).unwrap().sequence, Some(0));
    assert!(
        time::timeout(Duration::from_millis(40), server.recv(&mut received))
            .await
            .is_err(),
        "the unsent record has no sequence and must not be retried"
    );
}

#[test]
fn did_response_contains_signature_and_na_fragment() {
    let identity = Identity::random();
    let response = identity.response_payload(b"challenge").unwrap();
    let sig_len = u16::from_le_bytes([response[0], response[1]]) as usize;
    assert_eq!(sig_len, 64);
    let frag_start = 2 + sig_len;
    let frag_len = u16::from_le_bytes([response[frag_start], response[frag_start + 1]]) as usize;
    assert_eq!(&response[frag_start + 2..frag_start + 2 + frag_len], b"N/A");
    let sig = Signature::from_slice(&response[2..2 + sig_len]).unwrap();
    identity.verifying_key.verify(b"challenge", &sig).unwrap();
}

#[test]
fn duplicate_start_is_rejected_statefully() {
    let flag = AtomicBool::new(false);
    assert!(!flag.swap(true, Ordering::SeqCst));
    assert!(flag.swap(true, Ordering::SeqCst));
}

#[test]
fn transport_mappings_are_shared_with_server_transport() {
    assert_eq!(
        PacketProperty::from_byte(PacketProperty::ConnectRequest as u8 | (2 << 5)),
        Some(PacketProperty::ConnectRequest)
    );
    assert_eq!(
        DeliveryMethod::channel_id(channels::AUTH_IDENTITY, DeliveryMethod::ReliableOrdered),
        2
    );
    assert_eq!(
        DeliveryMethod::from_channel_id(DeliveryMethod::channel_id(
            channels::PLAYER_AVATAR_HIGH,
            DeliveryMethod::ReliableUnordered
        )),
        DeliveryMethod::ReliableUnordered
    );
    assert_eq!(
        DeliveryMethod::from_channel_id(DeliveryMethod::channel_id(
            channels::PLAYER_AVATAR_HIGH,
            DeliveryMethod::Sequenced
        )),
        DeliveryMethod::Sequenced
    );
}

fn build_ogg_page(packets: &[&[u8]]) -> Vec<u8> {
    let mut segments = Vec::new();
    let mut data = Vec::new();
    for packet in packets {
        assert!(packet.len() < 255);
        segments.push(packet.len() as u8);
        data.extend_from_slice(packet);
    }
    build_ogg_page_from_segments(&segments, &data)
}

fn build_ogg_page_from_segments(segments: &[u8], data: &[u8]) -> Vec<u8> {
    let mut page = vec![0; 27];
    page[0..4].copy_from_slice(b"OggS");
    page[26] = segments.len() as u8;
    page.extend_from_slice(segments);
    page.extend_from_slice(data);
    page
}

fn read_raw_len_string(bytes: &[u8], offset: usize) -> (String, usize) {
    let len = u16::from_le_bytes([bytes[offset], bytes[offset + 1]]) as usize;
    (
        String::from_utf8(bytes[offset + 2..offset + 2 + len].to_vec()).unwrap(),
        offset + 2 + len,
    )
}

#[path = "resilience_tests.rs"]
mod resilience_tests;

fn profile_full_unreliable(channel: u8, payload: &[u8]) -> Vec<u8> {
    let mut packet = Vec::with_capacity(payload.len() + 2);
    packet.push(PacketProperty::Unreliable as u8);
    packet.push(channel);
    packet.extend_from_slice(payload);
    packet
}

fn profile_build_merged(entries: &[Vec<u8>]) -> Vec<u8> {
    let mut datagram = vec![PacketProperty::Merged as u8];
    for entry in entries {
        datagram.extend_from_slice(&(entry.len() as u16).to_le_bytes());
        datagram.extend_from_slice(entry);
    }
    datagram
}

fn profile_build_compact(entries: &[Vec<u8>]) -> Vec<u8> {
    // Mirrors the server encoder: strip the 2-byte Unreliable header,
    // tag + u8 len for payloads <= 255, tag + LONG + u16 len above.
    let mut datagram = vec![PacketProperty::CompactMerged as u8];
    for entry in entries {
        let channel = entry[1];
        let payload = &entry[2..];
        if payload.len() <= u8::MAX as usize {
            datagram.push(channel);
            datagram.push(payload.len() as u8);
        } else {
            datagram.push(channel | 0x80);
            datagram.extend_from_slice(&(payload.len() as u16).to_le_bytes());
        }
        datagram.extend_from_slice(payload);
    }
    datagram
}

/// Epoll/receive-path profiler: measures `handle_packet` throughput for wire
/// datagrams shaped like a real avatar tick, comparing classic Merged against
/// the CompactMerged framing the server now sends.
///
/// Run explicitly (ignored by default, release mode for representative
/// numbers):
///   cargo test -p basis-client-core --release profile_receive_framing -- --ignored --nocapture
///
/// Context: the Linux-only shared epoll fast path consumes Merged inline
/// (`shared_receiver_process_merged`: no allocation, no Tokio handoff), while
/// CompactMerged falls through to this `handle_packet` path on every
/// platform. Load-sink clients additionally drop CompactMerged in kernel BPF,
/// so this profile covers the cost real (non-load-sink) clients pay per
/// avatar entry: framing parse plus, for compact only, one small Vec
/// alloc + memcpy per entry before the recursive dispatch.
#[tokio::test]
#[ignore]
async fn profile_receive_framing_throughput() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let client = test_client(0, server.local_addr().unwrap()).await;

    fn scenario(name: &str, sizes: &[usize]) -> (String, Vec<Vec<u8>>) {
        let entries: Vec<Vec<u8>> = sizes
            .iter()
            .enumerate()
            .map(|(i, &len)| {
                let fill = (i as u8).wrapping_mul(37).wrapping_add(11);
                profile_full_unreliable(channels::AVATAR, &vec![fill; len])
            })
            .collect();
        (name.to_string(), entries)
    }

    // Representative avatar tick: 10 entries, 60..276 B (8 short-form, 2 long-form).
    // Compressed-bundle tick: 3 large entries, all long-form.
    let scenarios = [
        scenario("tick", &[60, 84, 108, 132, 156, 180, 204, 228, 252, 276]),
        scenario("bundle", &[350, 350, 350]),
    ];

    const WARMUP: usize = 500;
    const MEASURED: usize = 5000;
    const ROUNDS: usize = 5;

    for (name, entries) in &scenarios {
        let merged = profile_build_merged(entries);
        let compact = profile_build_compact(entries);
        println!(
            "scenario={name} entries={} merged_bytes={} compact_bytes={} saved={}",
            entries.len(),
            merged.len(),
            compact.len(),
            merged.len() - compact.len(),
        );

        for datagram in [&merged, &compact] {
            for _ in 0..WARMUP {
                client.handle_packet(datagram).await.unwrap();
            }
        }

        let mut merged_rounds = Vec::with_capacity(ROUNDS);
        let mut compact_rounds = Vec::with_capacity(ROUNDS);
        for _ in 0..ROUNDS {
            let start = std::time::Instant::now();
            for _ in 0..MEASURED {
                client.handle_packet(&merged).await.unwrap();
            }
            merged_rounds.push(start.elapsed());
            let start = std::time::Instant::now();
            for _ in 0..MEASURED {
                client.handle_packet(&compact).await.unwrap();
            }
            compact_rounds.push(start.elapsed());
        }
        merged_rounds.sort();
        compact_rounds.sort();
        let merged_best = merged_rounds[0];
        let compact_best = compact_rounds[0];
        let per_entry = |elapsed: std::time::Duration| {
            elapsed.as_nanos() as f64 / (MEASURED * entries.len()) as f64
        };
        println!(
            "  merged : {:>10.1} ns/datagram  {:>8.1} ns/entry (min of {ROUNDS} rounds x {MEASURED} datagrams)",
            merged_best.as_nanos() as f64 / MEASURED as f64,
            per_entry(merged_best),
        );
        println!(
            "  compact: {:>10.1} ns/datagram  {:>8.1} ns/entry (min of {ROUNDS} rounds x {MEASURED} datagrams)",
            compact_best.as_nanos() as f64 / MEASURED as f64,
            per_entry(compact_best),
        );
        println!(
            "  delta  : {:+.1} ns/entry ({:+.1}% vs merged)",
            per_entry(compact_best) - per_entry(merged_best),
            100.0 * (compact_best.as_nanos() as f64 / merged_best.as_nanos() as f64 - 1.0),
        );
    }
}

/// Full-population soak: 750 live `BasisClient` instances each process one
/// avatar-tick datagram per simulated server tick, in both framings, and the
/// aggregate receive CPU is compared. Answers whether the ~+30 ns/entry
/// compact cost measured in `profile_receive_framing_throughput` amounts to
/// meaningful CPU at population scale.
///
/// Run explicitly (ignored by default, release mode for representative
/// numbers):
///   cargo test -p basis-client-core --release profile_750_clients -- --ignored --nocapture
///
/// Methodology notes: clients are driven sequentially (no Tokio fan-out) so
/// the numbers isolate receive-path CPU from scheduler/sleep noise; payload
/// bytes are fixed per entry because no framing branch depends on content.
/// `avatar_observer` is unset (as in `test_client`), so per-entry application
/// work is identical between framings and only framing + dispatch is timed.
#[tokio::test]
#[ignore]
async fn profile_750_clients_framing_cpu() {
    const CLIENTS: usize = 750;
    const TICKS: usize = 250;
    const ROUNDS: usize = 3;

    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server_addr = server.local_addr().unwrap();
    let mut clients = Vec::with_capacity(CLIENTS);
    for index in 0..CLIENTS {
        clients.push(test_client(index, server_addr).await);
    }

    // Same 10-entry avatar tick shape as the framing profiler.
    let entries: Vec<Vec<u8>> = [60, 84, 108, 132, 156, 180, 204, 228, 252, 276]
        .iter()
        .enumerate()
        .map(|(i, &len)| {
            let fill = (i as u8).wrapping_mul(37).wrapping_add(11);
            profile_full_unreliable(channels::AVATAR, &vec![fill; len])
        })
        .collect();
    let merged = profile_build_merged(&entries);
    let compact = profile_build_compact(&entries);
    println!(
        "clients={CLIENTS} ticks={TICKS} entries/tick={} merged_bytes={} compact_bytes={}",
        entries.len(),
        merged.len(),
        compact.len(),
    );

    // Warmup: one tick per client in both framings (sockets, allocator, code).
    for client in &clients {
        client.handle_packet(&merged).await.unwrap();
        client.handle_packet(&compact).await.unwrap();
    }

    async fn run_soak(clients: &[Arc<BasisClient>], datagram: &[u8], ticks: usize) {
        for _ in 0..ticks {
            for client in clients {
                client.handle_packet(datagram).await.unwrap();
            }
        }
    }

    let mut merged_rounds = Vec::with_capacity(ROUNDS);
    let mut compact_rounds = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let start = std::time::Instant::now();
        run_soak(&clients, &merged, TICKS).await;
        merged_rounds.push(start.elapsed());
        let start = std::time::Instant::now();
        run_soak(&clients, &compact, TICKS).await;
        compact_rounds.push(start.elapsed());
    }
    merged_rounds.sort();
    compact_rounds.sort();
    let merged_best = merged_rounds[0];
    let compact_best = compact_rounds[0];
    let total_packets = (CLIENTS * TICKS) as f64;
    let total_entries = total_packets * entries.len() as f64;
    println!(
        "  merged : {:>8.1} ms total  {:>8.1} us/client-tick  {:>6.1} ns/entry",
        merged_best.as_secs_f64() * 1000.0,
        merged_best.as_nanos() as f64 / total_packets / 1000.0,
        merged_best.as_nanos() as f64 / total_entries,
    );
    println!(
        "  compact: {:>8.1} ms total  {:>8.1} us/client-tick  {:>6.1} ns/entry",
        compact_best.as_secs_f64() * 1000.0,
        compact_best.as_nanos() as f64 / total_packets / 1000.0,
        compact_best.as_nanos() as f64 / total_entries,
    );
    println!(
        "  delta  : {:+.1} ms total ({:+.1}% vs merged), {:+.1} extra us per client-tick",
        (compact_best.as_secs_f64() - merged_best.as_secs_f64()) * 1000.0,
        100.0 * (compact_best.as_secs_f64() / merged_best.as_secs_f64() - 1.0),
        (compact_best.as_nanos() as f64 - merged_best.as_nanos() as f64) / total_packets / 1000.0,
    );
}
