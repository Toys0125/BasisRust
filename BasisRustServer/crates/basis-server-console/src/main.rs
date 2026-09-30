use anyhow::{Context, Result};
use basis_protocol::{
    avatar::AVATAR_BUNDLE_DICTIONARY_GENERATION, config::ServerConfig, permissions::nodes,
};
use basis_server_core::{migrate_legacy_resource_dirs, BsrProfilerSnapshot, ServerState};
use basis_server_health::{
    format_system_time, start_health_server, AppMessageMetrics, AvatarSyncMetrics,
    AvatarTimingMetrics, BsrBundleMetrics, BsrHealthMetrics, BsrLoadMetrics, BsrMsPerTickMetrics,
    BsrWindowMetrics, BsrZstdMetrics, ExtendedHealthMetrics, HealthState, HealthStatistics,
    RawUdpMetrics, ReliableMetrics,
};
use clap::Parser;
use crossterm::{
    cursor,
    event::{self, Event, KeyCode},
    execute,
    terminal::{self, ClearType},
};
use rustyline::{
    completion::{Completer, FilenameCompleter, Pair},
    error::ReadlineError,
    highlight::Highlighter,
    hint::Hinter,
    history::DefaultHistory,
    validate::Validator,
    CompletionType, Config as LineEditorConfig, Context as LineEditorContext, Editor, Helper,
};
use std::{
    collections::HashMap,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
};
use tokio::sync::oneshot;
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(author, version, about = "Basis Rust Server Console")]
struct Args {
    #[arg(long, default_value = "config/config.xml")]
    config: PathBuf,
    #[arg(long)]
    base_dir: Option<PathBuf>,
    #[arg(long)]
    no_console: bool,
    #[arg(long)]
    port: Option<u16>,
    #[arg(long, default_value = "info")]
    log_level: String,
    #[arg(long)]
    health_host: Option<String>,
    #[arg(long)]
    health_port: Option<u16>,
}

type CommandHandler = Box<dyn Fn(&[&str]) + Send + Sync + 'static>;

struct ConsoleHelper {
    server: ServerState,
    filenames: FilenameCompleter,
}

impl ConsoleHelper {
    fn new(server: ServerState) -> Self {
        Self {
            server,
            filenames: FilenameCompleter::new(),
        }
    }

    fn command_pairs(&self, current: &str) -> Vec<Pair> {
        filter_pairs(
            current,
            [
                ("/players", "/players  - list connected players"),
                ("/status", "/status  - server status / live status"),
                ("/config", "/config  - inspect or live-edit configuration"),
                ("/perm", "/perm  - permission management"),
                ("/help", "/help  - show command help"),
                ("/clear", "/clear  - clear the console"),
                ("/shutdown", "/shutdown  - stop the server"),
            ],
        )
    }

    fn config_field_pairs(&self, current: &str) -> Vec<Pair> {
        let config = self.server.config.read();
        let mut fields = config.field_names();
        fields.sort_by_key(|field| field.to_ascii_lowercase());
        fields
            .into_iter()
            .filter(|field| starts_with_ignore_ascii_case(field, current))
            .map(|field| {
                let value = if ServerConfig::is_secret_field_name(&field) {
                    "<redacted>".to_string()
                } else {
                    config.get_field(&field).unwrap_or_default()
                };
                Pair {
                    display: format!("{field}  = {value}"),
                    replacement: field,
                }
            })
            .collect()
    }

    fn config_value_pairs(&self, field: &str, current: &str) -> Vec<Pair> {
        let config = self.server.config.read();
        let Some(value) = config.get_field(field) else {
            return Vec::new();
        };

        let mut values: Vec<(String, String)> = Vec::new();
        if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") {
            values.push(("true".to_string(), "true".to_string()));
            values.push(("false".to_string(), "false".to_string()));
        } else if field.eq_ignore_ascii_case("BasisUserRestrictionMode") {
            for value in ["None", "WhiteList", "BlackList"] {
                values.push((value.to_string(), value.to_string()));
            }
        } else if !ServerConfig::is_secret_field_name(field) && !value.is_empty() {
            values.push((value.clone(), format!("{value}  (current)")));
        }

        values
            .into_iter()
            .filter(|(replacement, _)| starts_with_ignore_ascii_case(replacement, current))
            .map(|(replacement, display)| Pair {
                display,
                replacement,
            })
            .collect()
    }

    fn permission_user_pairs(&self, current: &str) -> Vec<Pair> {
        let snapshot = self.server.permissions.snapshot();
        let mut users = snapshot
            .users
            .keys()
            .cloned()
            .map(|uuid| (uuid, None))
            .collect::<Vec<_>>();
        for peer in self.server.authenticated_peers.iter() {
            let uuid = peer.metadata.player_uuid.clone();
            if users.iter().all(|(known, _)| known != &uuid) {
                users.push((uuid, Some(peer.metadata.player_display_name.clone())));
            }
        }
        users.sort_by_key(|(uuid, _)| uuid.to_ascii_lowercase());
        users
            .into_iter()
            .filter(|(uuid, _)| starts_with_ignore_ascii_case(uuid, current))
            .map(|(uuid, display_name)| Pair {
                display: display_name
                    .map(|name| format!("{uuid}  ({name}, connected)"))
                    .unwrap_or_else(|| uuid.clone()),
                replacement: uuid,
            })
            .collect()
    }

    fn permission_group_pairs(&self, current: &str) -> Vec<Pair> {
        let snapshot = self.server.permissions.snapshot();
        let mut groups = snapshot.groups.keys().cloned().collect::<Vec<_>>();
        groups.sort_by_key(|group| group.to_ascii_lowercase());
        groups
            .into_iter()
            .filter(|group| starts_with_ignore_ascii_case(group, current))
            .map(|group| Pair {
                display: group.clone(),
                replacement: group,
            })
            .collect()
    }

    fn permission_node_pairs(&self, current: &str) -> Vec<Pair> {
        let mut nodes = nodes::ALL_NODES
            .iter()
            .flat_map(|node| [(*node).to_string(), format!("-{node}")])
            .collect::<Vec<_>>();
        nodes.sort_by_key(|node| node.to_ascii_lowercase());
        nodes
            .into_iter()
            .filter(|node| starts_with_ignore_ascii_case(node, current))
            .map(|node| Pair {
                display: node.clone(),
                replacement: node,
            })
            .collect()
    }

    fn completion_pairs(
        &self,
        line: &str,
        pos: usize,
        ctx: &LineEditorContext<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let prefix = &line[..pos];
        let current_start = prefix
            .char_indices()
            .rev()
            .find_map(|(index, ch)| ch.is_whitespace().then_some(index + ch.len_utf8()))
            .unwrap_or(0);
        let current = &prefix[current_start..];
        let completed = prefix[..current_start]
            .split_whitespace()
            .collect::<Vec<_>>();

        if completed.is_empty() {
            return Ok((current_start, self.command_pairs(current)));
        }

        match completed[0].to_ascii_lowercase().as_str() {
            "/status" if completed.len() == 1 => {
                return Ok((
                    current_start,
                    filter_pairs(
                        current,
                        [
                            ("live", "live  - continuously refresh status"),
                            ("watch", "watch  - alias for live"),
                            ("verbose", "verbose  - detailed counters"),
                            ("-v", "-v  - alias for verbose"),
                        ],
                    ),
                ));
            }
            "/config" => {
                if completed.len() == 1 {
                    let mut pairs = filter_pairs(
                        current,
                        [
                            ("get", "get  - read one config field"),
                            ("set", "set  - apply a config field live"),
                            ("save", "save  - persist current config to config.xml"),
                            ("list", "list  - show all config fields"),
                        ],
                    );
                    pairs.extend(self.config_field_pairs(current));
                    return Ok((current_start, pairs));
                }
                if completed[1].eq_ignore_ascii_case("get") && completed.len() == 2 {
                    return Ok((current_start, self.config_field_pairs(current)));
                }
                if completed[1].eq_ignore_ascii_case("set") {
                    if completed.len() == 2 {
                        return Ok((current_start, self.config_field_pairs(current)));
                    }
                    if completed.len() == 3 {
                        return Ok((
                            current_start,
                            self.config_value_pairs(completed[2], current),
                        ));
                    }
                } else if completed.len() == 2 {
                    return Ok((
                        current_start,
                        self.config_value_pairs(completed[1], current),
                    ));
                }
            }
            "/perm" => {
                if completed.len() == 1 {
                    return Ok((
                        current_start,
                        filter_pairs(
                            current,
                            [
                                ("help", "help  - permission command help"),
                                ("path", "path  - inspect/change permissions.xml path"),
                                ("load", "load  - load permissions"),
                                ("save", "save  - save permissions"),
                                ("reload", "reload  - save then reload permissions"),
                                ("defaults", "defaults  - ensure default groups"),
                                ("check", "check  - test a UUID against a node"),
                                ("user", "user  - user permission operations"),
                                ("group", "group  - group permission operations"),
                            ],
                        ),
                    ));
                }

                match completed[1].to_ascii_lowercase().as_str() {
                    "path" if completed.len() == 2 => {
                        return Ok((
                            current_start,
                            filter_pairs(current, [("set", "set  - change permissions.xml path")]),
                        ));
                    }
                    "load" if completed.len() == 2 => {
                        return Ok((
                            current_start,
                            filter_pairs(current, [("from", "from  - load from another path")]),
                        ));
                    }
                    "save" if completed.len() == 2 => {
                        return Ok((
                            current_start,
                            filter_pairs(current, [("to", "to  - save to another path")]),
                        ));
                    }
                    "check" => {
                        if completed.len() == 2 {
                            return Ok((current_start, self.permission_user_pairs(current)));
                        }
                        if completed.len() == 3 {
                            return Ok((current_start, self.permission_node_pairs(current)));
                        }
                    }
                    "user" => {
                        if completed.len() == 2 {
                            return Ok((
                                current_start,
                                filter_pairs(
                                    current,
                                    [
                                        ("list", "list  - list permission users"),
                                        ("create", "create  - ensure a UUID exists"),
                                        ("info", "info  - show a user's direct permissions"),
                                        ("node", "node  - add/remove user nodes"),
                                        ("group", "group  - add/remove user groups"),
                                        ("effective", "effective  - show effective rules"),
                                    ],
                                ),
                            ));
                        }
                        if completed.len() >= 3 {
                            match completed[2].to_ascii_lowercase().as_str() {
                                "info" | "effective" if completed.len() == 3 => {
                                    return Ok((
                                        current_start,
                                        self.permission_user_pairs(current),
                                    ));
                                }
                                "node" => {
                                    if completed.len() == 3 {
                                        return Ok((
                                            current_start,
                                            filter_pairs(
                                                current,
                                                [
                                                    ("add", "add  - grant/deny a user node"),
                                                    (
                                                        "remove",
                                                        "remove  - remove a direct user node",
                                                    ),
                                                ],
                                            ),
                                        ));
                                    }
                                    if completed.len() == 4 {
                                        return Ok((
                                            current_start,
                                            self.permission_user_pairs(current),
                                        ));
                                    }
                                    if completed.len() == 5 {
                                        return Ok((
                                            current_start,
                                            self.permission_node_pairs(current),
                                        ));
                                    }
                                }
                                "group" => {
                                    if completed.len() == 3 {
                                        return Ok((
                                            current_start,
                                            filter_pairs(
                                                current,
                                                [
                                                    ("add", "add  - add user to group"),
                                                    ("remove", "remove  - remove user from group"),
                                                ],
                                            ),
                                        ));
                                    }
                                    if completed.len() == 4 {
                                        return Ok((
                                            current_start,
                                            self.permission_user_pairs(current),
                                        ));
                                    }
                                    if completed.len() == 5 {
                                        return Ok((
                                            current_start,
                                            self.permission_group_pairs(current),
                                        ));
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    "group" => {
                        if completed.len() == 2 {
                            return Ok((
                                current_start,
                                filter_pairs(
                                    current,
                                    [
                                        ("list", "list  - list permission groups"),
                                        ("create", "create  - ensure a group exists"),
                                        ("info", "info  - show a group's direct permissions"),
                                        ("node", "node  - add/remove group nodes"),
                                        ("parent", "parent  - add/remove group inheritance"),
                                    ],
                                ),
                            ));
                        }
                        if completed.len() >= 3 {
                            match completed[2].to_ascii_lowercase().as_str() {
                                "info" if completed.len() == 3 => {
                                    return Ok((
                                        current_start,
                                        self.permission_group_pairs(current),
                                    ));
                                }
                                "node" => {
                                    if completed.len() == 3 {
                                        return Ok((
                                            current_start,
                                            filter_pairs(
                                                current,
                                                [
                                                    ("add", "add  - add group node"),
                                                    ("remove", "remove  - remove group node"),
                                                ],
                                            ),
                                        ));
                                    }
                                    if completed.len() == 4 {
                                        return Ok((
                                            current_start,
                                            self.permission_group_pairs(current),
                                        ));
                                    }
                                    if completed.len() == 5 {
                                        return Ok((
                                            current_start,
                                            self.permission_node_pairs(current),
                                        ));
                                    }
                                }
                                "parent" => {
                                    if completed.len() == 3 {
                                        return Ok((
                                            current_start,
                                            filter_pairs(
                                                current,
                                                [
                                                    ("add", "add  - add parent group"),
                                                    ("remove", "remove  - remove parent group"),
                                                ],
                                            ),
                                        ));
                                    }
                                    if completed.len() == 4 || completed.len() == 5 {
                                        return Ok((
                                            current_start,
                                            self.permission_group_pairs(current),
                                        ));
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    _ => {}
                }

                if matches!(
                    completed.as_slice(),
                    ["/perm", "path", "set", ..]
                        | ["/perm", "load", "from", ..]
                        | ["/perm", "save", "to", ..]
                ) {
                    return self.filenames.complete(line, pos, ctx);
                }
            }
            _ => {}
        }

        Ok((current_start, Vec::new()))
    }
}

impl Helper for ConsoleHelper {}
impl Highlighter for ConsoleHelper {}
impl Validator for ConsoleHelper {}

impl Hinter for ConsoleHelper {
    type Hint = String;
}

impl Completer for ConsoleHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        ctx: &LineEditorContext<'_>,
    ) -> rustyline::Result<(usize, Vec<Self::Candidate>)> {
        self.completion_pairs(line, pos, ctx)
    }
}

fn starts_with_ignore_ascii_case(value: &str, prefix: &str) -> bool {
    value
        .get(..prefix.len())
        .is_some_and(|start| start.eq_ignore_ascii_case(prefix))
}

fn filter_pairs<const N: usize>(current: &str, values: [(&str, &str); N]) -> Vec<Pair> {
    values
        .into_iter()
        .filter(|(replacement, _)| starts_with_ignore_ascii_case(replacement, current))
        .map(|(replacement, display)| Pair {
            display: display.to_string(),
            replacement: replacement.to_string(),
        })
        .collect()
}

// Tokio's worker threads otherwise retain glibc per-thread arenas after burst churn.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn bsr_window_metrics(snapshot: BsrProfilerSnapshot) -> BsrWindowMetrics {
    let ticks = snapshot.ticks.max(1) as f64;
    let per_tick_ms = |micros: u64| micros as f64 / 1000.0 / ticks;
    let ratio = |compressed: u64, raw: u64| {
        if raw == 0 {
            0.0
        } else {
            compressed as f64 / raw as f64
        }
    };
    let average = |total: u64, count: u64| {
        if count == 0 {
            0.0
        } else {
            total as f64 / count as f64
        }
    };

    let lz4_emitted = snapshot
        .bundles_emitted
        .saturating_sub(snapshot.bundle_zstd_emitted);
    let lz4_raw = snapshot
        .bundle_raw_bytes
        .saturating_sub(snapshot.bundle_zstd_raw_bytes);
    let lz4_compressed = snapshot
        .bundle_compressed_bytes
        .saturating_sub(snapshot.bundle_zstd_compressed_bytes);
    let lz4_micros = snapshot
        .bundle_deflate_micros
        .saturating_sub(snapshot.bundle_zstd_micros);

    BsrWindowMetrics {
        captured_time: format_system_time(snapshot.captured_at),
        ticks: snapshot.ticks,
        messages: snapshot.messages,
        sends: snapshot.sends,
        pre_serialized: snapshot.pre_serializations,
        pre_serialized_skipped: snapshot.pre_serializations_skipped,
        ms_per_tick: BsrMsPerTickMetrics {
            drain: per_tick_ms(snapshot.drain_micros),
            process: per_tick_ms(snapshot.process_micros),
            distance: per_tick_ms(snapshot.distance_micros),
            update: per_tick_ms(snapshot.update_micros),
            trigger: per_tick_ms(snapshot.trigger_micros),
            total: per_tick_ms(
                snapshot
                    .drain_micros
                    .saturating_add(snapshot.process_micros)
                    .saturating_add(snapshot.distance_micros)
                    .saturating_add(snapshot.update_micros)
                    .saturating_add(snapshot.trigger_micros),
            ),
        },
        bundles: BsrBundleMetrics {
            emitted: snapshot.bundles_emitted,
            messages: snapshot.bundle_messages,
            tail_uncompressed: snapshot.bundle_tail_uncompressed,
            fallbacks: snapshot.bundle_fallbacks,
            retries: snapshot.bundle_retries,
            raw_bytes: snapshot.bundle_raw_bytes,
            compressed_bytes: snapshot.bundle_compressed_bytes,
            saved_bytes: snapshot
                .bundle_raw_bytes
                .saturating_sub(snapshot.bundle_compressed_bytes),
            ratio: ratio(snapshot.bundle_compressed_bytes, snapshot.bundle_raw_bytes),
            per_tick: snapshot.bundles_emitted as f64 / ticks,
            avg_messages: average(snapshot.bundle_messages, snapshot.bundles_emitted),
            deflate_ms_per_tick: per_tick_ms(snapshot.bundle_deflate_micros),
            avg_deflate_us: average(snapshot.bundle_deflate_micros, snapshot.bundles_emitted),
            zstd: BsrZstdMetrics {
                dict_generation: u64::from(AVATAR_BUNDLE_DICTIONARY_GENERATION),
                emitted: snapshot.bundle_zstd_emitted,
                share_of_bundles: average(snapshot.bundle_zstd_emitted, snapshot.bundles_emitted),
                raw_bytes: snapshot.bundle_zstd_raw_bytes,
                compressed_bytes: snapshot.bundle_zstd_compressed_bytes,
                ratio: ratio(
                    snapshot.bundle_zstd_compressed_bytes,
                    snapshot.bundle_zstd_raw_bytes,
                ),
                ms_per_tick: per_tick_ms(snapshot.bundle_zstd_micros),
                avg_us: average(snapshot.bundle_zstd_micros, snapshot.bundle_zstd_emitted),
                lz4_ratio: ratio(lz4_compressed, lz4_raw),
                lz4_avg_us: average(lz4_micros, lz4_emitted),
            },
        },
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(args.log_level.clone())
        .init();

    let base_dir = args
        .base_dir
        .clone()
        .unwrap_or(std::env::current_dir().context("resolving current directory")?);
    std::fs::create_dir_all(base_dir.join(ServerConfig::CONFIG_FOLDER_NAME))?;
    std::fs::create_dir_all(base_dir.join(ServerConfig::LOGS_FOLDER_NAME))?;

    let config_path = if args.config.is_absolute() {
        args.config.clone()
    } else {
        base_dir.join(&args.config)
    };
    let mut config = ServerConfig::load_or_create(&config_path)?;
    config.process_environment_overrides();
    if args.no_console {
        config.enable_console = false;
    }
    if let Some(port) = args.port {
        config.set_port = port;
    }
    if let Some(host) = args.health_host {
        config.health_check_host = host;
    }
    if let Some(port) = args.health_port {
        config.health_check_port = port;
    }

    migrate_legacy_resource_dirs(&base_dir)?;
    std::fs::create_dir_all(base_dir.join(ServerConfig::INITIAL_RESOURCES_FOLDER_NAME))?;
    std::fs::create_dir_all(base_dir.join(ServerConfig::DEFAULT_LIBRARY_FOLDER_NAME))?;

    info!("Server Booting");
    let (server, shutdown_tx) = ServerState::start(config.clone(), &base_dir).await?;
    let _health_addr = start_health_server(HealthState {
        config: server.config.clone(),
        player_count: Arc::new({
            let server = server.clone();
            move || server.player_count()
        }),
        statistics: Arc::new({
            let server = server.clone();
            move || {
                let transport = server.transport.stats_snapshot();
                HealthStatistics {
                    sent: transport.raw_bytes_sent,
                    recv: transport.raw_bytes_received,
                    packets_sent: transport.raw_packets_sent,
                    packets_recv: transport.raw_packets_received,
                    dropped_unreliable: transport.raw_send_would_block,
                    dropped_voice: 0,
                    queue_per_peer: 0,
                    voice_queue_per_peer: 0,
                }
            }
        }),
        extended_metrics: Arc::new({
            let server = server.clone();
            move || {
                let transport = server.transport.stats_snapshot();
                let app = server.statistics.snapshot();
                let avatar = server.avatar_sync.stats();
                ExtendedHealthMetrics {
                    reliable: ReliableMetrics {
                        pending: server.transport.pending_reliable_count(),
                        queued: server.transport.queued_reliable_count(),
                        window_fills: transport.reliable_window_fills,
                        retransmits: transport.reliable_retransmits,
                        dispatch_passes: transport.reliable_dispatch_passes,
                        peers_visited: transport.reliable_peers_visited,
                        acks_in: transport.reliable_acks_received,
                        acks_released: transport.reliable_acks_released,
                        acks_unknown_channel: transport.reliable_acks_unknown_channel,
                        window_stalls: transport.reliable_window_stalls,
                    },
                    app_messages: AppMessageMetrics {
                        inbound: app.inbound_packets,
                        outbound: app.outbound_packets,
                        protocol_errors: app.protocol_errors,
                    },
                    raw_udp: RawUdpMetrics {
                        packets_in: transport.raw_packets_received,
                        packets_out: transport.raw_packets_sent,
                        bytes_in: transport.raw_bytes_received,
                        bytes_out: transport.raw_bytes_sent,
                        would_block: transport.raw_send_would_block,
                    },
                    avatar_sync: AvatarSyncMetrics {
                        inbound_updates: avatar.inbound_updates,
                        outbound_messages: avatar.outbound_messages,
                        outbound_logical_avatar_sends: avatar.outbound_logical_avatar_sends,
                        outbound_batches: avatar.outbound_batches,
                        active_states: avatar.active_states,
                        pending_updates: avatar.pending_updates,
                        receiver_slices: avatar.slice_count,
                    },
                    avatar_timing: AvatarTimingMetrics {
                        ticks: avatar.tick_count,
                        avg_tick_us: avatar.avg_tick_micros,
                        smooth_tick_us: avatar.smoothed_tick_micros,
                        avg_build_us: avatar
                            .build_micros
                            .checked_div(avatar.tick_count)
                            .unwrap_or(0),
                        avg_flush_us: avatar
                            .flush_micros
                            .checked_div(avatar.tick_count)
                            .unwrap_or(0),
                        max_tick_us: avatar.max_tick_micros,
                        receiver_cycle_ms: avatar.receiver_cycle_micros / 1000,
                        cycle_budget_ms: avatar.receiver_cycle_budget_micros / 1000,
                        tick_budget_ms: avatar.tick_budget_micros / 1000,
                    },
                }
            }
        }),
        bsr_metrics: Arc::new({
            let server = server.clone();
            move || {
                let config = server.config.read().clone();
                let avatar = server.avatar_sync.stats();
                let interval_ms = config.bsrsmillisecond_default_interval.max(1) as u64;
                BsrHealthMetrics {
                    load: BsrLoadMetrics {
                        tick_ms: avatar.smoothed_tick_micros as f64 / 1000.0,
                        overrun_ratio: 0.0,
                        interval_ms,
                        hz: 1000 / interval_ms,
                        shed_tier: 0,
                        shed_tier_name: "not_available".to_string(),
                        slice_count: avatar.slice_count,
                        send_workers: 0,
                        send_worker_cap: config.bsrmax_degree_of_parallelism.max(0) as usize,
                        send_budget_percent: config.bsrsend_phase_budget_percent,
                        send_duty: 0.0,
                        pairs_per_worker_ms: 0.0,
                    },
                    window: server
                        .avatar_sync
                        .bsr_profile_snapshot()
                        .map(bsr_window_metrics),
                }
            }
        }),
    })
    .await?;

    let running = Arc::new(AtomicBool::new(true));
    let console_running = running.clone();
    if config.enable_console {
        start_console_listener(server.clone(), config_path.clone(), console_running);
    }
    if let Ok(interval) = std::env::var("BASIS_STATUS_INTERVAL_SECS") {
        if let Ok(seconds) = interval.parse::<u64>() {
            if seconds > 0 {
                let server = server.clone();
                let running = running.clone();
                thread::spawn(move || {
                    while running.load(Ordering::Relaxed) {
                        thread::sleep(std::time::Duration::from_secs(seconds));
                        println!("{}", server.status_text_with_detail(true));
                    }
                });
            }
        }
    }

    loop {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(err) = result {
                    warn!("failed to listen for Ctrl+C: {err}");
                }
                running.store(false, Ordering::SeqCst);
                break;
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                if !running.load(Ordering::Relaxed) {
                    break;
                }
            }
        }
    }
    info!("Shutting down server...");
    request_shutdown(shutdown_tx);
    match tokio::time::timeout(std::time::Duration::from_secs(5), server.shutdown()).await {
        Ok(result) => result?,
        Err(_) => warn!("server shutdown timed out; exiting process"),
    }
    info!("Server shut down successfully.");
    Ok(())
}

fn request_shutdown(shutdown_tx: oneshot::Sender<()>) {
    let _ = shutdown_tx.send(());
}

fn start_console_listener(server: ServerState, config_path: PathBuf, running: Arc<AtomicBool>) {
    let runtime = tokio::runtime::Handle::current();
    thread::spawn(move || {
        let mut commands: HashMap<String, CommandHandler> = HashMap::new();
        let config_dirty = Arc::new(AtomicBool::new(false));
        {
            let server = server.clone();
            commands.insert(
                "/players".to_string(),
                Box::new(move |_| println!("{}", server.players_text())),
            );
        }
        {
            let server = server.clone();
            let status_running = running.clone();
            commands.insert(
                "/status".to_string(),
                Box::new(move |args| {
                    if args.first().is_some_and(|arg| {
                        arg.eq_ignore_ascii_case("live") || arg.eq_ignore_ascii_case("watch")
                    }) {
                        run_live_status(&server, &status_running);
                    } else if args.first().is_some_and(|arg| {
                        arg.eq_ignore_ascii_case("verbose") || arg.eq_ignore_ascii_case("-v")
                    }) {
                        println!("{}", server.status_text_with_detail(true));
                    } else {
                        println!("{}", server.status_text());
                    }
                }),
            );
        }
        {
            let running = running.clone();
            let dirty = config_dirty.clone();
            commands.insert(
                "/shutdown".to_string(),
                Box::new(move |_| {
                    if dirty.load(Ordering::Relaxed) {
                        println!("Warning: unsaved live config changes will be lost. Use /config save to persist them.");
                    }
                    println!("Shutting down the server...");
                    running.store(false, Ordering::SeqCst);
                }),
            );
        }
        commands.insert(
            "/clear".to_string(),
            Box::new(move |_| {
                print!("\x1B[2J\x1B[1;1H");
            }),
        );
        {
            let server = server.clone();
            let path = config_path.clone();
            let dirty = config_dirty.clone();
            let runtime = runtime.clone();
            commands.insert(
                "/config".to_string(),
                Box::new(move |args| handle_config_command(&server, &path, &dirty, &runtime, args)),
            );
        }
        {
            let server = server.clone();
            commands.insert(
                "/perm".to_string(),
                Box::new(move |args| handle_perm_command(&server, args)),
            );
        }
        commands.insert(
            "/help".to_string(),
            Box::new(move |_| {
                println!("Available commands:");
                println!("/players - Lists all connected players.");
                println!("/status - Shows the current server status.");
                println!(
                    "/status verbose - Shows detailed counters when HealthIncludeExtendedMetrics is enabled."
                );
                println!(
                    "/status live - Live status view. Press v for verbose, q to quit."
                );
                println!("/shutdown - Shuts down the server.");
                println!("/help - Displays all available commands.");
                println!("/clear - Clears the console.");
                println!("/config [list] - Lists current config values.");
                println!("/config get <field> - Reads a config value.");
                println!("/config set <field> <value> - Applies a config value live without saving.");
                println!("/config save - Saves the current in-memory config to config.xml.");
                println!("/config <field> [value] - Legacy read/live-set syntax.");
                println!("/perm help - Shows permission command help.");
            }),
        );

        let editor_config = LineEditorConfig::builder()
            .completion_type(CompletionType::List)
            .build();
        let Ok(mut editor) = Editor::<ConsoleHelper, DefaultHistory>::with_config(editor_config)
        else {
            println!("Failed to initialize interactive console editor.");
            return;
        };
        editor.set_helper(Some(ConsoleHelper::new(server.clone())));

        loop {
            let line = match editor.readline("> ") {
                Ok(line) => line,
                Err(ReadlineError::Interrupted) => {
                    if config_dirty.load(Ordering::Relaxed) {
                        println!("Warning: unsaved live config changes will be lost. Use /config save to persist them.");
                    }
                    println!("Shutting down the server...");
                    running.store(false, Ordering::SeqCst);
                    break;
                }
                Err(ReadlineError::Eof) => break,
                Err(err) => {
                    println!("Console input error: {err}");
                    break;
                }
            };
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let _ = editor.add_history_entry(line);
            let parts: Vec<_> = line.split_whitespace().collect();
            let mut matched = false;
            for len in (1..=parts.len()).rev() {
                let potential = parts[..len].join(" ").to_ascii_lowercase();
                if let Some(handler) = commands.get(&potential) {
                    handler(&parts[len..]);
                    matched = true;
                    break;
                }
            }
            if !matched {
                println!("Unknown command. Type /help for available commands.");
            }
            if !running.load(Ordering::Relaxed) {
                break;
            }
        }
    });
}

fn handle_config_command(
    server: &ServerState,
    config_path: &Path,
    dirty: &AtomicBool,
    runtime: &tokio::runtime::Handle,
    args: &[&str],
) {
    match args {
        [] | ["list"] => print_config(server, dirty.load(Ordering::Relaxed)),
        ["save"] => match server.config.read().save(config_path) {
            Ok(()) => {
                dirty.store(false, Ordering::Relaxed);
                println!("Saved config to: {}", config_path.display());
            }
            Err(err) => println!("Failed to save config: {err}"),
        },
        ["get", field] => print_config_field(server, field),
        ["set", field, rest @ ..] if !rest.is_empty() => {
            set_config_field_live(server, field, &rest.join(" "), dirty, runtime);
        }
        ["get"] => println!("Usage: /config get <field>"),
        ["set", ..] => println!("Usage: /config set <field> <value>"),
        [field] => print_config_field(server, field),
        [field, rest @ ..] => {
            set_config_field_live(server, field, &rest.join(" "), dirty, runtime);
        }
    }
}

fn print_config(server: &ServerState, dirty: bool) {
    let config = server.config.read();
    let mut fields = config.field_names();
    fields.sort_by_key(|field| field.to_ascii_lowercase());
    println!(
        "{} settings{}:",
        fields.len(),
        if dirty { " (unsaved live changes)" } else { "" }
    );
    for field in fields {
        let value = display_config_value(&config, &field);
        println!("  {field} = {value}");
    }
}

fn print_config_field(server: &ServerState, field: &str) {
    let config = server.config.read();
    if config.get_field(field).is_none() {
        println!("Unknown config field {field}");
        return;
    }
    println!("{field}: {}", display_config_value(&config, field));
}

fn display_config_value(config: &ServerConfig, field: &str) -> String {
    if ServerConfig::is_secret_field_name(field) {
        return match config.get_field(field).as_deref() {
            Some("") | None => "<empty>".to_string(),
            Some(_) => "<redacted>".to_string(),
        };
    }
    config.get_field(field).unwrap_or_default()
}

fn set_config_field_live(
    server: &ServerState,
    field: &str,
    value: &str,
    dirty: &AtomicBool,
    runtime: &tokio::runtime::Handle,
) {
    {
        let mut config = server.config.write();
        if let Err(err) = config.set_field(field, value) {
            println!("Failed to set {field}: {err}");
            return;
        }
    }

    runtime.block_on(server.refresh_runtime_config_live());
    dirty.store(true, Ordering::Relaxed);

    let displayed_value = if ServerConfig::is_secret_field_name(field) {
        "<redacted>"
    } else {
        value
    };
    if config_field_requires_restart(field) {
        println!(
            "Set {field} to {displayed_value} in the live config. This field requires a server restart to fully take effect. Use /config save to persist it."
        );
    } else {
        println!("Set {field} to {displayed_value} live. Use /config save to persist it.");
    }
}

fn config_field_requires_restart(field: &str) -> bool {
    [
        "NetworkStackId",
        "SetPort",
        "UseNativeSockets",
        "IPv6Enabled",
        "OverrideAutoDiscoveryOfIpv",
        "IPv4Address",
        "IPv6Address",
        "HealthCheckHost",
        "HealthCheckPort",
        "HealthPath",
        "EnableConsole",
    ]
    .iter()
    .any(|candidate| candidate.eq_ignore_ascii_case(field))
}

fn run_live_status(server: &ServerState, running: &Arc<AtomicBool>) {
    let mut stdout = io::stdout();
    let mut verbose = false;
    let mut rendered_rows = 0u16;
    let raw_mode_enabled = terminal::enable_raw_mode().is_ok();
    println!();

    loop {
        if rendered_rows > 0 {
            move_to_render_start(&mut stdout, rendered_rows);
            clear_rendered_rows(&mut stdout, rendered_rows);
            move_to_render_start(&mut stdout, rendered_rows);
        }

        let text = format!(
            "{}\n\n[q] quit  [v] {} verbose",
            server.status_text_with_detail(verbose),
            if verbose { "hide" } else { "show" }
        );
        let next_rows = physical_row_count(&text);
        let line_count = text.lines().count();
        for (index, line) in text.lines().enumerate() {
            let _ = execute!(
                stdout,
                cursor::MoveToColumn(0),
                terminal::Clear(ClearType::CurrentLine)
            );
            print!("{line}");
            if index + 1 < line_count {
                println!();
            }
        }
        rendered_rows = next_rows;
        let _ = stdout.flush();

        match event::poll(std::time::Duration::from_millis(500)) {
            Ok(true) => match event::read() {
                Ok(Event::Key(key))
                    if matches!(
                        key.kind,
                        crossterm::event::KeyEventKind::Press
                            | crossterm::event::KeyEventKind::Repeat
                    ) =>
                {
                    match key.code {
                        KeyCode::Char('c')
                            if key
                                .modifiers
                                .contains(crossterm::event::KeyModifiers::CONTROL) =>
                        {
                            running.store(false, Ordering::SeqCst);
                            break;
                        }
                        KeyCode::Char('q') | KeyCode::Esc => break,
                        KeyCode::Char('v') => verbose = !verbose,
                        _ => {}
                    }
                }
                Ok(_) => {}
                Err(_) => break,
            },
            Ok(false) => {}
            Err(_) => break,
        }
    }

    if raw_mode_enabled {
        let _ = terminal::disable_raw_mode();
    }
    println!();
}

fn move_to_render_start(stdout: &mut io::Stdout, rendered_rows: u16) {
    let _ = execute!(stdout, cursor::MoveToColumn(0));
    if rendered_rows > 1 {
        let _ = execute!(stdout, cursor::MoveUp(rendered_rows - 1));
    }
}

fn clear_rendered_rows(stdout: &mut io::Stdout, rendered_rows: u16) {
    for row in 0..rendered_rows {
        let _ = execute!(
            stdout,
            cursor::MoveToColumn(0),
            terminal::Clear(ClearType::CurrentLine)
        );
        if row + 1 < rendered_rows {
            let _ = execute!(stdout, cursor::MoveDown(1));
        }
    }
}

fn physical_row_count(text: &str) -> u16 {
    let width = terminal::size()
        .map(|(width, _)| width.saturating_sub(1).max(1))
        .unwrap_or(79) as usize;
    text.lines()
        .map(|line| {
            let chars = line.chars().count();
            ((chars / width) + 1) as u16
        })
        .sum::<u16>()
        .max(1)
}

fn handle_perm_command(server: &ServerState, args: &[&str]) {
    match args {
        [] | ["help"] => {
            println!("Permission commands:");
            println!("/perm path");
            println!("/perm path set <path>");
            println!("/perm load");
            println!("/perm load from <path>");
            println!("/perm save");
            println!("/perm save to <path>");
            println!("/perm reload");
            println!("/perm defaults");
            println!();
            println!("/perm user list");
            println!("/perm check <uuid> <node>");
            println!("/perm user create <uuid>");
            println!("/perm user info <uuid>");
            println!("/perm user node add <uuid> <node>");
            println!("/perm user node remove <uuid> <node>");
            println!("/perm user group add <uuid> <group>");
            println!("/perm user group remove <uuid> <group>");
            println!("/perm user effective <uuid>");
            println!();
            println!("/perm group list");
            println!("/perm group create <name>");
            println!("/perm group info <name>");
            println!("/perm group node add <group> <node>");
            println!("/perm group node remove <group> <node>");
            println!("/perm group parent add <group> <parent>");
            println!("/perm group parent remove <group> <parent>");
            println!();
            println!("Notes: Use '-node' to deny when adding nodes.");
        }
        ["path"] => println!(
            "permissions.xml path: {}",
            server.permissions.get_xml_path().display()
        ),
        ["path", "set", rest @ ..] if !rest.is_empty() => {
            let path = PathBuf::from(rest.join(" "));
            server.permissions.set_xml_path(path);
            println!(
                "Set permissions.xml path to: {}",
                server.permissions.get_xml_path().display()
            );
        }
        ["load"] => match server.permissions.load_from_xml() {
            Ok(()) => println!(
                "Loaded permissions from: {}",
                server.permissions.get_xml_path().display()
            ),
            Err(err) => println!("Failed to load permissions: {err}"),
        },
        ["load", "from", rest @ ..] if !rest.is_empty() => {
            let path = PathBuf::from(rest.join(" "));
            match server.permissions.load_from_xml_path(path.clone()) {
                Ok(()) => println!("Loaded permissions from: {}", path.display()),
                Err(err) => println!("Failed to load permissions: {err}"),
            }
        }
        ["save"] => match server.permissions.save_to_xml() {
            Ok(()) => println!(
                "Saved permissions to: {}",
                server.permissions.get_xml_path().display()
            ),
            Err(err) => println!("Failed to save permissions: {err}"),
        },
        ["save", "to", rest @ ..] if !rest.is_empty() => {
            let path = PathBuf::from(rest.join(" "));
            match server.permissions.save_to_xml_path(&path) {
                Ok(()) => println!("Saved permissions to: {}", path.display()),
                Err(err) => println!("Failed to save permissions: {err}"),
            }
        }
        ["reload"] => match server.permissions.save_to_xml() {
            Ok(()) => match server.permissions.load_from_xml() {
                Ok(()) => println!("Reloaded permissions (save -> load)."),
                Err(err) => println!("Saved permissions, but failed to load: {err}"),
            },
            Err(err) => println!("Failed to save permissions: {err}"),
        },
        ["defaults"] => {
            server.permissions.ensure_defaults();
            println!("Ensured default permission groups.");
        }
        ["user", "list"] => {
            let snapshot = server.permissions.snapshot();
            if snapshot.users.is_empty() {
                println!("No users.");
            } else {
                println!("Users ({}):", snapshot.users.len());
                for uuid in sorted_keys(snapshot.users.keys()) {
                    println!("- {uuid}");
                }
            }
        }
        ["check", uuid, rest @ ..] if !rest.is_empty() => {
            let node = rest.join(" ");
            println!(
                "Check: uuid={} node={} => {}",
                uuid,
                node,
                if server.permissions.has(uuid, &node) {
                    "ALLOW"
                } else {
                    "DENY"
                }
            );
        }
        ["user", "create", uuid] => {
            server.permissions.get_or_create_user(uuid);
            println!("User ensured: {uuid}");
        }
        ["user", "info", uuid] => {
            let snapshot = server.permissions.snapshot();
            if let Some(user) = snapshot.users.get(*uuid) {
                println!("User: {}", user.uuid);
                println!(
                    "Groups ({}): {}",
                    user.groups.len(),
                    sorted_values(&user.groups)
                );
                println!(
                    "Nodes ({}): {}",
                    user.nodes.len(),
                    sorted_values(&user.nodes)
                );
            } else {
                println!("User not found: {uuid}");
            }
        }
        ["user", "node", "add", uuid, rest @ ..] if !rest.is_empty() => {
            let node = rest.join(" ");
            server.permissions.add_user_node(uuid, &node);
            println!("Added user node: {uuid} -> {node}");
        }
        ["user", "node", "remove", uuid, rest @ ..] if !rest.is_empty() => {
            let node = rest.join(" ");
            let snapshot = server.permissions.snapshot();
            match snapshot.users.get(*uuid) {
                Some(user) if user.nodes.contains(&node) => {
                    server.permissions.remove_user_node(uuid, &node);
                    println!("Removed user node: {uuid} -> {node}");
                }
                Some(_) => println!("User node not found: {uuid} -> {node}"),
                None => println!("User not found: {uuid}"),
            }
        }
        ["user", "group", "add", uuid, rest @ ..] if !rest.is_empty() => {
            let group = rest.join(" ");
            server.permissions.add_user_to_group(uuid, &group);
            println!("Added user to group: {uuid} -> {group}");
        }
        ["user", "group", "remove", uuid, rest @ ..] if !rest.is_empty() => {
            let group = rest.join(" ");
            let snapshot = server.permissions.snapshot();
            match snapshot.users.get(*uuid) {
                Some(user) if user.groups.contains(&group) => {
                    server.permissions.remove_user_from_group(uuid, &group);
                    println!("Removed user from group: {uuid} -> {group}");
                }
                Some(_) => println!("User group not found: {uuid} -> {group}"),
                None => println!("User not found: {uuid}"),
            }
        }
        ["user", "effective", uuid] => {
            let mut allowed = server.permissions.allowed_rules(uuid);
            let mut denied = server.permissions.denied_rules(uuid);
            sort_case_insensitive(&mut allowed);
            sort_case_insensitive(&mut denied);
            println!("Effective rules for {uuid}:");
            println!("Allowed ({}): {}", allowed.len(), display_list(&allowed));
            println!("Denied ({}): {}", denied.len(), display_list(&denied));
        }
        ["group", "list"] => {
            let snapshot = server.permissions.snapshot();
            if snapshot.groups.is_empty() {
                println!("No groups.");
            } else {
                println!("Groups ({}):", snapshot.groups.len());
                for group in sorted_keys(snapshot.groups.keys()) {
                    println!("- {group}");
                }
            }
        }
        ["group", "create", rest @ ..] if !rest.is_empty() => {
            let group = rest.join(" ");
            server.permissions.get_or_create_group(&group);
            println!("Group ensured: {group}");
        }
        ["group", "info", rest @ ..] if !rest.is_empty() => {
            let group = rest.join(" ");
            let snapshot = server.permissions.snapshot();
            if let Some(group_info) = snapshot.groups.get(&group) {
                println!("Group: {}", group_info.name);
                println!(
                    "Parents ({}): {}",
                    group_info.parents.len(),
                    sorted_values(&group_info.parents)
                );
                println!(
                    "Nodes ({}): {}",
                    group_info.nodes.len(),
                    sorted_values(&group_info.nodes)
                );
            } else {
                println!("Group not found: {group}");
            }
        }
        ["group", "node", "add", group, rest @ ..] if !rest.is_empty() => {
            let node = rest.join(" ");
            server.permissions.add_group_node(group, &node);
            println!("Added group node: {group} -> {node}");
        }
        ["group", "node", "remove", group, rest @ ..] if !rest.is_empty() => {
            let node = rest.join(" ");
            server.permissions.remove_group_node(group, &node);
            println!("Removed group node: {group} -> {node}");
        }
        ["group", "parent", "add", group, rest @ ..] if !rest.is_empty() => {
            let parent = rest.join(" ");
            server.permissions.add_group_parent(group, &parent);
            println!("Added parent: {group} -> {parent}");
        }
        ["group", "parent", "remove", group, rest @ ..] if !rest.is_empty() => {
            let parent = rest.join(" ");
            server.permissions.remove_group_parent(group, &parent);
            println!("Removed parent: {group} -> {parent}");
        }
        _ => println!("Unknown /perm command. Type /perm help"),
    }
}

fn sorted_keys<'a, I>(keys: I) -> Vec<&'a String>
where
    I: Iterator<Item = &'a String>,
{
    let mut keys: Vec<_> = keys.collect();
    keys.sort_by_key(|key| key.to_ascii_lowercase());
    keys
}

fn sorted_values(values: &std::collections::HashSet<String>) -> String {
    let mut values: Vec<_> = values.iter().cloned().collect();
    sort_case_insensitive(&mut values);
    display_list(&values)
}

fn sort_case_insensitive(values: &mut [String]) {
    values.sort_by_key(|value| value.to_ascii_lowercase());
}

fn display_list(values: &[String]) -> String {
    if values.is_empty() {
        "(none)".to_string()
    } else {
        values.join(", ")
    }
}
