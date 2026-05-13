use anyhow::{Context, Result};
use basis_protocol::config::ServerConfig;
use basis_server_core::{migrate_legacy_resource_dirs, ServerState};
use basis_server_health::{start_health_server, HealthState};
use clap::Parser;
use std::{
    collections::HashMap,
    io::{self, BufRead},
    path::PathBuf,
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
    })
    .await?;

    let running = Arc::new(AtomicBool::new(true));
    let console_running = running.clone();
    if config.enable_console {
        start_console_listener(server.clone(), config_path.clone(), console_running);
    }

    let signal_running = running.clone();
    tokio::spawn(async move {
        if let Err(err) = tokio::signal::ctrl_c().await {
            warn!("failed to listen for Ctrl+C: {err}");
            return;
        }
        signal_running.store(false, Ordering::SeqCst);
    });

    while running.load(Ordering::Relaxed) {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    info!("Shutting down server...");
    request_shutdown(shutdown_tx);
    server.shutdown().await?;
    info!("Server shut down successfully.");
    Ok(())
}

fn request_shutdown(shutdown_tx: oneshot::Sender<()>) {
    let _ = shutdown_tx.send(());
}

fn start_console_listener(server: ServerState, config_path: PathBuf, running: Arc<AtomicBool>) {
    thread::spawn(move || {
        let mut commands: HashMap<String, CommandHandler> = HashMap::new();
        {
            let server = server.clone();
            commands.insert(
                "/players".to_string(),
                Box::new(move |_| println!("{}", server.players_text())),
            );
        }
        {
            let server = server.clone();
            commands.insert(
                "/status".to_string(),
                Box::new(move |_| {
                    println!(
                        "Server is running and healthy. Players: {}",
                        server.player_count()
                    )
                }),
            );
        }
        {
            let running = running.clone();
            commands.insert(
                "/shutdown".to_string(),
                Box::new(move |_| {
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
            commands.insert(
                "/config".to_string(),
                Box::new(move |args| {
                    if args.is_empty() {
                        println!("Usage: /config <field> [value]");
                        return;
                    }
                    if args.len() == 1 {
                        match server.config.read().get_field(args[0]) {
                            Some(value) => println!("{}: {}", args[0], value),
                            None => println!("Unknown config field {}", args[0]),
                        }
                        return;
                    }
                    let value = args[1..].join(" ");
                    let mut config = server.config.write();
                    match config.set_field(args[0], &value) {
                        Ok(()) => {
                            drop(config);
                            server.refresh_runtime_config();
                            let config = server.config.read();
                            if let Err(err) = config.save(&path) {
                                println!("Set {}, but failed to save: {err}", args[0]);
                            } else {
                                println!("Set {} to {}", args[0], value);
                            }
                        }
                        Err(err) => println!("Failed to set {}: {err}", args[0]),
                    }
                }),
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
                println!("/shutdown - Shuts down the server.");
                println!("/help - Displays all available commands.");
                println!("/clear - Clears the console.");
                println!("/config <field> [value] - Reads or updates config.");
                println!("/perm help - Shows permission command help.");
            }),
        );

        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            let Ok(line) = line else {
                break;
            };
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
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

fn handle_perm_command(server: &ServerState, args: &[&str]) {
    match args {
        [] | ["help"] => {
            println!("Permission commands:");
            println!("/perm path");
            println!("/perm load");
            println!("/perm save");
            println!("/perm defaults");
            println!("/perm check <uuid> <node>");
            println!("/perm user create <uuid>");
            println!("/perm user node add <uuid> <node>");
            println!("/perm user group add <uuid> <group>");
        }
        ["path"] => println!(
            "permissions.xml path: {}",
            server.permissions.get_xml_path().display()
        ),
        ["load"] => match server.permissions.load_from_xml() {
            Ok(()) => println!("Loaded permissions."),
            Err(err) => println!("Failed to load permissions: {err}"),
        },
        ["save"] => match server.permissions.save_to_xml() {
            Ok(()) => println!("Saved permissions."),
            Err(err) => println!("Failed to save permissions: {err}"),
        },
        ["defaults"] => {
            server.permissions.ensure_defaults();
            println!("Ensured default permission groups.");
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
        ["user", "node", "add", uuid, rest @ ..] if !rest.is_empty() => {
            let node = rest.join(" ");
            server.permissions.add_user_node(uuid, &node);
            println!("Added user node: {uuid} -> {node}");
        }
        ["user", "group", "add", uuid, rest @ ..] if !rest.is_empty() => {
            let group = rest.join(" ");
            server.permissions.add_user_to_group(uuid, &group);
            println!("Added user to group: {uuid} -> {group}");
        }
        _ => println!("Unknown /perm command. Type /perm help"),
    }
}
