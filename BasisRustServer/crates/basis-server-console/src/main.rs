use anyhow::{Context, Result};
use basis_protocol::config::ServerConfig;
use basis_server_core::{migrate_legacy_resource_dirs, ServerState};
use basis_server_health::{start_health_server, HealthState};
use clap::Parser;
use crossterm::{
    cursor,
    event::{self, Event, KeyCode},
    execute,
    terminal::{self, ClearType},
};
use std::{
    collections::HashMap,
    io::{self, BufRead, Write},
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
                println!("/status verbose - Shows detailed counters.");
                println!("/status live - Live status view. Press v for verbose, q to quit.");
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
