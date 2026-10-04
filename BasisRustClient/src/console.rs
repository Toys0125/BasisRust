use basis_client_core::ConsoleCommand;
use std::io::Write;
use tokio::{
    io::{self, AsyncBufReadExt, BufReader},
    sync::mpsc,
};

const CONSOLE_HELP: &str = "Console commands:\n  voice              enable voice simulation\n  add [count]        add clients (default: 100)\n  quit [batch] [ms]  disconnect in batches (default batch/delay: 100/250ms)";

#[derive(Debug, PartialEq, Eq)]
enum ConsoleInput {
    Client(ConsoleCommand),
    Help,
}

fn parse_console_command(line: &str) -> Option<ConsoleInput> {
    let mut parts = line.split_whitespace();
    let command = parts.next()?.to_ascii_lowercase();
    let command = match command.as_str() {
        "voice" | "v" => Some(ConsoleCommand::EnableVoice),
        "enable"
            if parts
                .next()
                .is_some_and(|value| value.eq_ignore_ascii_case("voice")) =>
        {
            Some(ConsoleCommand::EnableVoice)
        }
        "add" | "clients" => {
            let count = parts
                .next()
                .map(str::parse::<usize>)
                .transpose()
                .ok()?
                .unwrap_or(100);
            (count > 0).then_some(ConsoleCommand::AddClients(count))
        }
        "quit" | "exit" | "q" => {
            let batch_size = parts.next().map(str::parse::<usize>).transpose().ok()?;
            let delay_ms = parts.next().map(str::parse::<u64>).transpose().ok()?;
            Some(ConsoleCommand::Quit {
                batch_size,
                delay_ms,
            })
        }
        "help" | "h" | "?" => return Some(ConsoleInput::Help),
        _ => None,
    }?;
    Some(ConsoleInput::Client(command))
}

pub(crate) async fn console_input(commands: mpsc::UnboundedSender<ConsoleCommand>) {
    println!("{CONSOLE_HELP}");
    let stdin = BufReader::new(io::stdin());
    let mut lines = stdin.lines();
    print!("> ");
    let _ = std::io::stdout().flush();
    while let Ok(Some(line)) = lines.next_line().await {
        if let Some(input) = parse_console_command(&line) {
            let ConsoleInput::Client(command) = input else {
                println!("{CONSOLE_HELP}");
                print!("> ");
                let _ = std::io::stdout().flush();
                continue;
            };
            // Do not start another stdin read after quit. Tokio's stdin uses a blocking
            // helper thread, which can otherwise keep the runtime alive after shutdown.
            let quitting = matches!(command, ConsoleCommand::Quit { .. });
            if commands.send(command).is_err() || quitting {
                break;
            }
        } else if !line.trim().is_empty() {
            println!("Unknown command. Type 'help' for available commands.");
        }
        print!("> ");
        let _ = std::io::stdout().flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ConsoleInput::Client;
    #[test]
    fn console_commands_default_to_voice_add_100_and_batched_quit() {
        assert_eq!(
            parse_console_command("voice"),
            Some(Client(ConsoleCommand::EnableVoice))
        );
        assert_eq!(
            parse_console_command("enable voice"),
            Some(Client(ConsoleCommand::EnableVoice))
        );
        assert_eq!(
            parse_console_command("add"),
            Some(Client(ConsoleCommand::AddClients(100)))
        );
        assert_eq!(
            parse_console_command("add 250"),
            Some(Client(ConsoleCommand::AddClients(250)))
        );
        assert_eq!(
            parse_console_command("quit 25 500"),
            Some(Client(ConsoleCommand::Quit {
                batch_size: Some(25),
                delay_ms: Some(500),
            }))
        );
        assert_eq!(
            parse_console_command("q"),
            Some(Client(ConsoleCommand::Quit {
                batch_size: None,
                delay_ms: None,
            }))
        );
        for command in ["help", "h", "?"] {
            assert_eq!(parse_console_command(command), Some(ConsoleInput::Help));
        }
        assert_eq!(parse_console_command("unknown"), None);
    }
}
