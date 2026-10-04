mod cli;
mod console;

use anyhow::Result;
use clap::Parser;
use std::time::Duration;

#[cfg(all(windows, feature = "windows-mimalloc"))]
#[global_allocator]
// Default Windows allocator, matching the server; Linux keeps its existing path.
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()))
        .init();
    let args = cli::Args::parse();
    let worker_threads = std::env::var("BASIS_CLIENT_TOKIO_WORKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or_else(|| num_cpus::get().saturating_sub(1).max(1))
        .clamp(1, num_cpus::get().max(1));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .thread_name("basis-tokio")
        .build()?;
    let result = runtime.block_on(async {
        basis_client_core::run(args.into(), worker_threads, || {
            let (commands_tx, commands_rx) = tokio::sync::mpsc::unbounded_channel();
            tokio::spawn(console::console_input(commands_tx));
            commands_rx
        })
        .await
    });
    // Tokio's async stdin is backed by a blocking helper. Give normal tasks time to finish,
    // but do not wait forever for that helper when Ctrl+C interrupts the client.
    runtime.shutdown_timeout(Duration::from_secs(1));
    result
}
