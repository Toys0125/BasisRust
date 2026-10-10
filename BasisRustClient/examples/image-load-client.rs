use basis_client_core::{run_image_benchmark, ImageBenchmarkOptions};
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(about = "Real Basis static-image share and cache-replay benchmark client")]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    ip: String,
    #[arg(long, default_value_t = 4296)]
    port: u16,
    #[arg(long, default_value_t = 500)]
    clients: usize,
    #[arg(long)]
    image: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    live_start_file: PathBuf,
    #[arg(long)]
    cache_start_file: PathBuf,
    #[arg(long, default_value_t = 3)]
    sharers: usize,
    #[arg(long, default_value_t = 3)]
    cache_recipients: usize,
    #[arg(long, default_value_t = 200)]
    egress_mbps: u32,
    #[arg(long)]
    workers: Option<usize>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_owned()))
        .try_init();
    let workers = args
        .workers
        .unwrap_or_else(|| num_cpus::get().saturating_sub(1).max(1));
    let options = ImageBenchmarkOptions {
        config_path: args.config,
        ip: args.ip,
        port: args.port,
        image_path: args.image,
        output_path: args.output,
        live_start_file: args.live_start_file,
        cache_start_file: args.cache_start_file,
        clients: args.clients,
        sharers: args.sharers,
        cache_recipients: args.cache_recipients,
        egress_megabits_per_second: args.egress_mbps,
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers.max(1))
        .enable_all()
        .thread_name("basis-image-load")
        .build()?;
    runtime.block_on(run_image_benchmark(options, workers))
}
