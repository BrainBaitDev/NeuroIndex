// NeuroIndex Unified Server
// Runs both RESP and HTTP servers simultaneously

use clap::Parser;
use serde::Deserialize;
use std::path::PathBuf;
use tokio::signal;
use tokio::process::Child;
use tracing::{error, info, warn};

#[derive(Debug, Deserialize)]
struct Config {
    server: ServerConfig,
    storage: StorageConfig,
    #[allow(dead_code)]
    memory: MemoryConfig,
    #[allow(dead_code)]
    performance: PerformanceConfig,
    #[allow(dead_code)]
    logging: LoggingConfig,
}

#[derive(Debug, Deserialize)]
struct ServerConfig {
    resp_host: String,
    resp_port: u16,
    http_host: String,
    http_port: u16,
}

#[derive(Debug, Deserialize)]
struct StorageConfig {
    data_dir: String,
    #[allow(dead_code)]
    log_dir: String,
    #[allow(dead_code)]
    wal_enabled: bool,
    #[allow(dead_code)]
    wal_sync_interval_ms: u64,
    #[allow(dead_code)]
    snapshot_enabled: bool,
    #[allow(dead_code)]
    snapshot_interval_seconds: u64,
}

#[derive(Debug, Deserialize)]
struct MemoryConfig {
    #[allow(dead_code)]
    max_memory_mb: usize,
    #[allow(dead_code)]
    eviction_policy: String,
}

#[derive(Debug, Deserialize)]
struct PerformanceConfig {
    #[allow(dead_code)]
    worker_threads: usize,
    #[allow(dead_code)]
    io_buffer_size: usize,
}

#[derive(Debug, Deserialize)]
struct LoggingConfig {
    #[allow(dead_code)]
    level: String,
    #[allow(dead_code)]
    file: String,
}

#[derive(Parser, Debug)]
#[command(name = "neuroindex-server")]
#[command(about = "NeuroIndex - High-performance in-memory database", long_about = None)]
struct Args {
    /// Configuration file path
    #[arg(short, long, default_value = "/etc/neuroindex/config.toml")]
    config: PathBuf,

    /// RESP server port (overrides config file)
    #[arg(long)]
    resp_port: Option<u16>,

    /// HTTP server port (overrides config file)
    #[arg(long)]
    http_port: Option<u16>,

    /// Data directory for persistence (overrides config file)
    #[arg(short, long)]
    data_dir: Option<PathBuf>,

    /// Number of shards (power of 2)
    #[arg(long, default_value = "4")]
    shards_pow2: usize,

    /// Shard capacity (power of 2)
    #[arg(long, default_value = "16")]
    shard_cap_pow2: usize,

    /// Disable RESP server
    #[arg(long)]
    no_resp: bool,

    /// Disable HTTP server
    #[arg(long)]
    no_http: bool,
}

fn load_config(path: &PathBuf) -> Result<Config, Box<dyn std::error::Error>> {
    if !path.exists() {
        return Err(format!("Config file not found: {}", path.display()).into());
    }

    let content = std::fs::read_to_string(path)?;
    let config: Config = toml::from_str(&content)?;
    Ok(config)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .init();

    let args = Args::parse();

    info!("🚀 Starting NeuroIndex Server");

    // Load configuration
    let config = match load_config(&args.config) {
        Ok(cfg) => {
            info!("✅ Loaded configuration from: {}", args.config.display());
            cfg
        }
        Err(e) => {
            warn!("⚠️  Could not load config file: {}", e);
            warn!("   Using default values");
            // Return default config
            Config {
                server: ServerConfig {
                    resp_host: "127.0.0.1".to_string(),
                    resp_port: 6381,
                    http_host: "127.0.0.1".to_string(),
                    http_port: 8080,
                },
                storage: StorageConfig {
                    data_dir: "/var/lib/neuroindex".to_string(),
                    log_dir: "/var/log/neuroindex".to_string(),
                    wal_enabled: true,
                    wal_sync_interval_ms: 1000,
                    snapshot_enabled: true,
                    snapshot_interval_seconds: 3600,
                },
                memory: MemoryConfig {
                    max_memory_mb: 0,
                    eviction_policy: "lru".to_string(),
                },
                performance: PerformanceConfig {
                    worker_threads: 4,
                    io_buffer_size: 8192,
                },
                logging: LoggingConfig {
                    level: "info".to_string(),
                    file: "/var/log/neuroindex/neuroindex.log".to_string(),
                },
            }
        }
    };

    // Apply CLI overrides
    let resp_port = args.resp_port.unwrap_or(config.server.resp_port);
    let http_port = args.http_port.unwrap_or(config.server.http_port);
    let data_dir = args.data_dir.unwrap_or_else(|| PathBuf::from(&config.storage.data_dir));

    let shard_count = 1usize << args.shards_pow2;
    let shard_capacity = 1usize << args.shard_cap_pow2;

    info!("📋 Configuration:");
    info!("   RESP: {}:{}", config.server.resp_host, resp_port);
    info!("   HTTP: {}:{}", config.server.http_host, http_port);
    info!("   Data directory: {}", data_dir.display());
    info!("   Shards: {} (2^{})", shard_count, args.shards_pow2);
    info!("   Shard capacity: {} (2^{})", shard_capacity, args.shard_cap_pow2);

    // Create data directory if it doesn't exist
    if !data_dir.exists() {
        std::fs::create_dir_all(&data_dir)?;
        info!("   Created data directory");
    }

    let data_dir_str = data_dir.to_string_lossy().to_string();

    let mut children: Vec<(&str, Child)> = Vec::new();

    // Start RESP server if enabled
    if !args.no_resp {
        info!("🔌 Launching RESP server process");
        let resp_args = vec![
            "--host".to_string(),
            config.server.resp_host.clone(),
            "--port".to_string(),
            resp_port.to_string(),
            "--shards".to_string(),
            shard_count.to_string(),
            "--capacity".to_string(),
            shard_capacity.to_string(),
            "--persistence-dir".to_string(),
            data_dir_str.clone(),
        ];

        let child = launch_server_process("RESP", "neuroindex-resp-server", &resp_args)?;
        info!("✅ RESP server spawned (args: {:?})", resp_args);
        children.push(("RESP", child));
    }

    // Start HTTP server if enabled
    if !args.no_http {
        info!("🌐 Launching HTTP server process");
        let http_args = vec![
            "--host".to_string(),
            config.server.http_host.clone(),
            "--port".to_string(),
            http_port.to_string(),
            "--shards".to_string(),
            shard_count.to_string(),
            "--capacity".to_string(),
            shard_capacity.to_string(),
            "--persistence-dir".to_string(),
            data_dir_str.clone(),
        ];

        let child = launch_server_process("HTTP", "neuroindex-http", &http_args)?;
        info!("✅ HTTP server spawned (args: {:?})", http_args);
        children.push(("HTTP", child));
    }

    if children.is_empty() {
        error!("❌ No servers enabled! Use --no-resp or --no-http to disable specific servers.");
        return Ok(());
    }

    info!("🎯 NeuroIndex is ready to accept connections");
    info!("   Press Ctrl+C to shutdown gracefully");

    // Wait for shutdown signal
    match signal::ctrl_c().await {
        Ok(()) => {
            info!("🛑 Shutdown signal received, stopping servers...");
        }
        Err(err) => {
            error!("Unable to listen for shutdown signal: {}", err);
        }
    }

    // Try to stop child processes gracefully
    for (name, child) in children.iter_mut() {
        if let Some(pid) = child.id() {
            info!("🔻 Sending termination to {} server (pid {})", name, pid);
        }
        if let Err(e) = child.start_kill() {
            warn!("Failed to signal {} server for shutdown: {}", name, e);
        }
    }

    for (name, mut child) in children {
        match child.wait().await {
            Ok(status) => {
                if status.success() {
                    info!("{} server exited cleanly", name);
                } else {
                    warn!("{} server exited with status {:?}", name, status);
                }
            }
            Err(e) => warn!("Failed to wait for {} server: {}", name, e),
        }
    }

    info!("👋 NeuroIndex shutdown complete");
    Ok(())
}

fn launch_server_process(
    label: &str,
    binary: &str,
    args: &[String],
) -> Result<Child, Box<dyn std::error::Error>> {
    info!("Starting {} server: {} {}", label, binary, args.join(" "));
    tokio::process::Command::new(binary)
        .args(args)
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("Failed to start {} server ({}): {}", label, binary, e).into())
}
