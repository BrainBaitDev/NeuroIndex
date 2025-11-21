// NeuroIndex Unified Server
// Runs both RESP and HTTP servers simultaneously

use clap::Parser;
use serde::Deserialize;
use std::path::PathBuf;
use tokio::signal;
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

    info!("📋 Configuration:");
    info!("   RESP port: {}", resp_port);
    info!("   HTTP port: {}", http_port);
    info!("   Data directory: {}", data_dir.display());
    info!("   Shards: 2^{} = {}", args.shards_pow2, 1 << args.shards_pow2);
    info!(
        "   Shard capacity: 2^{} = {}",
        args.shard_cap_pow2,
        1 << args.shard_cap_pow2
    );

    // Create data directory if it doesn't exist
    if !data_dir.exists() {
        std::fs::create_dir_all(&data_dir)?;
        info!("   Created data directory");
    }

    // Initialize engine with persistence
    let wal_path = data_dir.join("wal");
    let snapshot_path = data_dir.join("snapshot");

    info!("   WAL path: {}", wal_path.display());
    info!("   Snapshot path: {}", snapshot_path.display());

    // Create engine (this will be shared between servers)
    info!("📦 Initializing engine...");

    // Here you would initialize your actual engine
    // For now, this is a placeholder - you'll need to adapt this
    // based on your actual Engine implementation

    info!("✅ Engine initialized");

    let mut handles = vec![];

    // Start RESP server if enabled
    if !args.no_resp {
        info!("🔌 Starting RESP server on port {}", resp_port);

        let resp_handle = tokio::spawn(async move {
            if let Err(e) = start_resp_server(resp_port).await {
                error!("RESP server error: {}", e);
            }
        });

        handles.push(resp_handle);
        info!("✅ RESP server listening on 0.0.0.0:{}", resp_port);
    }

    // Start HTTP server if enabled
    if !args.no_http {
        info!("🌐 Starting HTTP server on port {}", http_port);

        let http_handle = tokio::spawn(async move {
            if let Err(e) = start_http_server(http_port).await {
                error!("HTTP server error: {}", e);
            }
        });

        handles.push(http_handle);
        info!("✅ HTTP server listening on 0.0.0.0:{}", http_port);
    }

    if handles.is_empty() {
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

    // Wait for all server tasks to complete
    for handle in handles {
        let _ = handle.await;
    }

    info!("👋 NeuroIndex shutdown complete");
    Ok(())
}

async fn start_resp_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    // TODO: Integrate with actual RESP server implementation
    // For now, this is a placeholder
    use tokio::net::TcpListener;

    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;

    loop {
        match listener.accept().await {
            Ok((_socket, addr)) => {
                info!("RESP client connected from {}", addr);
                // TODO: Handle RESP protocol
            }
            Err(e) => {
                error!("RESP accept error: {}", e);
            }
        }
    }
}

async fn start_http_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    // TODO: Integrate with actual HTTP server implementation
    // For now, this is a placeholder
    use tokio::net::TcpListener;

    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;

    loop {
        match listener.accept().await {
            Ok((_socket, addr)) => {
                info!("HTTP client connected from {}", addr);
                // TODO: Handle HTTP requests
            }
            Err(e) => {
                error!("HTTP accept error: {}", e);
            }
        }
    }
}
