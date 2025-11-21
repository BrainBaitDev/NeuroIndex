// NeuroIndex RESP Protocol Server
// Compatible with standard wire protocol clients (Python, Node.js, .NET, etc.)

use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::{Buf, BytesMut};
use clap::Parser;
use engine::Engine;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tracing::{error, info, warn};

mod rate_limit;

use rate_limit::{RateLimiter, RateLimiterConfig};

mod resource_limits;
use resource_limits::RespResourceLimits;

type SharedEngine = Arc<Engine<String, String>>;

const BINARY_PREFIX: &str = "__BINARY__";

fn init_new_engine_with_persistence(
    shards_pow2: usize,
    shard_cap_pow2: usize,
    wal_path: PathBuf,
    snapshot_path: PathBuf,
) -> io::Result<Arc<Engine<String, String>>> {
    let config =
        engine::PersistenceConfig::new(shards_pow2, shard_cap_pow2, wal_path, snapshot_path);
    match Engine::with_persistence(config) {
        Ok(engine) => Ok(Arc::new(engine)),
        Err(e) => Err(e),
    }
}

#[derive(Parser, Debug)]
#[command(name = "neuroindex-resp-server")]
#[command(about = "NeuroIndex - High-performance in-memory database with RESP wire protocol", long_about = None)]
struct Args {
    /// Port to listen on
    #[arg(short, long, default_value = "6381")]
    port: u16,

    /// Host to bind to
    #[arg(long, default_value = "0.0.0.0")]
    host: String,

    /// Number of shards (must be power of 2)
    #[arg(short, long, default_value = "8")]
    shards: usize,

    /// Capacity per shard (power of 2)
    #[arg(short, long, default_value = "65536")]
    capacity: usize,

    /// Log level (error, warn, info, debug, trace)
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Directory where WAL and snapshot files will be stored
    #[arg(long)]
    persistence_dir: Option<PathBuf>,

    /// Maximum commands per second per client (0 disables rate limiting)
    #[arg(long, default_value = "1000")]
    rate_limit_per_second: u32,

    /// Burst capacity (commands) per client
    #[arg(long, default_value = "2000")]
    rate_limit_burst: u32,

    /// Background snapshot interval in seconds (0 disables)
    #[arg(long, default_value = "60")]
    snapshot_interval: u64,

    /// Maximum concurrent connections (0 = unlimited)
    #[arg(long, default_value = "0")]
    max_connections: usize,

    /// Maximum memory usage in MB (0 = unlimited)
    #[arg(long, default_value = "0")]
    max_memory_mb: usize,

    /// Maximum command size in KB
    #[arg(long, default_value = "512")]
    max_command_kb: usize,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

    // Setup logging
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(match args.log_level.as_str() {
            "error" => tracing::Level::ERROR,
            "warn" => tracing::Level::WARN,
            "info" => tracing::Level::INFO,
            "debug" => tracing::Level::DEBUG,
            "trace" => tracing::Level::TRACE,
            _ => tracing::Level::INFO,
        })
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");

    // Validate args
    if !args.shards.is_power_of_two() {
        error!("Shards must be a power of 2");
        std::process::exit(1);
    }
    if !args.capacity.is_power_of_two() {
        error!("Capacity must be a power of 2");
        std::process::exit(1);
    }

    // Load and display logo
    print_logo();

    // Create engine (optionally with persistence)
    info!(
        "Initializing NeuroIndex engine with {} shards, {} capacity per shard",
        args.shards, args.capacity
    );
    let engine = if let Some(dir) = &args.persistence_dir {
        std::fs::create_dir_all(dir).map_err(|e| {
            error!("Failed to create persistence directory {:?}: {}", dir, e);
            e
        })?;
        let wal_path = dir.join("neuroindex.wal");
        let snapshot_path = dir.join("neuroindex.snap");
        info!(
            "Attempting to recover from persistence (WAL: {:?}, snapshot: {:?})",
            wal_path, snapshot_path
        );
        let config = engine::PersistenceConfig::new(
            args.shards,
            args.capacity,
            wal_path.clone(),
            snapshot_path.clone(),
        );
        match Engine::recover(config.clone()) {
            Ok(engine) => {
                info!("Successfully recovered engine from persistence");
                Arc::new(engine)
            }
            Err(err) => match err.kind() {
                io::ErrorKind::NotFound => {
                    warn!(
                        "No persistence files found (WAL: {:?}, snapshot: {:?}). Starting fresh.",
                        wal_path, snapshot_path
                    );
                    init_new_engine_with_persistence(
                        args.shards,
                        args.capacity,
                        wal_path.clone(),
                        snapshot_path.clone(),
                    )?
                }
                io::ErrorKind::UnexpectedEof => {
                    warn!(
                        "Snapshot appears incomplete ({}). Attempting recovery without snapshot.",
                        err
                    );
                    if snapshot_path.exists() {
                        if let Err(e) = std::fs::remove_file(&snapshot_path) {
                            warn!(
                                "Failed to remove corrupt snapshot {:?}: {}",
                                snapshot_path, e
                            );
                        }
                    }
                    let retry_config = engine::PersistenceConfig::new(
                        args.shards,
                        args.capacity,
                        &wal_path,
                        &snapshot_path,
                    );
                    match Engine::recover(retry_config) {
                        Ok(engine) => {
                            info!("Recovery succeeded after removing snapshot.");
                            Arc::new(engine)
                        }
                        Err(e) if e.kind() == io::ErrorKind::NotFound => {
                            warn!(
                                "Snapshot removed but WAL missing (WAL: {:?}). Starting fresh.",
                                wal_path
                            );
                            init_new_engine_with_persistence(
                                args.shards,
                                args.capacity,
                                wal_path.clone(),
                                snapshot_path.clone(),
                            )?
                        }
                        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                            warn!(
                                "WAL appears incomplete even after removing snapshot ({}). Starting fresh and preserving old WAL at {:?}.bak",
                                e,
                                wal_path
                            );
                            if wal_path.exists() {
                                let backup = wal_path.with_extension("wal.bak");
                                if let Err(err) = std::fs::rename(&wal_path, &backup) {
                                    warn!(
                                        "Failed to back up WAL {:?} -> {:?}: {}",
                                        wal_path, backup, err
                                    );
                                }
                            }
                            init_new_engine_with_persistence(
                                args.shards,
                                args.capacity,
                                wal_path.clone(),
                                snapshot_path.clone(),
                            )?
                        }
                        Err(e) => {
                            error!(
                                "Recovery still failed after removing snapshot (WAL: {:?}): {}",
                                wal_path, e
                            );
                            return Err(e);
                        }
                    }
                }
                _ => {
                    error!(
                        "Failed to recover engine from persistence (WAL: {:?}, snapshot: {:?}): {}",
                        wal_path, snapshot_path, err
                    );
                    return Err(err);
                }
            },
        }
    } else {
        Arc::new(Engine::with_shards(args.shards, args.capacity))
    };

    // Start server
    let addr = format!("{}:{}", args.host, args.port);
    let listener = TcpListener::bind(&addr).await?;

    println!();
    info!("╔═══════════════════════════════════════════════════════════╗");
    info!("║     NeuroIndex RESP Protocol Server v0.1.0              ║");
    info!("╠═══════════════════════════════════════════════════════════╣");
    info!("║  Listening on: {}:{:<40}║", args.host, args.port);
    info!("║  Shards: {:<49}║", args.shards);
    info!("║  Capacity: {:<47}║", args.capacity * args.shards);
    info!("╚═══════════════════════════════════════════════════════════╝");
    info!("");
    info!("Connect with:");
    info!(
        "  CLI:     neuroindex-cli -h {} -p {}",
        args.host, args.port
    );
    info!(
        "  Python:  Client(host='{}', port={})",
        args.host, args.port
    );
    info!(
        "  Node.js: createClient({{host: '{}', port: {}}})",
        args.host, args.port
    );
    info!("");
    info!("Press Ctrl+C to stop");
    info!("");

    // Start background snapshots if enabled
    let snapshot_handle = if args.snapshot_interval > 0 && args.persistence_dir.is_some() {
        info!(
            "Starting background snapshots every {} seconds",
            args.snapshot_interval
        );
        let engine_snap = Arc::clone(&engine);
        let interval_secs = args.snapshot_interval;

        Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
            loop {
                interval.tick().await;
                info!("Executing background snapshot...");

                // Use spawn_blocking to avoid blocking tokio runtime
                let engine_clone = Arc::clone(&engine_snap);
                match tokio::task::spawn_blocking(move || engine_clone.snapshot()).await {
                    Ok(Ok(_)) => info!("Background snapshot completed successfully"),
                    Ok(Err(e)) => warn!("Background snapshot failed: {}", e),
                    Err(e) => warn!("Snapshot task panicked: {}", e),
                }
            }
        }))
    } else {
        info!("Background snapshots disabled");
        None
    };

    let rate_limiter = RateLimiter::new(RateLimiterConfig {
        commands_per_second: args.rate_limit_per_second,
        burst_capacity: args.rate_limit_burst,
    });

    let limits = RespResourceLimits::new(
        if args.max_connections > 0 {
            Some(args.max_connections)
        } else {
            None
        },
        if args.max_memory_mb > 0 {
            Some(args.max_memory_mb)
        } else {
            None
        },
        args.max_command_kb,
    );

    info!(
        "Resource limits: max_connections={}, max_memory_mb={}, max_command_kb={}",
        args.max_connections, args.max_memory_mb, args.max_command_kb
    );

    let engine_for_shutdown = Arc::clone(&engine);

    // Run server with graceful shutdown
    tokio::select! {
        _ = accept_loop(listener, engine, rate_limiter, limits) => {},
        _ = shutdown_signal(engine_for_shutdown) => {},
    }

    // Stop background snapshots cleanly
    if let Some(handle) = snapshot_handle {
        info!("Stopping background snapshot worker...");
        handle.abort();
    }

    Ok(())
}

/// Accept connections loop
async fn accept_loop(
    listener: TcpListener,
    engine: SharedEngine,
    rate_limiter: Option<RateLimiter>,
    limits: RespResourceLimits,
) {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                // Check connection limit
                match limits.acquire_connection().await {
                    Some(_guard) => {
                        info!(
                            "New connection from {} ({} active)",
                            addr,
                            limits.active_connections()
                        );
                        let engine = Arc::clone(&engine);
                        let limiter = rate_limiter.clone();
                        let limits_clone = limits.clone();

                        tokio::spawn(async move {
                            let _guard = _guard; // Keep guard alive for connection lifetime
                            if let Err(e) =
                                handle_client(stream, addr, engine, limiter, limits_clone).await
                            {
                                error!("Error handling client {}: {}", addr, e);
                            } else {
                                info!("Client {} disconnected", addr);
                            }
                        });
                    }
                    None => {
                        warn!("Connection limit reached, rejecting {}", addr);
                        // Close connection immediately
                        drop(stream);
                    }
                }
            }
            Err(e) => {
                error!("Failed to accept connection: {}", e);
            }
        }
    }
}

/// Graceful shutdown handler
async fn shutdown_signal(engine: SharedEngine) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Received Ctrl+C signal, initiating graceful shutdown...");
        },
        _ = terminate => {
            info!("Received SIGTERM signal, initiating graceful shutdown...");
        },
    }

    // Graceful shutdown sequence (max 30s timeout)
    let shutdown_timeout = Duration::from_secs(30);
    let start = Instant::now();

    info!("Flushing WAL...");
    if let Err(e) = engine.flush() {
        warn!("Failed to flush WAL: {}", e);
    }

    if start.elapsed() < shutdown_timeout {
        info!("Creating final snapshot...");
        if let Err(e) = engine.snapshot() {
            warn!("Failed to create snapshot: {}", e);
        }
    } else {
        warn!("Shutdown timeout reached, skipping snapshot");
    }

    info!("Shutdown complete in {:?}", start.elapsed());
}

async fn handle_client(
    mut stream: TcpStream,
    addr: SocketAddr,
    engine: SharedEngine,
    rate_limiter: Option<RateLimiter>,
    limits: RespResourceLimits,
) -> std::io::Result<()> {
    let mut buffer = BytesMut::with_capacity(8192);
    let client_ip = addr.ip();
    let mut transaction_queue: Option<Vec<Vec<Vec<u8>>>> = None;

    loop {
        // Check memory limit before processing
        let current_memory = engine.memory_usage();
        if !limits.check_memory(current_memory) {
            let err = encode_error("ERR memory limit exceeded");
            stream.write_all(&err).await?;
            stream.flush().await?;
            return Ok(());
        }

        // Read from client
        let n = stream.read_buf(&mut buffer).await?;
        if n == 0 {
            return Ok(()); // Connection closed
        }

        // Check command size limit
        if buffer.len() > limits.max_command_size() {
            let err = encode_error("ERR command too large");
            stream.write_all(&err).await?;
            stream.flush().await?;
            buffer.clear();
            continue;
        }

        // Parse RESP commands
        while let Some(command) = parse_resp_command(&mut buffer) {
            if let Some(ref limiter) = rate_limiter {
                if !limiter.allow(client_ip).await {
                    let err = encode_error("ERR rate limit exceeded");
                    stream.write_all(&err).await?;
                    stream.flush().await?;
                    continue;
                }
            }

            let cmd_name = match command.get(0).and_then(|c| bytes_to_string(c).ok()) {
                Some(name) => name,
                None => {
                    let err = encode_error("ERR invalid command name");
                    stream.write_all(&err).await?;
                    stream.flush().await?;
                    continue;
                }
            };
            let cmd_upper = cmd_name.to_uppercase();

            let response = if let Some(ref mut queue) = transaction_queue {
                // Inside MULTI block
                if command.is_empty() {
                    encode_error("ERR empty command")
                } else {
                    match cmd_upper.as_str() {
                        "EXEC" => {
                            // Execute all queued commands atomically
                            let results = execute_transaction(&queue, &engine).await;
                            transaction_queue = None;
                            encode_transaction_results(&results)
                        }
                        "DISCARD" => {
                            // Discard transaction
                            transaction_queue = None;
                            encode_simple_string("OK")
                        }
                        "MULTI" => encode_error("ERR MULTI calls can not be nested"),
                        _ => {
                            // Queue command
                            queue.push(command.clone());
                            encode_simple_string("QUEUED")
                        }
                    }
                }
            } else {
                // Not in transaction
                if cmd_upper == "MULTI" {
                    transaction_queue = Some(Vec::new());
                    encode_simple_string("OK")
                } else {
                    execute_command(&command, &engine).await
                }
            };

            // Send response
            stream.write_all(&response).await?;
            stream.flush().await?;
        }
    }
}

fn parse_resp_command(buffer: &mut BytesMut) -> Option<Vec<Vec<u8>>> {
    if buffer.is_empty() {
        return None;
    }

    if buffer[0] != b'*' {
        // Inline command (simple string)
        let pos = find_crlf(buffer)?;
        let line = &buffer[..pos];
        let line_str = std::str::from_utf8(line).ok()?;
        let args: Vec<Vec<u8>> = line_str
            .split_whitespace()
            .map(|s| s.as_bytes().to_vec())
            .collect();
        buffer.advance(pos + 2);
        return Some(args);
    }

    let mut pos = 0;
    let first_line_end = find_crlf_from(buffer, pos)?;
    if buffer[pos] != b'*' {
        return None;
    }

    let num_str = std::str::from_utf8(&buffer[pos + 1..first_line_end]).ok()?;
    let num_args: usize = num_str.parse().ok()?;
    pos = first_line_end + 2;

    let mut args = Vec::with_capacity(num_args);

    for _ in 0..num_args {
        if pos >= buffer.len() || buffer[pos] != b'$' {
            return None;
        }

        let len_line_end = find_crlf_from(buffer, pos)?;
        let len_str = std::str::from_utf8(&buffer[pos + 1..len_line_end]).ok()?;
        let data_len: usize = len_str.parse().ok()?;
        pos = len_line_end + 2;

        if pos + data_len + 2 > buffer.len() {
            return None;
        }

        let data_bytes = buffer[pos..pos + data_len].to_vec();
        args.push(data_bytes);
        pos += data_len + 2;
    }

    buffer.advance(pos);
    Some(args)
}

fn find_crlf(buffer: &[u8]) -> Option<usize> {
    find_crlf_from(buffer, 0)
}

fn find_crlf_from(buffer: &[u8], start: usize) -> Option<usize> {
    for i in start..buffer.len().saturating_sub(1) {
        if buffer[i] == b'\r' && buffer[i + 1] == b'\n' {
            return Some(i);
        }
    }
    None
}

fn bytes_to_string(bytes: &[u8]) -> Result<String, std::str::Utf8Error> {
    std::str::from_utf8(bytes).map(|s| s.to_string())
}

async fn execute_command(args: &[Vec<u8>], engine: &SharedEngine) -> Vec<u8> {
    if args.is_empty() {
        return encode_error("ERR empty command");
    }

    let cmd = match bytes_to_string(&args[0]) {
        Ok(s) => s.to_uppercase(),
        Err(_) => return encode_error("ERR invalid command name"),
    };

    match cmd.as_str() {
        "PING" => {
            if args.len() > 1 {
                match bytes_to_string(&args[1]) {
                    Ok(s) => encode_bulk_string(&s),
                    Err(_) => encode_error("ERR invalid UTF-8 in ping argument"),
                }
            } else {
                encode_simple_string("PONG")
            }
        }

        "ECHO" => {
            if args.len() < 2 {
                return encode_error("ERR wrong number of arguments for 'echo' command");
            }
            match bytes_to_string(&args[1]) {
                Ok(s) => encode_bulk_string(&s),
                Err(_) => encode_error("ERR invalid UTF-8 in echo argument"),
            }
        }

        "SET" => {
            if args.len() < 3 {
                return encode_error("ERR wrong number of arguments for 'set' command");
            }
            let key = match bytes_to_string(&args[1]) {
                Ok(s) => s,
                Err(_) => return encode_error("ERR invalid UTF-8 in key"),
            };
            let value = encode_value_for_storage(&args[2]);
            match engine.put(key, value) {
                Ok(_) => encode_simple_string("OK"),
                Err(e) => encode_error(&format!("ERR {}", e)),
            }
        }

        "SETB" => {
            if args.len() < 3 {
                return encode_error("ERR wrong number of arguments for 'setb' command");
            }
            let key = match bytes_to_string(&args[1]) {
                Ok(s) => s,
                Err(_) => return encode_error("ERR invalid UTF-8 in key"),
            };
            let value = format!("{}{}", BINARY_PREFIX, STANDARD.encode(&args[2]));
            match engine.put(key, value) {
                Ok(_) => encode_simple_string("OK"),
                Err(e) => encode_error(&format!("ERR {}", e)),
            }
        }

        "GET" => {
            if args.len() < 2 {
                return encode_error("ERR wrong number of arguments for 'get' command");
            }
            let key = match bytes_to_string(&args[1]) {
                Ok(s) => s,
                Err(_) => return encode_error("ERR invalid UTF-8 in key"),
            };
            match engine.get(&key) {
                Some(value) => {
                    if let Some(bytes) = decode_value_from_storage(&value) {
                        encode_bulk_bytes(&bytes)
                    } else {
                        encode_bulk_string(&value)
                    }
                }
                None => encode_null(),
            }
        }

        "GETB" => {
            if args.len() < 2 {
                return encode_error("ERR wrong number of arguments for 'getb' command");
            }
            let key = match bytes_to_string(&args[1]) {
                Ok(s) => s,
                Err(_) => return encode_error("ERR invalid UTF-8 in key"),
            };
            match engine.get(&key) {
                Some(value) => {
                    if let Some(bytes) = decode_value_from_storage(&value) {
                        encode_bulk_bytes(&bytes)
                    } else {
                        encode_bulk_bytes(value.as_bytes())
                    }
                }
                None => encode_null(),
            }
        }

        "DEL" => {
            if args.len() < 2 {
                return encode_error("ERR wrong number of arguments for 'del' command");
            }
            let mut deleted = 0;
            for key_bytes in &args[1..] {
                let key = match bytes_to_string(key_bytes) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                match engine.delete(&key) {
                    Ok(Some(_)) => deleted += 1,
                    _ => {}
                }
            }
            encode_integer(deleted)
        }

        "EXISTS" => {
            if args.len() < 2 {
                return encode_error("ERR wrong number of arguments for 'exists' command");
            }
            let mut count = 0;
            for key_bytes in &args[1..] {
                let key = match bytes_to_string(key_bytes) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                if engine.get(&key).is_some() {
                    count += 1;
                }
            }
            encode_integer(count)
        }

        "MGET" => {
            if args.len() < 2 {
                return encode_error("ERR wrong number of arguments for 'mget' command");
            }
            let mut values = Vec::new();
            for key_bytes in &args[1..] {
                let key = match bytes_to_string(key_bytes) {
                    Ok(s) => s,
                    Err(_) => {
                        values.push(None);
                        continue;
                    }
                };
                if let Some(value) = engine.get(&key) {
                    if let Some(bytes) = decode_value_from_storage(&value) {
                        values.push(Some(bytes));
                    } else {
                        values.push(Some(value.into_bytes()));
                    }
                } else {
                    values.push(None);
                }
            }
            encode_array_bytes(&values)
        }

        "MSET" => {
            if args.len() < 3 || args.len() % 2 == 0 {
                return encode_error("ERR wrong number of arguments for 'mset' command");
            }
            for chunk in args[1..].chunks(2) {
                if chunk.len() == 2 {
                    let key = match bytes_to_string(&chunk[0]) {
                        Ok(s) => s,
                        Err(_) => return encode_error("ERR invalid UTF-8 in key"),
                    };
                    let value = encode_value_for_storage(&chunk[1]);
                    if let Err(e) = engine.put(key, value) {
                        return encode_error(&format!("ERR {}", e));
                    }
                }
            }
            encode_simple_string("OK")
        }

        "KEYS" => {
            // Simplified: return all keys (pattern matching not implemented)
            use std::ops::Bound::Unbounded;
            let all = engine.range(Unbounded, Unbounded);
            let keys: Vec<String> = all.into_iter().map(|(k, _)| k).collect();
            encode_string_array(&keys)
        }

        "DBSIZE" => {
            use std::ops::Bound::Unbounded;
            let count = engine.range(Unbounded, Unbounded).len();
            encode_integer(count as i64)
        }

        "INCR" => {
            if args.len() < 2 {
                return encode_error("ERR wrong number of arguments for 'incr' command");
            }
            let key = match bytes_to_string(&args[1]) {
                Ok(s) => s,
                Err(_) => return encode_error("ERR invalid UTF-8 in key"),
            };

            // Get current value or default to 0
            let current: i64 = match engine.get(&key) {
                Some(val) => match val.parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => return encode_error("ERR value is not an integer or out of range"),
                },
                None => 0,
            };

            // Increment
            let new_value = current + 1;
            match engine.put(key, new_value.to_string()) {
                Ok(_) => encode_integer(new_value),
                Err(e) => encode_error(&format!("ERR {}", e)),
            }
        }

        "DECR" => {
            if args.len() < 2 {
                return encode_error("ERR wrong number of arguments for 'decr' command");
            }
            let key = match bytes_to_string(&args[1]) {
                Ok(s) => s,
                Err(_) => return encode_error("ERR invalid UTF-8 in key"),
            };

            // Get current value or default to 0
            let current: i64 = match engine.get(&key) {
                Some(val) => match val.parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => return encode_error("ERR value is not an integer or out of range"),
                },
                None => 0,
            };

            // Decrement
            let new_value = current - 1;
            match engine.put(key, new_value.to_string()) {
                Ok(_) => encode_integer(new_value),
                Err(e) => encode_error(&format!("ERR {}", e)),
            }
        }

        "INCRBY" => {
            if args.len() < 3 {
                return encode_error("ERR wrong number of arguments for 'incrby' command");
            }
            let key = match bytes_to_string(&args[1]) {
                Ok(s) => s,
                Err(_) => return encode_error("ERR invalid UTF-8 in key"),
            };
            let increment: i64 = match bytes_to_string(&args[2]) {
                Ok(s) => match s.parse() {
                    Ok(n) => n,
                    Err(_) => return encode_error("ERR value is not an integer or out of range"),
                },
                Err(_) => return encode_error("ERR value is not an integer or out of range"),
            };

            // Get current value or default to 0
            let current: i64 = match engine.get(&key) {
                Some(val) => match val.parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => return encode_error("ERR value is not an integer or out of range"),
                },
                None => 0,
            };

            // Increment by amount
            let new_value = current + increment;
            match engine.put(key, new_value.to_string()) {
                Ok(_) => encode_integer(new_value),
                Err(e) => encode_error(&format!("ERR {}", e)),
            }
        }

        "DECRBY" => {
            if args.len() < 3 {
                return encode_error("ERR wrong number of arguments for 'decrby' command");
            }
            let key = match bytes_to_string(&args[1]) {
                Ok(s) => s,
                Err(_) => return encode_error("ERR invalid UTF-8 in key"),
            };
            let decrement: i64 = match bytes_to_string(&args[2]) {
                Ok(s) => match s.parse() {
                    Ok(n) => n,
                    Err(_) => return encode_error("ERR value is not an integer or out of range"),
                },
                Err(_) => return encode_error("ERR value is not an integer or out of range"),
            };

            // Get current value or default to 0
            let current: i64 = match engine.get(&key) {
                Some(val) => match val.parse::<i64>() {
                    Ok(n) => n,
                    Err(_) => return encode_error("ERR value is not an integer or out of range"),
                },
                None => 0,
            };

            // Decrement by amount
            let new_value = current - decrement;
            match engine.put(key, new_value.to_string()) {
                Ok(_) => encode_integer(new_value),
                Err(e) => encode_error(&format!("ERR {}", e)),
            }
        }

        "FLUSHDB" | "FLUSHALL" => match engine.flushdb() {
            Ok(_) => encode_simple_string("OK"),
            Err(err) => encode_error(&format!("ERR {}", err)),
        },

        "INFO" => {
            use std::ops::Bound::Unbounded;
            let count = engine.range(Unbounded, Unbounded).len();
            let info = format!(
                "# Server\r\n\
                 neuroindex_version:0.1.0\r\n\
                 mode:standalone\r\n\
                 os:Rust\r\n\
                 arch_bits:64\r\n\
                 \r\n\
                 # Clients\r\n\
                 connected_clients:1\r\n\
                 \r\n\
                 # Memory\r\n\
                 used_memory:0\r\n\
                 \r\n\
                 # Stats\r\n\
                 total_connections_received:1\r\n\
                 \r\n\
                 # Keyspace\r\n\
                 db0:keys={},expires=0\r\n",
                count
            );
            encode_bulk_string(&info)
        }

        "FLUSHWAL" => {
            if let Err(err) = engine.flush() {
                encode_error(&format!("ERR {}", err))
            } else {
                encode_simple_string("OK")
            }
        }

        "SNAPSHOT" => {
            if let Err(err) = engine.snapshot() {
                encode_error(&format!("ERR {}", err))
            } else {
                encode_simple_string("OK")
            }
        }

        "QUIT" => encode_simple_string("OK"),

        "COMMAND" => {
            // Return empty array for compatibility
            encode_string_array(&[])
        }

        _ => encode_error(&format!("ERR unknown command '{}'", cmd)),
    }
}

// RESP encoding functions
fn encode_simple_string(s: &str) -> Vec<u8> {
    format!("+{}\r\n", s).into_bytes()
}

fn encode_error(s: &str) -> Vec<u8> {
    format!("-{}\r\n", s).into_bytes()
}

fn encode_integer(i: i64) -> Vec<u8> {
    format!(":{}\r\n", i).into_bytes()
}

fn encode_bulk_string(s: &str) -> Vec<u8> {
    format!("${}\r\n{}\r\n", s.len(), s).into_bytes()
}

fn encode_bulk_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut out = format!("${}\r\n", bytes.len()).into_bytes();
    out.extend_from_slice(bytes);
    out.extend_from_slice(b"\r\n");
    out
}

fn encode_null() -> Vec<u8> {
    b"$-1\r\n".to_vec()
}

fn encode_array_bytes(items: &[Option<Vec<u8>>]) -> Vec<u8> {
    let mut result = format!("*{}\r\n", items.len()).into_bytes();
    for item in items {
        match item {
            Some(bytes) => result.extend_from_slice(&encode_bulk_bytes(bytes)),
            None => result.extend_from_slice(&encode_null()),
        }
    }
    result
}

fn encode_value_for_storage(bytes: &[u8]) -> String {
    match bytes_to_string(bytes) {
        Ok(s) => s,
        Err(_) => format!("{}{}", BINARY_PREFIX, STANDARD.encode(bytes)),
    }
}

fn decode_value_from_storage(value: &str) -> Option<Vec<u8>> {
    if value.starts_with(BINARY_PREFIX) {
        let data = &value[BINARY_PREFIX.len()..];
        STANDARD.decode(data).ok()
    } else {
        None
    }
}

fn encode_string_array(items: &[String]) -> Vec<u8> {
    let mut result = format!("*{}\r\n", items.len()).into_bytes();
    for item in items {
        result.extend_from_slice(&encode_bulk_string(item));
    }
    result
}

// === Transaction Support ===

async fn execute_transaction(commands: &[Vec<Vec<u8>>], engine: &SharedEngine) -> Vec<Vec<u8>> {
    let mut results = Vec::new();

    // Execute all commands sequentially
    // TODO: Make this truly atomic with 2PL or MVCC
    for command in commands {
        let result = execute_command(command, engine).await;
        results.push(result);
    }

    results
}

fn encode_transaction_results(results: &[Vec<u8>]) -> Vec<u8> {
    let mut output = format!("*{}\r\n", results.len()).into_bytes();
    for result in results {
        output.extend_from_slice(result);
    }
    output
}

fn print_logo() {
    // Try to load custom logo from file
    let logo_path = "logo.txt";
    let logo = if let Ok(content) = std::fs::read_to_string(logo_path) {
        content
    } else {
        // Default embedded logo
        include_str!("../logo.txt").to_string()
    };

    println!("{}", logo);
}
