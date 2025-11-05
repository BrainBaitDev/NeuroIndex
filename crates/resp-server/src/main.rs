// NeuroIndex RESP Protocol Server
// Compatible with standard wire protocol clients (Python, Node.js, .NET, etc.)

use bytes::{Buf, BytesMut};
use clap::Parser;
use engine::Engine;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

type SharedEngine = Arc<Engine<String, String>>;

fn init_new_engine_with_persistence(
    shards_pow2: usize,
    shard_cap_pow2: usize,
    wal_path: PathBuf,
    snapshot_path: PathBuf,
) -> io::Result<Arc<Engine<String, String>>> {
    let config = engine::PersistenceConfig::new(shards_pow2, shard_cap_pow2, wal_path, snapshot_path);
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
        let config = engine::PersistenceConfig::new(args.shards, args.capacity, wal_path.clone(), snapshot_path.clone());
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
                            warn!("Failed to remove corrupt snapshot {:?}: {}", snapshot_path, e);
                        }
                    }
                    let retry_config = engine::PersistenceConfig::new(args.shards, args.capacity, &wal_path, &snapshot_path);
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

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                info!("New connection from {}", addr);
                let engine = Arc::clone(&engine);

                tokio::spawn(async move {
                    if let Err(e) = handle_client(stream, engine).await {
                        error!("Error handling client {}: {}", addr, e);
                    } else {
                        info!("Client {} disconnected", addr);
                    }
                });
            }
            Err(e) => {
                error!("Failed to accept connection: {}", e);
            }
        }
    }
}

async fn handle_client(mut stream: TcpStream, engine: SharedEngine) -> std::io::Result<()> {
    let mut buffer = BytesMut::with_capacity(8192);

    loop {
        // Read from client
        let n = stream.read_buf(&mut buffer).await?;
        if n == 0 {
            return Ok(()); // Connection closed
        }

        // Parse RESP commands
        while let Some(command) = parse_resp_command(&mut buffer) {
            // Execute command
            let response = execute_command(&command, &engine).await;

            // Send response
            stream.write_all(&response).await?;
            stream.flush().await?;
        }
    }
}

fn parse_resp_command(buffer: &mut BytesMut) -> Option<Vec<String>> {
    // RESP (REdis Serialization Protocol) parser
    // Format: *<num_args>\r\n$<len>\r\n<data>\r\n...

    if buffer.is_empty() {
        return None;
    }

    // Check for array marker
    if buffer[0] != b'*' {
        // Inline command (simple string)
        let data = std::str::from_utf8(&buffer).ok()?;
        if let Some(pos) = data.find("\r\n") {
            let line = &data[..pos];
            let args: Vec<String> = line.split_whitespace().map(|s| s.to_string()).collect();
            buffer.advance(pos + 2);
            return Some(args);
        }
        return None;
    }

    // Find first \r\n
    let data = std::str::from_utf8(&buffer).ok()?;
    let mut lines = data.split("\r\n");

    // Parse *<num_args>
    let first = lines.next()?;
    if !first.starts_with('*') {
        return None;
    }

    let num_args: usize = first[1..].parse().ok()?;
    let mut args = Vec::with_capacity(num_args);
    let mut bytes_consumed = first.len() + 2;

    for _ in 0..num_args {
        // Parse $<len>
        let len_line = lines.next()?;
        if !len_line.starts_with('$') {
            return None;
        }

        let len: usize = len_line[1..].parse().ok()?;
        bytes_consumed += len_line.len() + 2;

        // Parse <data>
        let data_line = lines.next()?;
        if data_line.len() < len {
            return None; // Incomplete
        }

        args.push(data_line[..len].to_string());
        bytes_consumed += len + 2;
    }

    buffer.advance(bytes_consumed);
    Some(args)
}

async fn execute_command(args: &[String], engine: &SharedEngine) -> Vec<u8> {
    if args.is_empty() {
        return encode_error("ERR empty command");
    }

    let cmd = args[0].to_uppercase();

    match cmd.as_str() {
        "PING" => {
            if args.len() > 1 {
                encode_bulk_string(&args[1])
            } else {
                encode_simple_string("PONG")
            }
        }

        "ECHO" => {
            if args.len() < 2 {
                return encode_error("ERR wrong number of arguments for 'echo' command");
            }
            encode_bulk_string(&args[1])
        }

        "SET" => {
            if args.len() < 3 {
                return encode_error("ERR wrong number of arguments for 'set' command");
            }
            match engine.put(args[1].clone(), args[2].clone()) {
                Ok(_) => encode_simple_string("OK"),
                Err(e) => encode_error(&format!("ERR {}", e)),
            }
        }

        "GET" => {
            if args.len() < 2 {
                return encode_error("ERR wrong number of arguments for 'get' command");
            }
            match engine.get(&args[1]) {
                Some(value) => encode_bulk_string(&value),
                None => encode_null(),
            }
        }

        "DEL" => {
            if args.len() < 2 {
                return encode_error("ERR wrong number of arguments for 'del' command");
            }
            let mut deleted = 0;
            for key in &args[1..] {
                match engine.delete(key) {
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
            for key in &args[1..] {
                if engine.get(key).is_some() {
                    count += 1;
                }
            }
            encode_integer(count)
        }

        "MGET" => {
            if args.len() < 2 {
                return encode_error("ERR wrong number of arguments for 'mget' command");
            }
            let values: Vec<Option<String>> = args[1..].iter().map(|key| engine.get(key)).collect();
            encode_array(&values)
        }

        "MSET" => {
            if args.len() < 3 || args.len() % 2 == 0 {
                return encode_error("ERR wrong number of arguments for 'mset' command");
            }
            for chunk in args[1..].chunks(2) {
                if chunk.len() == 2 {
                    if let Err(e) = engine.put(chunk[0].clone(), chunk[1].clone()) {
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

        "FLUSHDB" | "FLUSHALL" => {
            match engine.flushdb() {
                Ok(_) => encode_simple_string("OK"),
                Err(err) => encode_error(&format!("ERR {}", err)),
            }
        }

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

fn encode_null() -> Vec<u8> {
    b"$-1\r\n".to_vec()
}

fn encode_array(items: &[Option<String>]) -> Vec<u8> {
    let mut result = format!("*{}\r\n", items.len()).into_bytes();
    for item in items {
        match item {
            Some(s) => result.extend_from_slice(&encode_bulk_string(s)),
            None => result.extend_from_slice(&encode_null()),
        }
    }
    result
}

fn encode_string_array(items: &[String]) -> Vec<u8> {
    let mut result = format!("*{}\r\n", items.len()).into_bytes();
    for item in items {
        result.extend_from_slice(&encode_bulk_string(item));
    }
    result
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
