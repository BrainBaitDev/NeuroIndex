// NeuroIndex HTTP REST API Server
// Provides JSON REST API for NeuroIndex database

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use clap::Parser;
use engine::{Engine, PersistenceConfig};
use serde::{Deserialize, Serialize};
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::{info, Level};

// Shared state
type SharedEngine = Arc<Engine<String, String>>;

#[derive(Clone)]
struct AppState {
    engine: SharedEngine,
    shards: usize,
    #[allow(dead_code)]
    capacity: usize,
}

#[derive(Parser, Debug)]
#[command(name = "neuroindex-http")]
#[command(about = "NeuroIndex HTTP REST API Server", long_about = None)]
struct Args {
    /// Port to listen on
    #[arg(short, long, default_value = "8080")]
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

    /// Directory used for WAL + snapshot persistenza (compatibile con RESP server)
    #[arg(long)]
    persistence_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // Setup logging
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(match args.log_level.as_str() {
            "error" => Level::ERROR,
            "warn" => Level::WARN,
            "info" => Level::INFO,
            "debug" => Level::DEBUG,
            "trace" => Level::TRACE,
            _ => Level::INFO,
        })
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");

    // Validate args
    if !args.shards.is_power_of_two() {
        eprintln!("Error: Shards must be a power of 2");
        std::process::exit(1);
    }
    if !args.capacity.is_power_of_two() {
        eprintln!("Error: Capacity must be a power of 2");
        std::process::exit(1);
    }

    // Load and display logo
    print_logo();

    info!(
        "Initializing NeuroIndex engine with {} shards, {} capacity per shard",
        args.shards, args.capacity
    );

    let engine: SharedEngine = if let Some(dir) = &args.persistence_dir {
        info!("Persistence enabled. Directory: {:?}", dir);
        if let Err(e) = std::fs::create_dir_all(dir) {
            eprintln!("Failed to create persistence directory {:?}: {}", dir, e);
            std::process::exit(1);
        }

        let wal_path = dir.join("neuroindex.wal");
        let snapshot_path = dir.join("neuroindex.snap");
        info!(
            "Attempting to recover from persistence (WAL: {:?}, snapshot: {:?})",
            wal_path, snapshot_path
        );
        let config = PersistenceConfig::new(
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
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                info!(
                    "No existing persistence state (wal: {:?}, snapshot: {:?}). Starting fresh.",
                    wal_path, snapshot_path
                );
                let engine = Engine::with_persistence(config).unwrap_or_else(|e| {
                    eprintln!("Failed to initialize engine with persistence: {}", e);
                    std::process::exit(1);
                });
                Arc::new(engine)
            }
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                info!(
                    "Persistence data incomplete ({}). Attempting fresh start and preserving WAL backup.",
                    err
                );
                if wal_path.exists() {
                    let backup = wal_path.with_extension("wal.bak");
                    if let Err(e) = std::fs::rename(&wal_path, &backup) {
                        eprintln!("Failed to backup WAL {:?}: {}", wal_path, e);
                    } else {
                        info!("Existing WAL moved to {:?}", backup);
                    }
                }
                if snapshot_path.exists() {
                    if let Err(e) = std::fs::remove_file(&snapshot_path) {
                        eprintln!("Failed to remove snapshot {:?}: {}", snapshot_path, e);
                    } else {
                        info!("Removed incomplete snapshot {:?}", snapshot_path);
                    }
                }
                let engine = Engine::with_persistence(config).unwrap_or_else(|e| {
                    eprintln!("Failed to initialize engine with persistence: {}", e);
                    std::process::exit(1);
                });
                Arc::new(engine)
            }
            Err(err) => {
                eprintln!("Failed to recover engine from persistence: {}", err);
                std::process::exit(1);
            }
        }
    } else {
        Arc::new(Engine::with_shards(args.shards, args.capacity))
    };

    let app_state = AppState {
        engine: Arc::clone(&engine),
        shards: args.shards,
        capacity: args.capacity,
    };

    // Build router
    let app = Router::new()
        // CRUD operations
        .route("/api/v1/records/:key", get(get_record))
        .route("/api/v1/records/:key", post(put_record))
        .route("/api/v1/records/:key", delete(delete_record))
        // Bulk operations
        .route("/api/v1/records/bulk", post(bulk_insert))
        // Range queries
        .route("/api/v1/records/range", get(range_query))
        // Aggregations
        .route("/api/v1/aggregations/count", get(count_aggregation))
        // Health & Stats
        .route("/api/v1/health", get(health_check))
        .route("/api/v1/stats", get(stats))
        // Root
        .route("/", get(root))
        // State
        .with_state(app_state)
        // Middleware
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http());

    // Start server
    let addr = format!("{}:{}", args.host, args.port);
    let listener = TcpListener::bind(&addr).await.unwrap();

    println!();
    info!("╔═══════════════════════════════════════════════════════════╗");
    info!("║     NeuroIndex HTTP REST API Server v0.1.0             ║");
    info!("╠═══════════════════════════════════════════════════════════╣");
    info!("║  Listening on: http://{}:{:<36}║", args.host, args.port);
    info!("║  Shards: {:<49}║", args.shards);
    info!("║  Capacity: {:<47}║", args.capacity * args.shards);
    info!("╚═══════════════════════════════════════════════════════════╝");
    info!("");
    info!("API Endpoints:");
    info!(
        "  Health:       GET  http://{}:{}/api/v1/health",
        args.host, args.port
    );
    info!(
        "  Stats:        GET  http://{}:{}/api/v1/stats",
        args.host, args.port
    );
    info!(
        "  Get Record:   GET  http://{}:{}/api/v1/records/{{key}}",
        args.host, args.port
    );
    info!(
        "  Put Record:   POST http://{}:{}/api/v1/records/{{key}}",
        args.host, args.port
    );
    info!(
        "  Delete Record: DEL http://{}:{}/api/v1/records/{{key}}",
        args.host, args.port
    );
    info!(
        "  Bulk Insert:  POST http://{}:{}/api/v1/records/bulk",
        args.host, args.port
    );
    info!(
        "  Range Query:  GET  http://{}:{}/api/v1/records/range",
        args.host, args.port
    );
    info!(
        "  Count Agg:    GET  http://{}:{}/api/v1/aggregations/count",
        args.host, args.port
    );
    info!("");
    info!("Press Ctrl+C to stop");
    info!("");

    axum::serve(listener, app).await.unwrap();
}

// === Root Handler ===

async fn root() -> Json<RootResponse> {
    Json(RootResponse {
        name: "NeuroIndex HTTP REST API".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        endpoints: vec![
            "/api/v1/health".to_string(),
            "/api/v1/stats".to_string(),
            "/api/v1/records/{key}".to_string(),
            "/api/v1/records/bulk".to_string(),
            "/api/v1/records/range".to_string(),
            "/api/v1/aggregations/count".to_string(),
        ],
        documentation: "See CLIENT_USAGE.md for complete API documentation".to_string(),
    })
}

// === CRUD Handlers ===

async fn get_record(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Json<RecordResponse>, ApiError> {
    match state.engine.get(&key) {
        Some(raw) => {
            let value = decode_value(raw);
            Ok(Json(RecordResponse { key, value }))
        }
        None => Err(ApiError::NotFound),
    }
}

async fn put_record(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(payload): Json<PutRequest>,
) -> Result<Json<PutResponse>, ApiError> {
    let encoded =
        serde_json::to_string(&payload.value).map_err(|e| ApiError::Internal(e.to_string()))?;

    state
        .engine
        .put(key.clone(), encoded)
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    Ok(Json(PutResponse { key, success: true }))
}

async fn delete_record(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<StatusCode, ApiError> {
    state
        .engine
        .delete(&key)
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    Ok(StatusCode::NO_CONTENT)
}

// === Bulk Insert Handler ===

async fn bulk_insert(
    State(state): State<AppState>,
    Json(payload): Json<BulkInsertRequest>,
) -> Result<Json<BulkInsertResponse>, ApiError> {
    let mut inserted = 0;
    let mut failed = Vec::new();

    for record in payload.records {
        match serde_json::to_string(&record.value) {
            Ok(encoded) => match state.engine.put(record.key.clone(), encoded) {
                Ok(_) => inserted += 1,
                Err(e) => failed.push(BulkError {
                    key: record.key,
                    error: e.to_string(),
                }),
            },
            Err(e) => failed.push(BulkError {
                key: record.key,
                error: e.to_string(),
            }),
        }
    }

    Ok(Json(BulkInsertResponse { inserted, failed }))
}

// === Range Query Handler ===

async fn range_query(
    State(state): State<AppState>,
    Query(params): Query<RangeParams>,
) -> Result<Json<RangeResponse>, ApiError> {
    use std::ops::Bound::*;

    let lower = params
        .start
        .as_ref()
        .map(|s| Included(s))
        .unwrap_or(Unbounded);
    let upper = params
        .end
        .as_ref()
        .map(|s| Included(s))
        .unwrap_or(Unbounded);

    let limit = params.limit;
    let username_filter = params.username_contains.as_deref();
    let mut total = 0usize;
    let mut items = Vec::new();

    for (key, raw_value) in state.engine.range(lower, upper) {
        let value = decode_value(raw_value);
        if matches_username_filter(&value, username_filter) {
            total += 1;
            if limit.map_or(true, |max| items.len() < max) {
                items.push(RecordResponse { key, value });
            }
        }
    }

    Ok(Json(RangeResponse {
        total,
        results: items,
    }))
}

// === Aggregation Handler ===

async fn count_aggregation(
    State(state): State<AppState>,
    Query(params): Query<CountParams>,
) -> Result<Json<CountResponse>, ApiError> {
    use std::ops::Bound::*;

    let lower = params
        .start
        .as_ref()
        .map(|s| Included(s))
        .unwrap_or(Unbounded);
    let upper = params
        .end
        .as_ref()
        .map(|s| Included(s))
        .unwrap_or(Unbounded);

    let username_filter = params.username_contains.as_deref();
    let count = state
        .engine
        .range(lower, upper)
        .into_iter()
        .map(|(_, raw)| decode_value(raw))
        .filter(|value| matches_username_filter(value, username_filter))
        .count();

    Ok(Json(CountResponse { count }))
}

// === Health & Stats ===

async fn health_check() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

async fn stats(State(state): State<AppState>) -> Json<StatsResponse> {
    use std::ops::Bound::Unbounded;
    let total_keys = state.engine.range(Unbounded, Unbounded).len();

    Json(StatsResponse {
        total_keys,
        shards: state.shards,
        memory_mb: 0, // TODO: Calculate actual memory usage
    })
}

// === Request/Response Types ===

#[derive(Serialize)]
struct RootResponse {
    name: String,
    version: String,
    endpoints: Vec<String>,
    documentation: String,
}

#[derive(Deserialize)]
struct PutRequest {
    value: serde_json::Value,
}

#[derive(Serialize)]
struct PutResponse {
    key: String,
    success: bool,
}

#[derive(Serialize)]
struct RecordResponse {
    key: String,
    value: serde_json::Value,
}

#[derive(Deserialize)]
struct BulkInsertRequest {
    records: Vec<BulkRecord>,
}

#[derive(Deserialize)]
struct BulkRecord {
    key: String,
    value: serde_json::Value,
}

#[derive(Serialize)]
struct BulkInsertResponse {
    inserted: usize,
    failed: Vec<BulkError>,
}

#[derive(Serialize)]
struct BulkError {
    key: String,
    error: String,
}

#[derive(Deserialize)]
struct RangeParams {
    start: Option<String>,
    end: Option<String>,
    limit: Option<usize>,
    username_contains: Option<String>,
}

#[derive(Serialize)]
struct RangeResponse {
    total: usize,
    results: Vec<RecordResponse>,
}

#[derive(Deserialize)]
struct CountParams {
    start: Option<String>,
    end: Option<String>,
    username_contains: Option<String>,
}

#[derive(Serialize)]
struct CountResponse {
    count: usize,
}

#[derive(Serialize)]
struct HealthResponse {
    status: String,
    version: String,
}

#[derive(Serialize)]
struct StatsResponse {
    total_keys: usize,
    shards: usize,
    memory_mb: usize,
}

fn decode_value(raw: String) -> serde_json::Value {
    serde_json::from_str(&raw).unwrap_or_else(|_| serde_json::Value::String(raw))
}

fn matches_username_filter(value: &serde_json::Value, username_contains: Option<&str>) -> bool {
    if let Some(substr) = username_contains {
        match value.get("username").and_then(|v| v.as_str()) {
            Some(username) => username.contains(substr),
            None => false,
        }
    } else {
        true
    }
}

// === Error Handling ===

enum ApiError {
    NotFound,
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match self {
            ApiError::NotFound => (StatusCode::NOT_FOUND, "Record not found".to_string()),
            ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };

        let body = serde_json::json!({
            "error": message,
            "status": status.as_u16(),
        });

        (status, Json(body)).into_response()
    }
}

fn print_logo() {
    // Try to load custom logo from file
    let logo_path = "logo.txt";
    let logo = if let Ok(content) = std::fs::read_to_string(logo_path) {
        content
    } else {
        // Default embedded logo
        r#"
    _   __                      ____          __
   / | / /__  __  ___________  /  _/___  ____/ /__  _  __
  /  |/ / _ \/ / / / ___/ __ \ / // __ \/ __  / _ \| |/_/
 / /|  /  __/ /_/ / /  / /_/ // // / / / /_/ /  __/>  <
/_/ |_/\___/\__,_/_/   \____/___/_/ /_/\__,_/\___/_/|_|

      High-Performance In-Memory Database Engine
        Powered with Love and Rust by Antonio & Alessia ❤️
"#
        .to_string()
    };

    println!("{}", logo);
}
