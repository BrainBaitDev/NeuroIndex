// NeuroIndex HTTP REST API Server
// Provides JSON REST API for NeuroIndex database

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    middleware,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use clap::Parser;
use engine::{Engine, PersistenceConfig, SqlExecutor};
use serde::{Deserialize, Serialize};
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::signal;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::{info, warn, Level};

mod metrics;
use metrics::MetricsCollector;

mod resource_limits;
use resource_limits::ResourceLimits;

mod connection_limit;
use connection_limit::RequestLimitLayer;

use axum_server::Handle;

// Shared state
type SharedEngine = Arc<Engine<String, String>>;

#[derive(Clone)]
struct AppState {
    engine: SharedEngine,
    shards: usize,
    #[allow(dead_code)]
    capacity: usize,
    start_time: Instant,
    metrics: MetricsCollector,
    limits: ResourceLimits,
    auth_token: Option<String>,
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

    /// Background snapshot interval in seconds (0 disables)
    #[arg(long, default_value = "60")]
    snapshot_interval: u64,

    /// Maximum concurrent connections (0 = unlimited)
    #[arg(long, default_value = "0")]
    max_connections: usize,

    /// Maximum memory usage in MB (0 = unlimited)
    #[arg(long, default_value = "0")]
    max_memory_mb: usize,

    /// Maximum request size in MB
    #[arg(long, default_value = "10")]
    max_request_mb: usize,

    /// TLS certificate file (PEM format)
    #[arg(long)]
    tls_cert: Option<PathBuf>,

    /// TLS private key file (PEM format)
    #[arg(long)]
    tls_key: Option<PathBuf>,

    /// Authentication token (Bearer token for API access)
    #[arg(long)]
    auth_token: Option<String>,
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

    let limits = ResourceLimits::new(
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
        args.max_request_mb,
    );

    info!(
        "Resource limits: max_connections={}, max_memory_mb={}, max_request_mb={}",
        args.max_connections, args.max_memory_mb, args.max_request_mb
    );

    let app_state = AppState {
        engine: Arc::clone(&engine),
        shards: args.shards,
        capacity: args.capacity,
        start_time: Instant::now(),
        metrics: MetricsCollector::new(),
        limits,
        auth_token: args.auth_token.clone(),
    };

    if args.auth_token.is_some() {
        info!("Authentication enabled (Bearer token required)");
    }

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
        // SQL queries
        .route("/api/v1/sql", post(execute_sql))
        // Health & Stats
        .route("/api/v1/health", get(health_check))
        .route("/api/v1/stats", get(stats))
        // Prometheus metrics
        .route("/api/v1/metrics", get(prometheus_metrics))
        // Root
        .route("/", get(root))
        // State
        .with_state(app_state.clone())
        // Middleware
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .layer(middleware::from_fn_with_state(
            app_state.clone(),
            auth_middleware,
        ))
        .layer(middleware::from_fn_with_state(
            app_state.clone(),
            memory_limit_middleware,
        ))
        .layer(tower_http::limit::RequestBodyLimitLayer::new(
            args.max_request_mb * 1024 * 1024,
        ));

    // Apply request limit layer if max_connections is set
    let app = if args.max_connections > 0 {
        info!("Limiting concurrent requests to {}", args.max_connections);
        app.layer(RequestLimitLayer::new(args.max_connections))
    } else {
        app
    };

    // Print server info
    let protocol = if args.tls_cert.is_some() && args.tls_key.is_some() {
        "https"
    } else {
        "http"
    };

    println!();
    info!("╔═══════════════════════════════════════════════════════════╗");
    info!("║     NeuroIndex HTTP REST API Server v0.1.0             ║");
    info!("╠═══════════════════════════════════════════════════════════╣");
    info!(
        "║  Listening on: {}://{}:{:<35}║",
        protocol, args.host, args.port
    );
    if protocol == "https" {
        info!("║  TLS: ENABLED                                             ║");
    }
    info!("║  Shards: {:<49}║", args.shards);
    info!("║  Capacity: {:<47}║", args.capacity * args.shards);
    info!("╚═══════════════════════════════════════════════════════════╝");
    info!("");
    info!("API Endpoints:");
    info!(
        "  Health:       GET  {}://{}:{}/api/v1/health",
        protocol, args.host, args.port
    );
    info!(
        "  Stats:        GET  {}://{}:{}/api/v1/stats",
        protocol, args.host, args.port
    );
    info!(
        "  Get Record:   GET  {}://{}:{}/api/v1/records/{{key}}",
        protocol, args.host, args.port
    );
    info!(
        "  Put Record:   POST {}://{}:{}/api/v1/records/{{key}}",
        protocol, args.host, args.port
    );
    info!(
        "  Delete Record: DEL {}://{}:{}/api/v1/records/{{key}}",
        protocol, args.host, args.port
    );
    info!(
        "  Bulk Insert:  POST {}://{}:{}/api/v1/records/bulk",
        protocol, args.host, args.port
    );
    info!(
        "  Range Query:  GET  {}://{}:{}/api/v1/records/range",
        protocol, args.host, args.port
    );
    info!(
        "  Count Agg:    GET  {}://{}:{}/api/v1/aggregations/count",
        protocol, args.host, args.port
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

    // Setup graceful shutdown
    let engine_for_shutdown = Arc::clone(&engine);
    let addr = format!("{}:{}", args.host, args.port);

    // Start server with TLS or plain HTTP
    if let (Some(cert_path), Some(key_path)) = (&args.tls_cert, &args.tls_key) {
        let tls_config = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert_path, key_path)
            .await
            .expect("Failed to load TLS config");

        let handle = Handle::new();
        let shutdown_handle = handle.clone();

        tokio::spawn(async move {
            shutdown_signal(engine_for_shutdown).await;
            shutdown_handle.shutdown();
        });

        axum_server::bind_rustls(addr.parse().unwrap(), tls_config)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .unwrap();
    } else {
        let listener = match TcpListener::bind(&addr).await {
            Ok(listener) => listener,
            Err(e) => {
                eprintln!("Failed to bind to address {}: {}", addr, e);
                std::process::exit(1);
            }
        };

        tokio::select! {
            result = axum::serve(listener, app) => {
                if let Err(e) = result {
                    eprintln!("Server error: {}", e);
                    std::process::exit(1);
                }
            },
            _ = shutdown_signal(engine_for_shutdown) => {},
        }
    }

    // Stop background snapshots cleanly
    if let Some(handle) = snapshot_handle {
        info!("Stopping background snapshot worker...");
        handle.abort();
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
    state.metrics.record_get();

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
    state.metrics.record_set();

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
    state.metrics.record_delete();

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
    state.metrics.record_scan();

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

    for (key, raw_value) in state.engine.range_streaming(lower, upper) {
        let value = decode_value(raw_value);
        if matches_username_filter(&value, username_filter) {
            total += 1;
            if let Some(max) = limit {
                if items.len() < max {
                    items.push(RecordResponse { key, value });
                }
            } else {
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
        .range_streaming(lower, upper)
        .map(|(_, raw)| decode_value(raw))
        .filter(|value| matches_username_filter(value, username_filter))
        .count();

    Ok(Json(CountResponse { count }))
}

// === Health & Stats ===

async fn health_check(State(state): State<AppState>) -> Json<HealthResponse> {
    let uptime_secs = state.start_time.elapsed().as_secs();
    let total_keys = state.engine.total_keys() as usize;
    let memory_bytes = state.engine.memory_usage();

    Json(HealthResponse {
        status: "healthy".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_seconds: uptime_secs,
        total_keys,
        memory_mb: memory_bytes / (1024 * 1024),
        shards: state.shards,
    })
}

async fn stats(State(state): State<AppState>) -> Json<StatsResponse> {
    let total_keys = state.engine.total_keys() as usize;
    let memory_bytes = state.engine.memory_usage();

    Json(StatsResponse {
        total_keys,
        shards: state.shards,
        memory_mb: memory_bytes / (1024 * 1024),
    })
}

async fn prometheus_metrics(State(state): State<AppState>) -> impl IntoResponse {
    let total_keys = state.engine.total_keys();
    let memory_bytes = state.engine.memory_usage();

    let metrics_text = state
        .metrics
        .export_prometheus(total_keys, (memory_bytes / (1024 * 1024)) as u64);

    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        metrics_text,
    )
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
    uptime_seconds: u64,
    total_keys: usize,
    memory_mb: usize,
    shards: usize,
}

#[derive(Serialize)]
struct StatsResponse {
    total_keys: usize,
    shards: usize,
    memory_mb: usize,
}

#[derive(Deserialize)]
struct SqlRequest {
    query: String,
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

// === SQL Handler ===

async fn execute_sql(
    State(state): State<AppState>,
    Json(payload): Json<SqlRequest>,
) -> impl IntoResponse {
    match SqlExecutor::execute(&state.engine, &payload.query) {
        Ok(result) => {
            let json = serde_json::to_value(&result).unwrap();
            (StatusCode::OK, Json(json)).into_response()
        }
        Err(e) => {
            let body = serde_json::json!({
                "error": e,
                "status": 400,
            });
            (StatusCode::BAD_REQUEST, Json(body)).into_response()
        }
    }
}

fn print_logo() {
    // Try to load custom logo from file
    let logo = include_str!("../../resp-server/logo.txt");
    println!("{}", logo);
}

// === Middleware ===

async fn auth_middleware(
    State(state): State<AppState>,
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> impl IntoResponse {
    // Skip auth for health endpoint
    if req.uri().path() == "/api/v1/health" {
        return next.run(req).await;
    }

    // Check if auth is required
    if let Some(required_token) = &state.auth_token {
        // Extract Authorization header
        let auth_header = req
            .headers()
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok());

        match auth_header {
            Some(header) if header.starts_with("Bearer ") => {
                let token = &header[7..]; // Skip "Bearer "
                if token == required_token {
                    return next.run(req).await;
                }
            }
            _ => {}
        }

        // Authentication failed
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({
                "error": "Unauthorized",
                "message": "Valid Bearer token required",
            })),
        )
            .into_response();
    }

    // No auth required
    next.run(req).await
}

async fn memory_limit_middleware(
    State(state): State<AppState>,
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> impl IntoResponse {
    // Check memory limit before processing request
    let current_memory = state.engine.memory_usage();

    if !state.limits.check_memory(current_memory) {
        let max_mb = state.limits.max_memory_bytes().unwrap() / (1024 * 1024);
        let current_mb = current_memory / (1024 * 1024);

        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "Memory limit exceeded",
                "current_mb": current_mb,
                "max_mb": max_mb,
            })),
        )
            .into_response();
    }

    next.run(req).await
}
