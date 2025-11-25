/// Server integrato HTTP + WebSocket per testing
/// Espone sia le API REST che le WebSocket subscriptions sullo stesso Engine

use crate::{handle_socket, AppState, WebSocketBridge};
use axum::{
    extract::{ws::WebSocketUpgrade, Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use engine::Engine;
use serde::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    sync::{
        atomic::AtomicU64,
        Arc,
    },
};
use tower_http::cors::CorsLayer;

#[derive(Clone)]
struct IntegratedState {
    engine: Arc<Engine<String, String>>,
    ws_state: AppState,
}

#[derive(Deserialize)]
struct PutRequest {
    value: String,
}

#[derive(Serialize)]
struct GetResponse {
    value: String,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

/// GET /api/v1/records/:key
async fn get_handler(
    Path(key): Path<String>,
    State(state): State<IntegratedState>,
) -> impl IntoResponse {
    match state.engine.get(&key) {
        Some(value) => (StatusCode::OK, Json(GetResponse { value })).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "Key not found".to_string(),
            }),
        )
            .into_response(),
    }
}

/// POST /api/v1/records/:key
async fn put_handler(
    Path(key): Path<String>,
    State(state): State<IntegratedState>,
    Json(req): Json<PutRequest>,
) -> impl IntoResponse {
    match state.engine.put(key, req.value) {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// DELETE /api/v1/records/:key
async fn delete_handler(
    Path(key): Path<String>,
    State(state): State<IntegratedState>,
) -> impl IntoResponse {
    match state.engine.delete(&key) {
        Ok(Some(_)) => StatusCode::OK,
        Ok(None) => StatusCode::NOT_FOUND,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// WebSocket endpoint
async fn ws_handler(ws: WebSocketUpgrade, State(state): State<IntegratedState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state.ws_state))
}

/// Health check
async fn health() -> &'static str {
    "OK"
}

pub async fn run_integrated_server() {
    tracing_subscriber::fmt::init();

    // Create shared engine
    let engine = Arc::new(Engine::<String, String>::with_shards(4, 16));

    // Create WebSocket bridge and register it
    let bridge = WebSocketBridge::new();
    engine.register_subscriber(Arc::new(bridge.clone()));

    tracing::info!("Registered WebSocket bridge as change subscriber");

    // Create integrated state
    let ws_state = AppState {
        bridge,
        next_id: Arc::new(AtomicU64::new(1)),
    };

    let state = IntegratedState {
        engine: engine.clone(),
        ws_state: ws_state.clone(),
    };

    // Router with both HTTP and WebSocket endpoints
    let app = Router::new()
        // WebSocket
        .route("/ws", get(ws_handler))
        // REST API
        .route("/api/v1/records/:key", get(get_handler))
        .route("/api/v1/records/:key", post(put_handler))
        .route("/api/v1/records/:key", delete(delete_handler))
        // Health
        .route("/health", get(health))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 9090));
    tracing::info!("Integrated server listening on {}", addr);
    tracing::info!("  - REST API: http://localhost:9090/api/v1/records/:key");
    tracing::info!("  - WebSocket: ws://localhost:9090/ws");

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
