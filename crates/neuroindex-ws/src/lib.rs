pub mod integrated_server;

use axum::extract::ws::{Message, WebSocket};
use engine::change_events::ChangeSubscriber;
use futures_util::{SinkExt, StreamExt};
use parking_lot::RwLock;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::sync::mpsc;

/// Messaggio dal client al server
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum ClientMessage {
    #[serde(rename = "subscribe")]
    Subscribe { pattern: String },
    #[serde(rename = "unsubscribe")]
    Unsubscribe { pattern: String },
    #[serde(rename = "ping")]
    Ping,
}

/// Messaggio dal server al client
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum ServerMessage {
    #[serde(rename = "update")]
    Update {
        key: String,
        value: String,
        timestamp: i64,
    },
    #[serde(rename = "delete")]
    Delete { key: String, timestamp: i64 },
    #[serde(rename = "subscribed")]
    Subscribed { pattern: String },
    #[serde(rename = "unsubscribed")]
    Unsubscribed { pattern: String },
    #[serde(rename = "pong")]
    Pong,
    #[serde(rename = "error")]
    Error { message: String },
}

/// Cliente WebSocket connesso
#[derive(Debug)]
struct Client {
    id: u64,
    patterns: Vec<Regex>,
    tx: mpsc::UnboundedSender<ServerMessage>,
}

impl Client {
    fn matches(&self, key: &str) -> bool {
        if self.patterns.is_empty() {
            return true; // No filters = match all
        }
        self.patterns.iter().any(|pattern| pattern.is_match(key))
    }
}

/// Bridge tra Engine change events e WebSocket clients
#[derive(Debug, Clone)]
pub struct WebSocketBridge {
    clients: Arc<RwLock<HashMap<u64, Client>>>,
}

impl WebSocketBridge {
    pub fn new() -> Self {
        Self {
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn add_client(&self, id: u64, tx: mpsc::UnboundedSender<ServerMessage>) {
        let client = Client {
            id,
            patterns: Vec::new(),
            tx,
        };
        self.clients.write().insert(id, client);
        tracing::info!("Client {} connected", id);
    }

    pub fn remove_client(&self, id: u64) {
        self.clients.write().remove(&id);
        tracing::info!("Client {} disconnected", id);
    }

    pub fn subscribe(&self, id: u64, pattern: &str) -> Result<(), String> {
        let regex_pattern = glob_to_regex(pattern);
        let regex = Regex::new(&regex_pattern).map_err(|e| format!("Invalid pattern: {}", e))?;

        if let Some(client) = self.clients.write().get_mut(&id) {
            client.patterns.push(regex);
            tracing::info!("Client {} subscribed to pattern: {}", id, pattern);
            Ok(())
        } else {
            Err("Client not found".to_string())
        }
    }

    pub fn unsubscribe(&self, id: u64, pattern: &str) -> Result<(), String> {
        let regex_pattern = glob_to_regex(pattern);

        if let Some(client) = self.clients.write().get_mut(&id) {
            client.patterns.retain(|r| r.as_str() != regex_pattern);
            tracing::info!("Client {} unsubscribed from pattern: {}", id, pattern);
            Ok(())
        } else {
            Err("Client not found".to_string())
        }
    }

    fn broadcast_filtered(&self, key: &str, message: ServerMessage) {
        let clients = self.clients.read();
        for client in clients.values() {
            if client.matches(key) {
                let _ = client.tx.send(message.clone());
            }
        }
    }
}

impl ChangeSubscriber for WebSocketBridge {
    fn on_put(&self, key: &str, value: &str) {
        let message = ServerMessage::Update {
            key: key.to_string(),
            value: value.to_string(),
            timestamp: chrono::Utc::now().timestamp(),
        };
        self.broadcast_filtered(key, message);
    }

    fn on_delete(&self, key: &str) {
        let message = ServerMessage::Delete {
            key: key.to_string(),
            timestamp: chrono::Utc::now().timestamp(),
        };
        self.broadcast_filtered(key, message);
    }
}

/// Converte glob pattern (* e ?) in regex
fn glob_to_regex(pattern: &str) -> String {
    let mut regex = String::from("^");
    for ch in pattern.chars() {
        match ch {
            '*' => regex.push_str(".*"),
            '?' => regex.push('.'),
            '.' | '+' | '(' | ')' | '[' | ']' | '{' | '}' | '^' | '$' | '|' | '\\' => {
                regex.push('\\');
                regex.push(ch);
            }
            _ => regex.push(ch),
        }
    }
    regex.push('$');
    regex
}

/// Stato condiviso dell'applicazione
#[derive(Clone)]
pub struct AppState {
    pub bridge: WebSocketBridge,
    pub next_id: Arc<AtomicU64>,
}

/// Gestisce una connessione WebSocket
pub async fn handle_socket(socket: WebSocket, state: AppState) {
    let client_id = state.next_id.fetch_add(1, Ordering::SeqCst);
    let (mut sender, mut receiver) = socket.split();

    // Canale per inviare messaggi al client
    let (tx, mut rx) = mpsc::unbounded_channel::<ServerMessage>();

    // Aggiungi client al bridge
    state.bridge.add_client(client_id, tx);

    // Task per inviare messaggi al WebSocket
    let mut send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let json = serde_json::to_string(&msg).unwrap();
            if sender.send(Message::Text(json)).await.is_err() {
                break;
            }
        }
    });

    // Task per ricevere messaggi dal WebSocket
    let bridge = state.bridge.clone();
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(Message::Text(text))) = receiver.next().await {
            match serde_json::from_str::<ClientMessage>(&text) {
                Ok(ClientMessage::Subscribe { pattern }) => {
                    let response = match bridge.subscribe(client_id, &pattern) {
                        Ok(_) => ServerMessage::Subscribed { pattern },
                        Err(e) => ServerMessage::Error { message: e },
                    };
                    if let Some(client) = bridge.clients.read().get(&client_id) {
                        let _ = client.tx.send(response);
                    }
                }
                Ok(ClientMessage::Unsubscribe { pattern }) => {
                    let response = match bridge.unsubscribe(client_id, &pattern) {
                        Ok(_) => ServerMessage::Unsubscribed { pattern },
                        Err(e) => ServerMessage::Error { message: e },
                    };
                    if let Some(client) = bridge.clients.read().get(&client_id) {
                        let _ = client.tx.send(response);
                    }
                }
                Ok(ClientMessage::Ping) => {
                    if let Some(client) = bridge.clients.read().get(&client_id) {
                        let _ = client.tx.send(ServerMessage::Pong);
                    }
                }
                Err(e) => {
                    tracing::warn!("Invalid message from client {}: {}", client_id, e);
                    if let Some(client) = bridge.clients.read().get(&client_id) {
                        let _ = client.tx.send(ServerMessage::Error {
                            message: format!("Invalid message: {}", e),
                        });
                    }
                }
            }
        }
    });

    // Aspetta che uno dei task finisca
    tokio::select! {
        _ = &mut send_task => recv_task.abort(),
        _ = &mut recv_task => send_task.abort(),
    }

    // Rimuovi client dal bridge
    state.bridge.remove_client(client_id);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_to_regex() {
        assert_eq!(glob_to_regex("user:*"), r"^user:.*$");
        assert_eq!(glob_to_regex("user:?23"), r"^user:.23$");
        assert_eq!(glob_to_regex("user.*"), r"^user\..*$");
    }

    #[test]
    fn test_client_matches() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut client = Client {
            id: 1,
            patterns: vec![],
            tx,
        };

        // No patterns = match all
        assert!(client.matches("anything"));

        // Add pattern
        client
            .patterns
            .push(Regex::new(&glob_to_regex("user:*")).unwrap());
        assert!(client.matches("user:123"));
        assert!(client.matches("user:abc"));
        assert!(!client.matches("session:123"));

        // Multiple patterns (OR logic)
        client
            .patterns
            .push(Regex::new(&glob_to_regex("session:*")).unwrap());
        assert!(client.matches("user:123"));
        assert!(client.matches("session:456"));
        assert!(!client.matches("product:789"));
    }
}
