use crate::raft::LogEntry;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

/// RPC error type
#[derive(Debug)]
pub enum RpcError {
    NetworkError(String),
    SerializationError(String),
    Timeout,
    InvalidResponse,
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RpcError::NetworkError(msg) => write!(f, "Network error: {}", msg),
            RpcError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            RpcError::Timeout => write!(f, "Request timeout"),
            RpcError::InvalidResponse => write!(f, "Invalid response"),
        }
    }
}

impl Error for RpcError {}

/// RequestVote RPC request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    pub term: u64,
    pub candidate_id: String,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

/// RequestVote RPC response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

/// AppendEntries RPC request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest<K, V> {
    pub term: u64,
    pub leader_id: String,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry<K, V>>,
    pub leader_commit: u64,
}

/// AppendEntries RPC response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
}

/// Trait for Raft RPC communication
pub trait RaftRpc<K, V>: Send + Sync {
    /// Send RequestVote RPC to a peer
    fn request_vote(
        &self,
        addr: &str,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, RpcError>;

    /// Send AppendEntries RPC to a peer
    fn append_entries(
        &self,
        addr: &str,
        req: AppendEntriesRequest<K, V>,
    ) -> Result<AppendEntriesResponse, RpcError>;
}

/// HTTP-based RPC client implementation
pub struct HttpRaftRpc;

impl HttpRaftRpc {
    pub fn new() -> Self {
        Self
    }
}

impl<K, V> RaftRpc<K, V> for HttpRaftRpc
where
    K: Serialize + for<'de> Deserialize<'de>,
    V: Serialize + for<'de> Deserialize<'de>,
{
    fn request_vote(
        &self,
        addr: &str,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, RpcError> {
        let url = format!("http://{}/raft/request_vote", addr);
        let body = serde_json::to_string(&req)
            .map_err(|e| RpcError::SerializationError(e.to_string()))?;

        let response = ureq::post(&url)
            .set("Content-Type", "application/json")
            .send_string(&body)
            .map_err(|e| RpcError::NetworkError(e.to_string()))?;

        let resp: RequestVoteResponse = response
            .into_json()
            .map_err(|e| RpcError::SerializationError(e.to_string()))?;

        Ok(resp)
    }

    fn append_entries(
        &self,
        addr: &str,
        req: AppendEntriesRequest<K, V>,
    ) -> Result<AppendEntriesResponse, RpcError> {
        let url = format!("http://{}/raft/append_entries", addr);
        let body = serde_json::to_string(&req)
            .map_err(|e| RpcError::SerializationError(e.to_string()))?;

        let response = ureq::post(&url)
            .set("Content-Type", "application/json")
            .send_string(&body)
            .map_err(|e| RpcError::NetworkError(e.to_string()))?;

        let resp: AppendEntriesResponse = response
            .into_json()
            .map_err(|e| RpcError::SerializationError(e.to_string()))?;

        Ok(resp)
    }
}

/// HTTP server for handling incoming Raft RPCs
pub struct RaftHttpServer<K, V> {
    addr: String,
    _phantom: std::marker::PhantomData<(K, V)>,
}

impl<K, V> RaftHttpServer<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
    V: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    pub fn new(addr: String) -> Self {
        Self {
            addr,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Start the HTTP server (blocking)
    /// Handlers should be provided as callbacks
    pub fn start<F1, F2>(
        &self,
        request_vote_handler: F1,
        append_entries_handler: F2,
    ) -> Result<(), Box<dyn Error>>
    where
        F1: Fn(RequestVoteRequest) -> RequestVoteResponse + Send + Sync + 'static,
        F2: Fn(AppendEntriesRequest<K, V>) -> AppendEntriesResponse + Send + Sync + 'static,
    {
        let server = tiny_http::Server::http(&self.addr)
            .map_err(|e| format!("Failed to start server: {}", e))?;

        eprintln!("Raft HTTP server listening on {}", self.addr);

        for mut request in server.incoming_requests() {
            let url = request.url().to_string();
            
            if url == "/raft/request_vote" {
                // Handle RequestVote RPC
                let mut body = String::new();
                if let Err(e) = request.as_reader().read_to_string(&mut body) {
                    eprintln!("Error reading request body: {}", e);
                    continue;
                }

                match serde_json::from_str::<RequestVoteRequest>(&body) {
                    Ok(req) => {
                        let resp = request_vote_handler(req);
                        let resp_json = serde_json::to_string(&resp).unwrap();
                        let response = tiny_http::Response::from_string(resp_json)
                            .with_header(
                                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap()
                            );
                        let _ = request.respond(response);
                    }
                    Err(e) => {
                        eprintln!("Error parsing RequestVote: {}", e);
                        let _ = request.respond(tiny_http::Response::from_string("Invalid request").with_status_code(400));
                    }
                }
            } else if url == "/raft/append_entries" {
                // Handle AppendEntries RPC
                let mut body = String::new();
                if let Err(e) = request.as_reader().read_to_string(&mut body) {
                    eprintln!("Error reading request body: {}", e);
                    continue;
                }

                match serde_json::from_str::<AppendEntriesRequest<K, V>>(&body) {
                    Ok(req) => {
                        let resp = append_entries_handler(req);
                        let resp_json = serde_json::to_string(&resp).unwrap();
                        let response = tiny_http::Response::from_string(resp_json)
                            .with_header(
                                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap()
                            );
                        let _ = request.respond(response);
                    }
                    Err(e) => {
                        eprintln!("Error parsing AppendEntries: {}", e);
                        let _ = request.respond(tiny_http::Response::from_string("Invalid request").with_status_code(400));
                    }
                }
            } else {
                let _ = request.respond(tiny_http::Response::from_string("Not found").with_status_code(404));
            }
        }

        Ok(())
    }
}
