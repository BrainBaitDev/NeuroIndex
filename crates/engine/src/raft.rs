use crate::replication::ReplicationOp;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::cmp::min;
use std::thread;
use crate::raft_rpc::{RaftRpc, HttpRaftRpc};

/// Raft node state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftState {
    Follower,
    Candidate,
    Leader,
}

/// Log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry<K, V> {
    pub term: u64,
    pub index: u64,
    pub command: ReplicationOp<K, V>,
}

/// Raft node
pub struct RaftNode<K, V> {
    id: String,
    state: RwLock<RaftState>,
    current_term: AtomicU64,
    voted_for: RwLock<Option<String>>,
    leader_id: RwLock<Option<String>>,
    last_heartbeat: RwLock<Instant>,
    election_timeout: Duration,
    _heartbeat_interval: Duration,
    peers: Arc<RwLock<Vec<String>>>,
    
    // Log replication
    log: RwLock<Vec<LogEntry<K, V>>>,
    commit_index: AtomicU64,
    last_applied: AtomicU64,
    
    // Leader state (reinitialized after election)
    next_index: RwLock<HashMap<String, u64>>,
    match_index: RwLock<HashMap<String, u64>>,
    
    // Heartbeat control
    heartbeat_thread_running: Arc<AtomicU64>, // 0 = stopped, 1 = running
    
    // Network layer
    rpc_client: Option<Arc<dyn RaftRpc<K, V>>>,
    listen_addr: Option<String>,
}

impl<K, V> RaftNode<K, V> 
where 
    K: Clone + Serialize + for<'de> serde::Deserialize<'de>,
    V: Clone + Serialize + for<'de> serde::Deserialize<'de>,
{
    /// Create new Raft node
    pub fn new(id: String) -> Self {
        Self {
            id,
            state: RwLock::new(RaftState::Follower),
            current_term: AtomicU64::new(0),
            voted_for: RwLock::new(None),
            leader_id: RwLock::new(None),
            last_heartbeat: RwLock::new(Instant::now()),
            election_timeout: Duration::from_millis(150),
            _heartbeat_interval: Duration::from_millis(50),
            peers: Arc::new(RwLock::new(Vec::new())),
            
            log: RwLock::new(Vec::new()),
            commit_index: AtomicU64::new(0),
            last_applied: AtomicU64::new(0),
            
            next_index: RwLock::new(HashMap::new()),
            match_index: RwLock::new(HashMap::new()),
            
            heartbeat_thread_running: Arc::new(AtomicU64::new(0)),
            
            rpc_client: None,
            listen_addr: None,
        }
    }

    /// Configure network layer
    pub fn with_network(mut self, listen_addr: String) -> Self {
        self.rpc_client = Some(Arc::new(HttpRaftRpc::new()));
        self.listen_addr = Some(listen_addr);
        self
    }

    /// Get node ID
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Get current state
    pub fn state(&self) -> RaftState {
        *self.state.read()
    }

    /// Get current term
    pub fn term(&self) -> u64 {
        self.current_term.load(Ordering::SeqCst)
    }

    /// Add peer
    pub fn add_peer(&self, peer_id: String) {
        let mut peers = self.peers.write();
        if !peers.contains(&peer_id) && peer_id != self.id {
            peers.push(peer_id);
        }
    }

    /// Start election (become candidate)
    pub fn start_election(&self) -> u64 {
        // Increment term
        let new_term = self.current_term.fetch_add(1, Ordering::SeqCst) + 1;

        // Become candidate
        *self.state.write() = RaftState::Candidate;

        // Vote for self
        *self.voted_for.write() = Some(self.id.clone());

        new_term
    }

    /// Request vote from this node
    pub fn request_vote(&self, candidate_id: &str, term: u64, last_log_index: u64, last_log_term: u64) -> bool {
        let current_term = self.term();

        // Reject if term is old
        if term < current_term {
            return false;
        }

        // Update term if newer
        if term > current_term {
            self.current_term.store(term, Ordering::SeqCst);
            *self.voted_for.write() = None;
            *self.state.write() = RaftState::Follower;
        }

        // Check log up-to-date
        let log = self.log.read();
        let my_last_index = log.last().map(|e| e.index).unwrap_or(0);
        let my_last_term = log.last().map(|e| e.term).unwrap_or(0);
        
        if last_log_term < my_last_term {
            return false;
        }
        if last_log_term == my_last_term && last_log_index < my_last_index {
            return false;
        }

        // Grant vote if haven't voted yet or voted for candidate
        let mut voted = self.voted_for.write();
        if voted.is_none() || voted.as_deref() == Some(candidate_id) {
            *voted = Some(candidate_id.to_string());
            true
        } else {
            false
        }
    }

    /// Become leader
    pub fn become_leader(&self) {
        *self.state.write() = RaftState::Leader;
        *self.leader_id.write() = Some(self.id.clone());
        
        // Initialize leader state
        let last_log_index = self.log.read().last().map(|e| e.index).unwrap_or(0);
        let peers = self.peers.read();
        
        let mut next_index = self.next_index.write();
        let mut match_index = self.match_index.write();
        
        next_index.clear();
        match_index.clear();
        
        for peer in peers.iter() {
            next_index.insert(peer.clone(), last_log_index + 1);
            match_index.insert(peer.clone(), 0);
        }
        
        // Start heartbeat thread
        self.start_heartbeat();
    }

    /// Append entries (Heartbeat or Log Replication)
    pub fn append_entries(
        &self, 
        leader_id: &str, 
        term: u64, 
        prev_log_index: u64, 
        prev_log_term: u64, 
        entries: Vec<LogEntry<K, V>>, 
        leader_commit: u64
    ) -> bool {
        let current_term = self.term();

        if term < current_term {
            return false;
        }

        // Update term and leader
        if term >= current_term {
            self.current_term.store(term, Ordering::SeqCst);
            *self.state.write() = RaftState::Follower;
            *self.leader_id.write() = Some(leader_id.to_string());
            *self.last_heartbeat.write() = Instant::now();
        }

        let mut log = self.log.write();
        
        // Reply false if log doesn't contain an entry at prev_log_index whose term matches prev_log_term
        if prev_log_index > 0 {
            if log.len() < prev_log_index as usize {
                return false;
            }
            let entry = &log[prev_log_index as usize - 1];
            if entry.term != prev_log_term {
                return false;
            }
        }

        // If an existing entry conflicts with a new one (same index but different terms),
        // delete the existing entry and all that follow it
        for (i, entry) in entries.iter().enumerate() {
            let index = (prev_log_index as usize) + i;
            if index < log.len() {
                if log[index].term != entry.term {
                    log.truncate(index);
                    log.push(entry.clone());
                }
            } else {
                log.push(entry.clone());
            }
        }

        // Update commit index
        if leader_commit > self.commit_index.load(Ordering::SeqCst) {
            let last_new_index = entries.last().map(|e| e.index).unwrap_or(prev_log_index);
            self.commit_index.store(
                min(leader_commit, last_new_index), 
                Ordering::SeqCst
            );
        }

        true
    }

    /// Propose a new command (Leader only)
    pub fn propose(&self, command: ReplicationOp<K, V>) -> Option<u64> {
        if self.state() != RaftState::Leader {
            return None;
        }

        let mut log = self.log.write();
        let index = log.len() as u64 + 1;
        let term = self.term();
        
        log.push(LogEntry {
            term,
            index,
            command,
        });
        
        Some(index)
    }

    /// Check if election timeout elapsed
    pub fn election_timeout_elapsed(&self) -> bool {
        let last = *self.last_heartbeat.read();
        last.elapsed() > self.election_timeout
    }

    /// Get leader ID
    pub fn leader(&self) -> Option<String> {
        self.leader_id.read().clone()
    }

    /// Reset to follower
    pub fn step_down(&self, term: u64) {
        self.stop_heartbeat();
        self.current_term.store(term, Ordering::SeqCst);
        *self.state.write() = RaftState::Follower;
        *self.voted_for.write() = None;
    }

    /// Get stats
    pub fn stats(&self) -> RaftNodeStats {
        RaftNodeStats {
            id: self.id.clone(),
            state: self.state(),
            term: self.term(),
            leader: self.leader(),
            peer_count: self.peers.read().len(),
        }
    }

    /// Get committed entries that haven't been applied yet
    pub fn get_committed_entries(&self) -> Vec<LogEntry<K, V>> {
        let commit_idx = self.commit_index.load(Ordering::SeqCst);
        let last_applied = self.last_applied.load(Ordering::SeqCst);
        
        if last_applied >= commit_idx {
            return Vec::new();
        }
        
        let log = self.log.read();
        let start = last_applied as usize;
        let end = commit_idx as usize;
        
        if end > log.len() {
            return Vec::new();
        }
        
        log[start..end].to_vec()
    }

    /// Update last_applied index
    pub fn update_last_applied(&self, index: u64) {
        self.last_applied.store(index, Ordering::SeqCst);
    }

    /// Get commit index
    pub fn commit_index(&self) -> u64 {
        self.commit_index.load(Ordering::SeqCst)
    }

    /// Get last applied index
    pub fn last_applied(&self) -> u64 {
        self.last_applied.load(Ordering::SeqCst)
    }

    /// Start heartbeat thread (leader only)
    fn start_heartbeat(&self) {
        // Stop any existing heartbeat thread
        self.stop_heartbeat();
        
        // Set running flag
        self.heartbeat_thread_running.store(1, Ordering::Relaxed);
        
        let running = self.heartbeat_thread_running.clone();
        let peers = self.peers.clone();
        
        // Spawn heartbeat thread
        thread::spawn(move || {
            while running.load(Ordering::Relaxed) == 1 {
                let peers_list = peers.read();
                
                // In a real implementation with network layer, we would:
                // for peer in peers_list.iter() {
                //     peer.send_append_entries(id, term, 0, 0, Vec::new(), commit_index);
                // }
                // For now, this thread just demonstrates the heartbeat mechanism
                
                drop(peers_list);
                thread::sleep(Duration::from_millis(50));
            }
        });
    }

    /// Stop heartbeat thread
    fn stop_heartbeat(&self) {
        self.heartbeat_thread_running.store(0, Ordering::Relaxed);
    }
}

/// Raft node statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftNodeStats {
    pub id: String,
    pub state: RaftState,
    pub term: u64,
    pub leader: Option<String>,
    pub peer_count: usize,
}

impl std::fmt::Display for RaftNodeStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {:?} (term {}, leader: {:?}, peers: {})",
            self.id, self.state, self.term, self.leader, self.peer_count
        )
    }
}

/// Simple Raft cluster for simulation
pub struct RaftCluster<K, V> {
    nodes: HashMap<String, Arc<RaftNode<K, V>>>,
}

impl<K, V> RaftCluster<K, V> 
where
    K: Clone + Serialize + for<'de> serde::Deserialize<'de>,
    V: Clone + Serialize + for<'de> serde::Deserialize<'de>,
{
    /// Create new cluster
    pub fn new(node_ids: Vec<String>) -> Self {
        let mut nodes = HashMap::new();

        // Create nodes
        for id in &node_ids {
            let node = Arc::new(RaftNode::new(id.clone()));
            nodes.insert(id.clone(), node);
        }

        // Add peers to each node
        for id in &node_ids {
            let node = &nodes[id];
            for peer_id in &node_ids {
                if peer_id != id {
                    node.add_peer(peer_id.clone());
                }
            }
        }

        Self { nodes }
    }

    /// Run election
    pub fn run_election(&self, candidate_id: &str) -> bool {
        let candidate = &self.nodes[candidate_id];

        // Start election
        let term = candidate.start_election();

        // Request votes
        let mut votes = 1; // Vote for self
        let total = self.nodes.len();
        
        let log = candidate.log.read();
        let last_log_index = log.last().map(|e| e.index).unwrap_or(0);
        let last_log_term = log.last().map(|e| e.term).unwrap_or(0);
        drop(log);

        for (id, node) in &self.nodes {
            if id != candidate_id {
                if node.request_vote(candidate_id, term, last_log_index, last_log_term) {
                    votes += 1;
                }
            }
        }

        // Check if won
        let won = votes > total / 2;

        if won {
            candidate.become_leader();

            // Send heartbeats to all followers
            for (id, node) in &self.nodes {
                if id != candidate_id {
                    node.append_entries(candidate_id, term, 0, 0, Vec::new(), 0);
                }
            }
        }

        won
    }

    /// Get node
    pub fn get_node(&self, id: &str) -> Option<&Arc<RaftNode<K, V>>> {
        self.nodes.get(id)
    }

    /// List all nodes
    pub fn nodes(&self) -> Vec<RaftNodeStats> {
        self.nodes.values().map(|n| n.stats()).collect()
    }

    /// Get current leader
    pub fn leader(&self) -> Option<String> {
        for node in self.nodes.values() {
            if node.state() == RaftState::Leader {
                return Some(node.id().to_string());
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raft_node_creation() {
        let node = RaftNode::<String, String>::new("node-1".to_string());
        assert_eq!(node.state(), RaftState::Follower);
        assert_eq!(node.term(), 0);
    }

    #[test]
    fn test_election() {
        let node1 = RaftNode::<String, String>::new("node-1".to_string());
        let node2 = RaftNode::<String, String>::new("node-2".to_string());

        node1.add_peer("node-2".to_string());
        node2.add_peer("node-1".to_string());

        // Node 1 starts election
        let term = node1.start_election();
        assert_eq!(node1.state(), RaftState::Candidate);

        // Node 2 votes for node 1
        let voted = node2.request_vote("node-1", term, 0, 0);
        assert!(voted);

        // Node 1 becomes leader
        node1.become_leader();
        assert_eq!(node1.state(), RaftState::Leader);
    }

    #[test]
    fn test_cluster_election() {
        let cluster = RaftCluster::<String, String>::new(vec![
            "node-1".to_string(),
            "node-2".to_string(),
            "node-3".to_string(),
        ]);

        let won = cluster.run_election("node-1");
        assert!(won);

        assert_eq!(cluster.leader(), Some("node-1".to_string()));

        let node1 = cluster.get_node("node-1").unwrap();
        assert_eq!(node1.state(), RaftState::Leader);
    }
}
