use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Raft node state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftState {
    Follower,
    Candidate,
    Leader,
}

/// Raft node
pub struct RaftNode {
    id: String,
    state: RwLock<RaftState>,
    current_term: AtomicU64,
    voted_for: RwLock<Option<String>>,
    leader_id: RwLock<Option<String>>,
    last_heartbeat: RwLock<Instant>,
    election_timeout: Duration,
    _heartbeat_interval: Duration,
    peers: Arc<RwLock<Vec<String>>>,
}

impl RaftNode {
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
        }
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
    pub fn request_vote(&self, candidate_id: &str, term: u64) -> bool {
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

        // Grant vote if haven't voted yet
        let mut voted = self.voted_for.write();
        if voted.is_none() {
            *voted = Some(candidate_id.to_string());
            true
        } else {
            voted.as_ref() == Some(&candidate_id.to_string())
        }
    }

    /// Become leader
    pub fn become_leader(&self) {
        *self.state.write() = RaftState::Leader;
        *self.leader_id.write() = Some(self.id.clone());
    }

    /// Receive heartbeat (follower receives from leader)
    pub fn receive_heartbeat(&self, leader_id: &str, term: u64) {
        let current_term = self.term();

        if term >= current_term {
            self.current_term.store(term, Ordering::SeqCst);
            *self.state.write() = RaftState::Follower;
            *self.leader_id.write() = Some(leader_id.to_string());
            *self.last_heartbeat.write() = Instant::now();
        }
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
pub struct RaftCluster {
    nodes: HashMap<String, Arc<RaftNode>>,
}

impl RaftCluster {
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

        for (id, node) in &self.nodes {
            if id != candidate_id {
                if node.request_vote(candidate_id, term) {
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
                    node.receive_heartbeat(candidate_id, term);
                }
            }
        }

        won
    }

    /// Get node
    pub fn get_node(&self, id: &str) -> Option<&Arc<RaftNode>> {
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
        let node = RaftNode::new("node-1".to_string());
        assert_eq!(node.state(), RaftState::Follower);
        assert_eq!(node.term(), 0);
    }

    #[test]
    fn test_election() {
        let node1 = RaftNode::new("node-1".to_string());
        let node2 = RaftNode::new("node-2".to_string());

        node1.add_peer("node-2".to_string());
        node2.add_peer("node-1".to_string());

        // Node 1 starts election
        let term = node1.start_election();
        assert_eq!(node1.state(), RaftState::Candidate);

        // Node 2 votes for node 1
        let voted = node2.request_vote("node-1", term);
        assert!(voted);

        // Node 1 becomes leader
        node1.become_leader();
        assert_eq!(node1.state(), RaftState::Leader);
    }

    #[test]
    fn test_cluster_election() {
        let cluster = RaftCluster::new(vec![
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
