use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Replication role
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationRole {
    Master,
    Replica,
}

/// Replication log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationEntry<K, V> {
    pub sequence: u64,
    pub timestamp: u64,
    pub operation: ReplicationOp<K, V>,
}

/// Replication operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicationOp<K, V> {
    Put { key: K, value: V },
    Delete { key: K },
    Tag { key: K, tag: u64 },
}

/// Replication state
pub struct ReplicationState<K, V> {
    role: RwLock<ReplicationRole>,
    sequence: AtomicU64,
    replication_log: Arc<RwLock<Vec<ReplicationEntry<K, V>>>>,
    replicas: Arc<RwLock<HashMap<String, ReplicaInfo>>>,
}

/// Replica information
#[derive(Debug, Clone)]
pub struct ReplicaInfo {
    pub id: String,
    pub last_sequence: u64,
    pub last_sync: SystemTime,
    pub lag_ms: u64,
}

impl<K, V> ReplicationState<K, V>
where
    K: Clone + Serialize + art::AsBytes,
    V: Clone + Serialize,
{
    /// Create new replication state as master
    pub fn new_master() -> Self {
        Self {
            role: RwLock::new(ReplicationRole::Master),
            sequence: AtomicU64::new(0),
            replication_log: Arc::new(RwLock::new(Vec::new())),
            replicas: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create new replication state as replica
    pub fn new_replica() -> Self {
        Self {
            role: RwLock::new(ReplicationRole::Replica),
            sequence: AtomicU64::new(0),
            replication_log: Arc::new(RwLock::new(Vec::new())),
            replicas: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get current role
    pub fn role(&self) -> ReplicationRole {
        *self.role.read()
    }

    /// Promote replica to master
    pub fn promote_to_master(&self) {
        let mut role = self.role.write();
        *role = ReplicationRole::Master;
    }

    /// Demote master to replica
    pub fn demote_to_replica(&self) {
        let mut role = self.role.write();
        *role = ReplicationRole::Replica;
    }

    /// Append operation to replication log (master only)
    pub fn append_operation(&self, op: ReplicationOp<K, V>) -> u64 {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let entry = ReplicationEntry {
            sequence: seq,
            timestamp,
            operation: op,
        };

        let mut log = self.replication_log.write();
        log.push(entry);

        seq
    }

    /// Get entries since sequence number (for replica sync)
    pub fn get_entries_since(&self, since: u64) -> Vec<ReplicationEntry<K, V>> {
        let log = self.replication_log.read();
        log.iter().filter(|e| e.sequence > since).cloned().collect()
    }

    /// Apply entries from master (replica only)
    pub fn apply_entries(&self, entries: Vec<ReplicationEntry<K, V>>) {
        let mut log = self.replication_log.write();
        for entry in entries {
            if entry.sequence >= self.sequence.load(Ordering::SeqCst) {
                log.push(entry.clone());
                self.sequence.store(entry.sequence + 1, Ordering::SeqCst);
            }
        }
    }

    /// Register replica (master only)
    pub fn register_replica(&self, replica_id: String) {
        let mut replicas = self.replicas.write();
        replicas.insert(
            replica_id.clone(),
            ReplicaInfo {
                id: replica_id,
                last_sequence: 0,
                last_sync: SystemTime::now(),
                lag_ms: 0,
            },
        );
    }

    /// Update replica sync status (master only)
    pub fn update_replica_sync(&self, replica_id: &str, sequence: u64) {
        let mut replicas = self.replicas.write();
        if let Some(info) = replicas.get_mut(replica_id) {
            let now = SystemTime::now();
            let lag = now.duration_since(info.last_sync).unwrap_or(Duration::ZERO);
            info.last_sequence = sequence;
            info.last_sync = now;
            info.lag_ms = lag.as_millis() as u64;
        }
    }

    /// Get replication stats
    pub fn stats(&self) -> ReplicationStats {
        let role = *self.role.read();
        let sequence = self.sequence.load(Ordering::SeqCst);
        let log_size = self.replication_log.read().len();
        let replicas = self.replicas.read();

        ReplicationStats {
            role,
            sequence,
            log_size,
            num_replicas: replicas.len(),
            max_lag_ms: replicas.values().map(|r| r.lag_ms).max().unwrap_or(0),
            min_synced_sequence: replicas
                .values()
                .map(|r| r.last_sequence)
                .min()
                .unwrap_or(sequence),
        }
    }

    /// Truncate log up to safe point (all replicas synced)
    pub fn truncate_log(&self) {
        let replicas = self.replicas.read();
        let min_synced = replicas
            .values()
            .map(|r| r.last_sequence)
            .min()
            .unwrap_or(0);

        let mut log = self.replication_log.write();
        log.retain(|e| e.sequence >= min_synced);
    }

    /// Get current sequence number
    pub fn current_sequence(&self) -> u64 {
        self.sequence.load(Ordering::SeqCst)
    }

    /// List all replicas
    pub fn list_replicas(&self) -> Vec<ReplicaInfo> {
        let replicas = self.replicas.read();
        replicas.values().cloned().collect()
    }
}

/// Replication statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationStats {
    pub role: ReplicationRole,
    pub sequence: u64,
    pub log_size: usize,
    pub num_replicas: usize,
    pub max_lag_ms: u64,
    pub min_synced_sequence: u64,
}

impl std::fmt::Display for ReplicationStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Role: {:?}, Seq: {}, Log: {} entries, Replicas: {}, Max lag: {}ms, Min synced: {}",
            self.role,
            self.sequence,
            self.log_size,
            self.num_replicas,
            self.max_lag_ms,
            self.min_synced_sequence
        )
    }
}

/// Simple replication protocol (async replication)
pub struct ReplicationProtocol;

impl ReplicationProtocol {
    /// Sync replica from master
    pub fn sync_replica<K, V>(
        master: &ReplicationState<K, V>,
        replica: &ReplicationState<K, V>,
        replica_id: &str,
    ) -> io::Result<usize>
    where
        K: Clone + Serialize + art::AsBytes,
        V: Clone + Serialize,
    {
        // Get replica's current sequence
        let replica_seq = replica.current_sequence();

        // Get entries from master
        let entries = master.get_entries_since(replica_seq);
        let count = entries.len();

        // Apply to replica
        replica.apply_entries(entries);

        // Update master's view of replica
        master.update_replica_sync(replica_id, replica.current_sequence());

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_roles() {
        let master = ReplicationState::<String, String>::new_master();
        assert_eq!(master.role(), ReplicationRole::Master);

        let replica = ReplicationState::<String, String>::new_replica();
        assert_eq!(replica.role(), ReplicationRole::Replica);

        replica.promote_to_master();
        assert_eq!(replica.role(), ReplicationRole::Master);
    }

    #[test]
    fn test_replication_log() {
        let master = ReplicationState::<String, String>::new_master();

        let seq1 = master.append_operation(ReplicationOp::Put {
            key: "key1".to_string(),
            value: "value1".to_string(),
        });
        assert_eq!(seq1, 0);

        let seq2 = master.append_operation(ReplicationOp::Delete {
            key: "key2".to_string(),
        });
        assert_eq!(seq2, 1);

        let entries = master.get_entries_since(0);
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_replica_sync() {
        let master = ReplicationState::<String, String>::new_master();
        let replica = ReplicationState::<String, String>::new_replica();

        // Master operations
        master.append_operation(ReplicationOp::Put {
            key: "k1".to_string(),
            value: "v1".to_string(),
        });
        master.append_operation(ReplicationOp::Put {
            key: "k2".to_string(),
            value: "v2".to_string(),
        });

        // Register replica
        master.register_replica("replica-1".to_string());

        // Sync
        let synced = ReplicationProtocol::sync_replica(&master, &replica, "replica-1").unwrap();
        assert_eq!(synced, 2);
        assert_eq!(replica.current_sequence(), 2);
    }

    #[test]
    fn test_incremental_sync() {
        let master = ReplicationState::<String, String>::new_master();
        let replica = ReplicationState::<String, String>::new_replica();

        master.register_replica("replica-1".to_string());

        // First sync
        master.append_operation(ReplicationOp::Put {
            key: "k1".to_string(),
            value: "v1".to_string(),
        });
        ReplicationProtocol::sync_replica(&master, &replica, "replica-1").unwrap();

        // Second sync (incremental)
        master.append_operation(ReplicationOp::Put {
            key: "k2".to_string(),
            value: "v2".to_string(),
        });
        let synced = ReplicationProtocol::sync_replica(&master, &replica, "replica-1").unwrap();

        assert_eq!(synced, 1); // Only new entry
        assert_eq!(replica.current_sequence(), 2);
    }
}
