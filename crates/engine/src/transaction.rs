use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::hash::Hash;

/// Transaction state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Active,
    Preparing,
    Committed,
    Aborted,
}

/// Lock type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockType {
    Read,
    Write,
}

/// Transaction error
#[derive(Debug, Clone)]
pub enum TxError {
    Deadlock,
    Conflict,
    Aborted,
    InvalidState,
}

impl std::fmt::Display for TxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TxError::Deadlock => write!(f, "Transaction deadlock detected"),
            TxError::Conflict => write!(f, "Transaction conflict"),
            TxError::Aborted => write!(f, "Transaction aborted"),
            TxError::InvalidState => write!(f, "Invalid transaction state"),
        }
    }
}

impl std::error::Error for TxError {}

/// Lock state for a key
struct LockState {
    read_locks: HashSet<u64>,   // Transaction IDs holding read locks
    write_lock: Option<u64>,    // Transaction ID holding write lock
}

impl LockState {
    fn new() -> Self {
        Self {
            read_locks: HashSet::new(),
            write_lock: None,
        }
    }

    fn can_acquire_read(&self, tx_id: u64) -> bool {
        // Can acquire read lock if no write lock or we hold the write lock
        self.write_lock.is_none() || self.write_lock == Some(tx_id)
    }

    fn can_acquire_write(&self, tx_id: u64) -> bool {
        // Can acquire write lock if no other locks
        self.write_lock.is_none() && 
        (self.read_locks.is_empty() || (self.read_locks.len() == 1 && self.read_locks.contains(&tx_id)))
    }

    fn acquire_read(&mut self, tx_id: u64) -> bool {
        if self.can_acquire_read(tx_id) {
            self.read_locks.insert(tx_id);
            true
        } else {
            false
        }
    }

    fn acquire_write(&mut self, tx_id: u64) -> bool {
        if self.can_acquire_write(tx_id) {
            self.write_lock = Some(tx_id);
            self.read_locks.remove(&tx_id); // Upgrade from read to write
            true
        } else {
            false
        }
    }

    fn release(&mut self, tx_id: u64) {
        self.read_locks.remove(&tx_id);
        if self.write_lock == Some(tx_id) {
            self.write_lock = None;
        }
    }
}

/// Transaction
pub struct Transaction<K, V> {
    id: u64,
    read_set: HashMap<K, V>,      // Snapshot of read values
    write_set: HashMap<K, V>,     // Pending writes
    delete_set: HashSet<K>,       // Pending deletes
    locks: Vec<(K, LockType)>,    // Acquired locks
    state: TransactionState,
}

impl<K, V> Transaction<K, V> 
where
    K: Clone + Hash + Eq,
    V: Clone,
{
    fn new(id: u64) -> Self {
        Self {
            id,
            read_set: HashMap::new(),
            write_set: HashMap::new(),
            delete_set: HashSet::new(),
            locks: Vec::new(),
            state: TransactionState::Active,
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn state(&self) -> TransactionState {
        self.state
    }

    pub fn add_read(&mut self, key: K, value: V) {
        self.read_set.insert(key, value);
    }

    pub fn add_write(&mut self, key: K, value: V) {
        self.delete_set.remove(&key);
        self.write_set.insert(key, value);
    }

    pub fn add_delete(&mut self, key: K) {
        self.delete_set.insert(key.clone());
        self.write_set.remove(&key);
    }

    fn add_lock(&mut self, key: K, lock_type: LockType) {
        self.locks.push((key, lock_type));
    }
}

/// Transaction Manager
pub struct TransactionManager<K, V> {
    next_tx_id: AtomicU64,
    active_txs: RwLock<HashMap<u64, Arc<RwLock<Transaction<K, V>>>>>,
    lock_table: Arc<RwLock<HashMap<K, RwLock<LockState>>>>,
}

impl<K, V> TransactionManager<K, V> 
where
    K: Clone + Hash + Eq,
    V: Clone,
{
    pub fn new() -> Self {
        Self {
            next_tx_id: AtomicU64::new(1),
            active_txs: RwLock::new(HashMap::new()),
            lock_table: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Begin a new transaction
    pub fn begin(&self) -> Arc<RwLock<Transaction<K, V>>> {
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        let tx = Arc::new(RwLock::new(Transaction::new(tx_id)));
        
        self.active_txs.write().insert(tx_id, tx.clone());
        
        tx
    }

    /// Acquire read lock
    pub fn acquire_read_lock(&self, tx: &Arc<RwLock<Transaction<K, V>>>, key: &K) -> Result<(), TxError> {
        let tx_id = tx.read().id();
        
        let mut lock_table = self.lock_table.write();
        let lock_state = lock_table.entry(key.clone()).or_insert_with(|| RwLock::new(LockState::new()));
        
        let mut state = lock_state.write();
        if state.acquire_read(tx_id) {
            tx.write().add_lock(key.clone(), LockType::Read);
            Ok(())
        } else {
            Err(TxError::Conflict)
        }
    }

    /// Acquire write lock
    pub fn acquire_write_lock(&self, tx: &Arc<RwLock<Transaction<K, V>>>, key: &K) -> Result<(), TxError> {
        let tx_id = tx.read().id();
        
        let mut lock_table = self.lock_table.write();
        let lock_state = lock_table.entry(key.clone()).or_insert_with(|| RwLock::new(LockState::new()));
        
        let mut state = lock_state.write();
        if state.acquire_write(tx_id) {
            tx.write().add_lock(key.clone(), LockType::Write);
            Ok(())
        } else {
            Err(TxError::Conflict)
        }
    }

    /// Release all locks held by transaction
    pub fn release_locks(&self, tx: &Arc<RwLock<Transaction<K, V>>>) {
        let tx_guard = tx.read();
        let tx_id = tx_guard.id();
        let locks = tx_guard.locks.clone();
        drop(tx_guard);
        
        let mut lock_table = self.lock_table.write();
        for (key, _) in locks {
            if let Some(lock_state) = lock_table.get_mut(&key) {
                lock_state.write().release(tx_id);
            }
        }
    }

    /// Commit transaction
    pub fn commit(&self, tx: Arc<RwLock<Transaction<K, V>>>) -> Result<(HashMap<K, V>, HashSet<K>), TxError> {
        let mut tx_guard = tx.write();
        
        if tx_guard.state != TransactionState::Active {
            return Err(TxError::InvalidState);
        }
        
        tx_guard.state = TransactionState::Preparing;
        
        // Extract write set and delete set
        let write_set = tx_guard.write_set.clone();
        let delete_set = tx_guard.delete_set.clone();
        
        tx_guard.state = TransactionState::Committed;
        
        let tx_id = tx_guard.id();
        drop(tx_guard);
        
        // Release locks
        self.release_locks(&tx);
        
        // Remove from active transactions
        self.active_txs.write().remove(&tx_id);
        
        Ok((write_set, delete_set))
    }

    /// Abort transaction
    pub fn abort(&self, tx: Arc<RwLock<Transaction<K, V>>>) {
        let mut tx_guard = tx.write();
        tx_guard.state = TransactionState::Aborted;
        let tx_id = tx_guard.id();
        drop(tx_guard);
        
        // Release locks
        self.release_locks(&tx);
        
        // Remove from active transactions
        self.active_txs.write().remove(&tx_id);
    }
}

impl<K, V> Default for TransactionManager<K, V> 
where
    K: Clone + Hash + Eq,
    V: Clone,
{
    fn default() -> Self {
        Self::new()
    }
}
