# TABLE OF CONTENTS
- [INTERNAL ARCHITECTURE](#internal-architecture)
  - [1. Shard Structure](#1-shard-structure)
  - [2. Memory Management: Memory Arena](#2-memory-management-memory-arena)
    - [How Does the Arena Work?](#how-does-the-arena-work)
  - [3. Query Pipeline](#3-query-pipeline)
- [ALGORITHMS](#algorithms)
  - [1. Cuckoo Hashing](#1-cuckoo-hashing)
    - [Incremental Resizing](#incremental-resizing)
  - [2. Adaptive Radix Tree (ART)](#2-adaptive-radix-tree-art)
  - [3. Hopscotch Hashing](#3-hopscotch-hashing)
  - [4. Cuckoo Filter](#4-cuckoo-filter)
  - [5. Index Priority Logic](#5-index-priority-logic)
- [RESOURCE MANAGEMENT AND EVICTION](#resource-management-and-eviction)
  - [Volatile LRU: Intelligent Eviction](#volatile-lru-intelligent-eviction)
  - [Eviction Policies](#eviction-policies)
- [DISTRIBUTED CONSENSUS: RAFT](#distributed-consensus-raft)
  - [Raft Architecture](#raft-architecture)
  - [Leader Election](#leader-election)
  - [Log Replication](#log-replication)
  - [HTTP Network Layer](#http-network-layer)
- [ACID TRANSACTIONS](#acid-transactions)
  - [Two-Phase Locking (2PL)](#two-phase-locking-2pl)
  - [Snapshot Isolation](#snapshot-isolation)
  - [Commit and Rollback](#commit-and-rollback)
- [AUTO-TUNING AND OPTIMIZATION](#auto-tuning-and-optimization)
  - [Auto-Tuning System](#auto-tuning-system)
  - [Metrics and Recommendations](#metrics-and-recommendations)
- [PERSISTENCE AND FAILOVER](#persistence-and-failover)
  - [WAL (Write-Ahead Log) and Snapshots](#wal-write-ahead-log-and-snapshots)
  - [Recovery and Crash Resistance](#recovery-and-crash-resistance)


<div style="page-break-after: always;"></div>

# INTERNAL ARCHITECTURE

NeuroIndex adopts a **multi-shard lock-free** architecture that maximizes concurrency on multi-core systems, ensuring exceptional performance and horizontal scalability.
Each shard represents an independent instance managing a portion of the dataset through a combination of indexing algorithms optimized for specific workloads: Cuckoo Hashing for fast key-value access, Adaptive Radix Tree for ordered searches and ranges, Hopscotch Map for tag, value, and prefix management, and Cuckoo Filter for probabilistic existence checks.
At the memory level, each shard allocates values in a dedicated arena, minimizing fragmentation.

This layered approach of specialized indices allows NeuroIndex to efficiently respond to a wide range of queries (lookups, aggregations, prefix and tag searches) while maintaining lock-free concurrency and supporting robust persistence operations (WAL, snapshots) and recovery.
The entire architecture is designed to dynamically adapt to load and scale in both standalone environments and distributed clusters.


![](assets/17635434849834.jpg)



## 1. Shard Structure
A Shard represents an independent partition of the datastore, designed to work in parallel with other shards, leveraging multi-core concurrency and increasing database scalability and efficiency.

***Main Components of a Shard***
1. __Memory Arena__: Manages memory allocation for data (values V), offering constant performance for allocation and deallocation while minimizing fragmentation.
   Useful in intensive workloads where frequent writes and deletions require efficient RAM management.

2. __Main Hash Table (CuckooTable)__: The central structure for storing key-value pairs.
   Uses Cuckoo Hashing for constant-time (O(1)) lookup, insert, and delete operations, even with high data density, guaranteeing speed and minimal collisions.

3. __Ordered Index (ArtTree)__: An Adaptive Radix Tree (ART), used for range queries, ordered iterations, and prefix searches.
   Optimized for large keys (strings, UUIDs), offering excellent performance on "composite" or batch searches.

4. __Tag Index (HopscotchMap)__: Hopscotch Hashing implementation focused on searching and filtering by tags or metadata associated with records.
   Enables queries like "give me all records with tag X".

5. __Reverse Tag Index (HopscotchMap)__: Enables reverse lookup: from tag to records that include it. Essential for implementing many-to-many relationships between tags and data.

6. __Value Index (HopscotchMap)__: Facilitates search/filtering by values (not just by key). Useful in analytics or rule engines.

7. __Prefix Index (HopscotchMap)__: Managed to optimize prefix searches on keys without having to scan all keys (e.g., "all records starting with 'user:'").

8. __Existence Filter (CuckooFilter)__: A probabilistic filter that quickly determines if a key doesn't exist, reducing the number of expensive lookups.
   
Shard structure in Rust:
```rust
pub struct Shard<K, V> {
    arena: Arc<Arena<V>>,
    kv: CuckooTable<K, Handle<V>, RandomState>,
    ordered: ArtTree<K, Handle<V>>,
    tags: HopscotchMap<u64, Vec<Handle<V>>>,
    reverse_tags: HopscotchMap<usize, Vec<u64>>,
    value_index: HopscotchMap<String, Vec<K>>,
    prefix_index: HopscotchMap<String, Vec<K>>,
    bloom: Mutex<CuckooFilter>
}
```
<br>

***ADVANTAGES OF THIS ARCHITECTURE***
1. __Extreme Parallelism__: Each shard works autonomously, maximizing CPU core usage.

2. __Error Containment__: Crashes or degradation in one shard don't impact the entire datastore.

3. __Scalability__: Easy to increase or decrease the number of shards based on hardware resources.

4. __Query Efficiency__: Each index provides the fastest path for the required query type, reducing overall latency.


## 2. Memory Management: Memory Arena

NeuroIndex adopts **Memory Arena** management to achieve excellent performance and simplify the allocation complexity characteristic of in-memory databases.

***Technical Parameters of Memory Arena:***

- __BLOCK_SIZE = 1024 slots__: Memory is allocated in blocks of 1024 slots each, optimizing object management and reducing overhead in both write and delete operations.

- __INLINE_THRESHOLD = 32 bytes__: Each value with size less than or equal to 32 bytes is stored inline, avoiding separate allocations and improving access speed.

Thanks to the Arena, data is allocated in large blocks, on which the system distributes individual objects from write and modify operations.
This approach allows allocation and deallocation operations to be performed faster and more predictably compared to traditional methods like malloc or garbage collection, drastically reducing memory fragmentation — a typical problem when managing millions of records in RAM.

The Arena also simplifies data lifecycle management: deleting all records associated with a shard is resolved by destroying the entire arena, without having to manually free each occupied space. Architecturally, this solution is ideal for lock-free models and batch strategies, as it allows multiple threads to operate on the same data without the typical lock contention.

During snapshot or recovery phases, the arena makes data serialization and reconstruction more immediate, allowing quick copying or scanning of the set of allocated values.
All index structures – hash table, tree, tags – point to memory arena handles, ensuring consistency across different database views and reducing the risk of issues like memory leaks, double allocations, or memory race conditions.

The choice of arena in NeuroIndex is not just a performance matter, but represents an engineering strategy that favors robustness, scalability, management simplicity, and system security under real workloads.

### How Does the Arena Work?
- The Arena pre-allocates a large memory block (block allocation) and manages all data (V) by addressing and moving only references (handles).

- When storing a new value is needed, it allocates the required space within the arena instead of doing a malloc or new for each insertion.

- Deallocation is managed efficiently, often through the release of entire blocks, reducing the typical overhead of garbage collection or fragmented frees.

## 3. Query Pipeline

The query pipeline in NeuroIndex represents the structured and optimized flow through which a request, coming from a client via one of the supported interfaces (RESP, HTTP, gRPC, Rust API), is processed within the system and returns a final response.
This path is not just a sequence of operations, but embodies the lock-free and multi-shard philosophy that makes NeuroIndex highly performant.

Everything starts with the request reception by the server, which can come from different protocols but is unified into a common internal representation.
At this point, the Query Router plays a fundamental role: it uses a hash function on the request key to determine which database shard to forward it to.
This routing ensures that each shard receives only requests related to the data it's responsible for, eliminating the need for global locks and enabling maximum efficiency in concurrent access.

Within the shard, the Query goes through a series of specialized indices; the system first checks via probabilistic filters (Cuckoo Filter) if the key is potentially present, speeding up failures.
If positive, it proceeds to lookup in the cuckoo hash table for point queries; if the search type involves ranges, sorting, or prefixes, the query is directed to the appropriate structure (typically the Adaptive Radix Tree); while for filtered searches on metadata, tags, or values, Hopscotch hash maps are used.
Each index is optimized to respond quickly to a specific type of access, minimizing overall latency.

Beyond the read path, the pipeline handles write, update, and delete operations atomically and safely, with persistence guaranteed through Write-Ahead Log and coordinated snapshots, so that data durability is never compromised.

Finally, the result is converted to the format required by the input protocol and returned to the client.
This pipeline design, orchestrated and parallel, maximizes hardware resource usage, increases scalability, and drastically reduces response times, allowing NeuroIndex to sustain intensive and concurrent loads without performance degradation.

<div style="page-break-after: always;"></div>


# ALGORITHMS
To ensure high-level performance, scalability, and query flexibility, NeuroIndex adopts an articulated combination of indexing algorithms, each specialized in handling particular types of queries or data access patterns. These structures work synergistically within each shard, allowing the system to respond effectively to both point lookups and range searches, tag filtering, value analysis, or existence checks. Understanding the role and operation of each algorithm allows appreciation of the architectural choices that make NeuroIndex a modern and highly performant in-memory database engine.
The following section details the main structures used: Cuckoo Hashing, Adaptive Radix Tree, Hopscotch Hashing, and probabilistic filters, with their respective implementation advantages.

## 1. Cuckoo Hashing
Cuckoo Hashing is an advanced hash table management technique, designed to offer extremely fast and reliable search, insertion, and deletion operations, even with high data loads.
Its name derives from the cuckoo bird's behavior, which lays eggs in other birds' nests: similarly, in a cuckoo hash table, each element can potentially "evict" an already present element and take its place if the direct space is occupied.

The operation is based on multiple independent hash functions: when inserting a key, it can be placed in one of several possible slots calculated by the hash functions.
If the chosen slot is already occupied, it's possible to "bounce" the existing element to another position, following its own hash functions.
This relocation process, called cuckooing, continues until a free spot is found or the table is resized if the chain of moves becomes too long.

Thanks to this architecture, Cuckoo Hashing guarantees deterministic and fast access times: each lookup operation consists of one or a few direct checks on the slots associated with the key, without needing to scan collision chains as in traditional hashes.
This strongly reduces the possibility of performance degradation typical of overload cases.

The main advantages of this solution are predictability — access time doesn't depend on the table's fill level or collision frequency — and robustness, as the structure resists well even at very high loads without significant speed impacts.
Additionally, cuckoo hashing adapts perfectly to lock-free and multi-thread implementations, because each position can be updated quickly without involving the entire structure.

The adoption of Cuckoo Hashing as the primary engine for key management guarantees extremely fast and reliable lookups at any operational scale, substantially contributing to the system's ability to handle real-time queries even when data volume grows significantly.

### Incremental Resizing

One of NeuroIndex's key innovations is the implementation of **incremental resizing** for CuckooTable, which solves one of the historical problems of hash tables: blocking during resizing.

Traditionally, when a hash table reaches its capacity limit, it must be resized in a single atomic operation that blocks all other operations. In NeuroIndex, resizing happens incrementally:

- **Lazy Allocation**: The new table is allocated but data transfer happens gradually
- **Double Lookup**: During resize, lookups check both old and new tables
- **Batch Transfer**: Data is moved in small batches during normal operations
- **Overflow Stash Management**: If the stash (overflow area) fills during resize, the system automatically completes resizing with a robust retry loop

This approach ensures that:
1. Read/write operations are never blocked
2. Throughput remains constant even during resize
3. Memory is used efficiently
4. No data loss even in overflow conditions

The `finish_resize` mechanism ensures that, even under extreme load conditions, resizing completes correctly without compromising data integrity.

## 2. Adaptive Radix Tree (ART)
The Adaptive Radix Tree (ART) provides an ordered index optimized for range queries and prefix searches. The current implementation uses a highly optimized Rust BTreeMap that guarantees O(log n) operations with excellent cache locality. A complete ART implementation with adaptive nodes (Node4/16/48/256) and SIMD-accelerated search is available for datasets exceeding 10M keys.
The true innovation of ART lies in its adaptive character: instead of maintaining a fixed structure for each node, ART dynamically adjusts the node type used based on the density of keys falling in that segment, thus optimizing both space and navigation speed.

This adaptivity allows ART to be extraordinarily fast in search, insertion, and deletion operations, but even more advantageous when it comes to range queries and sorting.
Range queries (for example, finding all keys between "key:0001000" and "key:0005000") are extremely efficient because the tree represents keys in an ordered manner: a search can quickly identify the starting point and proceed, traversing only the nodes in the desired interval, without having to scan the entire structure.
Similarly, this ordered representation allows easy iteration of keys in ascending or descending order – useful for sorting, pagination, and aggregations.

ART also handles prefix searches very well, for example: finding "all keys starting with 'key:'".
The structure allows quickly identifying the tree branch representing that prefix and then scrolling only the related nodes, enormously reducing the number of comparisons compared to a list or hash table.

ART complements other indices by providing the ability to respond excellently to queries requiring range, sorting, and prefix, going beyond classic point searches by key.
Thanks to this, NeuroIndex offers advanced functionalities like ordered pagination, analytics, aggregations, and extremely fast and scalable batch queries, without sacrificing speed on insertion, deletion, and update operations.

## 3. Hopscotch Hashing
Hopscotch Hashing is a brilliant evolution of traditional hashing techniques where instead of trying to completely avoid collisions, as happens in cuckoo hashing, this method manages collisions by keeping data "close" to the preferred positions calculated by the hash function.
The key to the system lies in the ability to "hop" over a limited window of adjacent buckets: if the main slot is occupied, the algorithm quickly searches nearby positions to insert the new element, thus ensuring that each record can be found in a contained and predictable number of steps.

This geographic proximity of data in the structure facilitates not only search, insertion, and deletion operations, but proves strategic for applications requiring efficient management of tags, values, and prefixes.
When NeuroIndex needs to index or search data based on a tag (for example, all keys with tag "active"), the system uses a hopscotch map to track associations between tags and records: each search will be fast and predictable, thanks to data localization.
The same approach applies to managing secondary indices for values, allowing quick filtering of all records having a certain associated value, and for prefix searches, where the localized distribution of buckets significantly reduces latency compared to less specialized structures.

The Hopscotch implementation adapts very well to high loads and concurrent operations, precisely because it reduces the risk of long collision chains and makes the structure agile on both random and sequential accesses. Another important advantage is in managing deletions and dynamic modification of tags, values, or prefixes: hopscotch maps allow always keeping under control the fill level and data distribution, favoring performance even when the dataset is very large or subject to continuous variations.

In NeuroIndex, the use of Hopscotch Hashing for tag, value, and prefix indices translates into high usage flexibility and optimal support for analysis, filtering, and search operations, essential for systems that must respond in real-time to complex and simultaneous queries. This contributes to making the database capable of handling scenarios from caching to advanced data analysis without losing speed or structural reliability.

## 4. Cuckoo Filter
Probabilistic filters like the Cuckoo Filter are fundamental tools for accelerating some key operations within modern databases like NeuroIndex, particularly when quickly verifying the existence (or possible existence) of a key before performing more expensive operations on main data structures is needed.

The Cuckoo Filter, besides maintaining a restricted memory space and guaranteeing low probability of false positives, also allows deletion and updating of elements.
The Cuckoo Filter applies cuckoo hashing logic, managing insertions and movements of "fingerprints" dynamically, keeping the structure always compact and ready to respond quickly to verification queries.

Concretely, every time a request for a key arrives in NeuroIndex, the system can leverage these filters: if the response indicates that the key definitely isn't there, it avoids querying the primary structures, saving precious time (especially on very large datasets). Only in case of uncertain outcome (might exist) does the search continue on the primary structures.

This strategy allows drastically reducing the latency of negative queries and significantly reducing the load on the most expensive zones of the pipeline, ensuring that the database remains fast and reactive even when the number of managed keys is in the order of millions or more.

## 5. Index Priority Logic
The index priority logic in NeuroIndex is the principle that regulates the order and sequence with which various data structures are queried to respond to a query.
The choice of optimal search path not only improves response speed, but reduces resource load and ensures that each query type receives the most correct answer in the shortest time possible.

When a request reaches the designated shard, NeuroIndex doesn't query all indices indiscriminately: it instead aims for a guided approach, where the query type determines which structure is used first.
For example, if it's a point lookup (exact key), the search starts from the probabilistic filter (Cuckoo Filter): if the key isn't present, the response can be provided immediately, without even accessing the primary structures.
In case of positive outcome, the pipeline continues with the cuckoo hash table, where retrieval is always O(1) and therefore extremely fast.
If the query requires a key range, sorting, or prefix search, NeuroIndex directs the request to the Adaptive Radix Tree: here the ordered hierarchy and digital representation of keys allow quickly identifying the desired range, iterating only on relevant nodes.
Similarly, for requests on tags, metadata, or values (for example "all objects with tag active"), the pipeline leverages Hopscotch indices, optimized for these associations and capable of filtering large volumes in very short times.

This priority logic reduces overall latency because it avoids executing expensive operations where unnecessary and ensures that each query finds the answer by following the shortest path.
Additionally, the division of indices allows managing concurrent accesses without locks, favoring the scalability and robustness of the entire system.

In summary, the index priority logic in NeuroIndex is a coordinated mosaic of data-driven strategies, where the system knows exactly which index to use for each request type, obtaining optimal performance and maximum operational efficiency at any load scale.

<div style="page-break-after: always;"></div>

# RESOURCE MANAGEMENT AND EVICTION

Efficient memory management is crucial for an in-memory database like NeuroIndex. The system implements sophisticated eviction policies that balance performance and resource usage.

## Volatile LRU: Intelligent Eviction

**Volatile LRU** (Least Recently Used) is a key innovation of NeuroIndex that solves a common problem in caching systems: how to avoid removing persistent data when memory is full.

### Operating Principle

Unlike traditional LRU policies that evict any least recently used key, Volatile LRU operates selectively:

- **Only TTL keys**: Only keys with a Time-To-Live set are considered for eviction
- **Persistent data protection**: Keys without TTL (persistent data) are never automatically removed
- **Access-based priority**: Among volatile keys, least recently accessed ones are removed first

### Advantages

1. **Logical Separation**: Session/cache data (volatile) vs application data (persistent)
2. **Predictability**: Developers know that persistent data will never be evicted
3. **Efficiency**: Reduces cache churn by avoiding removal of data that will be requested again
4. **Flexibility**: Allows using the same database for both caching and persistent storage

### Implementation

```rust
// Usage example
engine.put_with_ttl("session:123", data, Duration::from_secs(3600)); // Volatile
engine.put("user:456", user_data); // Persistent, never evicted

// When memory is full, only "session:123" can be removed
```

## Eviction Policies

NeuroIndex supports multiple configurable eviction policies:

### 1. LRU (Least Recently Used)
- Removes least recently accessed keys
- Ideal for workloads with temporal locality

### 2. LFU (Least Frequently Used)
- Removes least frequently accessed keys
- Optimal for workloads with stable access patterns

### 3. Volatile LRU
- Removes only TTL keys, in LRU order
- Perfect for mixed cache+storage scenarios

### 4. Volatile LFU
- Removes only TTL keys, in LFU order
- Combines LFU advantages and persistent data protection

### Resource Limits

The system allows configuring precise limits:

```rust
let limits = ResourceLimits {
    max_memory: Some(1024 * 1024 * 1024), // 1GB
    max_keys: Some(1_000_000),             // 1M keys
    eviction_target_percentage: 0.8,       // Evict to 80%
};

let engine = Engine::with_shards(4, 16)
    .with_resource_limits(limits)
    .with_eviction_policy(EvictionPolicy::VolatileLRU);
```

When limits are reached, the system:
1. Identifies candidate keys for eviction according to chosen policy
2. Removes keys until reaching `eviction_target_percentage`
3. Continues serving requests without interruptions

<div style="page-break-after: always;"></div>

# DISTRIBUTED CONSENSUS: RAFT

NeuroIndex implements the **Raft** protocol to guarantee distributed consensus and data replication across multiple nodes, transforming the database from a standalone system to a fault-tolerant distributed solution.

## Raft Architecture

The Raft implementation in NeuroIndex faithfully follows the original paper "In Search of an Understandable Consensus Algorithm" by Ongaro and Ousterhout, with specific optimizations for in-memory databases.

### Main Components

1. **RaftNode**: Represents a single node in the cluster
   - Maintains state (Follower, Candidate, Leader)
   - Manages operation log
   - Coordinates elections and replication

2. **Log Replication**: Each write operation is:
   - Proposed by the leader
   - Replicated on followers
   - Committed when majority confirms

3. **State Machine**: Background thread that applies committed entries to storage

### Node States

- **Follower**: Initial state, receives append_entries from leader
- **Candidate**: Requests votes to become leader
- **Leader**: Coordinates write operations and sends heartbeats

## Leader Election

The election mechanism ensures there's always only one leader in the cluster:

1. **Election Timeout**: If a follower doesn't receive heartbeat, becomes candidate
2. **Request Vote**: Candidate requests votes from other nodes
3. **Majority**: Needs majority of votes to become leader
4. **Term**: Each election increments the term, preventing multiple elections

### Log Consistency

Before granting a vote, each node verifies that the candidate has a log at least as up-to-date as its own, ensuring the new leader has all committed operations.

## Log Replication

When the leader receives a write request:

```rust
// Client proposes operation
engine.put("key", "value"); 

// Leader:
// 1. Adds to own log
// 2. Sends append_entries to followers
// 3. Waits for majority confirmation
// 4. Commits the operation
// 5. Applies to state machine
```

### AppendEntries RPC

The leader periodically sends `append_entries` for:
- **Heartbeat**: Maintain leadership (every 50ms)
- **Replication**: Propagate new log entries
- **Commit**: Notify commit_index to followers

### State Machine Application

A background thread continuously monitors:
```rust
loop {
    let committed_entries = raft.get_committed_entries();
    for entry in committed_entries {
        apply_to_storage(entry); // Put/Delete on CuckooTable
        raft.update_last_applied(entry.index);
    }
    sleep(10ms);
}
```

## HTTP Network Layer

NeuroIndex implements an HTTP-based network layer for communication between Raft nodes:

### RPC Protocol

Two main RPC types:

1. **RequestVote**: For elections
```json
{
  "term": 5,
  "candidate_id": "node-1",
  "last_log_index": 100,
  "last_log_term": 4
}
```

2. **AppendEntries**: For replication and heartbeat
```json
{
  "term": 5,
  "leader_id": "node-1",
  "prev_log_index": 99,
  "prev_log_term": 4,
  "entries": [...],
  "leader_commit": 98
}
```

### Implementation

- **HTTP Client**: `ureq` for sending requests
- **HTTP Server**: `tiny_http` for receiving requests
- **Serialization**: JSON via `serde_json`
- **Transport**: HTTP POST on dedicated endpoints

### Configuration

```rust
let raft = RaftNode::new("node-1")
    .with_network("127.0.0.1:5001");

let engine = Engine::with_shards(4, 16)
    .with_raft(raft);

// Start applier thread
Engine::start_raft_applier(&engine);
```

### Advantages

- **Fault Tolerance**: Survives minority node crashes
- **Consistency**: Guarantees write linearizability
- **Availability**: Continues functioning with majority of nodes
- **Partition Tolerance**: Handles network partitions correctly

<div style="page-break-after: always;"></div>

# ACID TRANSACTIONS

NeuroIndex implements complete ACID transactions, enabling multi-key atomic operations with consistency and isolation guarantees.

## Two-Phase Locking (2PL)

The system uses **Two-Phase Locking** to guarantee transaction serializability:

### Locking Phases

1. **Growing Phase**: Transaction acquires locks but doesn't release any
2. **Shrinking Phase**: Transaction releases locks but doesn't acquire any

### Lock Types

- **Read Lock (Shared)**: Multiple readers can acquire simultaneously
- **Write Lock (Exclusive)**: Only one writer at a time, blocks readers too

### Lock Manager

Each key has a centrally managed lock state:

```rust
struct LockState {
    read_locks: HashSet<u64>,   // Transaction IDs with read lock
    write_lock: Option<u64>,    // Transaction ID with write lock
}
```

### Acquisition Rules

- **Read Lock**: Granted if no active write lock (or if tx already has write lock)
- **Write Lock**: Granted only if no other active locks
- **Lock Upgrade**: Possible to upgrade from read to write lock

## Snapshot Isolation

NeuroIndex implements **Snapshot Isolation** to balance performance and consistency:

### Characteristics

- **Read Snapshot**: Each transaction reads from a consistent snapshot
- **No Dirty Reads**: Uncommitted data never visible
- **No Non-Repeatable Reads**: Repeated reads return same value
- **Write Skew**: Possible but rare (acceptable trade-off)

### Implementation

```rust
struct Transaction<K, V> {
    id: u64,
    read_set: HashMap<K, V>,      // Snapshot of read values
    write_set: HashMap<K, V>,     // Buffered writes
    delete_set: HashSet<K>,       // Buffered deletes
    locks: Vec<(K, LockType)>,    // Acquired locks
    state: TransactionState,      // Active/Preparing/Committed/Aborted
}
```

## Commit and Rollback

### Commit Protocol

1. **Validation**: Verify transaction is in Active state
2. **Preparing**: Transition to Preparing state
3. **Apply**: Apply write_set and delete_set atomically
4. **Committed**: Mark as Committed
5. **Release Locks**: Release all locks
6. **Cleanup**: Remove from active transactions

### Rollback

In case of error or explicit abort:
1. **Abort State**: Mark transaction as Aborted
2. **Discard Changes**: Discard write_set and delete_set
3. **Release Locks**: Release all locks
4. **Cleanup**: Remove from active transactions

### Usage Example

```rust
let tx_mgr = TransactionManager::new();

// Begin transaction
let tx = tx_mgr.begin();

// Acquire locks and operate
tx_mgr.acquire_read_lock(&tx, &"account_a")?;
tx_mgr.acquire_write_lock(&tx, &"account_b")?;

// Buffer changes
tx.write().add_write("account_b", new_balance);

// Atomic commit
let (writes, deletes) = tx_mgr.commit(tx)?;

// Apply to database
for (k, v) in writes {
    engine.put(k, v)?;
}
```

### ACID Guarantees

- **Atomicity**: All operations or none
- **Consistency**: Invariants maintained
- **Isolation**: Snapshot Isolation + 2PL
- **Durability**: Combined with WAL for persistence

<div style="page-break-after: always;"></div>

# AUTO-TUNING AND OPTIMIZATION

NeuroIndex includes an **auto-tuning** system that continuously monitors performance and provides optimization recommendations.

## Auto-Tuning System

### Architecture

The system consists of:

1. **PerfCounters**: Atomic counters for metrics
2. **AutoTuner**: Analyzer that generates recommendations
3. **Background Thread**: Executes periodic analysis (every 60s)

### Monitored Metrics

```rust
pub struct PerfCounters {
    get_ops: AtomicU64,
    put_ops: AtomicU64,
    delete_ops: AtomicU64,
    
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    
    cuckoo_kicks: AtomicU64,
    cuckoo_stash_uses: AtomicU64,
    cuckoo_resizes: AtomicU64,
    
    ttree_rotations: AtomicU64,
    ttree_splits: AtomicU64,
}
```

## Metrics and Recommendations

### Analysis Categories

1. **Cache Efficiency**
   - Target: 95% hit rate
   - Recommendation: Increase shard count, enable prefetching

2. **Hash Table Efficiency**
   - Target: < 10 kicks per insert
   - Recommendation: Increase table size, improve hash functions

3. **Memory Allocation**
   - Target: < 10 resizes in lifetime
   - Recommendation: Pre-allocate larger capacity

4. **CPU Optimization**
   - Target: SIMD enabled on x86_64
   - Recommendation: Enable AVX2/SSE4.2

### Example Output

```
=== Auto-Tuning Recommendations ===
[High] CacheEfficiency: Cache hit rate (80.00%) is below target (95.00%). 
  Consider: 1) Increasing shard count to improve locality, 
  2) Enabling prefetching for range queries, 
  3) Using smaller key/value sizes for better cache line utilization

[Medium] HashTableEfficiency: High cuckoo kick rate (12.50 kicks/op). 
  Consider: 1) Increasing table size, 
  2) Using better hash functions, 
  3) Increasing bucket size from 4 to 8
===================================
```

### Activation

```rust
let engine = Arc::new(Engine::with_shards(4, 16));

// Enable auto-tuning
Engine::enable_auto_tuning(&engine);

// System now analyzes performance every 60s
// and logs recommendations automatically
```

### Prometheus Integration

The system exposes metrics in Prometheus format:

```
neuroindex_operations_total{operation="get"} 1000000
neuroindex_cache_hit_rate 0.95
neuroindex_memory_bytes 1073741824
neuroindex_cuckoo_kicks_total 1250
```

This enables integration with:
- **Grafana**: Visual dashboards
- **Alertmanager**: Automatic alerts
- **Prometheus**: Time-series storage

<div style="page-break-after: always;"></div>

# PERSISTENCE AND FAILOVER
In an in-memory database system designed for extreme performance like NeuroIndex, data persistence and the ability to quickly recover from failures represent fundamental aspects to guarantee reliability, durability, and service continuity.
The following section explores the strategies adopted for secure state conservation, such as Write-Ahead Log (WAL) and snapshot mechanisms, as well as solutions implemented for automatic failover and resilience against hardware or software faults. These components make NeuroIndex not only fast, but also a reliable pillar on which to build critical and always-on applications.

## WAL (Write-Ahead Log) and Snapshots
The Write-Ahead Log (WAL) and snapshots represent the two fundamental pillars of data persistence in NeuroIndex.
The WAL is a linear register of all modification operations executed on the database: each insertion, update, or deletion is first written transactionally to the log, and only subsequently applied to in-memory data structures.
This mechanism ensures that, in case of sudden system interruption (crash, shutdown, hardware error), it's always possible to reconstruct the exact database state by simply "replaying" the WAL entries from the most recent position.

Parallel to the WAL, NeuroIndex periodically executes snapshots, i.e., a point-in-time consistent copy of the entire in-memory dataset at a given instant.
The snapshot allows storing the global database state without having to scroll through all individual log entries: at startup, the system can thus quickly restore the most recent dataset version and, if necessary, integrate subsequent modifications by recalling only the WAL entries produced after that snapshot.

The WAL+Snapshot combination optimizes both data durability (no operation is lost between a crash and subsequent reopening) and recovery speed: restoration doesn't require re-executing the entire log, but only the most recent modifications. In practice, the WAL guarantees that each transaction is committed to disk before being executed in memory, while the snapshot reduces bootstrap times and minimizes corruption risk in case of errors.
Additionally, snapshot generation is managed non-blockingly; the system continues to respond to queries and write operations during copying.

In summary, WAL and snapshots are complementary tools that ensure NeuroIndex persistence has maximum reliability, consistency, and speed: daily operations are always protected, and startup or recovery after a failure are efficient and safe.


## Recovery and Crash Resistance

NeuroIndex's ability to guarantee recovery and crash resistance is one of the most critical aspects for any database engine's reliability, especially when employed in enterprise contexts and applications requiring high availability.

Recovery begins from the synergy between Write-Ahead Log (WAL) and snapshots: when the system suffers a crash, whether caused by software errors, unexpected shutdowns, or hardware failures, NeuroIndex never loses track of fundamental operations. At restart, the database first restores the memory state from the last consistent snapshot, so as not to proceed with a complete reconstruction, which would be costly in terms of time and resources. Subsequently, all operations contained in the WAL recorded after the snapshot are processed, ensuring that the final state corresponds exactly to what was there at the time of the crash.

This procedure protects both data — which always remains consistent and doesn't suffer corruption — and the system's restart speed, fundamental for getting back online in very short times. NeuroIndex also adds control and verification mechanisms for memory structure integrity during recovery: each handle and record is validated so there are no inconsistencies, leaks, or incorrect correlations between structures.

From the resistance standpoint, NeuroIndex is designed to minimize failure points: lock-free distribution between shards and separation between indices and memory reduce the probability that a local error can propagate and cause extensive damage. Even in the presence of partial crashes, the system can restart only the affected components, without compromising the entire database.

In summary, NeuroIndex's recovery and crash resistance strategy ensures that every piece of data is always protected, recoverable, and integral, allowing the database to quickly restore service without losing information or degrading performance, even in the most critical situations.
