
# INDEX
- [INTERNAL ARCHITECTURE](#internal-architecture)
  - [1. Shard Structure](#1-shard-structure)
  - [2. Memory Management: Memory Arena](#2-memory-management-memory-arena)
  - [3. Memory Arena Mechanics](#3-memory-arena-mechanics)
  - [4. The Query Pipeline](#4-the-query-pipeline)
- [THE ALGORITHMS](#the-algorithms)
  - [1. Cuckoo Hashing](#1-cuckoo-hashing)
  - [2. Adaptive Radix Tree (ART)](#2-adaptive-radix-tree-art)
  - [3. Hopscotch Hashing: tag/value/prefix](#3-hopscotch-hashing-tagvalueprefix)
  - [4. Bloom/Cuckoo Filter: probabilistic checks](#4-bloomcuckoo-filter-probabilistic-checks)
  - [5. Index Priority Logic](#5-index-priority-logic)
- [PERSISTENCE AND FAILOVER](#persistence-and-failover)
  - [WAL (Write-Ahead Log) and snapshots](#wal-write-ahead-log-and-snapshots)
  - [Recovery and Crash Resilience](#recovery-and-crash-resilience)

# INTERNAL ARCHITECTURE

NeuroIndex adopts a **multi-shard lock-free architecture** that fully leverages concurrency on multi-core systems, ensuring ultra-high performance and horizontal scalability.
Each shard represents an independent instance that manages a portion of the dataset through a combination of indexing algorithms optimized for specific workloads: Cuckoo Hashing for fast key-value access, Adaptive Radix Tree for sorted and range searches, Hopscotch Map for handling tags, values, and prefixes, and Cuckoo Filter for probabilistic existence checks.
At the memory level, each shard allocates values in a dedicated arena, minimizing fragmentation.

This layering of specialized indices enables NeuroIndex to efficiently answer a wide variety of queries (lookup, aggregations, prefix and tag searches) while maintaining lock-free concurrency and supporting robust persistence (WAL, snapshots) and recovery operations.
The entire architecture is designed to dynamically adapt to system load and scale both in standalone environments and distributed clusters.
![](assets/17635487362227.jpg)


## 1. Shard Structure

A Shard is an independent partition of the datastore, designed to operate in parallel with other shards, leveraging multi-core concurrency and increasing database scalability and efficiency.

***Main Components of a Shard***

1. **Memory Arena**: Manages data allocation (values V) with consistent performance for allocation/deallocation and minimal fragmentation.
Useful under intensive workloads where frequent writes and deletions require highly efficient RAM management.
    - **Technical Parameters:**
        - **BLOCK_SIZE = 1024 slots:** Memory is allocated in blocks of 1024 slots each, optimizing object management and reducing overhead for writes and deletions.
        - **INLINE_THRESHOLD = 32 bytes:** Values ≤ 32 bytes are stored inline, avoiding separate allocations and improving access speed.
2. **Main Hash Table (CuckooTable)**: The central structure for key-value pairs, using Cuckoo Hashing for constant-time lookup, insert, and delete (O(1)), even with high-density data loads, and ensuring speed with very low collision rates.
3. **Lock-Free Garbage Collection**: Deallocation and object lifecycle managed through the *crossbeam-epoch* library, providing epoch-based, fully lock-free garbage collection. Multiple threads can operate in parallel without contention, increasing efficiency and throughput.
4. **Ordered Index (ArtTree)**: An Adaptive Radix Tree (ART) for range queries, ordered iterations, and prefix searches.
Optimized for large keys (strings, UUIDs), delivers excellent composite and batch query performance.
5. **Tag Index (HopscotchMap)**: Implementation specialized for searching and filtering records by tag or metadata.
Enables queries like "return all records with tag X."
6. **Reverse Tag Index (HopscotchMap)**: Enables reverse searching: from tag to all records containing that tag. Essential for many-to-many relationships between tags and data.
7. **Value Index (HopscotchMap)**: Facilitates search/filter by value (not just by key). Useful for analytics and rule engines.
8. **Prefix Index (HopscotchMap)**: Optimized for prefix searches on keys, avoiding full key scans (e.g., "all records starting with 'user:'").
9. **Existence Filter (CuckooFilter)**: A probabilistic filter for quickly determining if a key does not exist, reducing costly lookups.

**Shard structure in Rust**

```rust
pub struct Shard<K, V> {
    arena: Arc<Arena<V>>,
    kv: CuckooTable<K, Handle<V>>,
    ordered: ArtTree<K, Handle<V>>,
    tags: HopscotchMap<Tag, KeySet>,
    reverse_tags: HopscotchMap<Key, TagSet>,
    value_index: HopscotchMap<Value, KeySet>,
    prefix_index: HopscotchMap<Prefix, KeySet>,
    bloom: CuckooFilter<K>
}
```

**Advantages of this type of architecture**

- **Extreme Parallelism:** Every shard operates autonomously, maximizing CPU core usage.
- **Error Containment:** Failure or degradation in a shard doesn’t affect the entire datastore.
- **Scalability:** Easily increase or reduce shard count based on hardware resources.
- **Query Efficiency:** Each index provides the fastest path for its query type, reducing overall latency.


## 2. Memory Management: Memory Arena

NeuroIndex uses **Memory Arena** for outstanding performance and to simplify allocation complexity in in-memory databases.

**Memory Arena Technical Parameters:**

- **BLOCK_SIZE = 1024 slots:** Memory is managed in blocks of 1024 slots each, optimizing object handling and minimizing overhead in writing and deleting operations.
- **INLINE_THRESHOLD = 32 bytes:** Values up to 32 bytes are stored inline, eliminating separate allocations and speeding up access.

With Arena, data are allocated in large blocks onto which the system distributes write and modification objects.
This approach delivers faster, more predictable allocation and deallocation compared to traditional malloc or garbage collector methods, drastically reducing fragmentation—a critical problem when handling millions of records in RAM.

The Arena also streamlines data lifecycle: removing all records in a shard is resolved by destroying the entire arena, with no need for manual freeing of every occupied space. Architecturally ideal for lock-free and batch strategies, it allows multiple threads to operate on shared data without common locking issues.

During snapshot or recovery, the arena enables quick serialization and reconstruction of data, allowing fast copying or scanning of allocated values. All indices—hash table, tree, tag—point to memory arena handles, ensuring coherence across database views and minimizing issues like memory leaks, double allocations, or race conditions.

Choosing Arena in NeuroIndex is not just for performance but also an engineering strategy to increase robustness, scalability, simplicity, and data safety under real workloads.



## 3. Memory Arena Mechanics

- The Arena pre-allocates a large memory block (block allocation), managing all data (V) by addressing and moving only references (handles).
- When storing a new value, space is allocated inside the arena rather than a malloc or new per insertion.
- Deallocation is handled efficiently, often by freeing whole blocks, reducing the overhead typical of garbage collectors or fragmented frees.



## 4. The Query Pipeline

NeuroIndex’s query pipeline is a structured, optimized flow for client requests (via RESP, HTTP, gRPC, Rust API) processed inside the system and returned as final responses.
More than just a sequence, it embodies the lock-free, multi-shard philosophy that powers NeuroIndex’s performance.

Every request, regardless of protocol, is normalized into a common internal representation. The **Query Router** then hashes the request key to route it to the responsible shard—ensuring shards only receive relevant data, eliminating global locks, and maximizing concurrent access efficiency.

Within the shard, the query passes through specialized indices; probabilistic filters (Cuckoo Filter) first check for possible existence, speeding up failures. If positive, a lookup occurs in the cuckoo hash table; range, ordering, or prefix searches use the appropriate structure (typically Adaptive Radix Tree), while metadata, tag or value searches use Hopscotch hash maps.
Each index is tuned for its query type, minimizing global latency.

Write, update, and delete operations are managed atomically and safely, with durability via Write-Ahead Log (WAL) and coordinated snapshots—thus data durability is always ensured.

Results are converted to the input protocol format and returned to the client. The orchestrated and parallel pipeline design maximizes hardware use, increases scalability and drastically cuts response times, enabling NeuroIndex to handle intense, concurrent loads without performance drops.



# THE ALGORITHMS

To guarantee top-level performance, scalability, and query flexibility, NeuroIndex uses a mix of advanced indexing algorithms, each specialized for certain query or data access patterns.
These structures work synergistically within each shard, allowing the system to respond effectively to lookups, range searches, tag filtering, value analysis, and existence checks. Understanding each algorithm’s function helps in appreciating the architectural choices that make NeuroIndex a modern, high-performance in-memory database engine.

## 1. Cuckoo Hashing

Cuckoo Hashing is an advanced hash table technique, delivering extremely fast and reliable search, insert, and delete operations, even under heavy data loads.
Named after the cuckoo bird (which lays eggs in the nests of others): in a cuckoo hash table, every element can potentially “evict” an existing entry and take its slot if the direct space is occupied.

The mechanism relies on multiple independent hash functions: when inserting a key, it can be placed in any slot calculated by those functions. If the target slot is taken, the existing element is bounced to another location via its own hash functions.
This “cuckooing” continues until an empty space is found or, if moves become too many, the table is resized.

This architecture grants deterministic, fast access: lookups require only one or a few direct slot checks, bypassing long collision chains of classic hashes—greatly reducing performance degradation under overload.

Key benefits: predictable access times (not dependent on table fill or collision rate), and robustness—even under extreme load with no major speed drops.
Cuckoo hashing integrates perfectly in lock-free and multi-threaded implementations, as each slot can be quickly updated with no impact on the whole structure.

As NeuroIndex’s primary key engine, Cuckoo Hashing ensures lightning-fast, reliable lookups at any scale, supporting real-time queries even as data volume grows.

## 2. Adaptive Radix Tree (ART)

The Adaptive Radix Tree (ART) is designed for representing and efficiently searching sets of keys, especially strings, numbers, or byte sequences.
Instead of direct key storage, the ART builds a giant digital tree where each level represents part of a key (character or byte).

Its innovation is the *adaptive* nature—nodes change type dynamically based on how dense the key segment is, optimizing both space and traversal speed.

ART is extremely fast for search, insert, and delete, and excels at range queries and ordering.
With ordered tree representation, a search quickly locates the start of a range and traverses only relevant nodes—avoiding full scans.
Likewise, it allows easy ordered iteration for sorting, pagination, and aggregations.

ART also handles prefix searches (e.g., “all keys starting with ‘key:’”) with speed: quickly locating the prefix branch then scanning only related nodes—dramatically reducing comparisons versus lists or hashes.

With ART, NeuroIndex provides advanced features like ordered pagination, analytics, aggregation and batch queries—offering exceptional speed for insert, delete, and update.

## 3. Hopscotch Hashing: tag/value/prefix

Hopscotch Hashing is a brilliant evolution over traditional hash techniques: instead of avoiding collisions, it manages them by keeping data *close* to preferred locations computed by the hash function.

The key is “hopscotching” around a limited window of adjacent buckets—if the main slot is used, the algorithm quickly finds nearby open spots, ensuring every record can be retrieved in few, predictable steps.

Data locality aids not only searching, inserting, and deleting, but is strategic for tag, value and prefix management.
For indexing/filtering by tag (“all keys with tag active”), hopscotch maps track tag-record associations—making queries fast and predictable by localizing data.

The same logic applies to secondary indices for values (filter all records with a certain value) and for prefixes, where bucket localization sharply reduces latency compared to less specialized structures.

Hopscotch works well under high loads and concurrent operations, since it limits long collision chains; it keeps performance high even when deleting/updating tags, values, or prefixes as data grows or changes.

In NeuroIndex, using Hopscotch Hashing for tag, value and prefix indices delivers flexible support for analysis/filtering/search—essential for real-time systems with complex queries.
This makes NeuroIndex capable of anything from caching to advanced analytics, without losing speed or structural reliability.

## 4. Bloom/Cuckoo Filter: probabilistic checks

Probabilistic filters like Bloom and Cuckoo Filters are essential for speeding up key database operations—usually to rapidly check the existence (or possible existence) of a key before expensive main structure queries.

The Bloom Filter is compact and fast for verifying absence/presence: multiple hash functions map an item to positions in a bit array; if any is zero, the item is definitely not present, if all are one, it might be (allowing some false positives). It uses very little memory and offers constant, fast time—great for “existence checks.”

The Cuckoo Filter, adopted in NeuroIndex as a Bloom alternative/extension, adds deletion and update support—hard or inefficient in classic Blooms. Using cuckoo-style hashing, it inserts/moves “fingerprints” dynamically, keeping the structure compact and ready for fast verification.

When a query arrives in NeuroIndex, these filters are used: if the filter declares the key not present, no need to scan the main structures—saving precious time with huge datasets.
If uncertain, the full search continues.

This combined strategy slashes negative query latency and lightens the most expensive pipeline layers, keeping the DB fast and responsive even as managed keys reach millions.
The joint use of Bloom and Cuckoo Filters offers a practical, effective way to optimize existence checks without wasting resources.

## 5. Index Priority Logic

The index priority logic in NeuroIndex determines the order and sequence of data structure queries. This optimal search path not only speeds up response times, but also reduces resource load and ensures every query gets the best answer in the shortest possible time.

When a request reaches a designated shard, NeuroIndex doesn’t query all indices blindly—it takes a guided approach, where query type governs which structure is used first.
For exact lookups, probabilistic filters (Cuckoo/Bloom) are checked first; instant answers if absent, avoiding major structure queries.
If positive, the pipeline continues to the cuckoo hash table—O(1) lookups. For range/ordering/prefix queries, ART is used; its hierarchy and key representation allow quick location and traversal of the relevant range.
For tag, metadata, or value requests (“all objects tagged active”), the pipeline uses Hopscotch indices, optimized for these associations and able to filter large volumes rapidly.

Priority logic eliminates unnecessary expensive operations, reduces general latency, and the split indices allow concurrent access with no locks—boosting scalability and robustness.

In short, NeuroIndex’s index priority is a coordinated mosaic of data-driven strategies, with the system knowing exactly which index for which query type, yielding optimal performance and maximum efficiency at any load scale.



# PERSISTENCE AND FAILOVER

In high-performance in-memory databases like NeuroIndex, data persistence and fast failure recovery are fundamental for reliability, durability, and service continuity.
The following strategies include secure state retention (Write-Ahead Log, snapshots) and automated failover/resilience features—making NeuroIndex not only fast, but also a trusted backbone for critical and always-on applications.

## WAL (Write-Ahead Log) and snapshots

The WAL and snapshots are the two pillars of NeuroIndex’s data persistence.
The WAL is a linear log of all modifications performed on the database: every insert, update or delete is first transactionally written to the log, and only then applied to the in-memory structures.
This ensures that, even with sudden system failures (crash, shutdown, hardware error), the database state can be exactly rebuilt by replaying WAL entries from the most recent position.

In parallel, NeuroIndex periodically creates snapshots—a consistent point-in-time copy of the whole in-memory dataset.
Snapshots allow fast restoration of the latest state and, if needed, integrate subsequent modifications by replaying only those WAL entries produced after the last snapshot.

Together, WAL and snapshot optimize data durability (no operation lost between crash/restart) and fast recovery—restart doesn't require full log replay, just the latest changes.
WAL ensures every transaction is committed to disk before memory, while snapshots shorten bootstrap times and minimize corruption risks in case of error.
Snapshot generation is non-blocking: the system keeps handling queries/writes during copying.

In summary, WAL and snapshots are complementary tools providing maximum reliability, consistency, and speed for NeuroIndex’s persistence; daily operations are always protected, and post-crash recovery is efficient and safe.

## Recovery and Crash Resilience

NeuroIndex’s ability to guarantee recovery and crash resistance is a critical aspect for database engine reliability, especially for enterprise contexts and applications demanding high availability.

Recovery begins with the synergy of WAL and snapshots: after a crash (be it software error, power loss, hardware fault), NeuroIndex never loses track of key operations.
On restart, it first restores memory from the latest consistent snapshot, so there's no need for a full rebuild (which would be costly in time/resources).
Then, all WAL operations after the snapshot are processed, ensuring the final state matches exactly what it was at crash time.

This procedure keeps both data and system speed safe—data stay consistent and uncorrupted, and fast restarts are critical for quick online recovery.
NeuroIndex adds memory integrity checks during recovery: every handle/record is validated to avoid leaks or wrong structures.

Architecturally, NeuroIndex minimizes failure points: lock-free shard distribution and separate indices/memory keep local errors from spreading.
Even with partial crashes, only affected components restart—no impact on the full database.

In summary, NeuroIndex’s recovery and crash resilience strategies ensure all data remain protected, recoverable and integral—enabling rapid service restoration without data loss or performance degradation, even under the most critical scenarios.






