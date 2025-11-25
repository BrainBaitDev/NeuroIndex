<div align="center">
  <img width="496" height="312" alt="logo_neuro_index" src="https://github.com/user-attachments/assets/875d11e0-aa42-4086-8f18-c298eeb0998e" />
</div>

<div align="center">

[🚀 Quick Start](https://github.com/BrainBaitDev/NeuroIndex/blob/main/docs/QUICK_START.md) • [📖 Docs](https://github.com/BrainBaitDev/NeuroIndex/blob/main/docs) 

</div>
 
 # <center>NEUROINDEX </center>

NeuroIndex is a next-generation in-memory database engine designed to deliver high performance, horizontal scalability and robust lock-free operation. Featuring a multi-shard architecture, native support for parallelism on multi-core systems, and a range of specialized indexes—hash tables, trees, probabilistic filters—it enables efficient management of intensive data loads with fast and reliable queries. The system combines advanced indexing techniques: Cuckoo Hashing, Adaptive Radix Tree (ART), Hopscotch Map, and Cuckoo Filter to handle both point queries and searches on ranges, tags, values, and existence checks.

Memory management is extremely optimized thanks to the use of a Memory Arena, with pre-allocated blocks and inline insertion thresholds, reducing fragmentation and speeding up access and batch operations. NeuroIndex integrates reliable persistence mechanisms via Write-Ahead Log (WAL) and periodic snapshots, ensuring crash resilience and fast data recovery.

## TECHNOLOGIES

NeuroIndex is natively developed in __Rust__, selected for its unique guarantees of memory safety and performance. The lock-free multi-shard architecture makes extensive use of Rust’s ownership and concurrency model, allowing parallel data access and epoch-based garbage collection without the risk of race conditions or memory corruption.

The ownership model and Rust’s type system facilitated efficient handle-based memory management, including arena allocator usage and automatic cleanup via the Drop trait. The crossbeam-epoch crate is used for lock-free garbage collection based on epochs, enabling multi-threading and automating memory lifecycle management—a key aspect for in-memory databases with millions of concurrent entries.

The code makes wide use of zero-cost abstractions through generics and traits, resulting in flexible engines (Engine<K, V>) and modular index structures without runtime performance impact. Traits allow clear separations for serialization, hashing, and custom ordering, while iterator adapters enable efficient range queries without heap allocation.

Rust has provided a significant advantage in building a lock-free, concurrency-safe, and persistent database. The result is a modern backend with lower risk of memory bugs, fewer production crashes, and a scalable core, designed for mission-critical applications and always-on distributed environments. Although experimentation was slower, Rust’s safety and maintainability guarantee long-term benefits for next-generation database infrastructures.

## PERFORMANCE AND METRICS

The growing need to handle real-time, high-volume data has made traditional database solutions often inadequate for modern applications requiring ultra-low latency, high scalability, and operational robustness. Distributed systems and microservice architectures require not only high performance but also reliability in conditions of extreme concurrency, fault resilience, and dynamic adaptation to load.

NeuroIndex was created to overcome the traditional limits of general-purpose databases, offering:

- An __in-memory database__ truly designed to be lock-free and horizontally scalable.

- __Native support for concurrent queries__, batches, and operations on structured, tagged, or frequently updated data.

- The ability to guarantee __persistence__ even in the presence of faults, thanks to robust snapshot and Write-Ahead Log mechanisms.

- Architecture enabling __advanced optimizations on underlying data structures__ (hashing, trees, probabilistic filters) for specific scenarios such as search engines, distributed caching, data analytics, and real-time systems.

The goal of NeuroIndex is to provide a reliable and modern platform for developers and companies needing efficient management of large data volumes, with concurrency safety, in-memory operation speed, and the peace of mind of guaranteed persistence and immediate recovery in case of failures.

## KEY FEATURES

NeuroIndex stands out for advanced features meeting the needs of modern systems in terms of performance, integrability, and scalability.

**Extreme Performance**

- Lock-free multi-shard architecture with native concurrency: key, value, tag, and range operations managed in constant time (O(1)) or logarithmic time (O(log N)).
- Memory pre-allocation with Arena allocator and inline optimization for small-sized data.
- Parallel query pipeline, optimized for high throughput and minimal latency, even under heavy simultaneous loads.

**Flexible and Modern Interfaces**

- REST and gRPC APIs for fast integrations with microservices, cloud platforms, analytics, and e-commerce systems.
- Native compatibility with RESP (Redis-like protocol) for use as an ultra-fast key-value store.
- SDKs for Rust and Python, with possible export of metrics and logs to monitoring systems like Prometheus/Grafana.

**Horizontal Scalability**

- Distributed management via multi-shards: able to increase or decrease the number of shards according to available resources, both in standalone and clustered environments (Kubernetes, VM, bare-metal).
- Support for data replication and automatic failover to guarantee high availability and service continuity with no single point of failure.
- Elastic load balancing and management of large datasets.

**Persistence and Resilience**

- Robust Write-Ahead Log (WAL) mechanisms and consistent snapshots for rapid recovery in case of failures.
- Lock-free garbage collection, data integrity validation, and automatic recovery to reduce risk of loss or corruption.

**Security and Reliability**

- The Rust model drastically reduces bugs like buffer overflows and use-after-free, lowering CVE incidence and production crashes.
- Automatic validation and error isolation at shard level.


## HIGH-LEVEL ARCHITECTURAL DIAGRAM

NeuroIndex's architecture is modular and scalability-oriented. The core is the multi-shard engine, with each shard representing an independent, lock-free partition of the datastore. Incoming client requests through REST, gRPC, RESP, or SDK interfaces are managed by a Query Router, which applies a hash function on the key to route each operation to the responsible shard.

Each shard is structured with specialized indexes:

- **Cuckoo Hash Table**: ultra-fast key-value operations
- **Adaptive Radix Tree (ART)**: range queries and sorting
- **Hopscotch Map**: filtering on tags, values, and prefixes
- **Cuckoo Filter**: probabilistic existence checks

Shard memory is managed using an arena allocator with lock-free, epoch-based garbage collection (crossbeam-epoch). Persistence operations are guaranteed by a dedicated module that handles Write-Ahead Log (WAL) and periodic database state snapshots.

*Typical request flow:*

1. The client sends a request through one of the available interfaces.
2. The Query Router determines the responsible shard.
3. The shard processes the query using appropriate indexes (hash table, tree, maps, filters).
4. Writes are transactionally logged to WAL and synchronized in snapshots.
5. The response is returned to the client.

This architecture ensures high efficiency, fault resilience, and the ability to scale dynamically according to application needs.

## PERFORMANCE AND METRICS

In in-memory databases, performance is not only a matter of speed but also the system’s ability to sustain heavy loads, maintain reactivity under stress, and provide reliable metrics for continuous tuning and monitoring. NeuroIndex’s official benchmarks are designed to provide a concrete and reproducible assessment of the engine's performance, showcasing its capabilities in typical and extreme usage scenarios.

**Methodology:**

- Automated tests and specialized scripts measure throughput, latency, and resilience for major database operations: insertion (INSERT), data retrieval (GET), deletion (DELETE), scalability via sharding, recovery speed after crashes, and software build times.

- Each test is conducted on standardized hardware, with scalable configurations for cores and RAM, to study system behavior relative to available resources

- Operations are monitored via internal counters and metrics capturing average and peak performance, validated with profiling utilities such as PerfCounters and direct response verification.

- The methodology includes:
    - Single and batch measurements on millions of records, in both cold (just started) and hot (under prolonged load) database conditions.
    - Throughput and latency tests for each type of operation, with particular attention to consistency of results over time.
    - Recovery benchmarks simulating crash scenarios and evaluating end-to-end time to bring the system back online and make data available.
    - Horizontal scalability verification by activating multiple shards and monitoring aggregate operation peaks.

These benchmarks demonstrate NeuroIndex’s performance value and provide a solid foundation for tuning and comparison with competing solutions, ensuring transparency and reliability of published metrics.

**Benchmark Results:**

- **INSERT:** 60 million ops/sec — Stable even under intensive loads; ideal for massive data ingestion scenarios (logging, IoT, real-time events).
- **GET SIMD:** 30.88 million ops/sec — Ultra-rapid response times for point queries; perfect for low-latency applications (cache, user sessions, fast analytics).
- **DELETE:** 15.04 million ops/sec — High responsiveness even with rapid data turnover; supports use cases such as TTL, purging, or temporary archives.
- **Sharding:** 114.51 million ops/sec (16 shards) — Almost linear scalability; adding shard cores increases throughput, suitable for multi-core and clustered systems.
- **Recovery:** 1 second/100K keys — Negligible downtime after crash or restart; ensures high availability and fast restoration, even on large datasets.
- **Build time:** 13.9s — Very fast development, test, and release times; enables advanced DevOps practices and smooth CI/CD.

NeuroIndex delivers excellent performance in all main operations, guaranteeing speed, scalability, and resilience—key elements for modern and mission-critical applications.
