// Benchmark code moved from root benches directory
use engine::Engine;
use std::collections::Bound;
use std::time::{Duration, Instant};
use std::sync::Arc;

fn main() {
    println!("=== NeuroIndex Performance Benchmark ===\n");

    // Configuration
    let num_operations = 1_000_000;
    let num_shards = 4;
    let initial_capacity = 16;

    println!("Configuration:");
    println!("  - Operations: {}", num_operations);
    println!("  - Shards: {}", num_shards);
    println!("  - Initial Capacity: {}\n", initial_capacity);

    // Create engine
    let engine = Arc::new(Engine::<String, String>::with_shards(num_shards, initial_capacity));

    // Enable auto-tuning
    Engine::enable_auto_tuning(&engine);

    println!("--- WRITE PERFORMANCE ---");

    // Benchmark: Sequential Writes
    let start = Instant::now();
    for i in 0..num_operations {
        let key = format!("key:{:010}", i);
        let value = format!("value:{}", i);
        engine.put(key, value).unwrap();
    }
    let write_duration = start.elapsed();
    let write_ops_per_sec = num_operations as f64 / write_duration.as_secs_f64();

    println!("Sequential Writes:");
    println!("  - Total time: {:?}", write_duration);
    println!("  - Throughput: {:.0} ops/sec", write_ops_per_sec);
    println!("  - Latency (avg): {:.2} µs\n", write_duration.as_micros() as f64 / num_operations as f64);

    println!("--- READ PERFORMANCE ---");

    // Benchmark: Sequential Reads
    let start = Instant::now();
    for i in 0..num_operations {
        let key = format!("key:{:010}", i);
        let _ = engine.get(&key);
    }
    let read_duration = start.elapsed();
    let read_ops_per_sec = num_operations as f64 / read_duration.as_secs_f64();

    println!("Sequential Reads:");
    println!("  - Total time: {:?}", read_duration);
    println!("  - Throughput: {:.0} ops/sec", read_ops_per_sec);
    println!("  - Latency (avg): {:.2} µs\n", read_duration.as_micros() as f64 / num_operations as f64);

    // Benchmark: Random Reads
    use rand::Rng;
    let mut rng = rand::thread_rng();

    let start = Instant::now();
    for _ in 0..num_operations / 10 {
        let i = rng.gen_range(0..num_operations);
        let key = format!("key:{:010}", i);
        let _ = engine.get(&key);
    }
    let random_read_duration = start.elapsed();
    let random_read_ops_per_sec = (num_operations / 10) as f64 / random_read_duration.as_secs_f64();

    println!("Random Reads (100k):");
    println!("  - Total time: {:?}", random_read_duration);
    println!("  - Throughput: {:.0} ops/sec", random_read_ops_per_sec);
    println!("  - Latency (avg): {:.2} µs\n", random_read_duration.as_micros() as f64 / (num_operations / 10) as f64);

    println!("--- MIXED WORKLOAD ---");

    // Benchmark: Mixed Read/Write (80/20)
    let mixed_ops = num_operations / 10;
    let start = Instant::now();
    for i in 0..mixed_ops {
        if i % 5 == 0 {
            // 20% writes
            let key = format!("mixed:{:010}", i);
            let value = format!("value:{}", i);
            engine.put(key, value).unwrap();
        } else {
            // 80% reads
            let read_i = rng.gen_range(0..num_operations);
            let key = format!("key:{:010}", read_i);
            let _ = engine.get(&key);
        }
    }
    let mixed_duration = start.elapsed();
    let mixed_ops_per_sec = mixed_ops as f64 / mixed_duration.as_secs_f64();

    println!("Mixed Workload (80% read, 20% write, 100k ops):");
    println!("  - Total time: {:?}", mixed_duration);
    println!("  - Throughput: {:.0} ops/sec", mixed_ops_per_sec);
    println!("  - Latency (avg): {:.2} µs\n", mixed_duration.as_micros() as f64 / mixed_ops as f64);

    println!("--- TTL OPERATIONS ---");

    // Benchmark: TTL operations
    let ttl_ops = 100_000;
    let start = Instant::now();
    for i in 0..ttl_ops {
        let key = format!("session:{:010}", i);
        let value = format!("data:{}", i);
        engine.put_with_ttl(key, value, Duration::from_secs(3600)).unwrap();
    }
    let ttl_duration = start.elapsed();
    let ttl_ops_per_sec = ttl_ops as f64 / ttl_duration.as_secs_f64();

    println!("TTL Writes (100k):");
    println!("  - Total time: {:?}", ttl_duration);
    println!("  - Throughput: {:.0} ops/sec", ttl_ops_per_sec);
    println!("  - Latency (avg): {:.2} µs\n", ttl_duration.as_micros() as f64 / ttl_ops as f64);

    println!("--- RANGE QUERIES ---");

    // Benchmark: Range queries
    let range_queries = 1000;
    let start = Instant::now();
    for i in 0..range_queries {
        let start_key = format!("key:{:010}", i * 100);
        let end_key = format!("key:{:010}", (i + 1) * 100);
        let _ = engine.range(Bound::Included(&start_key), Bound::Included(&end_key));
    }
    let range_duration = start.elapsed();
    let range_ops_per_sec = range_queries as f64 / range_duration.as_secs_f64();

    println!("Range Queries (1k queries, 100 keys each):");
    println!("  - Total time: {:?}", range_duration);
    println!("  - Throughput: {:.0} queries/sec", range_ops_per_sec);
    println!("  - Latency (avg): {:.2} ms\n", range_duration.as_millis() as f64 / range_queries as f64);

    println!("--- DELETE PERFORMANCE ---");

    // Benchmark: Deletes
    let delete_ops = 100_000;
    let start = Instant::now();
    for i in 0..delete_ops {
        let key = format!("key:{:010}", i);
        let _ = engine.delete(&key);
    }
    let delete_duration = start.elapsed();
    let delete_ops_per_sec = delete_ops as f64 / delete_duration.as_secs_f64();

    println!("Sequential Deletes (100k):");
    println!("  - Total time: {:?}", delete_duration);
    println!("  - Throughput: {:.0} ops/sec", delete_ops_per_sec);
    println!("  - Latency (avg): {:.2} µs\n", delete_duration.as_micros() as f64 / delete_ops as f64);

    println!("--- SUMMARY ---");
    println!("Write Throughput:        {:.0} ops/sec", write_ops_per_sec);
    println!("Read Throughput:         {:.0} ops/sec", read_ops_per_sec);
    println!("Random Read Throughput:  {:.0} ops/sec", random_read_ops_per_sec);
    println!("Mixed Workload:          {:.0} ops/sec", mixed_ops_per_sec);
    println!("TTL Operations:          {:.0} ops/sec", ttl_ops_per_sec);
    println!("Range Queries:           {:.0} queries/sec", range_ops_per_sec);
    println!("Delete Throughput:       {:.0} ops/sec\n", delete_ops_per_sec);

    // Export Prometheus metrics
    println!("--- PROMETHEUS METRICS ---");
    let metrics = engine.export_prometheus();
    println!("{:?}", metrics);
}
