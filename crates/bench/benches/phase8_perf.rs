// Phase 8 Performance Benchmarks
// Tests all optimizations: SIMD, prefetching, cache-alignment, batching

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use engine::{AutoTuner, Engine};
use rand::{Rng, SeedableRng};

fn bench_point_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("point_operations");

    for size in [1000, 10_000, 100_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        group.bench_with_input(BenchmarkId::new("put", size), size, |b, &size| {
            let engine = Engine::<u64, u64>::with_shards(8, 1 << 12);
            let mut rng = rand::rngs::StdRng::seed_from_u64(42);
            let keys: Vec<u64> = (0..size).map(|_| rng.gen()).collect();

            b.iter(|| {
                for &k in &keys {
                    black_box(engine.put(k, k.wrapping_mul(2)).unwrap());
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("get", size), size, |b, &size| {
            let engine = Engine::<u64, u64>::with_shards(8, 1 << 12);
            let mut rng = rand::rngs::StdRng::seed_from_u64(42);
            let keys: Vec<u64> = (0..size).map(|_| rng.gen()).collect();

            // Pre-populate
            for &k in &keys {
                engine.put(k, k.wrapping_mul(2)).unwrap();
            }

            b.iter(|| {
                for &k in &keys {
                    black_box(engine.get(&k));
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("mixed_50_50", size), size, |b, &size| {
            let engine = Engine::<u64, u64>::with_shards(8, 1 << 12);
            let mut rng = rand::rngs::StdRng::seed_from_u64(42);
            let keys: Vec<u64> = (0..size).map(|_| rng.gen()).collect();

            // Pre-populate 50%
            for &k in keys.iter().step_by(2) {
                engine.put(k, k.wrapping_mul(2)).unwrap();
            }

            b.iter(|| {
                for (i, &k) in keys.iter().enumerate() {
                    if i % 2 == 0 {
                        black_box(engine.get(&k));
                    } else {
                        black_box(engine.put(k, k.wrapping_mul(2)).unwrap());
                    }
                }
            });
        });
    }

    group.finish();
}

fn bench_range_operations(c: &mut Criterion) {
    use std::ops::Bound::{Included, Unbounded};

    let mut group = c.benchmark_group("range_operations");

    for size in [1000, 10_000, 100_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        group.bench_with_input(
            BenchmarkId::new("range_full_scan", size),
            size,
            |b, &size| {
                let engine = Engine::<u64, u64>::with_shards(8, 1 << 14);

                // Sequential keys for predictable range
                for i in 0..size as u64 {
                    engine.put(i, i * 10).unwrap();
                }

                b.iter(|| {
                    let results = engine.range(Unbounded, Unbounded);
                    black_box(results);
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("range_10_percent", size),
            size,
            |b, &size| {
                let engine = Engine::<u64, u64>::with_shards(8, 1 << 14);

                for i in 0..size as u64 {
                    engine.put(i, i * 10).unwrap();
                }

                let range_size = size as u64 / 10;
                let start = size as u64 / 2;
                let end = start + range_size;

                b.iter(|| {
                    let results = engine.range(Included(&start), Included(&end));
                    black_box(results);
                });
            },
        );
    }

    group.finish();
}

fn bench_join_operations(c: &mut Criterion) {
    use std::ops::Bound::Included;

    let mut group = c.benchmark_group("join_operations");

    for size in [100, 1000, 10_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        group.bench_with_input(BenchmarkId::new("hash_join", size), size, |b, &size| {
            let left = Engine::<u64, String>::with_shards(4, 1 << 12);
            let right = Engine::<u64, u64>::with_shards(4, 1 << 12);

            // Populate left table
            for i in 0..size as u64 {
                left.put(i, format!("value_{}", i)).unwrap();
            }

            // Populate right table (50% overlap)
            for i in 0..size as u64 {
                if i % 2 == 0 {
                    right.put(i, i * 100).unwrap();
                }
            }

            b.iter(|| {
                let results = left.join().hash_join(
                    &right,
                    (Included(&0), Included(&(size as u64))),
                    (Included(&0), Included(&(size as u64))),
                    |k, _v| *k,
                    |k, _v| *k,
                );
                black_box(results);
            });
        });
    }

    group.finish();
}

fn bench_aggregations(c: &mut Criterion) {
    use engine::Aggregator;

    let mut group = c.benchmark_group("aggregations");

    for size in [1000, 10_000, 100_000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        group.bench_with_input(
            BenchmarkId::new("group_by_count", size),
            size,
            |b, &size| {
                let records: Vec<(u64, u64)> =
                    (0..size).map(|i| (i as u64 % 100, i as u64)).collect();

                b.iter(|| {
                    let mut agg = Aggregator::new();
                    agg.group_by(records.clone(), |&(group, _val)| group);
                    let counts = agg.count();
                    black_box(counts);
                });
            },
        );

        group.bench_with_input(BenchmarkId::new("group_by_sum", size), size, |b, &size| {
            let records: Vec<(u64, u64)> = (0..size).map(|i| (i as u64 % 100, i as u64)).collect();

            b.iter(|| {
                let mut agg = Aggregator::new();
                agg.group_by(records.clone(), |&(group, _val)| group);
                let sums = agg.sum(|&(_group, val)| val);
                black_box(sums);
            });
        });
    }

    group.finish();
}

fn bench_with_tuner(c: &mut Criterion) {
    let mut group = c.benchmark_group("auto_tuner");

    group.bench_function("tuner_overhead", |b| {
        let tuner = AutoTuner::with_default();
        let engine = Engine::<u64, u64>::with_shards(8, 1 << 12);

        for i in 0..1000u64 {
            engine.put(i, i * 2).unwrap();
        }

        b.iter(|| {
            tuner
                .counters()
                .get_ops
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            for i in 0..1000u64 {
                black_box(engine.get(&i));
            }
            let recommendations = tuner.analyze();
            black_box(recommendations);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_point_operations,
    bench_range_operations,
    bench_join_operations,
    bench_aggregations,
    bench_with_tuner
);
criterion_main!(benches);
