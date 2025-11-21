// Benchmark comparison: ART vs T-Tree for ordered index operations
// Tests GET, PUT, RANGE queries with various key distributions

use art::ArtTree;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::distributions::{Distribution, Uniform};
use rand::{Rng, SeedableRng};
use std::ops::Bound;
use ttree::TTree;

const DATASET_SIZES: &[usize] = &[1_000, 10_000, 100_000];
const LOOKUP_COUNT: usize = 10_000;
const RANGE_SIZE: usize = 100;

/// Zipfian distribution for realistic skewed access
struct ZipfianDist {
    items: usize,
    theta: f64,
    zeta_n: f64,
    alpha: f64,
}

impl ZipfianDist {
    fn new(items: usize, theta: f64) -> Self {
        let zeta_n = Self::compute_zeta(items, theta);
        let alpha = 1.0 / (1.0 - theta);
        Self {
            items,
            theta,
            zeta_n,
            alpha,
        }
    }

    fn compute_zeta(n: usize, theta: f64) -> f64 {
        let mut sum = 0.0;
        for i in 1..=n.min(10000) {
            sum += 1.0 / (i as f64).powf(theta);
        }
        sum
    }
}

impl Distribution<u64> for ZipfianDist {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u64 {
        let u: f64 = rng.gen_range(0.0..1.0);
        let uz = u * self.zeta_n;

        if uz < 1.0 {
            return 0;
        }

        let rank = (self.items as f64 * uz.powf(-self.alpha)) as usize;
        rank.min(self.items - 1) as u64
    }
}

// ============================================================================
// SEQUENTIAL INSERT BENCHMARKS
// ============================================================================

fn bench_insert_sequential_art(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_sequential/art");

    for &size in DATASET_SIZES {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            b.iter(|| {
                let tree = ArtTree::<u64, u64>::new();
                for i in 0..n {
                    black_box(tree.insert(i as u64, i as u64 * 2));
                }
                tree
            });
        });
    }

    group.finish();
}

fn bench_insert_sequential_ttree(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_sequential/ttree");

    for &size in DATASET_SIZES {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            b.iter(|| {
                let tree = TTree::<u64, u64>::new();
                for i in 0..n {
                    black_box(tree.insert(i as u64, i as u64 * 2));
                }
                tree
            });
        });
    }

    group.finish();
}

// ============================================================================
// RANDOM INSERT BENCHMARKS
// ============================================================================

fn bench_insert_random_art(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_random/art");

    for &size in DATASET_SIZES {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            let mut rng = rand::rngs::StdRng::seed_from_u64(42);
            let keys: Vec<u64> = (0..n).map(|_| rng.gen()).collect();

            b.iter(|| {
                let tree = ArtTree::<u64, u64>::new();
                for &key in &keys {
                    black_box(tree.insert(key, key * 2));
                }
                tree
            });
        });
    }

    group.finish();
}

fn bench_insert_random_ttree(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_random/ttree");

    for &size in DATASET_SIZES {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            let mut rng = rand::rngs::StdRng::seed_from_u64(42);
            let keys: Vec<u64> = (0..n).map(|_| rng.gen()).collect();

            b.iter(|| {
                let tree = TTree::<u64, u64>::new();
                for &key in &keys {
                    black_box(tree.insert(key, key * 2));
                }
                tree
            });
        });
    }

    group.finish();
}

// ============================================================================
// POINT LOOKUP BENCHMARKS (uniform distribution)
// ============================================================================

fn bench_get_uniform_art(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_uniform/art");
    group.throughput(Throughput::Elements(LOOKUP_COUNT as u64));

    for &size in DATASET_SIZES {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            let tree = ArtTree::<u64, u64>::new();
            for i in 0..n {
                tree.insert(i as u64, i as u64 * 2);
            }

            let mut rng = rand::rngs::StdRng::seed_from_u64(123);
            let dist = Uniform::new(0, n as u64);

            b.iter(|| {
                for _ in 0..LOOKUP_COUNT {
                    let key = dist.sample(&mut rng);
                    black_box(tree.get(&key));
                }
            });
        });
    }

    group.finish();
}

fn bench_get_uniform_ttree(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_uniform/ttree");
    group.throughput(Throughput::Elements(LOOKUP_COUNT as u64));

    for &size in DATASET_SIZES {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            let tree = TTree::<u64, u64>::new();
            for i in 0..n {
                tree.insert(i as u64, i as u64 * 2);
            }

            let mut rng = rand::rngs::StdRng::seed_from_u64(123);
            let dist = Uniform::new(0, n as u64);

            b.iter(|| {
                for _ in 0..LOOKUP_COUNT {
                    let key = dist.sample(&mut rng);
                    black_box(tree.get(&key));
                }
            });
        });
    }

    group.finish();
}

// ============================================================================
// POINT LOOKUP BENCHMARKS (zipfian/skewed distribution)
// ============================================================================

fn bench_get_zipfian_art(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_zipfian/art");
    group.throughput(Throughput::Elements(LOOKUP_COUNT as u64));

    for &size in DATASET_SIZES {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            let tree = ArtTree::<u64, u64>::new();
            for i in 0..n {
                tree.insert(i as u64, i as u64 * 2);
            }

            let mut rng = rand::rngs::StdRng::seed_from_u64(456);
            let zipf = ZipfianDist::new(n, 0.99);

            b.iter(|| {
                for _ in 0..LOOKUP_COUNT {
                    let key = zipf.sample(&mut rng);
                    black_box(tree.get(&key));
                }
            });
        });
    }

    group.finish();
}

fn bench_get_zipfian_ttree(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_zipfian/ttree");
    group.throughput(Throughput::Elements(LOOKUP_COUNT as u64));

    for &size in DATASET_SIZES {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            let tree = TTree::<u64, u64>::new();
            for i in 0..n {
                tree.insert(i as u64, i as u64 * 2);
            }

            let mut rng = rand::rngs::StdRng::seed_from_u64(456);
            let zipf = ZipfianDist::new(n, 0.99);

            b.iter(|| {
                for _ in 0..LOOKUP_COUNT {
                    let key = zipf.sample(&mut rng);
                    black_box(tree.get(&key));
                }
            });
        });
    }

    group.finish();
}

// ============================================================================
// RANGE QUERY BENCHMARKS
// ============================================================================

fn bench_range_art(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_query/art");

    for &size in DATASET_SIZES {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            let tree = ArtTree::<u64, u64>::new();
            for i in 0..n {
                tree.insert(i as u64, i as u64 * 2);
            }

            let mut rng = rand::rngs::StdRng::seed_from_u64(789);

            b.iter(|| {
                let start = rng.gen_range(0..(n - RANGE_SIZE)) as u64;
                let end = start + RANGE_SIZE as u64;

                let results: Vec<_> = tree
                    .range(Bound::Included(&start), Bound::Excluded(&end))
                    .collect();
                black_box(results)
            });
        });
    }

    group.finish();
}

fn bench_range_ttree(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_query/ttree");

    for &size in DATASET_SIZES {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            let tree = TTree::<u64, u64>::new();
            for i in 0..n {
                tree.insert(i as u64, i as u64 * 2);
            }

            let mut rng = rand::rngs::StdRng::seed_from_u64(789);

            b.iter(|| {
                let start = rng.gen_range(0..(n - RANGE_SIZE)) as u64;
                let end = start + RANGE_SIZE as u64;

                let results: Vec<_> = tree
                    .range(Bound::Included(&start), Bound::Excluded(&end))
                    .collect();
                black_box(results)
            });
        });
    }

    group.finish();
}

// ============================================================================
// MEMORY USAGE COMPARISON
// ============================================================================

fn bench_memory_usage(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_usage");

    for &size in DATASET_SIZES {
        group.bench_function(BenchmarkId::new("art", size), |b| {
            let tree = ArtTree::<u64, u64>::new();
            for i in 0..size {
                tree.insert(i as u64, i as u64 * 2);
            }

            b.iter(|| black_box(tree.memory_usage()));
        });

        group.bench_function(BenchmarkId::new("ttree", size), |b| {
            let tree = TTree::<u64, u64>::new();
            for i in 0..size {
                tree.insert(i as u64, i as u64 * 2);
            }

            b.iter(|| black_box(tree.memory_usage()));
        });
    }

    group.finish();
}

// ============================================================================
// MIXED WORKLOAD (70% read, 30% write)
// ============================================================================

fn bench_mixed_workload_art(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload/art");
    group.throughput(Throughput::Elements(LOOKUP_COUNT as u64));

    for &size in DATASET_SIZES {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            let tree = ArtTree::<u64, u64>::new();
            for i in 0..n {
                tree.insert(i as u64, i as u64 * 2);
            }

            let mut rng = rand::rngs::StdRng::seed_from_u64(999);
            let key_dist = Uniform::new(0, n as u64);

            b.iter(|| {
                for _ in 0..LOOKUP_COUNT {
                    let key = key_dist.sample(&mut rng);
                    let op = rng.gen_range(0..100);

                    if op < 70 {
                        // 70% reads
                        black_box(tree.get(&key));
                    } else {
                        // 30% writes
                        black_box(tree.insert(key, key * 3));
                    }
                }
            });
        });
    }

    group.finish();
}

fn bench_mixed_workload_ttree(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload/ttree");
    group.throughput(Throughput::Elements(LOOKUP_COUNT as u64));

    for &size in DATASET_SIZES {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &n| {
            let tree = TTree::<u64, u64>::new();
            for i in 0..n {
                tree.insert(i as u64, i as u64 * 2);
            }

            let mut rng = rand::rngs::StdRng::seed_from_u64(999);
            let key_dist = Uniform::new(0, n as u64);

            b.iter(|| {
                for _ in 0..LOOKUP_COUNT {
                    let key = key_dist.sample(&mut rng);
                    let op = rng.gen_range(0..100);

                    if op < 70 {
                        // 70% reads
                        black_box(tree.get(&key));
                    } else {
                        // 30% writes
                        black_box(tree.insert(key, key * 3));
                    }
                }
            });
        });
    }

    group.finish();
}

criterion_group!(
    inserts,
    bench_insert_sequential_art,
    bench_insert_sequential_ttree,
    bench_insert_random_art,
    bench_insert_random_ttree,
);

criterion_group!(
    lookups,
    bench_get_uniform_art,
    bench_get_uniform_ttree,
    bench_get_zipfian_art,
    bench_get_zipfian_ttree,
);

criterion_group!(ranges, bench_range_art, bench_range_ttree,);

criterion_group!(
    mixed,
    bench_mixed_workload_art,
    bench_mixed_workload_ttree,
    bench_memory_usage,
);

criterion_main!(inserts, lookups, ranges, mixed);
