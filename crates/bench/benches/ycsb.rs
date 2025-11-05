// YCSB-style benchmark suite for NeuroIndex
// Based on Yahoo! Cloud Serving Benchmark workloads
// https://github.com/brianfrankcooper/YCSB

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use engine::Engine;
use rand::distributions::{Distribution, Uniform, WeightedIndex};
use rand::{Rng, SeedableRng};

const RECORD_COUNT: usize = 100_000;
const OP_COUNT: usize = 10_000;

/// YCSB Workload generator
struct YcsbWorkload {
    rng: rand::rngs::StdRng,
    key_dist: Box<dyn Distribution<u64>>,
    op_dist: WeightedIndex<u32>,
}

impl YcsbWorkload {
    /// Workload A: Update heavy (50% read, 50% update)
    fn workload_a(seed: u64) -> Self {
        Self {
            rng: rand::rngs::StdRng::seed_from_u64(seed),
            key_dist: Box::new(Zipfian::new(RECORD_COUNT as u64)),
            op_dist: WeightedIndex::new(&[50, 50]).unwrap(), // read, update
        }
    }

    /// Workload B: Read mostly (95% read, 5% update)
    fn workload_b(seed: u64) -> Self {
        Self {
            rng: rand::rngs::StdRng::seed_from_u64(seed),
            key_dist: Box::new(Zipfian::new(RECORD_COUNT as u64)),
            op_dist: WeightedIndex::new(&[95, 5]).unwrap(),
        }
    }

    /// Workload C: Read only (100% read)
    fn workload_c(seed: u64) -> Self {
        Self {
            rng: rand::rngs::StdRng::seed_from_u64(seed),
            key_dist: Box::new(Zipfian::new(RECORD_COUNT as u64)),
            op_dist: WeightedIndex::new(&[100, 0]).unwrap(),
        }
    }

    /// Workload D: Read latest (95% read, 5% insert) with recency bias
    fn workload_d(seed: u64) -> Self {
        Self {
            rng: rand::rngs::StdRng::seed_from_u64(seed),
            key_dist: Box::new(Latest::new(RECORD_COUNT as u64)),
            op_dist: WeightedIndex::new(&[95, 5]).unwrap(),
        }
    }

    /// Workload E: Short ranges (95% scan, 5% insert)
    fn workload_e(seed: u64) -> Self {
        Self {
            rng: rand::rngs::StdRng::seed_from_u64(seed),
            key_dist: Box::new(Uniform::new(0, RECORD_COUNT as u64)),
            op_dist: WeightedIndex::new(&[95, 5]).unwrap(),
        }
    }

    /// Workload F: Read-modify-write (50% read, 50% update)
    fn workload_f(seed: u64) -> Self {
        Self {
            rng: rand::rngs::StdRng::seed_from_u64(seed),
            key_dist: Box::new(Zipfian::new(RECORD_COUNT as u64)),
            op_dist: WeightedIndex::new(&[50, 50]).unwrap(),
        }
    }

    fn next_key(&mut self) -> u64 {
        self.key_dist.sample(&mut self.rng)
    }

    fn next_op(&mut self) -> usize {
        self.op_dist.sample(&mut self.rng)
    }
}

/// Zipfian distribution (power law)
/// Simulates real-world skewed access patterns
struct Zipfian {
    uniform: Uniform<f64>,
    items: u64,
    theta: f64,
    zeta_2: f64,
    alpha: f64,
    eta: f64,
}

impl Zipfian {
    fn new(items: u64) -> Self {
        let theta = 0.99; // Skew parameter
        let zeta_2 = Self::zeta(2, theta);
        let alpha = 1.0 / (1.0 - theta);
        let eta = (1.0 - ((2.0 / items as f64).powf(1.0 - theta)))
            / (1.0 - zeta_2 / Self::zeta(items, theta));

        Self {
            uniform: Uniform::new(0.0, 1.0),
            items,
            theta,
            zeta_2,
            alpha,
            eta,
        }
    }

    fn zeta(n: u64, theta: f64) -> f64 {
        let mut sum = 0.0;
        for i in 1..=n.min(10000) {
            sum += 1.0 / (i as f64).powf(theta);
        }
        sum
    }
}

impl Distribution<u64> for Zipfian {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u64 {
        let u: f64 = self.uniform.sample(rng);
        let uz = u * Self::zeta(self.items, self.theta);
        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + 0.5_f64.powf(self.theta) {
            return 1;
        }
        let rank = (self.items as f64 * ((self.eta * u - self.eta + 1.0).powf(self.alpha))) as u64;
        rank.min(self.items - 1)
    }
}

/// Latest distribution (recency bias)
struct Latest {
    zipf: Zipfian,
    max: u64,
}

impl Latest {
    fn new(items: u64) -> Self {
        Self {
            zipf: Zipfian::new(items),
            max: items,
        }
    }
}

impl Distribution<u64> for Latest {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u64 {
        let zipf_val = self.zipf.sample(rng);
        self.max.saturating_sub(zipf_val)
    }
}

fn ycsb_workload_a(c: &mut Criterion) {
    let mut group = c.benchmark_group("ycsb_a");
    group.throughput(Throughput::Elements(OP_COUNT as u64));

    let engine = Engine::<u64, u64>::with_shards(8, 1 << 14);

    // Load phase
    for i in 0..RECORD_COUNT {
        engine.put(i as u64, i as u64);
    }

    group.bench_function("update_heavy_50_50", |b| {
        let mut wl = YcsbWorkload::workload_a(42);
        b.iter(|| {
            for _ in 0..OP_COUNT {
                let key = wl.next_key();
                let op = wl.next_op();
                if op == 0 {
                    black_box(engine.get(&key));
                } else {
                    black_box(engine.put(key, key + 1));
                }
            }
        });
    });

    group.finish();
}

fn ycsb_workload_b(c: &mut Criterion) {
    let mut group = c.benchmark_group("ycsb_b");
    group.throughput(Throughput::Elements(OP_COUNT as u64));

    let engine = Engine::<u64, u64>::with_shards(8, 1 << 14);

    for i in 0..RECORD_COUNT {
        engine.put(i as u64, i as u64);
    }

    group.bench_function("read_mostly_95_5", |b| {
        let mut wl = YcsbWorkload::workload_b(42);
        b.iter(|| {
            for _ in 0..OP_COUNT {
                let key = wl.next_key();
                let op = wl.next_op();
                if op == 0 {
                    black_box(engine.get(&key));
                } else {
                    black_box(engine.put(key, key + 1));
                }
            }
        });
    });

    group.finish();
}

fn ycsb_workload_c(c: &mut Criterion) {
    let mut group = c.benchmark_group("ycsb_c");
    group.throughput(Throughput::Elements(OP_COUNT as u64));

    let engine = Engine::<u64, u64>::with_shards(8, 1 << 14);

    for i in 0..RECORD_COUNT {
        engine.put(i as u64, i as u64);
    }

    group.bench_function("read_only", |b| {
        let mut wl = YcsbWorkload::workload_c(42);
        b.iter(|| {
            for _ in 0..OP_COUNT {
                let key = wl.next_key();
                black_box(engine.get(&key));
            }
        });
    });

    group.finish();
}

fn ycsb_mixed_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("ycsb_sizes");

    for size in [1_000, 10_000, 100_000, 1_000_000] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let engine = Engine::<u64, u64>::with_shards(8, size / 4);

            for i in 0..size {
                engine.put(i as u64, i as u64);
            }

            let mut rng = rand::rngs::StdRng::seed_from_u64(42);
            let dist = Uniform::new(0, size as u64);

            b.iter(|| {
                let key = dist.sample(&mut rng);
                black_box(engine.get(&key))
            });
        });
    }

    group.finish();
}

fn ycsb_concurrent_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("ycsb_concurrent");
    group.throughput(Throughput::Elements(OP_COUNT as u64));

    let engine = Engine::<u64, u64>::with_shards(8, 1 << 14);

    for i in 0..RECORD_COUNT {
        engine.put(i as u64, i as u64);
    }

    group.bench_function("uniform_read_write", |b| {
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let key_dist = Uniform::new(0, RECORD_COUNT as u64);
        let op_dist = Uniform::new(0, 100);

        b.iter(|| {
            for _ in 0..OP_COUNT {
                let key = key_dist.sample(&mut rng);
                let op = op_dist.sample(&mut rng);
                if op < 80 {
                    // 80% reads
                    black_box(engine.get(&key));
                } else {
                    // 20% writes
                    black_box(engine.put(key, key + 1));
                }
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    ycsb_workload_a,
    ycsb_workload_b,
    ycsb_workload_c,
    ycsb_mixed_sizes,
    ycsb_concurrent_access
);
criterion_main!(benches);
