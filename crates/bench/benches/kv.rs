use criterion::{black_box, criterion_group, criterion_main, Criterion};
use engine::Engine;
use rand::{Rng, SeedableRng};
use std::ops::Bound::{Excluded, Included};

fn bench_put_get(c: &mut Criterion) {
    let eng = Engine::<u64, u64>::with_shards(8, 1 << 12);

    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    let keys: Vec<u64> = (0..100_000).map(|_| rng.gen()).collect();

    c.bench_function("put_100k", |b| {
        b.iter(|| {
            for &k in &keys {
                black_box(eng.put(k, k.wrapping_add(1)).unwrap());
            }
        });
    });

    c.bench_function("get_100k", |b| {
        b.iter(|| {
            for &k in &keys {
                black_box(eng.get(&k));
            }
        });
    });
}

fn bench_range_and_join(c: &mut Criterion) {
    let eng = Engine::<u64, u64>::with_shards(8, 1 << 12);
    for i in 0..50_000u64 {
        eng.put(i, i * 2).unwrap();
        if i % 2 == 0 {
            eng.tag(&i, 42).unwrap();
        }
    }
    c.bench_function("range_filter_limit", |b| {
        b.iter(|| {
            black_box(
                eng.range_query(Included(&1_000), Included(&20_000))
                    .filter(|k, _| *k % 3 == 0)
                    .with_limit(128)
                    .collect(),
            );
        });
    });

    let right = Engine::<u64, u64>::with_shards(8, 1 << 12);
    for i in 0..25_000u64 {
        right.put(i * 2, i * 3).unwrap();
    }
    c.bench_function("hash_join_even_keys", |b| {
        b.iter(|| {
            black_box(eng.join().hash_join(
                &right,
                (Included(&0), Included(&40_000)),
                (Included(&0), Excluded(&50_000)),
                |k, _| *k,
                |k, _| *k,
            ));
        });
    });
}

criterion_group!(benches, bench_put_get, bench_range_and_join);
criterion_main!(benches);
