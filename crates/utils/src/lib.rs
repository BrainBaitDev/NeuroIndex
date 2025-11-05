use rand::{Rng, SeedableRng};

pub fn rand_bytes(len: usize, seed: u64) -> Vec<u8> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    (0..len).map(|_| rng.gen()).collect()
}
