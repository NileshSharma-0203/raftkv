//! Small helpers shared across the crate.

/// SplitMix64. Fast, seedable, and good enough for timeouts and fault plans.
/// Not for anything security-related.
#[derive(Clone, Debug)]
pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng { state: seed }
    }
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    /// Uniform in `lo..=hi`.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        debug_assert!(lo <= hi);
        lo + self.next_u64() % (hi - lo + 1)
    }
    pub fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
    /// True with probability `permille`/1000.
    pub fn chance(&mut self, permille: u32) -> bool {
        (self.next_u64() % 1000) < permille as u64
    }
    pub fn shuffle<T>(&mut self, v: &mut [T]) {
        for i in (1..v.len()).rev() {
            let j = self.below(i + 1);
            v.swap(i, j);
        }
    }
}

/// A seed that differs between processes and calls.
pub fn entropy_seed() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let t = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0);
    let mut r = Rng::new(t ^ ((std::process::id() as u64) << 32) ^ COUNTER.fetch_add(1, Ordering::Relaxed));
    r.next_u64()
}

/// Value at quantile `q` (0.0..=1.0) of an already sorted slice.
pub fn percentile(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}
