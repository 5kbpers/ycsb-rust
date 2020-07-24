use rand::distributions::Distribution;
use std::sync::atomic::{AtomicU64, Ordering};
use zipf::ZipfDistribution;

use super::Generator;

const DEFAULT_ZIPFIAN_EXPONENT: f64 = 0.99;

struct ZipfianGenerator {
    min: u64,
    max: u64,
    exponent: f64,
    zipf: Option<ZipfDistribution>,
    last_value: AtomicU64,
}

impl ZipfianGenerator {
    pub fn new() -> Self {
        Self {
            min: 0,
            max: std::u64::MAX,
            exponent: DEFAULT_ZIPFIAN_EXPONENT,
            zipf: None,
            last_value: AtomicU64::new(0),
        }
    }

    pub fn min(mut self, min: u64) -> Self {
        assert!(min < self.max);
        self.min = min;
        self
    }

    pub fn max(mut self, max: u64) -> Self {
        assert!(self.min < max);
        self.max = max;
        self
    }

    pub fn exponent(mut self, exponent: f64) -> Self {
        self.exponent = exponent;
        self
    }
}

impl Generator for ZipfianGenerator {
    fn next(&self) -> u64 {
        let mut rng = rand::thread_rng();
        let val = self
            .zipf
            .unwrap_or_else(|| {
                ZipfDistribution::new((self.max - self.min) as usize, self.exponent).unwrap()
            })
            .sample(&mut rng) as u64
            + self.min;
        self.last_value.store(val, Ordering::Release);
        val
    }

    fn last(&self) -> u64 {
        self.last_value.load(Ordering::Acquire)
    }
}
