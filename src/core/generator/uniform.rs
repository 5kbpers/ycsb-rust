use rand::{self, Rng};
use std::sync::atomic::{AtomicU64, Ordering};

use super::Generator;

pub struct UniformGenerator {
    min: u64,
    max: u64,
    last_value: AtomicU64,
}

impl UniformGenerator {
    pub fn new() -> Self {
        Self {
            min: 0,
            max: std::u64::MAX,
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
}

impl Generator for UniformGenerator {
    fn next(&self) -> u64 {
        let val = rand::thread_rng().gen_range(self.min, self.max);
        self.last_value.store(val, Ordering::Release);
        val
    }

    fn last(&self) -> u64 {
        self.last_value.load(Ordering::Acquire)
    }
}
