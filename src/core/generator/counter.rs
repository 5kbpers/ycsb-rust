use std::sync::atomic::{AtomicU64, Ordering};

use super::Generator;

pub struct CounterGenerator(AtomicU64);

impl CounterGenerator {
    pub fn new(start: u64) -> Self {
        Self(AtomicU64::new(start))
    }
}

impl Generator<u64> for CounterGenerator {
    fn next(&self) -> u64 {
        self.0.fetch_add(1, Ordering::Release)
    }

    fn last(&self) -> u64 {
        self.0.load(Ordering::Acquire) - 1
    }
}
