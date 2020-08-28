use dashmap::DashMap as HashMap;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::{Measurement, MeasurementInfo};

#[derive(Clone)]
pub struct Histogram {
    bucket_count: u64,
    buckets: Arc<HashMap<u64, u64>>,
    sum: Arc<AtomicU64>,
    count: Arc<AtomicU64>,
    min: Arc<AtomicU64>,
    max: Arc<AtomicU64>,
    start_time: Instant,
}

impl Histogram {
    pub fn new(bucket_count: u64) -> Self {
        Self {
            bucket_count,
            buckets: Arc::new(HashMap::new()),
            sum: Arc::new(AtomicU64::new(0)),
            count: Arc::new(AtomicU64::new(0)),
            min: Arc::new(AtomicU64::new(0)),
            max: Arc::new(AtomicU64::new(0)),
            start_time: Instant::now(),
        }
    }
}

impl Measurement for Histogram {
    fn measure(&self, latency: Duration) {
        self.sum.fetch_add(latency.as_secs(), Ordering::Release);
        self.count.fetch_add(1, Ordering::Release);
        let bound = latency.as_secs() / self.bucket_count;
        self.buckets
            .entry(bound)
            .and_modify(|v| {
                *v += 1;
            })
            .or_insert(1);
        loop {
            let min = self.min.load(Ordering::Acquire);
            if latency.as_secs() >= min {
                break;
            }
            if self
                .min
                .compare_exchange(min, latency.as_secs(), Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
        loop {
            let max = self.max.load(Ordering::Acquire);
            if latency.as_secs() <= max {
                break;
            }
            if self
                .max
                .compare_exchange(max, latency.as_secs(), Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    fn summary(&self) -> String {
        format!("{:?}", self.info())
    }

    fn info(&self) -> MeasurementInfo {
        let min = Duration::from_secs(self.min.load(Ordering::Acquire));
        let max = Duration::from_secs(self.max.load(Ordering::Acquire));
        let sum = self.sum.load(Ordering::Acquire);
        let count = self.count.load(Ordering::Acquire);
        let avg = Duration::from_secs((sum as f64 / count as f64) as u64);

        let mut bounds = self
            .buckets
            .shards()
            .iter()
            .map(|s| s.read().keys().into_iter().copied().collect::<Vec<_>>())
            .flatten()
            .collect::<Vec<_>>();
        bounds.sort_unstable();

        let mut per99 = 0;
        let mut per999 = 0;
        let mut per9999 = 0;

        let mut op_count = 0;
        for bound in bounds {
            let bound_count = self.buckets.get(&bound).unwrap();
            op_count += *bound_count;
            let per = op_count as f64 / count as f64;
            if per99 == 0 && per >= 0.99 {
                per99 = (bound + 1) * self.bucket_count
            }

            if per999 == 0 && per >= 0.999 {
                per999 = (bound + 1) * self.bucket_count
            }

            if per9999 == 0 && per >= 0.9999 {
                per9999 = (bound + 1) * self.bucket_count
            }
        }

        let elapsed = self.start_time.elapsed();
        MeasurementInfo {
            elapsed,
            count,
            avg,
            min,
            max,
            ops: count as f64 / elapsed.as_secs_f64(),
            per99th: Duration::from_secs(per99),
            per999th: Duration::from_secs(per999),
            per9999th: Duration::from_secs(per9999),
        }
    }
}
