use dashmap::DashMap as HashMap;

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct MeasurementInfo {
    pub elapsed: Duration,
    pub count: u64,
    pub ops: f64,
    pub avg: Duration,
    pub min: Duration,
    pub max: Duration,
    pub per99th: Duration,
    pub per999th: Duration,
    pub per9999th: Duration,
}

impl MeasurementInfo {
    pub fn delta(&self, prev: Self) -> Self {
        Self {
            elapsed: self.elapsed,
            per99th: self.per99th,
            per999th: self.per999th,
            per9999th: self.per9999th,
            avg: self.avg,
            min: self.min,
            max: self.max,

            count: self.count - prev.count,
            ops: (self.count - prev.count) as f64
                / (self.elapsed.as_secs_f64() - prev.elapsed.as_secs_f64()),
        }
    }
}

impl fmt::Display for MeasurementInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "elapsed {:?}, ops: {}, count: {}, avg: {:?}, min: {:?}, max: {:?}, per99th: {:?}, per999th: {:?}, per9999th: {:?}",
            self.elapsed, self.ops, self.count, self.avg, self.min, self.max, self.per99th, self.per999th, self.per9999th,
        )
    }
}

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
            min: Arc::new(AtomicU64::new(std::u64::MAX)),
            max: Arc::new(AtomicU64::new(std::u64::MIN)),
            start_time: Instant::now(),
        }
    }
}

impl Histogram {
    pub fn measure(&self, latency: Duration) {
        let latency = latency.as_nanos() as u64;
        self.sum.fetch_add(latency, Ordering::Release);
        self.count.fetch_add(1, Ordering::Release);
        let bound = latency / self.bucket_count;
        self.buckets
            .entry(bound)
            .and_modify(|v| {
                *v += 1;
            })
            .or_insert(1);
        loop {
            let min = self.min.load(Ordering::Acquire);
            if latency >= min {
                break;
            }
            if self
                .min
                .compare_exchange(min, latency, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
        loop {
            let max = self.max.load(Ordering::Acquire);
            if latency <= max {
                break;
            }
            if self
                .max
                .compare_exchange(max, latency, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    pub fn info(&self) -> MeasurementInfo {
        let min = Duration::from_nanos(self.min.load(Ordering::Acquire));
        let max = Duration::from_nanos(self.max.load(Ordering::Acquire));
        let sum = self.sum.load(Ordering::Acquire);
        let count = self.count.load(Ordering::Acquire);
        let avg = Duration::from_nanos((sum as f64 / count as f64) as u64);

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
            per99th: Duration::from_nanos(per99),
            per999th: Duration::from_nanos(per999),
            per9999th: Duration::from_nanos(per9999),
        }
    }
}
