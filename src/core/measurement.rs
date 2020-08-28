mod histogram;
pub use histogram::*;

use std::time::Duration;

#[derive(Debug)]
struct MeasurementInfo {
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

trait Measurement {
    fn measure(&self, latency: Duration);
    fn summary(&self) -> String;
    fn info(&self) -> MeasurementInfo;
}
