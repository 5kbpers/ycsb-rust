mod uniform;
mod zipfian;

pub use uniform::*;
pub use zipfian::*;

pub trait Generator {
    fn next(&self) -> u64;
    fn last(&self) -> u64;
}
