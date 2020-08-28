mod constant;
mod counter;
mod discrete;
mod uniform;
mod zipfian;

pub use constant::*;
pub use counter::*;
pub use discrete::*;
pub use uniform::*;
pub use zipfian::*;

pub trait Generator<T> {
    fn next(&self) -> T;
    fn last(&self) -> T;
}
