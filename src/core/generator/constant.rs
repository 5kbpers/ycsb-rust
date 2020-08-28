use super::Generator;

pub struct ConstantGenerator<T>(T);

impl<T> ConstantGenerator<T> {
    pub fn new(val: T) -> Self {
        Self(val)
    }
}

impl<T: Clone + Send + Sync> Generator<T> for ConstantGenerator<T> {
    fn next(&self) -> T {
        self.0.clone()
    }

    fn last(&self) -> T {
        self.0.clone()
    }
}
