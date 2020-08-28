use rand::{self, Rng};
use std::cell::RefCell;

use super::Generator;

pub struct DiscreteGenerator<T> {
    values: Vec<(T, f64)>,
    sum: f64,
    last: RefCell<Option<T>>,
}

impl<T> DiscreteGenerator<T> {
    pub fn new() -> Self {
        Self {
            values: Vec::default(),
            sum: 0.0,
            last: RefCell::new(None),
        }
    }

    pub fn add_value(&mut self, value: T, weight: f64) {
        self.values.push((value, weight));
        self.sum += weight;
    }
}

impl<T: Clone> Generator<T> for DiscreteGenerator<T> {
    fn next(&self) -> T {
        assert!(!self.values.is_empty());
        let mut chooser = rand::thread_rng().gen_range(0.0, self.sum);
        for (v, w) in &self.values {
            if chooser < *w {
                self.last.replace(Some(v.clone()));
                return v.clone();
            }
            chooser -= w;
        }

        panic!()
    }

    fn last(&self) -> T {
        assert!(!self.values.is_empty());
        self.last
            .replace(None)
            .unwrap_or_else(|| self.values.first().unwrap().0.clone())
    }
}
