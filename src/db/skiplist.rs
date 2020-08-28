use std::sync::Arc;

use anyhow::Result;
use crossbeam_skiplist::map::SkipMap;

use crate::core::db::{Db, KvPair};

pub struct SkiplistDb {
    // HashMap<table_name, SkipMap<key, fields>>
    inner: Arc<SkipMap<String, Vec<KvPair>>>,
}

impl SkiplistDb {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SkipMap::new()),
        }
    }
}

impl Db for SkiplistDb {
    fn init(&self) {}

    fn close(&self) {}

    fn read(&self, _: String, key: String, _: Vec<String>) -> Result<Vec<KvPair>> {
        self.inner
            .get(&key)
            .ok_or_else(|| anyhow!("key {} does not exist", key))
            .map(|e| e.value().clone())
    }

    fn scan(&self, _: String, key: String, _: Vec<String>, count: u64) -> Result<Vec<Vec<KvPair>>> {
        use std::ops::RangeFrom;

        Ok(self
            .inner
            .range(RangeFrom { start: key })
            .take(count as usize)
            .map(|e| e.value().clone())
            .collect())
    }

    fn update(&self, _: String, key: String, values: Vec<KvPair>) -> Result<()> {
        self.inner.insert(key, values);
        Ok(())
    }

    fn insert(&self, table: String, key: String, values: Vec<KvPair>) -> Result<()> {
        self.update(table, key, values)
    }
}
