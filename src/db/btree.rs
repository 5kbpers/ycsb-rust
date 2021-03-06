use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use anyhow::Result;

use crate::core::db::{Db, KvPair};

pub struct BTreeDb {
    inner: Arc<RwLock<BTreeMap<String, Vec<KvPair>>>>,
}

impl BTreeDb {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(BTreeMap::default())),
        }
    }
}

impl Db for BTreeDb {
    fn init(&self) {}

    fn close(&self) {}

    fn read(&self, _: String, key: String, _: Vec<String>) -> Result<Vec<KvPair>> {
        let db = self.inner.read().unwrap();
        db.get(&key)
            .cloned()
            .ok_or_else(|| anyhow!("key {} does not exist", key))
    }

    fn scan(&self, _: String, key: String, _: Vec<String>, count: u64) -> Result<Vec<Vec<KvPair>>> {
        use std::ops::RangeFrom;

        let db = self.inner.read().unwrap();
        Ok(db
            .range(RangeFrom { start: key })
            .take(count as usize)
            .map(|(_, value)| value.clone())
            .collect())
    }

    fn update(&self, _: String, key: String, values: Vec<KvPair>) -> Result<()> {
        let mut db = self.inner.write().unwrap();
        db.insert(key, values);
        Ok(())
    }

    fn insert(&self, table: String, key: String, values: Vec<KvPair>) -> Result<()> {
        self.update(table, key, values)
    }
}
