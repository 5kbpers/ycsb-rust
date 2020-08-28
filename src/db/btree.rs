use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

use anyhow::Result;

use crate::core::db::{Db, KvPair};

pub struct BTreeDb {
    // HashMap<table_name, BtreeDb<key, fields>>
    inner: Arc<RwLock<HashMap<String, BTreeMap<String, Vec<KvPair>>>>>,
}

impl BTreeDb {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::default())),
        }
    }
}

impl Db for BTreeDb {
    fn init(&self) {}

    fn close(&self) {}

    fn read(&self, table: String, key: String, _: Vec<String>) -> Result<Vec<KvPair>> {
        let lock = self.inner.read().unwrap();
        let db = lock
            .get(&table)
            .ok_or_else(|| anyhow!("table {} does not exist", table))?;
        db.get(&key)
            .cloned()
            .ok_or_else(|| anyhow!("key {} does not exist", key))
    }

    fn scan(
        &self,
        table: String,
        key: String,
        _: Vec<String>,
        count: u64,
    ) -> Result<Vec<Vec<KvPair>>> {
        use std::ops::RangeFrom;

        let lock = self.inner.read().unwrap();
        let db = lock
            .get(&table)
            .ok_or_else(|| anyhow!("table {} does not exist", table))?;
        Ok(db
            .range(RangeFrom { start: key })
            .take(count as usize)
            .map(|(_, value)| value.clone())
            .collect())
    }

    fn update(&self, table: String, key: String, values: Vec<KvPair>) -> Result<()> {
        let mut lock = self.inner.write().unwrap();
        let db = lock
            .get_mut(&table)
            .ok_or_else(|| anyhow!("table {} does not exist", table))?;
        db.insert(key, values);
        Ok(())
    }

    fn insert(&self, table: String, key: String, values: Vec<KvPair>) -> Result<()> {
        self.update(table, key, values)
    }
}
