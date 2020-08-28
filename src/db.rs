mod btree;
mod skiplist;

use std::sync::Arc;

use anyhow::Result;

use crate::core::db::Db;

pub fn create_db(name: &str) -> Result<Arc<dyn Db>> {
    let db: Arc<dyn Db> = match name {
        "btree" => Arc::new(btree::BTreeDb::new()),
        "skiplist" => Arc::new(skiplist::SkiplistDb::new()),
        _ => return Err(anyhow!("unsupported database {}", name)),
    };
    Ok(db)
}
