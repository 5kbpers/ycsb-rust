mod btree;

use anyhow::Result;

use crate::core::db::Db;

pub fn create_db(name: &str) -> Result<Box<dyn Db>> {
    let db: Box<dyn Db> = match name {
        "btree" => Box::new(btree::BTreeDb::new()),
        _ => return Err(anyhow!("unsupported database {}", name)),
    };
    Ok(db)
}
