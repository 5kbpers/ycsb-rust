use anyhow::Result;

use super::db::DB;
use super::workload::{CoreWorkload, Operation};

pub struct Client<T: DB> {
    db: T,
    workload: CoreWorkload,
}

impl<T: DB> Client<T> {
    pub fn new(db: T, workload: CoreWorkload) -> Self {
        Self { db, workload }
    }

    pub fn do_insert(&self) -> Result<()> {
        let key = self.workload.next_sequence_key();
        let values = self.workload.build_values();
        self.db.insert(self.workload.next_table(), key, values)
    }

    pub fn do_transaction(&self) -> Result<()> {
        let table = self.workload.next_table();

        match self.workload.next_operation() {
            Operation::Read => {
                let key = self.workload.next_transaction_key();
                self.db
                    .read(table, key, self.workload.read_fields())
                    .map(|_| ())
            }
            Operation::Update => {
                let key = self.workload.next_transaction_key();
                let values = if self.workload.write_all_fields() {
                    self.workload.build_values()
                } else {
                    self.workload.build_update()
                };
                self.db.update(table, key, values)
            }
            Operation::Insert => {
                let key = self.workload.next_sequence_key();
                self.db.insert(table, key, self.workload.build_values())
            }
            Operation::Scan => {
                let key = self.workload.next_transaction_key();
                let count = self.workload.next_scan_length();
                self.db
                    .scan(table, key, self.workload.read_fields(), count)
                    .map(|_| ())
            }
            Operation::ReadModifyWrite => {
                let key = self.workload.next_transaction_key();
                self.db
                    .read(table.clone(), key.clone(), self.workload.read_fields())?;
                let values = if self.workload.write_all_fields() {
                    self.workload.build_values()
                } else {
                    self.workload.build_update()
                };
                self.db.update(table, key, values)
            }
        }
    }
}
