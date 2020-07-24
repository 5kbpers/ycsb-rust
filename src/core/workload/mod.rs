use anyhow::Result;

trait Workload<DB> {
    fn close() -> Result<()>;
    fn init_thread(thread_id: usize, thread_count: usize);
    fn cleanup_thread();
    fn do_insert(db: DB) -> Result<()>;
    fn do_batch_insert(db: DB) -> Result<()>;
    fn do_txn(db: DB) -> Result<()>;
    fn do_batch_txn(db: DB) -> Result<()>;
}
