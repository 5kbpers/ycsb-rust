use anyhow::Result;

pub type KvPair = (String, String);

pub trait Db: Send + Sync {
    ///
    /// Initializes any state for accessing this DB.
    /// Called once per DB client (thread); there is a single DB instance globally.
    ///
    fn init(&self);
    ///
    /// Clears any state for accessing this DB.
    /// Called once per DB client (thread); there is a single DB instance globally.
    ///
    fn close(&self);
    ///
    /// Reads a record from the database.
    /// Field/value pairs from the result are stored in a vector.
    ///
    /// @param table The name of the table.
    /// @param key The key of the record to read.
    /// @param fields The list of fields to read, or be empty for all of them.
    /// @return Ok(KV) on success, or an Err on error/record-miss.
    ///
    fn read(&self, table: String, key: String, fields: Vec<String>) -> Result<Vec<KvPair>>;
    ///
    /// Performs a range scan for a set of records in the database.
    /// Field/value pairs from the result are stored in a vector.
    ///
    /// @param table The name of the table.
    /// @param key The key of the first record to read.
    /// @param record_count The number of records to read.
    /// @param fields The list of fields to read, or NULL for all of them.
    /// @return Ok(Vec<KV)> on success, or an Err on error/record-miss.
    ///
    fn scan(
        &self,
        table: String,
        key: String,
        fields: Vec<String>,
        count: u64,
    ) -> Result<Vec<Vec<KvPair>>>;
    ///
    /// Updates a record in the database.
    /// Field/value pairs in the specified vector are written to the record,
    /// overwriting any existing values with the same field names.
    ///
    /// @param table The name of the table.
    /// @param key The key of the record to write.
    /// @param values A vector of field/value pairs to update in the record.
    /// @return Ok() on success, or an Err on error/record-miss.
    ///
    fn update(&self, table: String, key: String, values: Vec<KvPair>) -> Result<()>;
    ///
    /// Inserts a record into the database.
    /// Field/value pairs in the specified vector are written into the record.
    ///
    /// @param table The name of the table.
    /// @param key The key of the record to insert.
    /// @param values A vector of field/value pairs to insert in the record.
    /// @return Ok() on success, or an Err on error/record-miss.
    ///
    fn insert(&self, table: String, key: String, values: Vec<KvPair>) -> Result<()>;
}
