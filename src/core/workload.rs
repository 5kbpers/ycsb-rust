use anyhow::Result;
use rand::{self, distributions, Rng};
use std::io::BufRead;

use super::db::KvPair;
use super::generator::*;
use super::properties::*;

#[derive(Clone, Debug)]
pub enum Operation {
    Insert,
    Read,
    Update,
    Scan,
    ReadModifyWrite,
}

pub struct CoreWorkload {
    table_name: String,

    ordered_inserts: bool,
    read_all_fields: bool,
    write_all_fields: bool,

    key_generator: CounterGenerator,
    op_chooser: DiscreteGenerator<Operation>,
    request_generator: Box<dyn Generator<u64>>,
    insert_key_sequence: CounterGenerator,

    scan_len_chooser: Box<dyn Generator<u64>>,
    field_chooser: UniformGenerator,
    field_len_generator: Box<dyn Generator<u64>>,

    fields: Vec<String>,
}

impl CoreWorkload {
    fn build_key_name(&self, mut num: u64) -> String {
        if !self.ordered_inserts {
            num = fxhash::hash64(&num);
        }
        format!("user{}", num)
    }

    pub fn next_sequence_key(&self) -> String {
        self.build_key_name(self.key_generator.next())
    }

    pub fn next_transaction_key(&self) -> String {
        let mut num = self.request_generator.next();
        while num > self.insert_key_sequence.last() {
            num = self.request_generator.next();
        }
        self.build_key_name(num)
    }

    fn next_field_name(&self) -> String {
        format!("field{}", self.field_chooser.next())
    }

    pub fn next_operation(&self) -> Operation {
        self.op_chooser.next()
    }

    pub fn next_table(&self) -> String {
        self.table_name.clone()
    }

    pub fn next_scan_length(&self) -> u64 {
        self.scan_len_chooser.next()
    }

    pub fn read_fields(&self) -> Vec<String> {
        if self.read_all_fields {
            self.fields.clone()
        } else {
            vec![self.next_field_name()]
        }
    }

    pub fn write_all_fields(&self) -> bool {
        self.write_all_fields
    }

    pub fn new<R: BufRead>(reader: R) -> Result<Self> {
        let props = PropsLoader::load(reader)?;

        let table_name = props.get_property(TABLENAME_PROPERTY);
        let field_count: u64 = props.get_property(FIELD_COUNT_PROPERTY).parse()?;

        let insert_start: u64 = props.get_property(INSERT_START_PROPERTY).parse()?;
        let key_generator = CounterGenerator::new(insert_start);

        let record_count = props.get_property(RECORD_COUNT_PROPERTY).parse()?;
        let insert_key_sequence = CounterGenerator::new(record_count);

        let op_chooser = props.get_operation_generator()?;
        let request_generator = props.get_request_generator()?;
        let field_len_generator = props.get_field_len_generator()?;
        let scan_len_chooser = props.get_scan_length_generator()?;
        let field_chooser = UniformGenerator::new().max(field_count - 1);

        let read_all_fields: bool = props.get_property(READ_ALL_FIELDS_PROPERTY).parse()?;
        let write_all_fields: bool = props.get_property(WRITE_ALL_FIELDS_PROPERTY).parse()?;
        let ordered_inserts = props.get_property(INSERT_ORDER_PROPERTY) == "hashed";

        Ok(Self {
            table_name,

            ordered_inserts,
            read_all_fields,
            write_all_fields,

            key_generator,
            op_chooser,
            request_generator,
            insert_key_sequence,

            scan_len_chooser,
            field_chooser,
            field_len_generator,

            fields: (0..field_count).map(|i| format!("field{}", i)).collect(),
        })
    }

    pub fn build_values(&self) -> Vec<KvPair> {
        self.fields
            .iter()
            .map(|field| {
                let value: String =
                    std::iter::repeat(rand::thread_rng().sample(distributions::Alphanumeric))
                        .take(self.field_len_generator.next() as usize)
                        .collect();
                (field.clone(), value)
            })
            .collect()
    }

    pub fn build_update(&self) -> Vec<KvPair> {
        let field = self.next_field_name();
        let value: String =
            std::iter::repeat(rand::thread_rng().sample(distributions::Alphanumeric))
                .take(self.field_len_generator.next() as usize)
                .collect();
        vec![(field, value)]
    }
}
