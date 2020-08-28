use anyhow::Result;

use std::collections::HashMap;
use std::io::BufRead;

use super::generator::*;
use super::workload::Operation;

pub const TABLENAME_PROPERTY: &str = "table";
pub const FIELD_COUNT_PROPERTY: &str = "fieldcount";
pub const FIELD_LENGTH_DISTRIBUTION_PROPERTY: &str = "field_len_dist";
pub const FIELD_LENGTH_PROPERTY: &str = "fieldlength";
pub const READ_ALL_FIELDS_PROPERTY: &str = "readallfields";
pub const WRITE_ALL_FIELDS_PROPERTY: &str = "writeallfields";
pub const READ_PROPORTION_PROPERTY: &str = "readproportion";
pub const UPDATE_PROPORTION_PROPERTY: &str = "updateproportion";
pub const INSERT_PROPORTION_PROPERTY: &str = "insertproportion";
pub const SCAN_PROPORTION_PROPERTY: &str = "scanproportion";
pub const READMODIFYWRITE_PROPORTION_PROPERTY: &str = "readmodifywriteproportion";
pub const REQUEST_DISTRIBUTION_PROPERTY: &str = "requestdistribution";
pub const MAX_SCAN_LENGTH_PROPERTY: &str = "maxscanlength";
pub const SCAN_LENGTH_DISTRIBUTION_PROPERTY: &str = "scanlengthdistribution";
pub const INSERT_ORDER_PROPERTY: &str = "insertorder";
pub const INSERT_START_PROPERTY: &str = "insertstart";
pub const RECORD_COUNT_PROPERTY: &str = "recordcount";
pub const OPERATION_COUNT_PROPERTY: &str = "operationcount";

lazy_static! {
    static ref DEFAULT_PROPERTIES: HashMap<&'static str, &'static str> = [
        (TABLENAME_PROPERTY, "usertable"),
        (FIELD_COUNT_PROPERTY, "10"),
        (FIELD_LENGTH_DISTRIBUTION_PROPERTY, "constant"),
        (FIELD_LENGTH_PROPERTY, "100"),
        (READ_ALL_FIELDS_PROPERTY, "true"),
        (WRITE_ALL_FIELDS_PROPERTY, "false"),
        (READ_PROPORTION_PROPERTY, "0.95"),
        (UPDATE_PROPORTION_PROPERTY, "0.05"),
        (INSERT_PROPORTION_PROPERTY, "0.0"),
        (SCAN_PROPORTION_PROPERTY, "0.0"),
        (READMODIFYWRITE_PROPORTION_PROPERTY, "0.0"),
        (REQUEST_DISTRIBUTION_PROPERTY, "uniform"),
        (MAX_SCAN_LENGTH_PROPERTY, "1000"),
        (SCAN_LENGTH_DISTRIBUTION_PROPERTY, "uniform"),
        (INSERT_ORDER_PROPERTY, "hashed"),
        (INSERT_START_PROPERTY, "0"),
    ]
    .iter()
    .copied()
    .collect();
}

#[derive(Default)]
pub struct PropsLoader {
    inner: HashMap<String, String>,
}

impl PropsLoader {
    pub fn load<R: BufRead>(reader: R) -> Result<Self> {
        let mut props = Self::default();
        for line in reader.lines() {
            let line = line?;
            let line = line.trim();
            if line.len() == 0 || line.chars().nth(0).unwrap() == '#' {
                continue;
            }
            let params: Vec<&str> = line.split('=').collect();
            if params.len() != 2 {
                return Err(anyhow!("params length {} is not equal to 2"));
            }
            props
                .inner
                .insert(params[0].trim().to_string(), params[1].trim().to_string());
        }
        Ok(props)
    }

    pub fn get_property(&self, key: &str) -> String {
        self.inner
            .get(key)
            .cloned()
            .or_else(|| DEFAULT_PROPERTIES.get(key).map(|s| s.to_string()))
            .expect(format!("property {} not found", key).as_str())
    }

    pub fn get_field_len_generator(&self) -> Result<Box<dyn Generator<u64>>> {
        let field_len_dist = self.get_property(FIELD_LENGTH_DISTRIBUTION_PROPERTY);
        let field_len: u64 = self.get_property(FIELD_LENGTH_PROPERTY).parse()?;
        let field_len_generator: Box<dyn Generator<u64>> = match field_len_dist.as_str() {
            "uniform" => Box::new(UniformGenerator::new().min(1).max(field_len)),
            "zipfian" => Box::new(ZipfianGenerator::new().min(1).max(field_len)),
            "constant" => Box::new(ConstantGenerator::new(field_len)),
            _ => {
                return Err(anyhow!(
                    "unsupported field length distribution {}",
                    field_len_dist
                ))
            }
        };
        Ok(field_len_generator)
    }

    pub fn get_request_generator(&self) -> Result<Box<dyn Generator<u64>>> {
        let request_dist = self.get_property(REQUEST_DISTRIBUTION_PROPERTY);
        let record_count: u64 = self.get_property(RECORD_COUNT_PROPERTY).parse()?;

        let request_generator: Box<dyn Generator<u64>> = match request_dist.as_str() {
            "uniform" => Box::new(UniformGenerator::new().min(0).max(record_count - 1)),
            "zipfian" => {
                let op_count: u64 = self.get_property(OPERATION_COUNT_PROPERTY).parse()?;
                let insert_proportion: f64 =
                    self.get_property(INSERT_PROPORTION_PROPERTY).parse()?;
                let new_keys = op_count as f64 * insert_proportion * 2.0;
                Box::new(
                    ZipfianGenerator::new()
                        .scramble(true)
                        .max(record_count + new_keys as u64),
                )
            }
            _ => return Err(anyhow!("unsuppprted request distribution {}", request_dist)),
        };
        Ok(request_generator)
    }

    pub fn get_scan_length_generator(&self) -> Result<Box<dyn Generator<u64>>> {
        let scan_len_dist = self.get_property(SCAN_LENGTH_DISTRIBUTION_PROPERTY);
        let max_scan_len: u64 = self.get_property(MAX_SCAN_LENGTH_PROPERTY).parse()?;
        let scan_length_generator: Box<dyn Generator<u64>> = match scan_len_dist.as_str() {
            "uniform" => Box::new(UniformGenerator::new().min(1).max(max_scan_len)),
            "zipfian" => Box::new(ZipfianGenerator::new().min(1).max(max_scan_len)),
            _ => {
                return Err(anyhow!(
                    "unsuppprted scan length distribution {}",
                    scan_len_dist
                ))
            }
        };
        Ok(scan_length_generator)
    }

    pub fn get_operation_generator(&self) -> Result<DiscreteGenerator<Operation>> {
        let read_proportion: f64 = self.get_property(READ_PROPORTION_PROPERTY).parse()?;
        let update_proportion: f64 = self.get_property(UPDATE_PROPORTION_PROPERTY).parse()?;
        let insert_proportion: f64 = self.get_property(INSERT_PROPORTION_PROPERTY).parse()?;
        let scan_proportion: f64 = self.get_property(SCAN_PROPORTION_PROPERTY).parse()?;
        let readmodifywrite_proportion: f64 = self
            .get_property(READMODIFYWRITE_PROPORTION_PROPERTY)
            .parse()?;
        let mut op_chooser = DiscreteGenerator::new();
        if read_proportion > 0.0 {
            op_chooser.add_value(Operation::Read, read_proportion);
        }
        if update_proportion > 0.0 {
            op_chooser.add_value(Operation::Update, update_proportion);
        }
        if insert_proportion > 0.0 {
            op_chooser.add_value(Operation::Insert, insert_proportion);
        }
        if scan_proportion > 0.0 {
            op_chooser.add_value(Operation::Scan, scan_proportion);
        }
        if readmodifywrite_proportion > 0.0 {
            op_chooser.add_value(Operation::ReadModifyWrite, readmodifywrite_proportion);
        }
        Ok(op_chooser)
    }
}
