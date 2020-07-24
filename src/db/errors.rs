use thiserror::Error;

#[derive(Error, Debug)]
pub enum DBError {
    #[error("{0}")]
    Other(String),
}
