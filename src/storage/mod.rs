pub mod chain;
mod cursor;

pub use cursor::*;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("serde error {0}")]
    Serde(String),

    #[error("storage error {0}")]
    Storage(String),
}

impl Error {
    fn serde(error: impl ToString) -> Self {
        Self::Serde(error.to_string())
    }

    fn storage(error: impl ToString) -> Self {
        Self::Storage(error.to_string())
    }
}
