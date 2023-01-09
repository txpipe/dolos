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
