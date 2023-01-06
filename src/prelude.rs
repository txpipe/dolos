pub use super::model::*;
pub use super::storage::Cursor;
pub use super::upstream::prelude::*;

use miette::Diagnostic;
use std::fmt::Display;
use thiserror::Error;

#[derive(Error, Debug, Diagnostic)]
pub enum Error {
    #[error("configuration error: {0}")]
    ConfigError(String),

    #[error("client error: {0}")]
    ClientError(String),

    #[error("parse error: {0}")]
    ParseError(String),

    #[error("server error: {0}")]
    ServerError(String),

    #[error("storage error: {0}")]
    StorageError(String),

    #[error("{0}")]
    Message(String),

    #[error("{0}")]
    Custom(String),
}

impl Error {
    pub fn config(text: impl Into<String>) -> Error {
        Error::ConfigError(text.into())
    }

    pub fn client(error: impl Display) -> Error {
        Error::ClientError(error.to_string())
    }

    pub fn parse(error: impl Display) -> Error {
        Error::ParseError(error.to_string())
    }

    pub fn server(error: impl Display) -> Error {
        Error::ServerError(error.to_string())
    }

    pub fn storage(error: impl Display) -> Error {
        Error::StorageError(error.to_string())
    }

    pub fn message(text: impl Into<String>) -> Error {
        Error::Message(text.into())
    }

    pub fn custom(error: Box<dyn std::error::Error>) -> Error {
        Error::Custom(format!("{}", error))
    }
}

impl From<Box<dyn std::error::Error>> for Error {
    fn from(err: Box<dyn std::error::Error>) -> Self {
        Error::custom(err)
    }
}
