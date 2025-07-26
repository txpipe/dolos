pub use dolos_core::*;

use miette::Diagnostic;
use std::fmt::Display;
use thiserror::Error;

#[derive(Error, Debug, Diagnostic)]
pub enum Error {
    #[error("io error: {0}")]
    IO(#[from] std::io::Error),

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

    #[error("wal error: {0}")]
    WalError(#[from] WalError),

    #[error("chain error: {0}")]
    ArchiveError(#[from] ArchiveError),

    #[error("state error: {0}")]
    StateError(#[from] StateError),

    #[cfg(feature = "unstable")]
    #[error("state3 error: {0}")]
    State3Error(#[from] StateError3),

    #[error("{0}")]
    Message(String),

    #[error("{0}")]
    Custom(String),
}

impl Error {
    pub fn config(text: impl Display) -> Error {
        Error::ConfigError(text.to_string())
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

    pub fn message(text: impl Into<String>) -> Error {
        Error::Message(text.into())
    }

    pub fn custom(error: impl Display) -> Error {
        Error::Custom(error.to_string())
    }
}

impl From<Box<dyn std::error::Error>> for Error {
    fn from(err: Box<dyn std::error::Error>) -> Self {
        Error::custom(err)
    }
}

#[derive(Clone, Default)]
pub struct CancelTokenImpl(pub tokio_util::sync::CancellationToken);

impl CancelToken for CancelTokenImpl {
    async fn cancelled(&self) {
        self.0.cancelled().await;
    }
}
