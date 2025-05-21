use thiserror::Error;

use crate::ledger::BrokenInvariant;

#[derive(Debug, Error)]
pub enum ChainError {
    #[error("broken invariant")]
    BrokenInvariant(#[from] BrokenInvariant),

    #[error("storage error")]
    StorageError(#[from] Box<::redb::Error>),

    #[error("address decoding error")]
    AddressDecoding(#[from] pallas::ledger::addresses::Error),

    #[error("query not supported")]
    QueryNotSupported,

    #[error("invalid store version")]
    InvalidStoreVersion,

    #[error("decoding error")]
    DecodingError(#[from] pallas::codec::minicbor::decode::Error),

    #[error("block decoding error")]
    BlockDecodingError(#[from] pallas::ledger::traverse::Error),
}

impl From<::redb::DatabaseError> for ChainError {
    fn from(value: ::redb::DatabaseError) -> Self {
        Self::from(Box::new(::redb::Error::from(value)))
    }
}

impl From<::redb::TableError> for ChainError {
    fn from(value: ::redb::TableError) -> Self {
        Self::from(Box::new(::redb::Error::from(value)))
    }
}

impl From<::redb::CommitError> for ChainError {
    fn from(value: ::redb::CommitError) -> Self {
        Self::from(Box::new(::redb::Error::from(value)))
    }
}

impl From<::redb::StorageError> for ChainError {
    fn from(value: ::redb::StorageError) -> Self {
        Self::from(Box::new(::redb::Error::from(value)))
    }
}

impl From<::redb::TransactionError> for ChainError {
    fn from(value: ::redb::TransactionError) -> Self {
        Self::from(Box::new(::redb::Error::from(value)))
    }
}
