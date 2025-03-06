use thiserror::Error;

use crate::ledger::BrokenInvariant;

#[derive(Debug, Error)]
pub enum ChainError {
    #[error("broken invariant")]
    BrokenInvariant(#[source] BrokenInvariant),

    #[error("storage error")]
    StorageError(#[source] ::redb::Error),

    #[error("address decoding error")]
    AddressDecoding(pallas::ledger::addresses::Error),

    #[error("query not supported")]
    QueryNotSupported,

    #[error("invalid store version")]
    InvalidStoreVersion,

    #[error("decoding error")]
    DecodingError(#[source] pallas::codec::minicbor::decode::Error),

    #[error("block decoding error")]
    BlockDecodingError(#[source] pallas::ledger::traverse::Error),
}

impl From<::redb::TableError> for ChainError {
    fn from(value: ::redb::TableError) -> Self {
        Self::StorageError(value.into())
    }
}

impl From<::redb::CommitError> for ChainError {
    fn from(value: ::redb::CommitError) -> Self {
        Self::StorageError(value.into())
    }
}

impl From<::redb::StorageError> for ChainError {
    fn from(value: ::redb::StorageError) -> Self {
        Self::StorageError(value.into())
    }
}

impl From<::redb::TransactionError> for ChainError {
    fn from(value: ::redb::TransactionError) -> Self {
        Self::StorageError(value.into())
    }
}

impl From<pallas::ledger::addresses::Error> for ChainError {
    fn from(value: pallas::ledger::addresses::Error) -> Self {
        Self::AddressDecoding(value)
    }
}
