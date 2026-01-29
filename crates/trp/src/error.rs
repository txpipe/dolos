use dolos_core::DomainError;
use jsonrpsee::types::ErrorCode;
use serde_json::Value;
use tx3_resolver::trp::errors::TrpError as _;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    InternalError(String),

    #[error(transparent)]
    MinicborError(#[from] pallas::codec::minicbor::decode::Error),

    #[error(transparent)]
    TraverseError(#[from] pallas::ledger::traverse::Error),

    #[error(transparent)]
    Tx3Error(#[from] Box<tx3_resolver::Error>),

    #[error(transparent)]
    JsonRpcError(#[from] jsonrpsee::types::ErrorObjectOwned),

    #[error("only txs from Conway era are supported")]
    UnsupportedTxEra,

    #[error("node can't resolve txs while running at era {era}")]
    UnsupportedEra { era: String },
}

impl From<dolos_core::IndexError> for Error {
    fn from(error: dolos_core::IndexError) -> Self {
        Error::InternalError(error.to_string())
    }
}

impl From<dolos_core::StateError> for Error {
    fn from(error: dolos_core::StateError) -> Self {
        Error::InternalError(error.to_string())
    }
}

impl Error {
    pub fn code(&self) -> i32 {
        match self {
            Error::Tx3Error(x) => x.code(),
            Error::JsonRpcError(err) => err.code(),
            _ => ErrorCode::InternalError.code(),
        }
    }

    pub fn data(&self) -> Option<Value> {
        match self {
            Error::JsonRpcError(err) => err.data().and_then(|v| serde_json::to_value(v).ok()),
            Error::Tx3Error(x) => x.data(),
            _ => None,
        }
    }

    fn tx_not_accepted(msg: impl std::fmt::Display) -> Self {
        Error::Tx3Error(Box::new(tx3_resolver::Error::TxNotAccepted(
            msg.to_string(),
        )))
    }

    fn tx_script_failure(logs: Vec<String>) -> Self {
        Error::Tx3Error(Box::new(tx3_resolver::Error::TxScriptFailure(logs)))
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        Error::InternalError(error.to_string())
    }
}

impl From<dolos_core::MempoolError> for Error {
    fn from(error: dolos_core::MempoolError) -> Self {
        match error {
            dolos_core::MempoolError::PlutusNotSupported => {
                Error::tx_not_accepted("Plutus not supported")
            }
            dolos_core::MempoolError::InvalidTx(x) => Error::tx_not_accepted(x),
            x => Error::internal(x),
        }
    }
}

impl From<dolos_core::ChainError> for Error {
    fn from(error: dolos_core::ChainError) -> Self {
        match error {
            dolos_core::ChainError::BrokenInvariant(x) => Error::tx_not_accepted(x),
            dolos_core::ChainError::DecodingError(x) => Error::tx_not_accepted(x),
            dolos_core::ChainError::CborDecodingError(x) => Error::tx_not_accepted(x),
            dolos_core::ChainError::Phase1ValidationRejected(x) => Error::tx_not_accepted(x),
            dolos_core::ChainError::Phase2ValidationRejected(x) => Error::tx_script_failure(x),
            x => Error::internal(x),
        }
    }
}

impl From<DomainError> for Error {
    fn from(error: DomainError) -> Self {
        match error {
            dolos_core::DomainError::ChainError(e) => Error::from(e),
            dolos_core::DomainError::MempoolError(e) => Error::from(e),
            _ => Error::internal(error),
        }
    }
}

impl From<tx3_resolver::Error> for Error {
    fn from(error: tx3_resolver::Error) -> Self {
        Error::Tx3Error(Box::new(error))
    }
}

impl From<Error> for jsonrpsee::types::ErrorObject<'_> {
    fn from(error: Error) -> Self {
        let message = error.to_string();

        jsonrpsee::types::ErrorObject::owned(error.code(), message, error.data())
    }
}
