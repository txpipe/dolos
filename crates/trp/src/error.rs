use jsonrpsee::types::ErrorCode;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use tx3_resolver::inputs::{CanonicalQuery, SearchSpace};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("internal error: {0}")]
    InternalError(String),

    #[error(transparent)]
    TraverseError(#[from] pallas::ledger::traverse::Error),

    #[error(transparent)]
    AddressError(#[from] pallas::ledger::addresses::Error),

    #[error(transparent)]
    DecodeError(#[from] pallas::codec::minicbor::decode::Error),

    #[error(transparent)]
    ArgsError(#[from] tx3_sdk::trp::args::Error),

    #[error(transparent)]
    ResolveError(Box<tx3_resolver::Error>),

    #[error(transparent)]
    JsonRpcError(#[from] jsonrpsee::types::ErrorObjectOwned),

    #[error("TIR version {provided} is not supported, expected {expected}")]
    UnsupportedTir { expected: String, provided: String },

    #[error("invalid TIR envelope")]
    InvalidTirEnvelope,

    #[error("failed to decode IR bytes")]
    InvalidTirBytes,

    #[error("only txs from Conway era are supported")]
    UnsupportedTxEra,

    #[error("node can't resolve txs while running at era {era}")]
    UnsupportedEra { era: String },

    #[error("missing argument `{key}` of type {ty:?}")]
    MissingTxArg { key: String, ty: tx3_lang::ir::Type },

    #[error("input `{0}` not resolved")]
    InputNotResolved(String, Box<CanonicalQuery>, Box<SearchSpace>),

    #[error("tx was not accepted: {0}")]
    TxNotAccepted(String),

    #[error("tx script returned failure")]
    TxScriptFailure(Vec<String>),
}

trait IntoErrorData {
    type Output: Serialize + DeserializeOwned + Sized;

    fn into_error_data(self) -> Self::Output;
}

impl IntoErrorData for tx3_resolver::inputs::CanonicalQuery {
    type Output = tx3_sdk::trp::InputQueryDiagnostic;

    fn into_error_data(self) -> Self::Output {
        tx3_sdk::trp::InputQueryDiagnostic {
            address: self.address.as_ref().map(hex::encode),
            min_amount: self
                .min_amount
                .iter()
                .flat_map(|x| x.iter())
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            refs: self
                .refs
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            support_many: self.support_many,
            collateral: self.collateral,
        }
    }
}

impl IntoErrorData for tx3_resolver::inputs::SearchSpace {
    type Output = tx3_sdk::trp::SearchSpaceDiagnostic;

    fn into_error_data(self) -> Self::Output {
        tx3_sdk::trp::SearchSpaceDiagnostic {
            matched: self
                .take(Some(10))
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            by_address_count: self.by_address_count,
            by_asset_class_count: self.by_asset_class_count,
            by_ref_count: self.by_ref_count,
        }
    }
}

impl From<tx3_resolver::Error> for Error {
    fn from(error: tx3_resolver::Error) -> Self {
        let tx3_resolver::Error::InputsError(error) = error else {
            return Error::ResolveError(Box::new(error));
        };

        let tx3_resolver::inputs::Error::InputNotResolved(name, q, ss) = error else {
            return Error::ResolveError(Box::new(error.into()));
        };

        Error::InputNotResolved(name, Box::new(q), Box::new(ss))
    }
}

impl From<dolos_core::StateError> for Error {
    fn from(error: dolos_core::StateError) -> Self {
        Error::InternalError(error.to_string())
    }
}

impl From<dolos_core::State3Error> for Error {
    fn from(error: dolos_core::State3Error) -> Self {
        Error::InternalError(error.to_string())
    }
}

impl From<dolos_core::ArchiveError> for Error {
    fn from(error: dolos_core::ArchiveError) -> Self {
        Error::InternalError(error.to_string())
    }
}

impl From<dolos_core::WalError> for Error {
    fn from(error: dolos_core::WalError) -> Self {
        Error::InternalError(error.to_string())
    }
}

impl From<dolos_core::MempoolError> for Error {
    fn from(error: dolos_core::MempoolError) -> Self {
        match error {
            dolos_core::MempoolError::Internal(x) => Error::InternalError(x.to_string()),
            dolos_core::MempoolError::TraverseError(x) => Error::InternalError(x.to_string()),
            dolos_core::MempoolError::DecodeError(x) => Error::InternalError(x.to_string()),
            dolos_core::MempoolError::StateError(x) => Error::InternalError(x.to_string()),
            dolos_core::MempoolError::PlutusNotSupported => {
                Error::TxNotAccepted("Plutus not supported".to_string())
            }
            dolos_core::MempoolError::InvalidTx(x) => Error::TxNotAccepted(x.to_string()),
        }
    }
}

impl From<dolos_core::DomainError> for Error {
    fn from(error: dolos_core::DomainError) -> Self {
        match error {
            dolos_core::DomainError::ChainError(e) => Error::from(e),
            dolos_core::DomainError::StateError(e) => Error::from(e),
            dolos_core::DomainError::State3Error(e) => Error::from(e),
            dolos_core::DomainError::ArchiveError(e) => Error::from(e),
            dolos_core::DomainError::MempoolError(e) => Error::from(e),
            dolos_core::DomainError::WalError(e) => Error::from(e),
        }
    }
}

impl From<dolos_core::ChainError> for Error {
    fn from(error: dolos_core::ChainError) -> Self {
        match error {
            dolos_core::ChainError::State3Error(x) => Error::from(x),
            dolos_core::ChainError::StateError(x) => Error::from(x),
            dolos_core::ChainError::BrokenInvariant(x) => Error::TxNotAccepted(x.to_string()),
            dolos_core::ChainError::DecodingError(x) => Error::TxNotAccepted(x.to_string()),
            dolos_core::ChainError::MinicborError(x) => Error::TxNotAccepted(x.to_string()),
            dolos_core::ChainError::ValidationError(x) => Error::TxNotAccepted(x.to_string()),
            dolos_core::ChainError::ValidationPhase2Error(x) => Error::TxNotAccepted(x.to_string()),
            dolos_core::ChainError::ValidationExplicitPhase2Error(x) => Error::TxScriptFailure(x),
        }
    }
}

impl Error {
    pub const CODE_UNSUPPORTED_TIR: i32 = -32000;
    pub const CODE_MISSING_TX_ARG: i32 = -32001;
    pub const CODE_INPUT_NOT_RESOLVED: i32 = -32002;
    pub const CODE_TX_SCRIPT_FAILURE: i32 = -32003;
    pub const CODE_TX_NOT_ACCEPTED: i32 = -32004;

    pub fn code(&self) -> i32 {
        match self {
            Error::JsonRpcError(err) => err.code(),
            Error::InvalidTirEnvelope => ErrorCode::InvalidParams.code(),
            Error::InvalidTirBytes => ErrorCode::InvalidParams.code(),
            Error::ArgsError(_) => ErrorCode::InvalidParams.code(),
            Error::UnsupportedEra { .. } => ErrorCode::InternalError.code(),
            Error::UnsupportedTxEra => ErrorCode::InternalError.code(),
            Error::InternalError(_) => ErrorCode::InternalError.code(),
            Error::TraverseError(_) => ErrorCode::InternalError.code(),
            Error::AddressError(_) => ErrorCode::InternalError.code(),
            Error::DecodeError(_) => ErrorCode::InternalError.code(),
            Error::ResolveError(_) => ErrorCode::InternalError.code(),

            // custom errors
            Error::UnsupportedTir { .. } => Self::CODE_UNSUPPORTED_TIR,
            Error::MissingTxArg { .. } => Self::CODE_MISSING_TX_ARG,
            Error::InputNotResolved(_, _, _) => Self::CODE_INPUT_NOT_RESOLVED,
            Error::TxScriptFailure(_) => Self::CODE_TX_SCRIPT_FAILURE,
            Error::TxNotAccepted(_) => Self::CODE_TX_NOT_ACCEPTED,
        }
    }

    pub fn data(&self) -> Option<Value> {
        match self {
            Error::JsonRpcError(err) => err.data().and_then(|v| serde_json::to_value(v).ok()),
            Error::UnsupportedTir { provided, expected } => {
                let data = tx3_sdk::trp::UnsupportedTirDiagnostic {
                    provided: provided.to_string(),
                    expected: expected.to_string(),
                };

                Some(json!(data))
            }
            Error::InputNotResolved(name, q, ss) => {
                let data = tx3_sdk::trp::InputNotResolvedDiagnostic {
                    name: name.to_string(),
                    query: q.clone().into_error_data(),
                    search_space: ss.clone().into_error_data(),
                };

                Some(json!(data))
            }
            Error::TxScriptFailure(x) => {
                let data = tx3_sdk::trp::TxScriptFailureDiagnostic { logs: x.clone() };

                Some(json!(data))
            }
            Error::MissingTxArg { key, ty } => {
                let data = tx3_sdk::trp::MissingTxArgDiagnostic {
                    key: key.to_string(),
                    ty: format!("{ty:?}"),
                };

                Some(json!(data))
            }
            _ => None,
        }
    }
}

impl From<Error> for tx3_lang::backend::Error {
    fn from(error: Error) -> Self {
        tx3_lang::backend::Error::StoreError(error.to_string())
    }
}

impl From<Error> for jsonrpsee::types::ErrorObject<'_> {
    fn from(error: Error) -> Self {
        let message = error.to_string();

        jsonrpsee::types::ErrorObject::owned(error.code(), message, error.data())
    }
}
