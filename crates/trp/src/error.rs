use dolos_core::{ChainError, DomainError, MempoolError};
use jsonrpsee::types::ErrorCode;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use tx3_resolver::inputs::{CanonicalQuery, SearchSpace};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("internal error: {0}")]
    InternalError(String),

    #[error("invalid cbor: {0}")]
    InvalidCborError(String),

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

    #[error("pparams not available")]
    PParamsNotAvailable,

    #[error("missing argument `{key}` of type {ty:?}")]
    MissingTxArg { key: String, ty: tx3_lang::ir::Type },

    #[error("input `{0}` not resolved")]
    InputNotResolved(String, Box<CanonicalQuery>, Box<SearchSpace>),

    #[error("tx was not accepted: {0}")]
    TxNotAccepted(String),

    #[error("tx script returned failure")]
    TxScriptFailure(Vec<String>),
}

impl From<pallas::codec::minicbor::decode::Error> for Error {
    fn from(error: pallas::codec::minicbor::decode::Error) -> Self {
        Error::InvalidCborError(error.to_string())
    }
}

impl From<pallas::ledger::traverse::Error> for Error {
    fn from(error: pallas::ledger::traverse::Error) -> Self {
        Error::InvalidCborError(error.to_string())
    }
}

impl From<pallas::ledger::addresses::Error> for Error {
    fn from(error: pallas::ledger::addresses::Error) -> Self {
        Error::InvalidCborError(error.to_string())
    }
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

impl From<MempoolError> for Error {
    fn from(error: MempoolError) -> Self {
        match error {
            MempoolError::PlutusNotSupported => {
                Error::TxNotAccepted("Plutus not supported".to_string())
            }
            MempoolError::InvalidTx(x) => Error::TxNotAccepted(x.to_string()),
            e => Error::InternalError(e.to_string()),
        }
    }
}

impl From<DomainError> for Error {
    fn from(error: DomainError) -> Self {
        match error {
            DomainError::ChainError(e) => Error::from(e),
            DomainError::MempoolError(e) => Error::from(e),
            e => Error::InternalError(e.to_string()),
        }
    }
}

impl From<ChainError> for Error {
    fn from(error: ChainError) -> Self {
        match error {
            ChainError::BrokenInvariant(x) => Error::TxNotAccepted(x.to_string()),
            ChainError::DecodingError(x) => Error::TxNotAccepted(x.to_string()),
            ChainError::Phase1ValidationRejected(x) => Error::TxNotAccepted(x.to_string()),
            ChainError::Phase2ValidationRejected(x) => Error::TxScriptFailure(x),
            ChainError::EraNotFound => Error::TxNotAccepted("era not found".to_string()),
            e => Error::InternalError(e.to_string()),
        }
    }
}

impl Error {
    pub const CODE_UNSUPPORTED_TIR: i32 = -32000;
    pub const CODE_MISSING_TX_ARG: i32 = -32001;
    pub const CODE_INPUT_NOT_RESOLVED: i32 = -32002;
    pub const CODE_TX_SCRIPT_FAILURE: i32 = -32003;
    pub const CODE_TX_NOT_ACCEPTED: i32 = -32004;
    pub const CODE_INVALID_CBOR: i32 = -32005;

    pub fn code(&self) -> i32 {
        match self {
            Error::JsonRpcError(err) => err.code(),
            Error::InvalidTirEnvelope => ErrorCode::InvalidParams.code(),
            Error::InvalidTirBytes => ErrorCode::InvalidParams.code(),
            Error::ArgsError(_) => ErrorCode::InvalidParams.code(),
            Error::PParamsNotAvailable => ErrorCode::InternalError.code(),
            Error::UnsupportedTxEra => ErrorCode::InternalError.code(),
            Error::InternalError(_) => ErrorCode::InternalError.code(),
            Error::ResolveError(_) => ErrorCode::InternalError.code(),

            // custom errors
            Error::UnsupportedTir { .. } => Self::CODE_UNSUPPORTED_TIR,
            Error::MissingTxArg { .. } => Self::CODE_MISSING_TX_ARG,
            Error::InputNotResolved(_, _, _) => Self::CODE_INPUT_NOT_RESOLVED,
            Error::TxScriptFailure(_) => Self::CODE_TX_SCRIPT_FAILURE,
            Error::TxNotAccepted(_) => Self::CODE_TX_NOT_ACCEPTED,
            Error::InvalidCborError(_) => Self::CODE_INVALID_CBOR,
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
