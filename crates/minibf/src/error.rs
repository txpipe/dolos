use axum::{
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use std::borrow::Cow;

use crate::pagination::PaginationError;

#[derive(Debug)]
pub enum Error {
    Pagination(PaginationError),
    Code(StatusCode),
    InvalidAddress,
    InvalidStakeAddress,
    InvalidAsset,
    InvalidPolicy,
    InvalidPoolId,
    InvalidBlockNumber,
    InvalidBlockHash,
    InvalidEpochNumber,
    NumberNotInteger,
    EpochNumberNotInteger,
    InvalidSlotNumber,
    SlotNumberNotInteger,
    InvalidXpub,
    InvalidDerivationRole,
    InvalidDerivationIndex,
    /// A request scan stops when it reaches its item limit.
    ///
    /// The node uses a fixed limit. Thus, the same request always stops at the
    /// same point.
    ScanBudgetExceeded,
    InvalidCertIndex,
    InvalidGovActionId,
    InvalidCommitteeId,
    /// The path matched no route. Blockfrost answers those with a global
    /// `400`, not a `404`.
    InvalidPath,
    /// Every heavy-scan slot stayed taken for the whole admission queue.
    HeavyScanLimit,
    /// The request ran past the node's request deadline.
    RequestTimeout,
    /// `/tx/submit` refused the transaction, for the reason it holds.
    /// Blockfrost's message carries the submit API's reason; this is the
    /// node's.
    TxRejected(String),
    /// `/tx/submit` got a transaction this node's mempool holds already.
    TxInMempool,
    /// `/tx/submit` got a body not declared as CBOR, under the content type
    /// the request named, if any.
    UnsupportedMediaType(Option<String>),
}

#[derive(Serialize)]
struct ErrorBody {
    status_code: u16,
    error: &'static str,
    message: Cow<'static, str>,
}

impl ErrorBody {
    fn new(status_code: u16, error: &'static str, message: impl Into<Cow<'static, str>>) -> Self {
        Self {
            status_code,
            error,
            message: message.into(),
        }
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            Error::Pagination(pagination) => pagination.into_response(),
            Error::Code(status) => {
                if matches!(status, StatusCode::NOT_FOUND) {
                    (
                        status,
                        Json(ErrorBody::new(
                            404,
                            "Not Found",
                            "The requested component has not been found.",
                        )),
                    )
                        .into_response()
                } else {
                    status.into_response()
                }
            }
            Error::InvalidAddress => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(
                    400,
                    "Bad Request",
                    "Invalid address for this network or malformed address format.",
                )),
            )
                .into_response(),
            Error::InvalidStakeAddress => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(
                    400,
                    "Bad Request",
                    "Invalid or malformed stake address format.",
                )),
            )
                .into_response(),
            Error::InvalidAsset => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(
                    400,
                    "Bad Request",
                    "Invalid or malformed asset format.",
                )),
            )
                .into_response(),
            Error::InvalidPolicy => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(
                    400,
                    "Bad Request",
                    "Invalid or malformed policy format.",
                )),
            )
                .into_response(),
            Error::InvalidPoolId => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(
                    400,
                    "Bad Request",
                    "Invalid or malformed pool id format.",
                )),
            )
                .into_response(),
            Error::InvalidBlockNumber => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(
                    400,
                    "Bad Request",
                    "Missing, out of range or malformed block number.",
                )),
            )
                .into_response(),
            Error::InvalidBlockHash => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(
                    400,
                    "Bad Request",
                    "Missing or malformed block hash.",
                )),
            )
                .into_response(),
            Error::InvalidEpochNumber => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(
                    400,
                    "Bad Request",
                    "Missing, out of range or malformed epoch_number.",
                )),
            )
                .into_response(),
            Error::NumberNotInteger => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(
                    400,
                    "Bad Request",
                    "params/number must be integer",
                )),
            )
                .into_response(),
            Error::EpochNumberNotInteger => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(
                    400,
                    "Bad Request",
                    "params/epoch_number must be integer",
                )),
            )
                .into_response(),
            Error::InvalidSlotNumber => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(
                    400,
                    "Bad Request",
                    "Missing, out of range or malformed slot_number.",
                )),
            )
                .into_response(),
            Error::SlotNumberNotInteger => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(
                    400,
                    "Bad Request",
                    "params/slot_number must be integer",
                )),
            )
                .into_response(),
            Error::InvalidXpub => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(
                    400,
                    "Bad Request",
                    "The xpub is not valid. Use 128 hexadecimal characters.",
                )),
            )
                .into_response(),
            Error::InvalidDerivationRole => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(
                    400,
                    "Bad Request",
                    "The role is missing or is not valid. Use an integer from 0 through 2147483647.",
                )),
            )
                .into_response(),
            Error::InvalidDerivationIndex => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(
                    400,
                    "Bad Request",
                    "The index is missing or is not valid. Use an integer from 0 through 2147483647.",
                )),
            )
                .into_response(),
            Error::ScanBudgetExceeded => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(
                    400,
                    "Bad Request",
                    "The request exceeds the scan limit of this node. Reduce the page number or the count.",
                )),
            )
                .into_response(),
            Error::InvalidCertIndex => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(
                    400,
                    "Bad Request",
                    "params/cert_index must be integer",
                )),
            )
                .into_response(),
            Error::InvalidGovActionId => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(
                    400,
                    "Bad Request",
                    "Invalid or malformed gov action id.",
                )),
            )
                .into_response(),
            Error::InvalidCommitteeId => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(
                    400,
                    "Bad Request",
                    "Invalid or malformed cc credential id.",
                )),
            )
                .into_response(),
            Error::InvalidPath => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(
                    400,
                    "Bad Request",
                    "Invalid path. Please check https://docs.txpipe.io/dolos/apis/minibf",
                )),
            )
                .into_response(),
            Error::HeavyScanLimit => (
                StatusCode::SERVICE_UNAVAILABLE,
                [(header::RETRY_AFTER, "1")],
                Json(ErrorBody::new(
                    503,
                    "Service Unavailable",
                    "The node is serving its limit of heavy requests. Retry later.",
                )),
            )
                .into_response(),
            Error::RequestTimeout => (
                StatusCode::GATEWAY_TIMEOUT,
                Json(ErrorBody::new(
                    504,
                    "Gateway Timeout",
                    "The request took longer than this node's deadline.",
                )),
            )
                .into_response(),
            Error::TxRejected(reason) => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(400, "Bad Request", reason)),
            )
                .into_response(),
            Error::TxInMempool => (
                StatusCode::CONFLICT,
                Json(ErrorBody::new(
                    409,
                    "Conflict",
                    "The transaction is already in this node's mempool.",
                )),
            )
                .into_response(),
            Error::UnsupportedMediaType(content_type) => {
                let message = match content_type {
                    Some(content_type) => format!("Unsupported Media Type: {content_type}"),
                    None => "Unsupported Media Type".to_string(),
                };

                (
                    StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    Json(ErrorBody::new(415, "Unsupported Media Type", message)),
                )
                    .into_response()
            }
        }
    }
}

impl From<PaginationError> for Error {
    fn from(value: PaginationError) -> Self {
        Self::Pagination(value)
    }
}
impl From<StatusCode> for Error {
    fn from(value: StatusCode) -> Self {
        Self::Code(value)
    }
}
