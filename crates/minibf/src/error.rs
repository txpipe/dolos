use axum::{
    http::StatusCode,
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
    /// The path matched no route. Blockfrost answers those with a global
    /// `400`, not a `404`.
    InvalidPath,
    /// The request has no Content-Type header.
    MissingContentType,
    /// The Content-Type header names a media type other than this one.
    InvalidContentType(&'static str),
    /// The transaction text is neither base16 nor base64.
    InvalidTxPayload,
    /// The `version` query parameter is not an integer.
    InvalidOgmiosVersion,
    /// The evaluation request body is malformed, for this reason.
    InvalidEvaluationRequest(String),
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
            Error::InvalidPath => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(
                    400,
                    "Bad Request",
                    "Invalid path.",
                )),
            )
                .into_response(),
            // Blockfrost answers a request without Content-Type with this
            // exact body.
            Error::MissingContentType => (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                Json(ErrorBody::new(
                    415,
                    "Unsupported Media Type",
                    "Unsupported Media Type: undefined",
                )),
            )
                .into_response(),
            Error::InvalidContentType(expected) => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(
                    400,
                    "Bad Request",
                    format!("Content-Type must be {expected}"),
                )),
            )
                .into_response(),
            Error::InvalidTxPayload => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(
                    400,
                    "Bad Request",
                    "Invalid request: failed to decode payload from base64 or base16.",
                )),
            )
                .into_response(),
            Error::InvalidOgmiosVersion => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(
                    400,
                    "Bad Request",
                    "Invalid version. Use an integer: 6 selects the Ogmios v6 format, any other value the v5 format.",
                )),
            )
                .into_response(),
            Error::InvalidEvaluationRequest(message) => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(400, "Bad Request", message)),
            )
                .into_response(),
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
