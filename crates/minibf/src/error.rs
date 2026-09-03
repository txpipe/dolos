use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

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
    /// An archive scan ran out of block budget before covering the page.
    ///
    /// The budget is a fixed property of the node, so the same request always
    /// runs out at the same point: it is the page that has to shrink, not the
    /// node that has to recover.
    ScanBudgetExceeded,
}

#[derive(Serialize)]
struct ErrorBody {
    status_code: u16,
    error: &'static str,
    message: &'static str,
}

impl ErrorBody {
    fn new(status_code: u16, error: &'static str, message: &'static str) -> Self {
        Self {
            status_code,
            error,
            message,
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
                    "The requested page needs more archive blocks than this node is willing to scan, reduce page number or count.",
                )),
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
