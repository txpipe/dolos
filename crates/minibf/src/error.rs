use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

use crate::pagination::PaginationError;

pub enum Error {
    Pagination(PaginationError),
    Code(StatusCode),
    InvalidAddress,
    InvalidAsset,
    InvalidBlockNumber,
    InvalidBlockHash,
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
            Error::InvalidAsset => (
                StatusCode::BAD_REQUEST,
                Json(ErrorBody::new(
                    400,
                    "Bad Request",
                    "Invalid or malformed asset format.",
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
