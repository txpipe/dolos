use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};

use crate::pagination::PaginationError;

pub enum Error {
    Pagination(PaginationError),
    Code(StatusCode),
    InvalidAddress,
    InvalidAsset,
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            Error::Pagination(pagination) => pagination.into_response(),
            Error::Code(status) => {
                if matches!(status, StatusCode::NOT_FOUND) {
                    (
                        status,
                        Json(serde_json::json!({
                            "status_code": 404,
                            "error": "Not Found",
                            "message": "The requested component has not been found."
                        })),
                    )
                        .into_response()
                } else {
                    status.into_response()
                }
            }
            Error::InvalidAddress => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "status_code": 400,
                    "error": "Bad Request",
                    "message": "Invalid address for this network or malformed address format."
                })),
            )
                .into_response(),
            Error::InvalidAsset => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "status_code": 400,
                    "error": "Bad Request",
                    "message": "Invalid or malformed asset format."
                })),
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
