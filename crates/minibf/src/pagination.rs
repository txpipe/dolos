use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use dolos_core::{BlockSlot, Domain};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{error::Error, Facade};

#[derive(Debug, Serialize)]
pub enum PaginationError {
    CountLessThan1,
    CountTooLarge,
    CountNotAnInteger,
    PageTooLarge,
    PageLessThan1,
    PageNotAnInteger,
    OrderNotAllowed,
    InvalidFromTo,
    ScanLimitExceeded,
}

impl IntoResponse for PaginationError {
    fn into_response(self) -> Response {
        let message = match &self {
            PaginationError::CountLessThan1 => "querystring/count must be >= 1",
            PaginationError::CountTooLarge => "querystring/count must be <= 100",
            PaginationError::CountNotAnInteger => "querystring/count must be integer",
            PaginationError::PageLessThan1 => "querystring/page must be >= 1",
            PaginationError::PageTooLarge => "querystring/page must be <= 21474836",
            PaginationError::PageNotAnInteger => "querystring/page must be integer",
            PaginationError::OrderNotAllowed => {
                "querystring/order must be equal to one of the allowed values"
            }
            PaginationError::InvalidFromTo => {
                "Invalid (malformed or out of range) from/to parameter(s)."
            }
            PaginationError::ScanLimitExceeded => {
                "pagination scan limit exceeded, reduce page number or count"
            }
        };
        let body = Json(json!({
            "error": "Bad Request",
            "status_code": 400,
            "message": message,
        }));

        (StatusCode::BAD_REQUEST, body).into_response()
    }
}

#[derive(Default, Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Order {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub struct PaginationNumberAndIndex {
    pub number: u64,
    pub index: Option<usize>,
}

impl TryFrom<String> for PaginationNumberAndIndex {
    type Error = PaginationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let mut parts = value.split(':');
        let Some(number) = parts.next() else {
            return Err(PaginationError::InvalidFromTo);
        };
        let Ok(number) = number.parse() else {
            return Err(PaginationError::InvalidFromTo);
        };

        let index = if let Some(index) = parts.next() {
            Some(
                index
                    .parse::<usize>()
                    .map_err(|_| PaginationError::InvalidFromTo)?,
            )
        } else {
            None
        };

        Ok(Self { number, index })
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PaginationParameters {
    pub count: Option<String>,
    pub page: Option<String>,
    pub order: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Pagination {
    pub count: usize,
    pub page: u64,
    pub order: Order,
    pub from: Option<PaginationNumberAndIndex>,
    pub to: Option<PaginationNumberAndIndex>,
}

impl Default for Pagination {
    fn default() -> Self {
        Pagination {
            count: 100,
            page: 1,
            order: Order::Asc,
            from: None,
            to: None,
        }
    }
}

impl TryFrom<PaginationParameters> for Pagination {
    type Error = PaginationError;
    fn try_from(value: PaginationParameters) -> Result<Self, PaginationError> {
        let count = match value.count {
            Some(count) => match count.parse() {
                Ok(parsed) => {
                    if parsed < 1 {
                        return Err(PaginationError::CountLessThan1);
                    } else if parsed > 100 {
                        return Err(PaginationError::CountTooLarge);
                    } else {
                        parsed
                    }
                }
                Err(_) => return Err(PaginationError::CountNotAnInteger),
            },
            None => 100,
        };

        let page = match value.page {
            Some(page) => match page.parse() {
                Ok(parsed) => {
                    if parsed < 1 {
                        return Err(PaginationError::PageLessThan1);
                    } else if parsed > 21474836 {
                        return Err(PaginationError::PageTooLarge);
                    } else {
                        parsed
                    }
                }
                Err(_) => return Err(PaginationError::PageNotAnInteger),
            },
            None => 1,
        };

        let order = match value.order {
            Some(order) => match order.as_str() {
                "asc" => Order::Asc,
                "desc" => Order::Desc,
                _ => return Err(PaginationError::OrderNotAllowed),
            },
            None => Default::default(),
        };

        let from: Option<PaginationNumberAndIndex> = match value.from {
            Some(x) => Some(x.try_into()?),
            None => None,
        };

        let to: Option<PaginationNumberAndIndex> = match value.to {
            Some(x) => Some(x.try_into()?),
            None => None,
        };

        if let (Some(from), Some(to)) = (from.as_ref(), to.as_ref()) {
            if from.number > to.number {
                return Err(PaginationError::InvalidFromTo);
            }
            if from.number == to.number {
                if let (Some(from_idx), Some(to_idx)) = (from.index, to.index) {
                    if from_idx > to_idx {
                        return Err(PaginationError::InvalidFromTo);
                    }
                }
            }
        }

        Ok(Self {
            count,
            page,
            order,
            from,
            to,
        })
    }
}

/// Temporary workaround: maximum number of items that can be scanned via
/// page-based pagination. Endpoints that require decoding every block in the
/// result set (sub-block pagination) are capped to this limit until we refactor
/// the underlying data storage to support efficient offset-based access.
const MAX_SCAN_ITEMS: u64 = 110_000;

impl Pagination {
    /// Reject requests that would require scanning too many items. Call this
    /// on endpoints where each result requires decoding block data (sub-block
    /// element iteration) and efficient skipping is not yet supported.
    pub fn enforce_max_scan_limit(&self) -> Result<(), PaginationError> {
        if self.page * self.count as u64 > MAX_SCAN_ITEMS {
            return Err(PaginationError::ScanLimitExceeded);
        }
        Ok(())
    }

    pub fn from(&self) -> usize {
        ((self.page - 1) * self.count as u64) as usize
    }

    pub fn to(&self) -> usize {
        (self.count as u64 * self.page) as usize
    }

    pub fn includes(&self, i: usize) -> bool {
        i >= self.from() && i < self.to()
    }

    pub fn skip(&self) -> usize {
        self.from()
    }

    pub fn as_included_item<T>(&self, i: usize, item: T) -> Option<T> {
        if self.includes(i) {
            Some(item)
        } else {
            None
        }
    }

    pub fn should_skip(&self, number: u64, index: usize) -> bool {
        if let Some(from) = self.from.as_ref() {
            if number < from.number {
                return true;
            }
            if number == from.number {
                if let Some(idx) = from.index {
                    if index < idx {
                        return true;
                    }
                }
            }
        };

        if let Some(to) = self.to.as_ref() {
            if number > to.number {
                return true;
            }
            if number == to.number {
                if let Some(idx) = to.index {
                    if index > idx {
                        return true;
                    }
                }
            }
        };

        false
    }

    pub async fn start_and_end_slots<D: Domain>(
        &self,
        domain: &Facade<D>,
    ) -> Result<(BlockSlot, BlockSlot), Error> {
        let start_slot = match self.from.as_ref() {
            Some(x) => domain
                .query()
                .slot_by_number(x.number)
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                .ok_or(StatusCode::BAD_REQUEST)?,
            None => 0,
        };
        let end_slot = match self.to.as_ref() {
            Some(x) => domain
                .query()
                .slot_by_number(x.number)
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                .ok_or(StatusCode::BAD_REQUEST)?,
            None => domain.get_tip_slot()?,
        };

        Ok((start_slot, end_slot))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    #[test]
    fn test_should_skip() {
        let parameters = PaginationParameters {
            from: Some("123:1".to_string()),
            to: Some("124:3".to_string()),
            count: None,
            page: None,
            order: None,
        };
        let pagination = Pagination::try_from(parameters).unwrap();

        assert!(pagination.should_skip(10, 0));
        assert!(pagination.should_skip(123, 0));
        assert!(!pagination.should_skip(123, 1));
        assert!(!pagination.should_skip(123, 4));
        assert!(!pagination.should_skip(124, 1));
        assert!(!pagination.should_skip(124, 3));
        assert!(pagination.should_skip(124, 4));
    }

    #[tokio::test]
    async fn test_invalid_from_to_message() {
        let err = PaginationError::InvalidFromTo;
        let response = err.into_response();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("failed to read response body")
            .to_bytes();
        let parsed: serde_json::Value =
            serde_json::from_slice(&body).expect("failed to parse json");
        assert_eq!(
            parsed["message"],
            "Invalid (malformed or out of range) from/to parameter(s)."
        );
    }
}
