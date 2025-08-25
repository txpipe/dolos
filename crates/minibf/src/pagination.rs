use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

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
        };
        let body = Json(json!({
            "error": "Bad Request",
            "status_code": 400,
            "message": message,
        }));

        (StatusCode::BAD_REQUEST, body).into_response()
    }
}

#[derive(Default, Debug, Clone, Deserialize)]
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

        let from = match value.from {
            Some(x) => Some(x.try_into()?),
            None => None,
        };

        let to = match value.to {
            Some(x) => Some(x.try_into()?),
            None => None,
        };

        Ok(Self {
            count,
            page,
            order,
            from,
            to,
        })
    }
}

impl Pagination {
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
