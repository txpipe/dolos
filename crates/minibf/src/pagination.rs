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

#[derive(Debug, Clone, Deserialize)]
pub struct PaginationParameters {
    pub count: Option<String>,
    pub page: Option<String>,
    pub order: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Pagination {
    pub count: usize,
    pub page: u64,
    pub order: Order,
}

impl Default for Pagination {
    fn default() -> Self {
        Pagination {
            count: 100,
            page: 1,
            order: Order::Asc,
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

        Ok(Self { count, page, order })
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
}
