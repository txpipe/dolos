use axum::http::StatusCode;
use serde::Deserialize;

#[derive(Default, Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Order {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PaginationParameters {
    pub count: Option<u8>,
    pub page: Option<u64>,
    pub order: Option<Order>,
}

#[derive(Debug, Clone)]
pub struct Pagination {
    pub count: u8,
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
    type Error = StatusCode;
    fn try_from(value: PaginationParameters) -> Result<Self, StatusCode> {
        let count = match value.count {
            Some(count) => {
                if !(1..=100).contains(&count) {
                    return Err(StatusCode::BAD_REQUEST);
                } else {
                    count
                }
            }
            None => 100,
        };
        let page = match value.page {
            Some(page) => {
                if page < 1 {
                    return Err(StatusCode::BAD_REQUEST);
                } else {
                    page
                }
            }
            None => 1,
        };
        Ok(Self {
            count,
            page,
            order: value.order.unwrap_or_default(),
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
        i > self.from() && i <= self.to()
    }
}
