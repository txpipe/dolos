use rocket::{http::Status, FromFormField};

#[derive(Default, Debug, Clone, FromFormField)]
pub enum Order {
    #[default]
    Asc,
    Desc,
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
impl Pagination {
    pub fn try_new(
        count: Option<u8>,
        page: Option<u64>,
        order: Option<Order>,
    ) -> Result<Self, Status> {
        let count = match count {
            Some(count) => {
                if !(1..=100).contains(&count) {
                    return Err(Status::BadRequest);
                } else {
                    count
                }
            }
            None => 100,
        };
        let page = match page {
            Some(page) => {
                if page < 1 {
                    return Err(Status::BadRequest);
                } else {
                    page
                }
            }
            None => 1,
        };
        Ok(Self {
            count,
            page,
            order: order.unwrap_or_default(),
        })
    }

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
