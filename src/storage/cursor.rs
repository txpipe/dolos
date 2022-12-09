use pallas::network::miniprotocols::Point;

use crate::prelude::*;

pub enum Cursor {
    StaticCursor(Vec<Point>),
}

impl Cursor {
    pub fn intersections(&self) -> Result<Vec<Point>, Error> {
        match self {
            Cursor::StaticCursor(x) => Ok(x.clone()),
        }
    }
}
