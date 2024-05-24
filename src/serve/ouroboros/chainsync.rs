use itertools::*;
use pallas::{
    crypto::hash::Hash,
    network::miniprotocols::{
        chainsync::{ClientRequest, N2NServer, Tip},
        Point,
    },
};
use tracing::{debug, info, instrument};

use crate::{
    prelude::Error,
    wal::{self, redb::WalStore, ReadUtils, WalReader},
};

pub struct State<'a> {
    wal: WalStore,
    cursor: Option<wal::redb::WalIter<'a>>,
    connection: N2NServer,
}

fn db_tip_to_protocol(tip: (u64, Hash<32>)) -> Tip {
    // TODO: get block height from db
    Tip(Point::Specific(tip.0, tip.1.to_vec()), 0)
}

#[instrument(skip_all)]
async fn handle_next_request(state: &mut State<'_>) -> Result<(), Error> {
    info!("handling next request");

    let next = state
        .cursor
        .as_mut()
        .ok_or(Error::custom("requesting next without intersection"))?
        .filter_forward()
        .as_blocks()
        .flatten()
        .next();

    let tip = state
        .wal
        .find_tip()
        .map_err(Error::server)?
        .map(|(_, x)| Tip(x.into(), 0))
        .unwrap_or(Tip(Point::Origin, 0));

    if let Some(block) = next {
        state
            .connection
            .send_roll_forward(super::convert::header_cbor_to_chainsync(block)?, tip)
            .await
            .map_err(Error::server)?;
    } else {
        state
            .connection
            .send_await_reply()
            .await
            .map_err(Error::server)?;

        debug!("waiting for tip change notification");
        state.wal.tip_change().await;

        todo!("tip chainsync not implemented yet");
    }

    Ok(())
}

#[instrument(skip_all)]
async fn handle_intersect(state: &mut State<'_>, points: Vec<Point>) -> Result<(), Error> {
    info!(?points, "handling intersect request");

    let tip = state
        .wal
        .find_tip()
        .map_err(Error::server)?
        // TODO: send real value for block height
        .map(|(_, x)| Tip(x.into(), 0))
        .unwrap_or(Tip(Point::Origin, 0));

    // if points are empty it means that client wants to start sync from origin.
    if points.is_empty() {
        state
            .connection
            .send_intersect_found(Point::Origin, tip)
            .await
            .map_err(Error::server)?;

        return Ok(());
    }

    let points = points.into_iter().map(From::from).collect_vec();

    let seq = state.wal.find_intersect(&points).map_err(Error::server)?;

    if let Some((seq, point)) = seq {
        info!(?point, "found intersect point");

        state.cursor = state
            .wal
            .crawl_from(Some(seq))
            .map_err(Error::server)?
            .into();

        state
            .connection
            .send_intersect_found(point.into(), tip)
            .await
            .map_err(Error::server)
    } else {
        info!("could not intersect");

        state.cursor = None;

        state
            .connection
            .send_intersect_not_found(tip)
            .await
            .map_err(Error::server)
    }
}

async fn process_request(state: &mut State<'_>, req: ClientRequest) -> Result<(), Error> {
    match req {
        ClientRequest::Intersect(points) => handle_intersect(state, points).await,
        ClientRequest::RequestNext => handle_next_request(state).await,
    }
}

#[instrument(skip_all)]
pub async fn handle_session(wal: WalStore, connection: N2NServer) -> Result<(), Error> {
    let mut state = State {
        wal,
        connection,
        cursor: None,
    };

    while let Some(req) = state
        .connection
        .recv_while_idle()
        .await
        .map_err(Error::server)?
    {
        process_request(&mut state, req)
            .await
            .map_err(Error::server)?;
    }

    info!("client ended protocol");

    Ok(())
}
