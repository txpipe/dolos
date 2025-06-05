use itertools::*;
use pallas::network::miniprotocols::{
    chainsync::{BlockContent, ClientRequest, N2CServer, Tip},
    Point,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::{
    adapters::{WalAdapter, WalIter},
    prelude::*,
};

pub struct Session<'a> {
    wal: WalAdapter,
    current_iterator: Option<WalIter<'a>>,
    is_new_intersection: bool,
    last_known_seq: Option<LogSeq>,
    connection: N2CServer,
}

impl Session<'_> {
    fn prepare_tip(&self) -> Result<Tip, Error> {
        let tip = self
            .wal
            .find_tip()
            .map_err(Error::server)?
            .map(|(_, x)| Tip(x.into(), 0))
            .unwrap_or(Tip(Point::Origin, 0));

        Ok(tip)
    }

    fn restart_iterator(&mut self) -> Result<(), Error> {
        let seq = self
            .last_known_seq
            .as_ref()
            .expect("broken invariant, we should have a last seen seq");

        self.current_iterator = self
            .wal
            .crawl_from(Some(*seq))
            .map_err(Error::server)?
            .into();

        // we need to skip the first item since we're already seen it
        self.current_iterator.as_mut().unwrap().next();

        Ok(())
    }

    async fn send_intersect_found(&mut self, seq: LogSeq, point: ChainPoint) -> Result<(), Error> {
        debug!("sending intersection found");

        self.current_iterator = self
            .wal
            .crawl_from(Some(seq))
            .map_err(Error::server)?
            .into();

        self.is_new_intersection = true;

        let tip = self.prepare_tip()?;

        self.connection
            .send_intersect_found(point.into(), tip)
            .await
            .map_err(Error::server)?;

        Ok(())
    }

    fn read_next_wal(&mut self) -> Result<Option<LogEntry>, Error> {
        let next = self
            .current_iterator
            .as_mut()
            .ok_or(Error::custom("requesting next without intersection"))?
            // filter forward will just gives us Apply & Mark events. We can just skip Undo events
            // in Ouroboros since they are not required by the protocol.
            .filter_forward()
            .next();

        // since iterators might get exhausted, we need to keep our internal state of
        // how far we're in the WAL sequence in case we need to recreate a new
        // iterator from where we left of.
        if let Some((seen, _)) = &next {
            self.last_known_seq = Some(*seen);
        }

        Ok(next)
    }

    async fn wait_for_next_wal(&mut self) -> Result<LogEntry, Error> {
        loop {
            self.wal.tip_change().await;

            self.restart_iterator()?;

            if let Some(next) = self.read_next_wal()? {
                break Ok(next);
            }
        }
    }

    async fn send_forward(&mut self, block: RawBlock) -> Result<(), Error> {
        debug!("sending forward event");

        let tip = self.prepare_tip()?;

        // Ouroboros chain-sync always starts by sending the intersection point as an
        // initial rollback event. The `is_new_intersection`` flag allows us to track if
        // we have already sent that initial rollback or not
        if self.is_new_intersection {
            self.connection
                .send_roll_backward(Point::from(ChainPoint::from(&block)), tip)
                .await
                .map_err(Error::server)?;

            self.is_new_intersection = false;
        } else {
            self.connection
                .send_roll_forward(BlockContent(block.body), tip)
                .await
                .map_err(Error::server)?;
        }

        Ok(())
    }

    async fn send_rollback(&mut self, point: ChainPoint) -> Result<(), Error> {
        debug!("sending rollback event");

        let tip = self.prepare_tip()?;

        self.connection
            .send_roll_backward(Point::from(point), tip)
            .await
            .map_err(Error::server)
    }

    async fn send_next_wal(&mut self, log: LogValue) -> Result<(), Error> {
        match log {
            LogValue::Apply(x) => self.send_forward(x).await,
            LogValue::Mark(x) => self.send_rollback(x).await,
            // any other type of events should be already filtered by the `filter_forward`
            // predicate. We consider any other variant unreachable.
            _ => unreachable!(),
        }
    }

    async fn handle_next_request(&mut self) -> Result<(), Error> {
        debug!("handling next request");

        let next = self.read_next_wal()?;

        if let Some((_, log)) = next {
            self.send_next_wal(log).await?;
        } else {
            self.connection
                .send_await_reply()
                .await
                .map_err(Error::server)?;

            debug!("waiting for tip change notification");

            let (_, log) = self.wait_for_next_wal().await?;

            self.send_next_wal(log).await?;
        }

        Ok(())
    }

    async fn handle_intersect(&mut self, mut points: Vec<Point>) -> Result<(), Error> {
        debug!(?points, "handling intersect request");

        let tip = self.prepare_tip()?;

        // TODO: if points are empty it means that client wants to start sync from
        // origin?.
        if points.is_empty() {
            debug!("intersect candidates empty, using origin");
            points.push(Point::Origin);
        }

        let points = points.into_iter().map(From::from).collect_vec();

        let seq = self.wal.find_intersect(&points).map_err(Error::server)?;

        if let Some((seq, point)) = seq {
            debug!(?point, "found intersect point");
            self.send_intersect_found(seq, point).await
        } else {
            debug!("could not intersect");

            self.current_iterator = None;

            self.connection
                .send_intersect_not_found(tip)
                .await
                .map_err(Error::server)
        }
    }

    async fn process_requests(&mut self) -> Result<(), Error> {
        while let Some(req) = self
            .connection
            .recv_while_idle()
            .await
            .map_err(Error::server)?
        {
            let result = match req {
                ClientRequest::Intersect(points) => self.handle_intersect(points).await,
                ClientRequest::RequestNext => self.handle_next_request().await,
            };

            result.map_err(Error::server)?;
        }

        Ok(())
    }
}

pub async fn handle_session(
    wal: WalAdapter,
    connection: N2CServer,
    cancel: CancellationToken,
) -> Result<(), Error> {
    let mut session = Session {
        wal,
        connection,
        current_iterator: None,
        last_known_seq: None,
        is_new_intersection: false,
    };

    tokio::select! {
        _ = session.process_requests() => {
            info!("client ended protocol");
        },
        _ = cancel.cancelled() => {
            info!("protocol was cancelled");
        }
    }

    Ok(())
}
