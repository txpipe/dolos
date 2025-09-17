use dolos_core::crawl::ChainCrawler;
use itertools::*;
use pallas::network::miniprotocols::{
    chainsync::{ClientRequest, N2NServer, Tip},
    Point,
};
use tracing::{debug, info};

use crate::prelude::*;

pub struct Session<D: Domain> {
    domain: D,
    crawler: Option<ChainCrawler<D>>,
    is_new_intersection: bool,
    connection: N2NServer,
}

impl<D: Domain> Session<D> {
    fn assume_crawler(&mut self) -> &mut ChainCrawler<D> {
        if let Some(crawler) = self.crawler.as_mut() {
            crawler
        } else {
            unreachable!("crawler should be set");
        }
    }

    fn prepare_tip(&mut self) -> Result<Tip, Error> {
        let point = self
            .assume_crawler()
            .find_tip()
            .map_err(Error::server)?
            .unwrap_or(ChainPoint::Origin);

        let point = Point::try_from(point).map_err(|_| Error::custom("invalid point"))?;

        Ok(Tip(point, 0))
    }

    async fn send_intersect_found(&mut self, point: ChainPoint) -> Result<(), Error> {
        debug!("sending intersection found");

        self.is_new_intersection = true;

        let tip = self.prepare_tip()?;

        let point = Point::try_from(point).map_err(|_| Error::custom("invalid point"))?;

        self.connection
            .send_intersect_found(point, tip)
            .await
            .map_err(Error::server)?;

        Ok(())
    }

    async fn send_await_and_next(&mut self) -> Result<(), Error> {
        self.connection
            .send_await_reply()
            .await
            .map_err(Error::server)?;

        debug!("waiting for tip change notification");

        let crawler = self.assume_crawler();

        let event = crawler.next_tip().await;

        self.send_tip_event(event).await?;

        Ok(())
    }

    async fn send_forward(&mut self, point: ChainPoint, block: RawBlock) -> Result<(), Error> {
        debug!("sending forward event");

        let tip = self.prepare_tip()?;

        let point = Point::try_from(point).map_err(|_| Error::custom("invalid point"))?;

        // Ouroboros chain-sync always starts by sending the intersection point as an
        // initial rollback event. The `is_new_intersection`` flag allows us to track if
        // we have already sent that initial rollback or not
        if self.is_new_intersection {
            self.connection
                .send_roll_backward(point, tip)
                .await
                .map_err(Error::server)?;

            self.is_new_intersection = false;
        } else {
            self.connection
                .send_roll_forward(super::convert::header_cbor_to_chainsync(block)?, tip)
                .await
                .map_err(Error::server)?;
        }

        Ok(())
    }

    async fn send_rollback(&mut self, point: ChainPoint) -> Result<(), Error> {
        debug!("sending rollback event");

        let tip = self.prepare_tip()?;

        let point = Point::try_from(point).map_err(|_| Error::custom("invalid point"))?;

        self.connection
            .send_roll_backward(point, tip)
            .await
            .map_err(Error::server)
    }

    async fn send_tip_event(&mut self, log: TipEvent) -> Result<(), Error> {
        match log {
            TipEvent::Apply(p, b) => self.send_forward(p, b).await,
            TipEvent::Mark(p) => self.send_rollback(p).await,
            // we skip undo events and expect a Mark to come after
            _ => Ok(()),
        }
    }

    async fn handle_next_request(&mut self) -> Result<(), Error> {
        debug!("handling next request");

        let crawler = self.assume_crawler();

        let next = crawler.next_block();

        if let Some((point, block)) = next {
            self.send_forward(point, block).await?;
        } else {
            self.send_await_and_next().await?;
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

        let intersect = ChainCrawler::<D>::start(&self.domain, &points).unwrap();

        if let Some((crawler, point)) = intersect {
            debug!(%point, "found intersect point");
            self.crawler = Some(crawler);
            self.send_intersect_found(point).await
        } else {
            debug!("could not intersect");
            self.crawler = None;

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

pub async fn handle_session<D: Domain, C: CancelToken>(
    domain: D,
    connection: N2NServer,
    cancel: C,
) -> Result<(), ServeError> {
    let mut session = Session {
        domain,
        connection,
        crawler: None,
        is_new_intersection: false,
    };

    tokio::select! {
        _ = session.process_requests() => {
            info!("peer ended protocol");
        },
        _ = cancel.cancelled() => {
            info!("protocol was cancelled");
        }
    }

    Ok(())
}
