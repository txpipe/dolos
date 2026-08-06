use dolos_core::crawl::ChainCrawler;
use itertools::*;
use pallas::network::miniprotocols::{
    chainsync::{BlockContent, ClientRequest, N2CServer, Tip},
    Point,
};
use tracing::{debug, info, warn};

use crate::prelude::*;

pub struct Session<D: Domain> {
    domain: D,
    crawler: Option<ChainCrawler<D>>,
    pending_rollback: Option<ChainPoint>,
    connection: N2CServer,
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
            .domain
            .wal()
            .find_tip()
            .map_err(Error::server)?
            .map(|(point, _)| point)
            .unwrap_or(ChainPoint::Origin);

        let point = Point::try_from(point).map_err(|_| Error::custom("invalid point"))?;

        Ok(Tip(point, 0))
    }

    async fn send_intersect_found(&mut self, point: ChainPoint) -> Result<(), Error> {
        debug!("sending intersection found");

        // chain-sync always starts by rolling the client back to the
        // negotiated intersection before any forward event
        self.pending_rollback = Some(point.clone());

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

        // after an await-reply the client is owed exactly one chain-sync
        // message; undo events have no wire counterpart (the mark that
        // follows them becomes the rollback), so keep waiting through them
        loop {
            let event = self.assume_crawler().next_tip().await;

            match event {
                TipEvent::Apply(_, block) => return self.send_forward(block).await,
                TipEvent::Mark(point) => return self.send_rollback(point).await,
                _ => continue,
            }
        }
    }

    async fn send_forward(&mut self, block: RawBlock) -> Result<(), Error> {
        debug!("sending forward event");

        let tip = self.prepare_tip()?;

        self.connection
            .send_roll_forward(BlockContent((*block).clone()), tip)
            .await
            .map_err(Error::server)?;

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

    async fn handle_next_request(&mut self) -> Result<(), Error> {
        debug!("handling next request");

        // the rollback to the negotiated intersection is the reply to the
        // first next-request; no block is consumed by it
        if let Some(point) = self.pending_rollback.take() {
            self.send_rollback(point).await?;
            return Ok(());
        }

        let crawler = self.assume_crawler();

        let next = crawler.next_block();

        if let Some((_, block)) = next {
            self.send_forward(block).await?;
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
    connection: N2CServer,
    cancel: C,
) -> Result<(), ServeError> {
    let mut session = Session {
        domain,
        connection,
        crawler: None,
        pending_rollback: None,
    };

    tokio::select! {
        result = session.process_requests() => {
            if let Err(e) = result {
                warn!(?e, "chainsync session error");
                return Err(ServeError::Internal(e.into()));
            }
            info!("client ended protocol");
        },
        _ = cancel.cancelled() => {
            info!("protocol was cancelled");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio_util::sync::CancellationToken;

    use dolos_core::ChainPoint;
    use dolos_testing::blocks::make_conway_block;
    use dolos_testing::slot_to_chainpoint;
    use dolos_testing::toy_domain::ToyDomain;

    use pallas::network::facades::{NodeClient, NodeServer};
    use pallas::network::miniprotocols::chainsync::NextResponse;

    use crate::serve::CancelTokenImpl;

    fn spawn_server(
        domain: ToyDomain,
        listener: tokio::net::UnixListener,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let connection = NodeServer::accept(&listener, 0).await.unwrap();

            let NodeServer {
                plexer, chainsync, ..
            } = connection;

            let cancel = CancelTokenImpl(CancellationToken::new());

            handle_session(domain, chainsync, cancel).await.unwrap();

            plexer.abort().await;
        })
    }

    fn seed_archive_blocks(
        domain: &ToyDomain,
        slots: std::ops::Range<u64>,
    ) -> Vec<(ChainPoint, Vec<u8>)> {
        let writer = domain.archive().start_writer().unwrap();

        let mut blocks = vec![];

        for slot in slots {
            let (point, block) = make_conway_block(slot);
            writer.apply(&point, &block).unwrap();
            blocks.push((point, (*block).clone()));
        }

        writer.commit().unwrap();

        blocks
    }

    #[tokio::test]
    async fn chainsync_starts_with_rollback_to_intersection() {
        let domain = ToyDomain::new(None, None);

        let tempdir = tempfile::tempdir().unwrap();
        let socket = tempdir.path().join("node.socket");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();

        let server = spawn_server(domain.clone(), listener);

        let mut client = NodeClient::connect(&socket, 0).await.unwrap();

        let (point, _) = client
            .chainsync()
            .find_intersect(vec![Point::Origin])
            .await
            .unwrap();

        assert_eq!(point, Some(Point::Origin));

        // the reply to the first next-request is the rollback to the
        // negotiated intersection
        let next = client.chainsync().request_next().await.unwrap();
        assert!(matches!(next, NextResponse::RollBackward(Point::Origin, _)));

        // an empty chain then awaits at tip; undo events carry no chain-sync
        // message of their own, so the reply must come from the mark that
        // follows them
        let next = client.chainsync().request_next().await.unwrap();
        assert!(matches!(next, NextResponse::Await));

        let (undone_point, undone_block) = make_conway_block(9);
        domain.notify_tip(dolos_core::TipEvent::Undo(undone_point, undone_block));
        domain.notify_tip(dolos_core::TipEvent::Mark(slot_to_chainpoint(7)));

        let next = client.chainsync().recv_while_must_reply().await.unwrap();
        assert!(matches!(next, NextResponse::RollBackward(_, _)));

        client.chainsync().send_done().await.unwrap();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn chainsync_streams_archive_blocks_without_loss() {
        let domain = ToyDomain::new(None, None);

        let blocks = seed_archive_blocks(&domain, 1..15);

        let tempdir = tempfile::tempdir().unwrap();
        let socket = tempdir.path().join("node.socket");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();

        let server = spawn_server(domain.clone(), listener);

        let mut client = NodeClient::connect(&socket, 0).await.unwrap();

        let intersect = Point::try_from(blocks[0].0.clone()).unwrap();

        let (point, _) = client
            .chainsync()
            .find_intersect(vec![intersect.clone()])
            .await
            .unwrap();

        assert_eq!(point, Some(intersect.clone()));

        let next = client.chainsync().request_next().await.unwrap();
        match next {
            NextResponse::RollBackward(p, _) => assert_eq!(p, intersect),
            other => panic!("expected initial rollback, got {other:?}"),
        }

        // every block after the intersection must arrive, in order, starting
        // right after the intersection point and across the internal batch
        // boundary
        for (_, expected) in &blocks[1..13] {
            let next = client.chainsync().request_next().await.unwrap();
            match next {
                NextResponse::RollForward(content, _) => assert_eq!(&content.0, expected),
                other => panic!("expected roll forward, got {other:?}"),
            }
        }

        client.chainsync().send_done().await.unwrap();

        server.await.unwrap();
    }
}
