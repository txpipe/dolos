use async_recursion::async_recursion;
use pallas::{
    crypto::hash::Hash,
    ledger::traverse::MultiEraBlock,
    network::miniprotocols::{
        chainsync::{self, ClientRequest, HeaderContent, Tip},
        Point,
    },
};
use tracing::{debug, info, instrument, warn};

use crate::{
    prelude::{BlockHash, BlockSlot, Error},
    storage::rolldb::{wal::WalAction, RollDB},
};

pub struct N2NChainSyncHandler {
    roll_db: RollDB,
    protocol: chainsync::N2NServer,
    intersect: Option<Point>,
    cursor: Option<(u64, Hash<32>)>,
}

impl N2NChainSyncHandler {
    pub fn new(roll_db: RollDB, protocol: chainsync::N2NServer) -> Result<Self, Error> {
        Ok(Self {
            roll_db,
            protocol,
            intersect: None,
            cursor: None,
        })
    }

    #[instrument(skip_all)]
    pub async fn begin(&mut self) -> Result<(), Error> {
        info!("beginning n2n chainsync handler");
        match self
            .protocol
            .recv_while_idle()
            .await
            .map_err(Error::server)?
        {
            Some(ClientRequest::Intersect(points)) => self.handle_intersect(points).await,
            Some(ClientRequest::RequestNext) => self.handle_crawling(None).await,
            None => {
                debug!("client ended protocol");
                return Ok(());
            }
        }
    }

    // TODO: loop instead?
    #[async_recursion]
    #[instrument(skip_all)]
    async fn handle_intersect(&mut self, points: Vec<Point>) -> Result<(), Error> {
        info!(?points, "handling intersect request");

        let tip = self
            .roll_db
            .find_tip()
            .map_err(Error::server)?
            .map(db_tip_to_protocol)
            .unwrap_or(Tip(Point::Origin, 0));

        if let Some(found) = self.find_valid_intersection(&points) {
            info!(?found, "found intersect point");

            self.intersect = Some(found.clone());
            self.cursor = None;

            self.protocol
                .send_intersect_found(found, tip)
                .await
                .map_err(Error::server)?;
        } else {
            warn!("could not intersect");

            self.intersect = None;
            self.cursor = None;

            self.protocol
                .send_intersect_not_found(tip)
                .await
                .map_err(Error::server)?;
        }

        // ---

        match self
            .protocol
            .recv_while_idle()
            .await
            .map_err(Error::server)?
        {
            Some(ClientRequest::Intersect(points)) => return self.handle_intersect(points).await,
            Some(ClientRequest::RequestNext) => {
                return self
                    .handle_crawling(self.intersect.as_ref().map(|x| x.slot_or_default()))
                    .await
            }
            None => {
                debug!("client ended protocol");
                return Ok(());
            }
        }
    }

    #[instrument(skip_all)]
    async fn handle_crawling(&mut self, from: Option<u64>) -> Result<(), Error> {
        info!(?from, "entering chainkv crawling mode");

        // --- initialise new crawler

        let mut crawler = self.roll_db.crawl_chain_from(from);

        let tip = self
            .roll_db
            .find_tip()
            .map_err(Error::server)?
            .map(db_tip_to_protocol)
            .unwrap_or(Tip(Point::Origin, 0));

        let mutable_slot = tip
            .0
            .slot_or_default()
            .saturating_sub(self.roll_db.k_param());

        info!(?tip, ?mutable_slot, "fetched tip from db");

        // --- keep sending blocks while we receive RequestNexts

        loop {
            if let Some(next) = crawler.next() {
                debug!(?next, "next chainkv point");
                let (slot, hash) = next.map_err(Error::server)?;

                let tip = self
                    .roll_db
                    .find_tip()
                    .map_err(Error::server)?
                    .map(db_tip_to_protocol)
                    .unwrap_or(Tip(Point::Origin, 0));

                // --- if we have an intersect and this is the first response
                // (no cursor) then our first response is a rollback to the intersect
                if self.intersect.is_some() && self.cursor.is_none() {
                    info!("sending initial rollbackwards message");
                    self.protocol
                        .send_roll_backward(Point::Specific(slot, hash.to_vec()), tip)
                        .await
                        .map_err(Error::server)?;

                    self.cursor = Some((slot, hash));
                } else {
                    // --- if we have reached mutable part of chainKV snapshot,
                    // check if we can swap to the WAL, otherwise take new snapshot

                    if slot >= mutable_slot {
                        if let Some((slot, hash)) = self.cursor {
                            if self
                                .roll_db
                                .wal_contains(slot, &hash)
                                .map_err(Error::server)?
                            {
                                info!(?self.cursor, "cursor found on WAL, switching to WAL crawling");
                                drop(crawler);
                                return self.crawl_with_wal(self.cursor).await;
                            } else {
                                info!(?self.cursor, "mutable but no WAL intersect, refreshing chainKV crawler");

                                // take new chainKV snapshot
                                crawler = self.roll_db.crawl_chain_from(self.cursor.map(|x| x.0));

                                // skip cursor (iterator starts at cursor)
                                crawler.next();

                                continue;
                            }
                        } else {
                            // if we are immediately mutable (have no cursor),
                            // skip chainKV and crawl WAL from beginning

                            info!(?self.cursor, "mutable without cursor, switching to WAL crawling");

                            drop(crawler);
                            return self.crawl_with_wal(None).await;
                        }
                    }

                    // --- send block to client

                    let block = self
                        .roll_db
                        .get_block(hash)
                        .map_err(Error::server)?
                        .expect("block content not found");

                    let block = MultiEraBlock::decode(&block).expect("invalid block cbor");

                    let content = HeaderContent {
                        variant: 1, // TODO
                        byron_prefix: None,
                        cbor: block.header().cbor().to_vec(),
                    };

                    self.protocol
                        .send_roll_forward(content, tip)
                        .await
                        .map_err(Error::server)?;

                    self.cursor = Some((slot, hash));
                }

                // ---

                match self
                    .protocol
                    .recv_while_idle()
                    .await
                    .map_err(Error::server)?
                {
                    Some(ClientRequest::RequestNext) => debug!("client request next"),
                    Some(ClientRequest::Intersect(points)) => {
                        drop(crawler);
                        return self.handle_intersect(points).await;
                    }
                    None => {
                        debug!("client ended protocol");
                        return Ok(());
                    }
                }
            } else {
                return Err(Error::server(
                    "chainKV exhausted without finding WAL intersection",
                ));
            }
        }
    }

    #[instrument(skip_all)]
    async fn crawl_with_wal(&mut self, from: Option<(BlockSlot, BlockHash)>) -> Result<(), Error> {
        info!(?from, "entering WAL crawling mode");

        // TODO: race condition between checking wal contains point and creating iterator
        let mut crawler = self
            .roll_db
            .crawl_wal_from_cursor(from)
            .map_err(Error::server)?
            .ok_or(Error::server("point not found in wal"))?;

        // skip the WAL intersect
        crawler.next();

        // --- keep iterating WAL while we receive RequestNexts

        loop {
            if let Some(next) = crawler.next() {
                debug!(?next, "next WAL entry");
                let (_, wal_value) = next.map_err(Error::server)?;

                let tip = self
                    .roll_db
                    .find_tip()
                    .map_err(Error::server)?
                    .map(db_tip_to_protocol)
                    .unwrap_or(Tip(Point::Origin, 0));

                // ---

                let slot = wal_value.slot();
                let hash = *wal_value.hash();

                match wal_value.action() {
                    WalAction::Apply => {
                        let block = self
                            .roll_db
                            .get_block(hash)
                            .map_err(Error::server)?
                            .expect("block content not found");

                        let block = MultiEraBlock::decode(&block).expect("invalid block cbor");

                        let content = HeaderContent {
                            variant: 1, // TODO
                            byron_prefix: None,
                            cbor: block.header().cbor().to_vec(),
                        };

                        self.protocol
                            .send_roll_forward(content, tip)
                            .await
                            .map_err(Error::server)?;

                        self.cursor = Some((slot, hash));
                    }
                    WalAction::Mark => {
                        self.protocol
                            .send_roll_backward(Point::Specific(slot, hash.to_vec()), tip)
                            .await
                            .map_err(Error::server)?;

                        self.cursor = Some((slot, hash));
                    }
                    // skip this wal action without trying to receive a new message
                    WalAction::Undo => continue,
                };
            } else {
                info!(?self.cursor, "sending await reply");

                self.protocol
                    .send_await_reply()
                    .await
                    .map_err(Error::server)?;

                // TODO wait for new WAL entries
            }

            // ---

            match self
                .protocol
                .recv_while_idle()
                .await
                .map_err(Error::server)?
            {
                Some(ClientRequest::RequestNext) => debug!("client request next"),
                Some(ClientRequest::Intersect(points)) => {
                    drop(crawler);
                    return self.handle_intersect(points).await;
                }
                None => {
                    debug!("client ended protocol");
                    return Ok(());
                }
            }
        }
    }

    fn find_valid_intersection(&self, points: &[Point]) -> Option<Point> {
        for point in points {
            match point {
                Point::Origin => return Some(point.clone()),
                Point::Specific(slot, hash) => {
                    let hash: [u8; 32] = hash[0..32].try_into().unwrap();
                    let hash = Hash::<32>::from(hash);

                    if self.roll_db.chain_contains(*slot, &hash).unwrap() {
                        return Some(point.clone());
                    }
                }
            }
        }

        None
    }
}

fn db_tip_to_protocol(tip: (u64, Hash<32>)) -> Tip {
    // TODO: get block height from db
    Tip(Point::Specific(tip.0, tip.1.to_vec()), 0)
}
