use async_recursion::async_recursion;
use pallas::{
    crypto::hash::Hash,
    ledger::traverse::MultiEraBlock,
    network::miniprotocols::{
        chainsync::{self, ClientRequest, HeaderContent, Tip},
        Point,
    },
    storage::rolldb::{chain, wal},
};
use tracing::{debug, info, instrument, warn};

use crate::prelude::Error;

pub struct N2NChainSyncHandler {
    chain: chain::Store,
    wal: wal::Store,
    protocol: chainsync::N2NServer,
    intersect: Option<Point>,
    cursor: Option<(u64, Hash<32>)>,
}

impl N2NChainSyncHandler {
    pub fn new(
        chain: chain::Store,
        wal: wal::Store,
        protocol: chainsync::N2NServer,
    ) -> Result<Self, Error> {
        Ok(Self {
            chain,
            wal,
            protocol,
            intersect: None,
            cursor: None,
        })
    }

    async fn find_tip(&self) -> Result<(Tip, u64), Error> {
        let tip = self
            .wal
            .find_tip()
            .map_err(Error::server)?
            .map(db_tip_to_protocol)
            .unwrap_or(Tip(Point::Origin, 0));

        // HACK: 200 is a magic number to use as security margin to avoid trying to jump
        // from chain to wal right at the boundary which might cause race conditions.
        // TODO: rather that using a magic number, lets switch to reading what's the 1st
        // WAL entry that we have available, but only if we're at a reasonable proximity
        // to the WAL.
        let mutable_boundary = tip.0.slot_or_default().saturating_sub(200);

        Ok((tip, mutable_boundary))
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
            .wal
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

        let mut crawler = self.chain.crawl_after(from);

        let mut tip = self.find_tip().await?;

        info!(tip=?tip.0, mutable_slot=tip.1, "fetched tip from db");

        // --- keep sending blocks while we receive RequestNexts

        // if we intersected with crawler with a point then skip that point
        if matches!(self.intersect.as_ref(), Some(Point::Specific(_, _))) {
            crawler.next();
        }

        loop {
            if let Some(next) = crawler.next() {
                info!(?next, "next chainkv point");
                let (slot, hash) = next.map_err(Error::server)?;

                tip = self.find_tip().await?;

                // --- if we have reached mutable part of chainKV snapshot,
                // check if we can swap to the WAL, otherwise take new snapshot

                if slot >= tip.1 {
                    if let Some((slot, hash)) = self.cursor {
                        if let Some(seq) = self
                            .wal
                            .find_wal_seq(&[(slot, hash)])
                            .map_err(Error::server)?
                        {
                            info!(?self.cursor, "cursor found on WAL, switching to WAL crawling");
                            drop(crawler);
                            return self.crawl_with_wal(Some(seq)).await;
                        } else {
                            info!(?self.cursor, "mutable but no WAL intersect, refreshing chainKV crawler");

                            // take new chainKV snapshot
                            crawler = self.chain.crawl_after(self.cursor.map(|x| x.0));

                            // skip cursor (iterator starts at cursor)
                            crawler.next();

                            continue;
                        }
                    } else {
                        // if we are immediately mutable (have no cursor),
                        // skip chainKV and crawl WAL from beginning

                        info!(?self.cursor, "mutable without cursor, switching to WAL crawling");

                        drop(crawler);

                        if let Some(Point::Specific(i_slot, i_hash)) = self.intersect.as_ref() {
                            let i_hash: [u8; 32] = i_hash.clone().try_into().unwrap();

                            let seq = self
                                .wal
                                .find_wal_seq(&[(*i_slot, i_hash.into())])
                                .map_err(Error::server)?
                                .ok_or(Error::server("intersect in chainkv but not WAL despite being immediately mutable"))?;

                            return self.crawl_with_wal(Some(seq)).await;
                        } else {
                            return self.crawl_with_wal(None).await;
                        }
                    }
                }

                // --- send block to client

                let block = self
                    .chain
                    .get_block(hash)
                    .map_err(Error::server)?
                    .expect("block content not found");

                let block = MultiEraBlock::decode(&block).expect("invalid block cbor");

                let content = HeaderContent {
                    variant: block.era() as u8,
                    byron_prefix: match block.era() {
                        pallas::ledger::traverse::Era::Byron => Some((1, 0)),
                        _ => None,
                    },
                    cbor: block.header().cbor().to_vec(),
                };

                self.protocol
                    .send_roll_forward(content, tip.0)
                    .await
                    .map_err(Error::server)?;

                self.cursor = Some((slot, hash));

                // ---

                match self
                    .protocol
                    .recv_while_idle()
                    .await
                    .map_err(Error::server)?
                {
                    Some(ClientRequest::RequestNext) => info!("client request next"),
                    Some(ClientRequest::Intersect(points)) => {
                        drop(crawler);
                        return self.handle_intersect(points).await;
                    }
                    None => {
                        warn!("client ended protocol");
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
    async fn crawl_with_wal(&mut self, from: Option<u64>) -> Result<(), Error> {
        info!(?from, "entering WAL crawling mode");

        let mut last_seq = None;

        let intersected = from.is_some();

        // TODO: race condition between checking wal contains point and creating
        // iterator
        let mut crawler = self.wal.crawl_after(from);

        // skip the WAL intersect
        if intersected {
            crawler.next();
        }

        // --- keep iterating WAL while we receive RequestNexts

        loop {
            if let Some(next) = crawler.next() {
                let (seq, wal_value) = next.map_err(Error::server)?;
                info!(seq, "next WAL entry");

                last_seq = Some(seq);

                let tip = self
                    .wal
                    .find_tip()
                    .map_err(Error::server)?
                    .map(db_tip_to_protocol)
                    .unwrap_or(Tip(Point::Origin, 0));

                // ---

                match wal_value {
                    wal::Log::Apply(slot, hash, block) => {
                        let block = MultiEraBlock::decode(&block).expect("invalid block cbor");

                        let content = HeaderContent {
                            variant: block.era() as u8,
                            byron_prefix: None,
                            cbor: block.header().cbor().to_vec(),
                        };

                        self.protocol
                            .send_roll_forward(content, tip)
                            .await
                            .map_err(Error::server)?;

                        self.cursor = Some((slot, hash));
                    }
                    wal::Log::Mark(slot, hash, _) => {
                        self.protocol
                            .send_roll_backward(Point::Specific(slot, hash.to_vec()), tip)
                            .await
                            .map_err(Error::server)?;

                        self.cursor = Some((slot, hash));
                    }
                    // skip this wal action without trying to receive a new message
                    wal::Log::Undo(..) => continue,
                    // skip this wal action without trying to receive a new message
                    wal::Log::Origin => continue,
                };
            } else {
                info!(?self.cursor, "sending await reply");

                self.protocol
                    .send_await_reply()
                    .await
                    .map_err(Error::server)?;

                self.wal.tip_change.notified().await;
                info!(?last_seq, "tip change notified, refreshing WAL crawler");
                drop(crawler);
                crawler = self.wal.crawl_after(last_seq);
                crawler.next();

                continue;
            }

            // ---

            match self
                .protocol
                .recv_while_idle()
                .await
                .map_err(Error::server)?
            {
                Some(ClientRequest::RequestNext) => info!("client request next"),
                Some(ClientRequest::Intersect(points)) => {
                    drop(crawler);
                    return self.handle_intersect(points).await;
                }
                None => {
                    warn!("client ended protocol");
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

                    if self.chain.chain_contains(*slot, &hash).unwrap() {
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
