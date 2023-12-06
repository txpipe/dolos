use pallas::{
    crypto::hash::Hash,
    ledger::traverse::MultiEraBlock,
    network::miniprotocols::{
        blockfetch::{self, BlockRequest},
        chainsync::{self, HeaderContent, Tip},
        Point,
    },
};
use tracing::{debug, error, info, instrument, warn};

use crate::{prelude::Error, storage::rolldb::RollDB};

// blockfetch

pub async fn handle_blockfetch(db: RollDB, mut protocol: blockfetch::Server) -> Result<(), Error> {
    loop {
        match protocol.recv_while_idle().await {
            Ok(Some(BlockRequest((p1, p2)))) => {
                let from = match p1 {
                    Point::Origin => None,
                    Point::Specific(slot, hash) => {
                        let parsed_hash = TryInto::<[u8; 32]>::try_into(hash)
                            .map_err(|_| Error::client("malformed hash"))?
                            .into();

                        Some((slot, parsed_hash))
                    }
                };

                let to = match p2 {
                    Point::Origin => return protocol.send_no_blocks().await.map_err(Error::server),
                    Point::Specific(slot, hash) => {
                        let parsed_hash = TryInto::<[u8; 32]>::try_into(hash)
                            .map_err(|_| Error::client("malformed hash"))?
                            .into();

                        (slot, parsed_hash)
                    }
                };

                if let Some(mut iter) = db.read_chain_range(from, to).map_err(Error::storage)? {
                    protocol.send_start_batch().await.map_err(Error::server)?;

                    while let Some(point) = iter.next() {
                        let (_, hash) = point.map_err(Error::storage)?;

                        let block_bytes = match db.get_block(hash).map_err(Error::storage)? {
                            Some(b) => b,
                            None => {
                                error!("could not find block bytes for {hash}");
                                return Err(Error::server(
                                    "could not find block bytes for block in chainkv",
                                ));
                            }
                        };

                        protocol
                            .send_block(block_bytes)
                            .await
                            .map_err(Error::server)?;
                    }

                    protocol.send_batch_done().await.map_err(Error::server)?;
                } else {
                    return protocol.send_no_blocks().await.map_err(Error::server);
                }
            }
            Ok(None) => info!("peer ended blockfetch protocol"),
            Err(e) => {
                warn!("error receiving blockfetch message: {:?}", e);
                return Err(Error::client(e));
            }
        }
    }
}

fn find_valid_intersection(db: &RollDB, points: &[Point]) -> Option<Point> {
    for point in points {
        match point {
            Point::Origin => return Some(point.clone()),
            Point::Specific(slot, hash) => {
                let hash: [u8; 32] = hash[0..32].try_into().unwrap();
                let hash = Hash::<32>::from(hash);

                if db.chain_contains(*slot, &hash).unwrap() {
                    return Some(point.clone());
                }
            }
        }
    }

    None
}

// chainsync

fn db_tip_to_protocol(tip: (u64, Hash<32>)) -> Tip {
    // TODO: get block height from db
    Tip(Point::Specific(tip.0, tip.1.to_vec()), 0)
}

#[instrument(skip_all)]
pub async fn handle_n2n_chainsync(
    db: RollDB,
    mut protocol: chainsync::N2NServer,
) -> Result<(), Error> {
    let mut crawler = db.crawl_chain_from(None);
    let mut cursor = None;

    loop {
        debug!("waiting for request");

        let req = protocol.recv_while_idle().await.map_err(Error::server)?;

        debug!("new client request");

        let tip = db
            .find_tip()
            .map_err(Error::server)?
            .map(db_tip_to_protocol)
            .unwrap_or(Tip(Point::Origin, 0));

        debug!(?tip, "fetched tip from db");

        match req {
            Some(x) => match x {
                chainsync::ClientRequest::Intersect(points) => {
                    debug!(?points, "intersect request");

                    if let Some(found) = find_valid_intersection(&db, &points) {
                        crawler = match found {
                            Point::Origin => db.crawl_chain_from(None),
                            Point::Specific(x, _) => db.crawl_chain_from(Some(x)),
                        };

                        protocol
                            .send_intersect_found(found, tip)
                            .await
                            .map_err(Error::server)?;
                    } else {
                        protocol
                            .send_intersect_not_found(tip)
                            .await
                            .map_err(Error::server)?;
                    }
                }
                chainsync::ClientRequest::RequestNext => {
                    let next = crawler.next();

                    if let Some(next) = next {
                        let (slot, hash) = next.map_err(Error::server)?;

                        // if cursor is none, we need to send a rollback to the intersect point,
                        // which is the first point returned by the interator
                        if cursor.is_none() {
                            protocol
                                .send_roll_backward(Point::Specific(slot, hash.to_vec()), tip)
                                .await
                                .map_err(Error::server)?;

                            cursor = Some((slot, hash));

                            continue;
                        }

                        let block = db
                            .get_block(hash)
                            .map_err(Error::server)?
                            .expect("block content not found");

                        let block = MultiEraBlock::decode(&block).expect("invalid block cbor");

                        let content = HeaderContent {
                            variant: 1, // TODO
                            byron_prefix: None,
                            cbor: block.header().cbor().to_vec(),
                        };

                        protocol
                            .send_roll_forward(content, tip)
                            .await
                            .map_err(Error::server)?;

                        cursor = Some((slot, hash));
                    } else {
                        protocol.send_await_reply().await.map_err(Error::server)?;
                    }
                }
            },
            None => {
                info!("client ended chainsync miniprotocol");
                return Ok(());
            }
        }
    }
}
