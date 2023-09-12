use pallas::crypto::hash::Hash;
use pallas::ledger::traverse::MultiEraBlock;
use pallas::network::facades::PeerServer;
use pallas::network::miniprotocols::blockfetch::{self, BlockRequest};
use pallas::network::miniprotocols::chainsync::{HeaderContent, Tip};
use pallas::network::miniprotocols::{chainsync, Point};
use serde::{Deserialize, Serialize};
use tokio::join;
use tokio::net::TcpListener;

use tracing::{debug, error, info, instrument, warn};

use crate::prelude::*;
use crate::storage::rolldb::RollDB;

#[cfg(test)]
mod tests;

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    listen_address: String,
    magic: u64,
}

async fn handle_blockfetch(db: RollDB, mut protocol: blockfetch::Server) -> Result<(), Error> {
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

fn db_tip_to_protocol(tip: (u64, Hash<32>)) -> Tip {
    // TODO: get block height from db
    Tip(Point::Specific(tip.0, tip.1.to_vec()), 0)
}

#[instrument(skip_all)]
async fn handle_chainsync(db: RollDB, mut protocol: chainsync::N2NServer) -> Result<(), Error> {
    let mut intersect = db.crawl_chain_from(None);

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
                        intersect = match found {
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
                    let next = intersect.next();

                    if let Some(next) = next {
                        let (_, hash) = next.map_err(Error::server)?;

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
                    } else {
                        protocol.send_await_reply().await.map_err(Error::server)?;
                    }
                }
            },
            None => todo!(),
        }
    }
}

#[instrument(skip_all)]
async fn peer_session(db: RollDB, peer: PeerServer) -> Result<(), Error> {
    let PeerServer {
        blockfetch,
        chainsync,
        plexer_handle,
        ..
    } = peer;

    let l1 = handle_chainsync(db.clone(), chainsync);
    let l2 = handle_blockfetch(db.clone(), blockfetch);

    join!(l1, l2);

    plexer_handle.abort();

    Ok(())
}

#[instrument(skip_all)]
pub async fn serve(config: Config, db: RollDB) -> Result<(), Error> {
    let listener = TcpListener::bind(&config.listen_address)
        .await
        .map_err(Error::server)?;

    info!(addr = &config.listen_address, "ouroboros listening");

    loop {
        let peer = PeerServer::accept(&listener, config.magic)
            .await
            .map_err(Error::server)?;

        info!("accepted incoming connection");

        let db = db.clone();

        let handle = tokio::spawn(async move { peer_session(db, peer).await });
    }
}
