use pallas::network::miniprotocols::blockfetch::BlockRequest;
use pallas::network::miniprotocols::Point;
use pallas::{network::facades::PeerServer, storage::rolldb::chain};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

use tracing::{error, info, warn};

use crate::prelude::*;

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    listen_path: Option<String>,
    listen_address: Option<String>,
    allow_n2c_over_tcp: Option<bool>,
    magic: u64,
}

pub async fn serve(config: Config, store: chain::Store) -> Result<(), Error> {
    if let Some(addr) = config.listen_address {
        info!("serving via N2N Ouroboros on address: {addr}");

        let listener = TcpListener::bind(addr).await.unwrap();

        loop {
            tokio::select! {
                n2n_conn = PeerServer::accept(&listener, config.magic) => {
                    info!("accepted incoming peer connection");

                    let db = store.clone();
                    let conn = n2n_conn.unwrap();

                    tokio::spawn(async move {handle_blockfetch(db.clone(), conn).await});
                }
                // n2c_conn = NodeServer::accept(&listener, config.magic) => {}
            }
        }
    }

    Ok(())
}

async fn handle_blockfetch(store: chain::Store, mut peer: PeerServer) -> Result<(), Error> {
    let blockfetch = peer.blockfetch();
    loop {
        match blockfetch.recv_while_idle().await {
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
                    Point::Origin => {
                        return blockfetch.send_no_blocks().await.map_err(Error::server)
                    }
                    Point::Specific(slot, hash) => {
                        let parsed_hash = TryInto::<[u8; 32]>::try_into(hash)
                            .map_err(|_| Error::client("malformed hash"))?
                            .into();

                        (slot, parsed_hash)
                    }
                };

                if let Some(iter) = store.read_chain_range(from, to).map_err(Error::storage)? {
                    blockfetch.send_start_batch().await.map_err(Error::server)?;

                    for point in iter {
                        let (_, hash) = point.map_err(Error::storage)?;

                        let block_bytes = match store.get_block(hash).map_err(Error::storage)? {
                            Some(b) => b,
                            None => {
                                error!("could not find block bytes for {hash}");
                                return Err(Error::server(
                                    "could not find block bytes for block in chainkv",
                                ));
                            }
                        };

                        blockfetch
                            .send_block(block_bytes)
                            .await
                            .map_err(Error::server)?;
                    }

                    blockfetch.send_batch_done().await.map_err(Error::server)?;
                } else {
                    return blockfetch.send_no_blocks().await.map_err(Error::server);
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
