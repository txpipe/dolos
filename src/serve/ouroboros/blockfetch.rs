use pallas::network::miniprotocols::{
    blockfetch::{self, BlockRequest},
    Point,
};
use pallas::storage::rolldb::chain;
use tracing::{error, info, warn};

use crate::prelude::Error;

pub async fn handle_blockfetch(
    chain: chain::Store,
    mut protocol: blockfetch::Server,
) -> Result<(), Error> {
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

                if let Some(mut iter) = chain.read_chain_range(from, to).map_err(Error::storage)? {
                    protocol.send_start_batch().await.map_err(Error::server)?;

                    for point in iter.by_ref() {
                        let (_, hash) = point.map_err(Error::storage)?;

                        let block_bytes = match chain.get_block(hash).map_err(Error::storage)? {
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
