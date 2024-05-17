use pallas::network::facades::PeerServer;
use pallas::network::miniprotocols::keepalive;
use pallas::storage::rolldb::{chain, wal};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::net::TcpListener;

use tracing::{info, instrument};

use crate::prelude::*;

use self::blockfetch::handle_blockfetch;
use self::chainsync::N2NChainSyncHandler;

#[cfg(test)]
mod tests;

mod blockfetch;
mod chainsync;

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    listen_address: String,
    magic: u64,
}

#[instrument(skip_all)]
async fn peer_session(chain: chain::Store, wal: wal::Store, peer: PeerServer) -> Result<(), Error> {
    let PeerServer {
        plexer,
        chainsync,
        blockfetch,
        keepalive,
        ..
    } = peer;

    let mut n2n_chainsync_handler =
        N2NChainSyncHandler::new(chain.clone(), wal.clone(), chainsync)?;

    let l1 = n2n_chainsync_handler.begin();
    let l2 = handle_blockfetch(chain.clone(), blockfetch);
    let l3 = handle_keepalive(keepalive);

    let _ = tokio::try_join!(l1, l2, l3);

    plexer.abort().await;

    Ok(())
}

#[instrument(skip_all)]
pub async fn serve(config: Config, chain: chain::Store, wal: wal::Store) -> Result<(), Error> {
    let listener = TcpListener::bind(&config.listen_address)
        .await
        .map_err(Error::server)?;

    info!(addr = &config.listen_address, "ouroboros listening");

    loop {
        let peer = PeerServer::accept(&listener, config.magic)
            .await
            .map_err(Error::server)?;

        info!("accepted incoming connection");

        let _handle = tokio::spawn(peer_session(chain.clone(), wal.clone(), peer));
    }
}

async fn handle_keepalive(mut keepalive: keepalive::Server) -> Result<(), Error> {
    loop {
        keepalive
            .keepalive_roundtrip()
            .await
            .map_err(Error::server)?;
        tokio::time::sleep(Duration::from_secs(15)).await
    }
}
