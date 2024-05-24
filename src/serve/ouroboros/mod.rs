use pallas::network::facades::PeerServer;
use pallas::network::miniprotocols::keepalive;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::net::TcpListener;

use tracing::{info, instrument};

use crate::prelude::*;
use crate::wal::redb::WalStore;

#[cfg(test)]
mod tests;

mod blockfetch;
mod chainsync;
mod convert;

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    listen_address: String,
    magic: u64,
}

#[instrument(skip_all)]
async fn peer_session(wal: WalStore, peer: PeerServer) -> Result<(), Error> {
    let PeerServer {
        plexer,
        chainsync,
        blockfetch,
        keepalive,
        ..
    } = peer;

    let l1 = self::chainsync::handle_session(wal.clone(), chainsync);
    let l2 = self::blockfetch::handle_blockfetch(wal.clone(), blockfetch);
    let l3 = handle_keepalive(keepalive);

    let _ = tokio::try_join!(l1, l2, l3);

    plexer.abort().await;

    Ok(())
}

#[instrument(skip_all)]
pub async fn serve(config: Config, wal: WalStore) -> Result<(), Error> {
    let listener = TcpListener::bind(&config.listen_address)
        .await
        .map_err(Error::server)?;

    info!(addr = &config.listen_address, "ouroboros listening");

    loop {
        let peer = PeerServer::accept(&listener, config.magic)
            .await
            .map_err(Error::server)?;

        info!("accepted incoming connection");

        let _handle = tokio::spawn(peer_session(wal.clone(), peer));
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
