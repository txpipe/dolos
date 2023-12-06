mod protocols;

use pallas::network::facades::PeerServer;
use serde::{Deserialize, Serialize};
use tokio::join;
use tokio::net::TcpListener;

use tracing::{info, instrument};

use crate::prelude::*;
use crate::storage::rolldb::RollDB;

use protocols::{handle_blockfetch, handle_n2n_chainsync};

#[cfg(test)]
mod tests;

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    listen_address: String,
    magic: u64,
}

#[instrument(skip_all)]
async fn peer_session(db: RollDB, peer: PeerServer) -> Result<(), Error> {
    let PeerServer {
        blockfetch,
        chainsync,
        plexer_handle,
        ..
    } = peer;

    let l1 = handle_n2n_chainsync(db.clone(), chainsync);
    let l2 = handle_blockfetch(db.clone(), blockfetch);

    let _ = join!(l1, l2);

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

        let _handle = tokio::spawn(async move { peer_session(db, peer).await });
    }
}
