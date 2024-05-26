use pallas::network::facades::NodeServer;
use pallas::network::miniprotocols::keepalive;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::net::{TcpListener, UnixListener};

use tracing::{info, instrument};

use crate::prelude::*;
use crate::wal::redb::WalStore;

mod chainsync;
mod convert;

#[cfg(test)]
mod tests;

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    listen_address: String,
    magic: u64,
}

async fn client_session(wal: WalStore, server: NodeServer) -> Result<(), Error> {
    let NodeServer {
        plexer,
        chainsync,
        keepalive,
        ..
    } = server;

    let l1 = chainsync::handle_session(wal.clone(), chainsync);
    let l2 = handle_keepalive(keepalive);

    let _ = tokio::try_join!(l1, l2);

    plexer.abort().await;

    Ok(())
}

#[instrument(skip_all)]
pub async fn serve(config: Config, wal: WalStore) -> Result<(), Error> {
    let listener = UnixListener::bind(&config.listen_address).map_err(Error::server)?;

    info!(addr = &config.listen_address, "o7s listening");

    loop {
        let server = NodeServer::accept(&listener, config.magic)
            .await
            .map_err(Error::server)?;

        info!("accepted incoming connection");

        let _handle = tokio::spawn(client_session(wal.clone(), server));
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
