use pallas::network::facades::NodeServer;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::net::UnixListener;
use tracing::{info, instrument};

use crate::prelude::*;
use crate::wal::redb::WalStore;

mod chainsync;

#[cfg(test)]
mod tests;

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    listen_path: PathBuf,
    magic: u64,
}

async fn client_session(wal: WalStore, server: NodeServer) -> Result<(), Error> {
    let NodeServer {
        plexer, chainsync, ..
    } = server;

    let l1 = chainsync::handle_session(wal.clone(), chainsync);

    let _ = tokio::try_join!(l1); // leaving this here since there's more threads to come

    plexer.abort().await;

    Ok(())
}

#[instrument(skip_all)]
pub async fn serve(config: Config, wal: WalStore) -> Result<(), Error> {
    let listener = UnixListener::bind(&config.listen_path).map_err(Error::server)?;

    info!(addr = %config.listen_path.to_string_lossy(), "o7s listening");

    loop {
        let server = NodeServer::accept(&listener, config.magic)
            .await
            .map_err(Error::server)?;

        info!("accepted incoming connection");

        let _handle = tokio::spawn(client_session(wal.clone(), server));
    }
}
