use pallas::network::facades::PeerServer;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use tracing::{debug, info, instrument, warn};

use crate::adapters::WalAdapter;
use crate::prelude::*;

mod blockfetch;
mod chainsync;
mod convert;
mod hanshake;

#[cfg(test)]
mod tests;

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    pub listen_address: String,
    pub magic: u64,
}

async fn handle_session(
    wal: WalAdapter,
    peer: PeerServer,
    cancel: CancellationToken,
) -> Result<(), Error> {
    let PeerServer {
        plexer,
        chainsync,
        blockfetch,
        keepalive,
        ..
    } = peer;

    let l1 = chainsync::handle_session(wal.clone(), chainsync, cancel.clone());
    let l2 = blockfetch::handle_session(wal.clone(), blockfetch, cancel.clone());
    let l3 = hanshake::handle_session(keepalive, cancel.clone());

    let _ = tokio::try_join!(l1, l2, l3);

    plexer.abort().await;

    Ok(())
}

async fn accept_peer_connections(
    wal: WalAdapter,
    config: &Config,
    tasks: &mut TaskTracker,
    cancel: CancellationToken,
) -> Result<(), Error> {
    let listener = TcpListener::bind(&config.listen_address)
        .await
        .map_err(Error::server)?;

    info!(addr = &config.listen_address, "ouroboros listening");

    loop {
        let peer = PeerServer::accept(&listener, config.magic)
            .await
            .map_err(Error::server)?;

        info!(
            from = ?peer.accepted_address(),
            handshake = ?peer.accepted_version(),
            "accepting incoming connection"
        );

        tasks.spawn(handle_session(wal.clone(), peer, cancel.clone()));

        info!(active = tasks.len(), "relay peers changed");
    }
}

#[instrument(skip_all)]
pub async fn serve(
    config: Option<Config>,
    wal: WalAdapter,
    cancel: CancellationToken,
) -> Result<(), Error> {
    let config = match config {
        Some(x) => x,
        None => {
            warn!("relay not enabled, skipping serve");
            return Ok(());
        }
    };

    let mut tasks = TaskTracker::new();

    tokio::select! {
        res = accept_peer_connections(wal.clone(), &config, &mut tasks, cancel.clone()) => {
            res?;
        },
        _ = cancel.cancelled() => {
            warn!("exit requested");
        }
    }

    // notify the tracker that we're done receiving new tasks. Without this explicit
    // close, the wait will block forever.
    debug!("closing task manger");
    tasks.close();

    // now we wait until all nested tasks exit
    debug!("waiting for tasks to finish");
    tasks.wait().await;

    info!("graceful shutdown finished");

    Ok(())
}
