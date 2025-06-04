use pallas::network::facades::NodeServer;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::net::UnixListener;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, info, instrument, warn};

use crate::adapters::{DomainAdapter, WalAdapter};
use crate::prelude::*;

mod chainsync;

#[cfg(test)]
mod tests;

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    pub listen_path: PathBuf,
    pub magic: u64,
}

async fn handle_session(
    wal: WalAdapter,
    connection: NodeServer,
    cancel: CancellationToken,
) -> Result<(), Error> {
    let NodeServer {
        plexer, chainsync, ..
    } = connection;

    // leaving this here since there's more threads to come
    let l1 = tokio::spawn(chainsync::handle_session(wal.clone(), chainsync, cancel));

    let (l1,) = tokio::try_join!(l1).map_err(Error::server)?;

    l1?;

    plexer.abort().await;

    Ok(())
}

async fn accept_client_connections(
    wal: WalAdapter,
    config: &Config,
    tasks: &mut TaskTracker,
    cancel: CancellationToken,
) -> Result<(), Error> {
    let listener = UnixListener::bind(&config.listen_path).map_err(Error::server)?;
    info!(addr = %config.listen_path.to_string_lossy(), "Ouroboros socket is listening for clients");

    loop {
        let connection = NodeServer::accept(&listener, config.magic).await;

        match connection {
            Ok(connection) => {
                info!(
                    from = ?connection.accepted_address(),
                    handshake = ?connection.accepted_version(),
                    "accepting incoming connection"
                );

                tasks.spawn(handle_session(wal.clone(), connection, cancel.clone()));
                info!(connections = tasks.len(), "active connections changed");
            }
            Err(error) => {
                warn!(%error, "error on incoming connection");
            }
        }
    }
}

#[cfg(unix)]
#[instrument(skip_all)]
pub async fn serve(
    config: Config,
    domain: DomainAdapter,
    cancel: CancellationToken,
) -> Result<(), Error> {
    let mut tasks = TaskTracker::new();

    tokio::select! {
        res = accept_client_connections(domain.wal().clone(), &config, &mut tasks, cancel.clone()) => {
            res?;
        },
        _ = cancel.cancelled() => {
            warn!("exit requested");
        }
    }

    // removing socket file so that it's free for next run
    debug!("removing socket file");
    std::fs::remove_file(config.listen_path).map_err(Error::server)?;

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

#[cfg(windows)]
pub async fn serve(_: Config, _: WalStore, _: CancellationToken) -> Result<(), Error> {
    tracing::error!("ouroboros client socket not supported on windows");

    Ok(())
}
