use pallas::network::facades::NodeServer;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::net::UnixListener;
use tokio_util::task::TaskTracker;
use tracing::{debug, info, instrument, warn};

use crate::prelude::*;

mod chainsync;

// TODO: fix tests
//#[cfg(test)]
//mod tests;

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    pub listen_path: PathBuf,
    pub magic: u64,
}

async fn handle_session<W: WalStore, C: CancelToken>(
    wal: W,
    connection: NodeServer,
    cancel: C,
) -> Result<(), ServeError> {
    let NodeServer {
        plexer, chainsync, ..
    } = connection;

    // leaving this here since there's more threads to come
    let l1 = tokio::spawn(chainsync::handle_session(wal.clone(), chainsync, cancel));

    let (l1,) = tokio::try_join!(l1).map_err(|e| ServeError::Internal(e.into()))?;

    l1?;

    plexer.abort().await;

    Ok(())
}

async fn accept_client_connections<W: WalStore, C: CancelToken>(
    wal: W,
    config: &Config,
    tasks: &mut TaskTracker,
    cancel: C,
) -> Result<(), ServeError> {
    let listener = UnixListener::bind(&config.listen_path).map_err(ServeError::BindError)?;

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

pub struct Driver;

impl<D: Domain, C: CancelToken> dolos_core::Driver<D, C> for Driver {
    type Config = Config;

    #[instrument(skip_all)]
    async fn run(cfg: Self::Config, domain: D, cancel: C) -> Result<(), ServeError> {
        // preventive removal of socket file in case of unclean shutdown
        if std::fs::metadata(&cfg.listen_path).is_ok() {
            debug!("preventive removal of socket file");
            std::fs::remove_file(&cfg.listen_path).map_err(|e| ServeError::Internal(e.into()))?;
        }

        let mut tasks = TaskTracker::new();

        tokio::select! {
            res = accept_client_connections(domain.wal().clone(), &cfg, &mut tasks, cancel.clone()) => {
                res?;
            },
            _ = cancel.cancelled() => {
                warn!("exit requested");
            }
        }

        // removing socket file so that it's free for next run
        debug!("removing socket file");
        std::fs::remove_file(&cfg.listen_path).map_err(|e| ServeError::Internal(e.into()))?;

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
}
