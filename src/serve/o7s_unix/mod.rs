use dolos_core::config::OuroborosConfig;
use pallas::network::facades::NodeServer;
use tokio::net::UnixListener;
use tokio_util::task::TaskTracker;
use tracing::{debug, info, instrument, warn};

use crate::prelude::*;

mod chainsync;
mod statequery;
mod statequery_utils;

// TODO: fix tests
//#[cfg(test)]
//mod tests;

async fn handle_session<D: Domain, C: CancelToken>(
    domain: D,
    connection: NodeServer,
    cancel: C,
) -> Result<(), ServeError> {
    let NodeServer {
        plexer,
        chainsync,
        statequery,
        ..
    } = connection;

    let chainsync_task = tokio::spawn(chainsync::handle_session(
        domain.clone(),
        chainsync,
        cancel.clone(),
    ));

    let statequery_task = tokio::spawn(statequery::handle_session(
        domain.clone(),
        statequery,
        cancel.clone(),
    ));

    let result = tokio::try_join!(chainsync_task, statequery_task)
        .map_err(|e| ServeError::Internal(e.into()))?;

    result.0?;
    result.1?;

    plexer.abort().await;

    Ok(())
}

async fn accept_client_connections<D: Domain, C: CancelToken>(
    domain: D,
    config: &OuroborosConfig,
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

                tasks.spawn(handle_session(domain.clone(), connection, cancel.clone()));
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
    type Config = OuroborosConfig;

    #[instrument(skip_all)]
    async fn run(cfg: Self::Config, domain: D, cancel: C) -> Result<(), ServeError> {
        // preventive removal of socket file in case of unclean shutdown
        if std::fs::metadata(&cfg.listen_path).is_ok() {
            debug!("preventive removal of socket file");
            std::fs::remove_file(&cfg.listen_path).map_err(|e| ServeError::Internal(e.into()))?;
        }

        let mut tasks = TaskTracker::new();

        tokio::select! {
            res = accept_client_connections(domain.clone(), &cfg, &mut tasks, cancel.clone()) => {
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
