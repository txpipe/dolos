use dolos_core::config::OuroborosConfig;
use pallas::network::facades::{NodeServer, DEFAULT_HANDSHAKE_TIMEOUT};
use tokio::{net::UnixListener, task::JoinHandle};
use tokio_util::task::TaskTracker;
use tracing::{debug, info, instrument, warn};

use super::accept::AcceptFailure;
use crate::prelude::*;

mod chainsync;
mod statequery;
pub(crate) mod utils;

#[derive(Clone)]
pub struct DriverConfig {
    pub service: OuroborosConfig,
    pub network_magic: u64,
}

// TODO: fix tests
//#[cfg(test)]
//mod tests;

#[cfg(test)]
mod session_tests;

#[cfg(test)]
mod accept_tests;

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

    // A failed protocol ends the connection, so its client gets an EOF rather than
    // waiting on a reply while the other protocol stays idle.
    let result = tokio::try_join!(flatten(chainsync_task), flatten(statequery_task));
    plexer.abort().await;

    result?;

    Ok(())
}

async fn flatten(task: JoinHandle<Result<(), ServeError>>) -> Result<(), ServeError> {
    task.await.map_err(|e| ServeError::Internal(e.into()))?
}

async fn accept_client_connections<D: Domain, C: CancelToken>(
    domain: D,
    config: &DriverConfig,
    tasks: &mut TaskTracker,
    cancel: C,
) -> Result<(), ServeError> {
    let listener =
        UnixListener::bind(&config.service.listen_path).map_err(ServeError::BindError)?;

    info!(addr = %config.service.listen_path.to_string_lossy(), "Ouroboros socket is listening for clients");

    loop {
        let connection = NodeServer::accept_with_timeout(
            &listener,
            config.network_magic,
            DEFAULT_HANDSHAKE_TIMEOUT,
        )
        .await;

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
                let failure = AcceptFailure::of(&error);
                let cause = std::error::Error::source(&error).map(ToString::to_string);
                warn!(%error, ?cause, ?failure, "error on incoming connection");
                failure.settle().await;
            }
        }
    }
}

pub struct Driver;

impl<D: Domain, C: CancelToken> dolos_core::Driver<D, C> for Driver {
    type Config = DriverConfig;

    #[instrument(skip_all)]
    async fn run(cfg: Self::Config, domain: D, cancel: C) -> Result<(), ServeError> {
        // preventive removal of socket file in case of unclean shutdown
        if std::fs::metadata(&cfg.service.listen_path).is_ok() {
            debug!("preventive removal of socket file");
            std::fs::remove_file(&cfg.service.listen_path)
                .map_err(|e| ServeError::Internal(e.into()))?;
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
        if let Err(error) = std::fs::remove_file(&cfg.service.listen_path) {
            if error.kind() != std::io::ErrorKind::NotFound {
                return Err(ServeError::Internal(error.into()));
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
}
