use dolos_core::{config::RelayConfig, Driver as _};
use futures_util::stream::FuturesUnordered;
use pallas::network::facades::PeerServer;
use tokio::net::TcpListener;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, info, instrument, warn};

use crate::{adapters::DomainAdapter, prelude::*};

mod blockfetch;
mod chainsync;
mod convert;
mod hanshake;

// TODO: add tests
// #[cfg(test)]
// mod tests;

async fn handle_session<D: Domain, C: CancelToken>(
    domain: D,
    peer: PeerServer,
    cancel: C,
) -> Result<(), ServeError> {
    let PeerServer {
        plexer,
        chainsync,
        blockfetch,
        keepalive,
        ..
    } = peer;

    let l1 = chainsync::handle_session(domain.clone(), chainsync, cancel.clone());
    let l2 = blockfetch::handle_session(domain.wal().clone(), blockfetch, cancel.clone());
    let l3 = hanshake::handle_session(keepalive, cancel.clone());

    let _ = tokio::try_join!(l1, l2, l3);

    plexer.abort().await;

    Ok(())
}

async fn accept_peer_connections<D: Domain, C: CancelToken>(
    domain: D,
    config: &RelayConfig,
    tasks: &mut TaskTracker,
    cancel: C,
) -> Result<(), ServeError> {
    let listener = TcpListener::bind(&config.listen_address)
        .await
        .map_err(ServeError::BindError)?;

    info!(addr = &config.listen_address, "ouroboros listening");

    loop {
        let peer = PeerServer::accept(&listener, config.magic)
            .await
            .map_err(|e| ServeError::Internal(e.into()))?;

        info!(
            from = ?peer.accepted_address(),
            handshake = ?peer.accepted_version(),
            "accepting incoming connection"
        );

        tasks.spawn(handle_session(domain.clone(), peer, cancel.clone()));

        info!(active = tasks.len(), "relay peers changed");
    }
}

pub struct Driver;

impl<D: Domain, C: CancelToken> dolos_core::Driver<D, C> for Driver {
    type Config = RelayConfig;

    #[instrument(skip_all)]
    async fn run(cfg: Self::Config, domain: D, cancel: C) -> Result<(), ServeError> {
        let mut tasks = TaskTracker::new();

        tokio::select! {
            res = accept_peer_connections(domain.clone(), &cfg, &mut tasks, cancel.clone()) => {
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
}

pub fn load_drivers(
    all_drivers: &FuturesUnordered<tokio::task::JoinHandle<Result<(), ServeError>>>,
    config: Option<RelayConfig>,
    domain: DomainAdapter,
    exit: CancellationToken,
) {
    if let Some(cfg) = config {
        info!("found Ouroboros config");

        let driver = Driver::run(cfg.clone(), domain.clone(), CancelTokenImpl(exit.clone()));

        let task = tokio::spawn(driver);

        all_drivers.push(task);
    }
}
