use dolos_core::config::OuroborosConfig;
use pallas::network::facades::NodeServer;
use tokio::net::UnixListener;
use tokio_util::task::TaskTracker;
use tracing::{debug, info, instrument, warn};

use crate::prelude::*;

/// Check if a process with the given PID is still running
fn is_process_running(pid: u32) -> bool {
    // Send signal 0 to check if a process exists without affecting it.
    // Returns true if the process is running, false if it doesn't exist
    // or we lack permission (which means something else owns the pid).
    #[cfg(unix)]
    {
        unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        true
    }
}

mod chainsync;
mod statequery;
mod utils;

#[derive(Clone)]
pub struct DriverConfig {
    pub service: OuroborosConfig,
    pub network_magic: u64,
}

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

    let result = tokio::try_join!(chainsync_task, statequery_task);
    plexer.abort().await;

    let (chainsync_res, statequery_res) = result.map_err(|e| ServeError::Internal(e.into()))?;

    chainsync_res?;
    statequery_res?;

    Ok(())
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
        let connection = NodeServer::accept(&listener, config.network_magic).await;

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
    type Config = DriverConfig;

    #[instrument(skip_all)]
    async fn run(cfg: Self::Config, domain: D, cancel: C) -> Result<(), ServeError> {
        // preventive removal of socket file in case of unclean shutdown
        // check if a stale PID lockfile exists and the process is dead before removing
        let lock_path = cfg.service.listen_path.with_extension("pid");
        if std::fs::metadata(&cfg.service.listen_path).is_ok() {
            let stale = match std::fs::read_to_string(&lock_path) {
                Ok(pid_str) => {
                    let pid: u32 = pid_str.trim().parse().unwrap_or(0);
                    pid == 0 || !is_process_running(pid)
                }
                Err(_) => true, // no lockfile = stale, safe to remove
            };
            if stale {
                debug!("preventive removal of stale socket file");
                let _ = std::fs::remove_file(&lock_path);
                std::fs::remove_file(&cfg.service.listen_path)
                    .map_err(|e| ServeError::Internal(e.into()))?;
            } else {
                return Err(ServeError::Internal(
                    format!(
                        "socket {} is in use by PID {}",
                        cfg.service.listen_path.display(),
                        std::fs::read_to_string(&lock_path)
                            .unwrap_or_default()
                            .trim()
                    )
                    .into(),
                ));
            }
        }
        // write our PID to the lockfile
        std::fs::write(&lock_path, std::process::id().to_string())
            .map_err(|e| ServeError::Internal(e.into()))?;

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
        // clean up PID lockfile
        let _ = std::fs::remove_file(cfg.service.listen_path.with_extension("pid"));

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
