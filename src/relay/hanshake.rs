use dolos_core::{CancelToken, ServeError};
use pallas::network::miniprotocols::keepalive;
use std::time::Duration;
use tracing::info;

use super::Error;

pub async fn send_forever(mut keepalive: keepalive::Server) -> Result<(), Error> {
    loop {
        keepalive
            .keepalive_roundtrip()
            .await
            .map_err(Error::server)?;

        tokio::time::sleep(Duration::from_secs(15)).await
    }
}

pub async fn handle_session<C: CancelToken>(
    connection: keepalive::Server,
    cancel: C,
) -> Result<(), ServeError> {
    tokio::select! {
        _ = send_forever(connection) => {
            info!("peer ended protocol");
        },
        _ = cancel.cancelled() => {
            info!("protocol was cancelled");
        }
    }

    Ok(())
}
