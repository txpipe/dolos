use futures_util::future::join_all;
use pallas::storage::rolldb::{chain, wal};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::prelude::*;

pub mod grpc;
pub mod ouroboros;

#[derive(Deserialize, Serialize, Clone)]
pub struct Config {
    pub grpc: Option<grpc::Config>,
    pub ouroboros: Option<ouroboros::Config>,
}

/// Serve remote requests
///
/// Uses specified config to start listening for network connections on either
/// gRPC, Ouroboros or both protocols.
pub async fn serve(config: Config, wal: wal::Store, chain: chain::Store) -> Result<(), Error> {
    let mut tasks = vec![];

    if let Some(cfg) = config.grpc {
        info!("found gRPC config");
        tasks.push(tokio::spawn(grpc::serve(cfg, wal.clone(), chain.clone())));
    }

    if let Some(cfg) = config.ouroboros {
        info!("found Ouroboros config");
        tasks.push(tokio::spawn(ouroboros::serve(cfg, chain.clone())));
    }

    // TODO: we should stop if any of the tasks breaks
    join_all(tasks).await;

    Ok(())
}
