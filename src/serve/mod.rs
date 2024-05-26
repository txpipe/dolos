use futures_util::future::join_all;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;

use crate::ledger::store::LedgerStore;
use crate::prelude::*;
use crate::wal::redb::WalStore;

mod grpc;

/// Short for Ouroboros
#[cfg(unix)]
mod o7s;

#[derive(Deserialize, Serialize, Clone)]
pub struct Config {
    pub grpc: Option<grpc::Config>,

    #[cfg(unix)]
    pub ouroboros: Option<o7s::Config>,
}

/// Serve remote requests
///
/// Uses specified config to start listening for network connections on either
/// gRPC, Ouroboros or both protocols.
pub async fn serve(
    config: Config,
    wal: WalStore,
    ledger: LedgerStore,
    mempool: Arc<crate::submit::MempoolState>,
    txs_out: gasket::messaging::tokio::ChannelSendAdapter<Vec<crate::submit::Transaction>>,
) -> Result<(), Error> {
    let mut tasks = vec![];

    if let Some(cfg) = config.grpc {
        info!("found gRPC config");
        tasks.push(tokio::spawn(grpc::serve(
            cfg,
            wal.clone(),
            ledger,
            mempool,
            txs_out,
        )));
    }

    #[cfg(unix)]
    if let Some(cfg) = config.ouroboros {
        info!("found Ouroboros config");
        tasks.push(tokio::spawn(o7s::serve(cfg, wal.clone())));
    }

    // TODO: we should stop if any of the tasks breaks
    join_all(tasks).await;

    Ok(())
}
