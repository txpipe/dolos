use std::sync::Arc;

use futures_util::future::join_all;
use pallas::storage::rolldb::{chain, wal};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{ledger::store::LedgerStore, prelude::*};

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
pub async fn serve(
    config: Config,
    wal: wal::Store,
    chain: chain::Store,
    ledger: LedgerStore,
    mempool: Arc<crate::submit::MempoolState>,
    txs_out: gasket::messaging::tokio::ChannelSendAdapter<Vec<crate::submit::Transaction>>,
    cancellation_token: CancellationToken,
) -> Result<(), Error> {
    let mut tasks = vec![];

    if let Some(cfg) = config.grpc {
        info!("found gRPC config");

        let token_clone = cancellation_token.clone();
        let chain_clone = chain.clone();

        tasks.push(tokio::spawn(async move {
            token_clone.cancelled().await;
            warn!("gRPC service cancelled");
        }));

        tasks.push(tokio::spawn(async move {
            if let Err(e) =
                grpc::serve(cfg, wal.clone(), chain_clone, ledger, mempool, txs_out).await
            {
                error!("gRPC service failed: {:?}", e);
            }
        }));
    }

    if let Some(cfg) = config.ouroboros {
        info!("found Ouroboros config");

        let token_clone = cancellation_token.clone();
        let chain_clone = chain.clone();

        tasks.push(tokio::spawn(async move {
            token_clone.cancelled().await;
            warn!("Ouroboros service cancelled");
        }));

        tasks.push(tokio::spawn(async move {
            if let Err(e) = ouroboros::serve(cfg, chain_clone).await {
                error!("Ouroboros service failed: {:?}", e);
            }
        }));
    }

    // TODO: we should stop if any of the tasks breaks
    join_all(tasks).await;

    Ok(())
}
