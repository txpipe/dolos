use std::sync::Arc;

use futures_util::future::try_join3;
use miette::{bail, Context, IntoDiagnostic};
use pallas::ledger::traverse::wellknown::GenesisValues;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::ledger::pparams::Genesis;
use crate::mempool::Mempool;
use crate::state::LedgerStore;
use crate::wal::redb::WalStore;

pub mod grpc;
pub mod utils;

#[cfg(unix)]
pub mod o7s_unix;

#[cfg(unix)]
pub use o7s_unix as o7s;

#[cfg(windows)]
pub mod o7s_win;

#[cfg(windows)]
pub use o7s_win as o7s;

pub mod minibf;

#[derive(Deserialize, Serialize, Clone, Default)]
pub struct Config {
    pub grpc: Option<grpc::Config>,
    pub ouroboros: Option<o7s::Config>,
    pub minibf: Option<minibf::Config>,
}

/// Serve remote requests
///
/// Uses specified config to start listening for network connections on either
/// gRPC, Ouroboros or both protocols.
pub async fn serve(
    config: Config,
    genesis: Arc<Genesis>,
    wal: WalStore,
    ledger: LedgerStore,
    mempool: Mempool,
    exit: CancellationToken,
) -> miette::Result<()> {
    let grpc = async {
        if let Some(cfg) = config.grpc {
            info!("found gRPC config");

            grpc::serve(
                cfg,
                genesis.clone(),
                wal.clone(),
                ledger.clone(),
                mempool.clone(),
                exit.clone(),
            )
            .await
            .into_diagnostic()
            .context("serving gRPC")
        } else {
            Ok(())
        }
    };

    let o7s = async {
        if let Some(cfg) = config.ouroboros {
            info!("found Ouroboros config");

            o7s::serve(cfg, wal.clone(), exit.clone())
                .await
                .into_diagnostic()
                .context("serving Ouroboros")
        } else {
            Ok(())
        }
    };

    let minibf = async {
        if let Some(cfg) = config.minibf {
            info!("found minibf config");
            let Some(magic) = genesis.shelley.network_magic else {
                bail!("Network magic not found on genesis");
            };

            let Some(values) = GenesisValues::from_magic(magic as u64) else {
                bail!("Invalid network magic");
            };

            minibf::serve(
                cfg,
                genesis.clone(),
                values,
                wal.clone(),
                ledger.clone(),
                mempool.clone(),
                exit.clone(),
            )
            .await
            .into_diagnostic()
            .context("serving minibf")
        } else {
            Ok(())
        }
    };

    try_join3(grpc, o7s, minibf).await?;

    Ok(())
}
