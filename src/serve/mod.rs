use futures_util::future::try_join3;
use miette::{Context, IntoDiagnostic};
use pallas::ledger::configs::{alonzo, byron, shelley};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::balius::Runtime;
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

#[cfg(feature = "offchain")]
pub mod offchain;

#[derive(Deserialize, Serialize, Clone, Default)]
pub struct Config {
    pub grpc: Option<grpc::Config>,
    pub ouroboros: Option<o7s::Config>,
    pub offchain: Option<offchain::Config>,
}

pub type GenesisFiles = (
    alonzo::GenesisFile,
    byron::GenesisFile,
    shelley::GenesisFile,
);

/// Serve remote requests
///
/// Uses specified config to start listening for network connections on either
/// gRPC, Ouroboros or both protocols.
pub async fn serve(
    config: Config,
    genesis_files: GenesisFiles,
    wal: WalStore,
    ledger: LedgerStore,
    mempool: Mempool,
    offchain: Runtime,
    exit: CancellationToken,
) -> miette::Result<()> {
    let grpc = async {
        if let Some(cfg) = config.grpc {
            info!("found gRPC config");

            grpc::serve(
                cfg,
                genesis_files,
                wal.clone(),
                ledger,
                mempool,
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

    let offchain = async {
        if let Some(cfg) = config.offchain {
            info!("found offchain config");

            offchain::serve(cfg, offchain, exit.clone())
                .await
                .into_diagnostic()
                .context("serving offchain")
        } else {
            Ok(())
        }
    };

    try_join3(grpc, o7s, offchain).await?;

    Ok(())
}
