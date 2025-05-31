use std::sync::Arc;

use miette::{Context, IntoDiagnostic};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::info;

use dolos_cardano::pparams::Genesis;

use crate::chain::ChainStore;
use crate::mempool::Mempool;
use crate::state::LedgerStore;
use crate::wal::redb::WalStore;

pub mod utils;

#[cfg(feature = "grpc")]
pub mod grpc;

#[cfg(unix)]
pub mod o7s_unix;

#[cfg(unix)]
pub use o7s_unix as o7s;

#[cfg(windows)]
pub mod o7s_win;

#[cfg(windows)]
pub use o7s_win as o7s;

#[cfg(feature = "minibf")]
pub mod minibf;

#[cfg(feature = "trp")]
pub mod trp;

#[derive(Deserialize, Serialize, Clone, Default)]
pub struct Config {
    pub grpc: Option<grpc::Config>,
    pub ouroboros: Option<o7s::Config>,
    pub minibf: Option<minibf::Config>,
    pub trp: Option<trp::Config>,
}

#[allow(unused)]
macro_rules! feature_not_included {
    ($service_name:expr) => {
        panic!(
            "{} service is not available in this build of Dolos. Please rebuild with the '{}' feature enabled.",
            $service_name,
            $service_name.to_lowercase()
        )
    };
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
    chain: ChainStore,
    mempool: Mempool,
    exit: CancellationToken,
) -> miette::Result<()> {
    let grpc = async {
        if let Some(cfg) = config.grpc {
            info!("found gRPC config");

            #[cfg(not(feature = "grpc"))]
            feature_not_included!("gRPC");

            #[cfg(feature = "grpc")]
            grpc::serve(
                cfg,
                genesis.clone(),
                wal.clone(),
                ledger.clone(),
                chain.clone(),
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

            #[cfg(not(feature = "minibf"))]
            feature_not_included!("minibf");

            #[cfg(feature = "minibf")]
            minibf::serve(
                cfg,
                genesis.clone(),
                ledger.clone(),
                chain.clone(),
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

    let trp = async {
        if let Some(cfg) = config.trp {
            info!("found trp config");

            #[cfg(not(feature = "trp"))]
            feature_not_included!("trp");

            #[cfg(feature = "trp")]
            trp::serve(cfg, genesis.clone(), ledger.clone(), exit.clone())
                .await
                .into_diagnostic()
                .context("serving trp")
        } else {
            Ok(())
        }
    };

    tokio::try_join!(grpc, o7s, minibf, trp)?;

    Ok(())
}
