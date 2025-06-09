use futures_util::stream::FuturesUnordered;
use miette::{Context, IntoDiagnostic};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::adapters::DomainAdapter;
use crate::prelude::*;

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
pub use dolos_minibf as minibf;

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

pub fn load_drivers(
    all_drivers: &FuturesUnordered<tokio::task::JoinHandle<Result<(), ServeError>>>,
    config: Config,
    domain: DomainAdapter,
    exit: CancellationToken,
) {
    if let Some(cfg) = config.ouroboros {
        info!("found Ouroboros config");

        let driver = o7s::Driver::run(cfg.clone(), domain.clone(), CancelTokenImpl(exit.clone()));

        let task = tokio::spawn(driver);

        all_drivers.push(task);
    }

    #[cfg(feature = "grpc")]
    if let Some(cfg) = config.grpc {
        info!("found gRPC config");

        let driver = grpc::Driver::run(cfg.clone(), domain.clone(), CancelTokenImpl(exit.clone()));

        let task = tokio::spawn(driver);

        all_drivers.push(task);
    }

    #[cfg(feature = "minibf")]
    if let Some(cfg) = config.minibf {
        info!("found minibf config");

        let driver =
            minibf::Driver::run(cfg.clone(), domain.clone(), CancelTokenImpl(exit.clone()));

        let task = tokio::spawn(driver);

        all_drivers.push(task);
    }

    #[cfg(feature = "trp")]
    if let Some(cfg) = config.trp {
        info!("found trp config");

        let driver = trp::Driver::run(cfg.clone(), domain.clone(), CancelTokenImpl(exit.clone()));

        let task = tokio::spawn(driver);

        all_drivers.push(task);
    }
}
