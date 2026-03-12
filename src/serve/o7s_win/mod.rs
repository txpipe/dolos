use tracing::{instrument, warn};

use dolos_core::{config::OuroborosConfig, CancelToken, Domain, ServeError};

#[derive(Clone)]
pub struct DriverConfig {
    pub service: OuroborosConfig,
    pub network_magic: u64,
}

pub struct Driver;

impl<D: Domain, C: CancelToken> dolos_core::Driver<D, C> for Driver {
    type Config = DriverConfig;

    #[instrument(skip_all)]
    async fn run(_cfg: Self::Config, _domain: D, _cancel: C) -> Result<(), ServeError> {
        warn!("ouroboros client socket not yet supported on windows (soon)");
        Ok(())
    }
}
