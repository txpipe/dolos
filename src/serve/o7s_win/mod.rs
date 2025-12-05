use tracing::{error, instrument};

use dolos_core::{config::OuroborosConfig, CancelToken, Domain, ServeError};

pub struct Driver;

impl<D: Domain, C: CancelToken> dolos_core::Driver<D, C> for Driver {
    type Config = OuroborosConfig;

    #[instrument(skip_all)]
    async fn run(_cfg: Self::Config, _domain: D, _cancel: C) -> Result<(), ServeError> {
        error!("ouroboros client socket not yet supported on windows (soon)");
        Ok(())
    }
}
