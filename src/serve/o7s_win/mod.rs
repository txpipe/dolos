use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::{prelude::Error, wal::redb::WalStore};

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    pub listen_path: PathBuf,
    pub magic: u64,
}

pub struct Driver;

impl<D: Domain, C: CancelToken> dolos_core::Driver<D, C> for Driver {
    type Config = Config;

    #[instrument(skip_all)]
    async fn run(cfg: Self::Config, domain: D, cancel: C) -> Result<(), ServeError> {
        error!("ouroboros client socket not yet supported on windows (soon)");
        Ok(())
    }
}
