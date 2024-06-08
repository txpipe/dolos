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

pub async fn serve(_: Config, _: WalStore, _: CancellationToken) -> Result<(), Error> {
    error!("ouroboros client socket not yet supported on windows (soon)");

    Ok(())
}
