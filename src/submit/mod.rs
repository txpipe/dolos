use serde::{Deserialize, Serialize};

use crate::prelude::*;

pub mod grpc;

#[derive(Deserialize, Serialize, Clone)]
pub struct Config {
    pub grpc: grpc::Config,
}

/// Serve remote requests
pub async fn serve(config: Config) -> Result<(), Error> {
    grpc::pipeline(config.grpc).map_err(Error::server)?.block();

    Ok(())
}
