use std::path::Path;

use dolos::{prelude::*, rolldb::RollDB};

#[derive(Debug, clap::Args)]
pub struct Args {}

pub fn run(config: &super::Config, _args: &Args) -> Result<(), Error> {
    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::DEBUG)
            .finish(),
    )
    .unwrap();

    let rolldb_path = config
        .rolldb
        .path
        .as_deref()
        .unwrap_or_else(|| Path::new("/db"));

    let rolldb = RollDB::open(&rolldb_path, config.rolldb.k_param.unwrap_or(1000))
        .map_err(|err| Error::storage(err))?;

    dolos::sync::pipeline(&config.upstream, rolldb).block();

    Ok(())
}
