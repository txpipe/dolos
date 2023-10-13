use std::path::Path;

use dolos::prelude::*;
use pallas::storage::rolldb::chain;

#[derive(Debug, clap::Args)]
pub struct Args {}

pub fn run(config: &super::Config, _args: &Args) -> Result<(), Error> {
    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::INFO)
            .finish(),
    )
    .unwrap();

    let chain = chain::Store::open(
        config
            .rolldb
            .path
            .as_deref()
            .unwrap_or_else(|| Path::new("/rolldb"))
            .join("chain"),
    )
    .unwrap();

    for item in chain.crawl() {
        dbg!(item.unwrap());
    }

    Ok(())
}
