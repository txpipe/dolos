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

    let db = RollDB::open(
        config
            .rolldb
            .path
            .as_deref()
            .unwrap_or_else(|| Path::new("/db")),
    )
    .unwrap();

    for item in db.crawl_wal() {
        dbg!(item.unwrap());
    }

    Ok(())
}
