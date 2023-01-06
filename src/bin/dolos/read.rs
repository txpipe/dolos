use std::thread::sleep;

use dolos::{
    prelude::*,
    rolldb::RollDB,
    upstream::{blockfetch, chainsync, plexer, reducer},
};

#[derive(clap::Args)]
pub struct Args {
    #[clap(long, value_parser)]
    //#[clap(description = "config file to load by the daemon")]
    config: Option<std::path::PathBuf>,
}

pub fn run(args: &Args) -> Result<(), Error> {
    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::DEBUG)
            .finish(),
    )
    .unwrap();

    let db = RollDB::open("./tmp").unwrap();

    for item in db.crawl_wal() {
        dbg!(item.unwrap());
    }

    Ok(())
}
