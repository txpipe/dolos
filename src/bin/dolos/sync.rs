use std::path::Path;

use dolos::{prelude::*, upstream::cursor::Cursor};
use gasket::messaging::{RecvPort, SendPort};

#[derive(Debug, clap::Args)]
pub struct Args {}

pub fn run(config: &super::Config, _args: &Args) -> Result<(), Error> {
    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::DEBUG)
            .finish(),
    )
    .unwrap();

    let cursor = Cursor::new(dolos::upstream::cursor::Intersection::Origin);

    let (to_reducer, from_chainsync) = gasket::messaging::tokio::channel(50);

    let mut chainsync = dolos::upstream::chainsync::Stage::new(
        config.upstream.peer_address.clone(),
        config.upstream.network_magic,
        cursor,
    );

    chainsync.downstream.connect(to_reducer);

    let rolldb_path = config
        .rolldb
        .path
        .as_deref()
        .unwrap_or_else(|| Path::new("/db"));

    let mut reducer = dolos::upstream::reducer::Stage::new(rolldb_path);

    reducer.upstream.connect(from_chainsync);

    let chainsync = gasket::runtime::spawn_stage(chainsync, gasket::runtime::Policy::default());

    let reducer = gasket::runtime::spawn_stage(reducer, gasket::runtime::Policy::default());

    gasket::daemon::Daemon(vec![chainsync, reducer]).block();

    Ok(())
}
