use gasket::messaging::{RecvPort, SendPort};
use serde::Deserialize;

use crate::rolldb::RollDB;

pub mod reducer;
pub mod upstream;

#[derive(Deserialize)]
pub struct Config {
    peer_address: String,
    network_magic: u64,
}

pub fn pipeline(config: &Config, rolldb: RollDB) -> gasket::daemon::Daemon {
    let (to_reducer, from_chainsync) = gasket::messaging::tokio::channel(50);

    let mut chainsync = upstream::Stage::new(
        config.peer_address.clone(),
        config.network_magic,
        rolldb.clone(),
    );

    chainsync.downstream.connect(to_reducer);

    let mut reducer = reducer::Stage::new(rolldb);

    reducer.upstream.connect(from_chainsync);

    let chainsync = gasket::runtime::spawn_stage(chainsync, gasket::runtime::Policy::default());

    let reducer = gasket::runtime::spawn_stage(reducer, gasket::runtime::Policy::default());

    gasket::daemon::Daemon(vec![chainsync, reducer])
}
