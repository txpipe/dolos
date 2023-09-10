use gasket::messaging::{RecvPort, SendPort};
use serde::Deserialize;

use crate::prelude::*;
use crate::storage::{applydb::ApplyDB, rolldb::RollDB};

pub mod apply;
pub mod pull;
pub mod roll;

#[derive(Deserialize)]
pub struct Config {
    peer_address: String,
    network_magic: u64,
}

pub fn pipeline(
    config: &Config,
    rolldb: RollDB,
    applydb: ApplyDB,
    policy: &gasket::runtime::Policy,
) -> Result<gasket::daemon::Daemon, Error> {
    let pull_cursor = rolldb
        .intersect_options(5)
        .map_err(Error::storage)?
        .into_iter()
        .collect();

    let mut pull = pull::Stage::new(
        config.peer_address.clone(),
        config.network_magic,
        pull_cursor,
    );

    let roll_cursor = applydb.cursor().map_err(Error::storage)?;

    let mut roll = roll::Stage::new(rolldb, roll_cursor);

    let mut apply = apply::Stage::new(applydb);

    let (to_roll, from_pull) = gasket::messaging::tokio::mpsc_channel(50);
    pull.downstream.connect(to_roll);
    roll.upstream.connect(from_pull);

    let (to_apply, from_roll) = gasket::messaging::tokio::mpsc_channel(50);
    roll.downstream.connect(to_apply);
    apply.upstream.connect(from_roll);

    // output to outside of out pipeline
    // apply.downstream.connect(output);

    let pull = gasket::runtime::spawn_stage(pull, policy.clone());
    let roll = gasket::runtime::spawn_stage(roll, policy.clone());
    let apply = gasket::runtime::spawn_stage(apply, policy.clone());

    Ok(gasket::daemon::Daemon(vec![pull, roll, apply]))
}
