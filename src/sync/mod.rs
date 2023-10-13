use gasket::messaging::{RecvPort, SendPort};
use pallas::ledger::configs::byron::GenesisFile;
use pallas::storage::rolldb::{chain, wal};
use serde::Deserialize;

use crate::prelude::*;
use crate::storage::applydb::ApplyDB;

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
    wal: wal::Store,
    chain: chain::Store,
    ledger: ApplyDB,
    genesis: GenesisFile,
    policy: &gasket::runtime::Policy,
) -> Result<gasket::daemon::Daemon, Error> {
    let pull_cursor = wal
        .intersect_options(5)
        .map_err(Error::storage)?
        .into_iter()
        .collect();

    let mut pull = pull::Stage::new(
        config.peer_address.clone(),
        config.network_magic,
        pull_cursor,
    );

    let chain_cursor = chain.find_tip().map_err(Error::storage)?;
    let ledger_cursor = ledger.cursor().map_err(Error::storage)?;

    // this is a business invariant, the state of the chain and ledger stores should
    // "match". Since there's no concept of transaction spanning both stores, we do
    // an eager check at bootstrap
    assert_eq!(
        chain_cursor, ledger_cursor,
        "chain and ledger cursor don't match"
    );

    let mut roll = roll::Stage::new(wal, chain_cursor);

    let mut apply = apply::Stage::new(ledger, chain, genesis);

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
