use std::time::Duration;

use gasket::messaging::{RecvPort, SendPort};
use pallas::ledger::configs::{byron, shelley};
use pallas::storage::rolldb::chain::Store as ChainStore;
use pallas::storage::rolldb::wal::Store as WalStore;
use serde::Deserialize;
use tracing::info;

use crate::prelude::*;
use crate::storage::applydb::ApplyDB;

pub mod chain;
pub mod ledger;
pub mod pparams;
pub mod pull;
pub mod roll;

#[derive(Deserialize)]
pub struct Config {
    pub peer_address: String,
    pub network_magic: u64,
    pub network_id: u8,
}

fn define_gasket_policy(config: &Option<gasket::retries::Policy>) -> gasket::runtime::Policy {
    let default_retries = gasket::retries::Policy {
        max_retries: 20,
        backoff_unit: Duration::from_secs(1),
        backoff_factor: 2,
        max_backoff: Duration::from_secs(60),
        dismissible: false,
    };

    let retries = config.clone().unwrap_or(default_retries);

    gasket::runtime::Policy {
        //be generous with tick timeout to avoid timeout during block awaits
        tick_timeout: std::time::Duration::from_secs(600).into(),
        bootstrap_retry: retries.clone(),
        work_retry: retries.clone(),
        teardown_retry: retries.clone(),
    }
}

pub fn pipeline(
    config: &Config,
    wal: WalStore,
    chain: ChainStore,
    ledger: ApplyDB,
    byron: byron::GenesisFile,
    shelley: shelley::GenesisFile,
    retries: &Option<gasket::retries::Policy>,
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

    let cursor_chain = chain.find_tip().map_err(Error::storage)?;
    info!(?cursor_chain, "chain cursor found");

    let cursor_ledger = ledger.cursor().map_err(Error::storage)?;
    info!(?cursor_ledger, "ledger cursor found");

    let mut roll = roll::Stage::new(wal, cursor_chain, cursor_ledger);

    let mut chain = chain::Stage::new(chain);
    let mut ledger = ledger::Stage::new(ledger, byron, shelley);

    let (to_roll, from_pull) = gasket::messaging::tokio::mpsc_channel(50);
    pull.downstream.connect(to_roll);
    roll.upstream.connect(from_pull);

    let (to_chain, from_roll) = gasket::messaging::tokio::mpsc_channel(50);
    roll.downstream_chain.connect(to_chain);
    chain.upstream.connect(from_roll);

    let (to_ledger, from_roll) = gasket::messaging::tokio::mpsc_channel(50);
    roll.downstream_ledger.connect(to_ledger);
    ledger.upstream.connect(from_roll);

    // output to outside of out pipeline
    // apply.downstream.connect(output);

    let policy = define_gasket_policy(retries);

    let pull = gasket::runtime::spawn_stage(pull, policy.clone());
    let roll = gasket::runtime::spawn_stage(roll, policy.clone());
    let chain = gasket::runtime::spawn_stage(chain, policy.clone());
    let ledger = gasket::runtime::spawn_stage(ledger, policy.clone());

    Ok(gasket::daemon::Daemon(vec![pull, roll, chain, ledger]))
}
