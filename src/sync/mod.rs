use crate::ledger::store::LedgerStore;
use crate::prelude::*;
use crate::wal::redb::WalStore;
use pallas::ledger::configs::{byron, shelley};
use serde::{Deserialize, Serialize};
use std::time::Duration;

pub mod ledger;
pub mod pull;
pub mod roll;

#[derive(Serialize, Deserialize)]
pub struct Config {
    pub pull_batch_size: Option<usize>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            pull_batch_size: Some(100),
        }
    }
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

#[allow(clippy::too_many_arguments)]
pub fn pipeline(
    config: &Config,
    upstream: &UpstreamConfig,
    wal: WalStore,
    ledger: LedgerStore,
    byron: byron::GenesisFile,
    shelley: shelley::GenesisFile,
    retries: &Option<gasket::retries::Policy>,
) -> Result<Vec<gasket::runtime::Tether>, Error> {
    let mut pull = pull::Stage::new(
        upstream.peer_address.clone(),
        upstream.network_magic,
        config.pull_batch_size.unwrap_or(50),
        wal.clone(),
    );

    let mut roll = roll::Stage::new(wal.clone());

    let mut ledger = ledger::Stage::new(wal.clone(), ledger, byron, shelley);

    let (to_roll, from_pull) = gasket::messaging::tokio::mpsc_channel(50);
    pull.downstream.connect(to_roll);
    roll.upstream.connect(from_pull);

    let (to_ledger, from_roll) = gasket::messaging::tokio::mpsc_channel(50);
    roll.downstream.connect(to_ledger);
    ledger.upstream.connect(from_roll);

    // output to outside of out pipeline
    // apply.downstream.connect(output);

    let policy = define_gasket_policy(retries);

    let pull = gasket::runtime::spawn_stage(pull, policy.clone());
    let roll = gasket::runtime::spawn_stage(roll, policy.clone());
    let ledger = gasket::runtime::spawn_stage(ledger, policy.clone());

    Ok(vec![pull, roll, ledger])
}
