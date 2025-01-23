use crate::ledger::pparams::Genesis;
use crate::state::LedgerStore;
use crate::wal::redb::WalStore;
use crate::{mempool::Mempool, prelude::*};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

pub mod apply;
pub mod pull;
pub mod roll;
pub mod submit;

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
    storage: &StorageConfig,
    wal: WalStore,
    ledger: LedgerStore,
    genesis: Arc<Genesis>,
    mempool: Mempool,
    retries: &Option<gasket::retries::Policy>,
    quit_on_tip: bool,
) -> Result<Vec<gasket::runtime::Tether>, Error> {
    let mut pull = pull::Stage::new(
        upstream.peer_address.clone(),
        upstream.network_magic,
        config.pull_batch_size.unwrap_or(50),
        wal.clone(),
        quit_on_tip,
    );

    let mut roll = roll::Stage::new(wal.clone());

    let mut apply = apply::Stage::new(
        wal.clone(),
        ledger,
        mempool.clone(),
        genesis,
        storage.max_ledger_history,
    );

    let submit = submit::Stage::new(
        upstream.peer_address.clone(),
        upstream.network_magic,
        mempool,
    );

    let (to_roll, from_pull) = gasket::messaging::tokio::mpsc_channel(50);
    pull.downstream.connect(to_roll);
    roll.upstream.connect(from_pull);

    let (to_ledger, from_roll) = gasket::messaging::tokio::mpsc_channel(50);
    roll.downstream.connect(to_ledger);
    apply.upstream.connect(from_roll);

    // output to outside of out pipeline
    // apply.downstream.connect(output);

    let policy = define_gasket_policy(retries);

    let pull = gasket::runtime::spawn_stage(pull, policy.clone());
    let roll = gasket::runtime::spawn_stage(roll, policy.clone());
    let apply = gasket::runtime::spawn_stage(apply, policy.clone());
    let submit = gasket::runtime::spawn_stage(submit, policy.clone());

    Ok(vec![pull, roll, apply, submit])
}
