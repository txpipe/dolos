use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::adapters::DomainAdapter;
use crate::prelude::*;

pub mod apply;
pub mod emulator;
pub mod pull;
pub mod submit;

const HOUSEKEEPING_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

#[derive(Serialize, Deserialize, Clone, Default)]
pub enum SyncLimit {
    #[default]
    NoLimit,
    UntilTip,
    MaxBlocks(u64),
}

#[derive(Serialize, Deserialize)]
pub struct Config {
    pub pull_batch_size: Option<usize>,

    #[serde(default)]
    pub sync_limit: SyncLimit,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            pull_batch_size: Some(100),
            sync_limit: Default::default(),
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
    domain: DomainAdapter,
    retries: &Option<gasket::retries::Policy>,
) -> Result<Vec<gasket::runtime::Tether>, Error> {
    match upstream {
        UpstreamConfig::Peer(cfg) => sync(config, cfg, domain.clone(), retries),
        UpstreamConfig::Emulator(cfg) => devnet(cfg, domain.clone(), retries),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn sync(
    config: &Config,
    upstream: &PeerConfig,
    domain: DomainAdapter,
    retries: &Option<gasket::retries::Policy>,
) -> Result<Vec<gasket::runtime::Tether>, Error> {
    let mut pull = pull::Stage::new(config, upstream, domain.wal().clone());

    let mut apply = apply::Stage::new(domain.clone(), HOUSEKEEPING_INTERVAL);

    let submit = submit::Stage::new(
        upstream.peer_address.clone(),
        upstream.network_magic,
        domain.mempool().clone(),
    );

    let (to_apply, from_pull) = gasket::messaging::tokio::mpsc_channel(50);
    pull.downstream.connect(to_apply);
    apply.upstream.connect(from_pull);

    // output to outside of out pipeline
    // apply.downstream.connect(output);

    let policy = define_gasket_policy(retries);

    let pull = gasket::runtime::spawn_stage(pull, policy.clone());
    let apply = gasket::runtime::spawn_stage(apply, policy.clone());
    let submit = gasket::runtime::spawn_stage(submit, policy.clone());

    Ok(vec![pull, apply, submit])
}

#[allow(clippy::too_many_arguments)]
pub fn devnet(
    emulator_cfg: &EmulatorConfig,
    domain: DomainAdapter,
    retries: &Option<gasket::retries::Policy>,
) -> Result<Vec<gasket::runtime::Tether>, Error> {
    let mut emulator = emulator::Stage::new(
        domain.wal().clone(),
        domain.mempool().clone(),
        emulator_cfg.block_production_interval,
    );

    let mut apply = apply::Stage::new(domain.clone(), HOUSEKEEPING_INTERVAL);

    let (to_apply, from_pull) = gasket::messaging::tokio::mpsc_channel(50);
    emulator.downstream.connect(to_apply);
    apply.upstream.connect(from_pull);

    // output to outside of out pipeline
    // apply.downstream.connect(output);

    let policy = define_gasket_policy(retries);

    let emulator = gasket::runtime::spawn_stage(emulator, policy.clone());
    let apply = gasket::runtime::spawn_stage(apply, policy.clone());

    Ok(vec![emulator, apply])
}
