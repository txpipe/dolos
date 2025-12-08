use dolos_core::config::{EmulatorConfig, PeerConfig, RetryConfig, SyncConfig, UpstreamConfig};
use std::time::Duration;

use crate::adapters::DomainAdapter;
use crate::prelude::*;

pub mod apply;
pub mod emulator;
pub mod pull;
pub mod submit;

const HOUSEKEEPING_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

fn define_gasket_policy(config: &Option<RetryConfig>) -> gasket::runtime::Policy {
    let default_retries = RetryConfig {
        max_retries: 20,
        backoff_unit_sec: 1,
        backoff_factor: 2,
        max_backoff_sec: 60,
        dismissible: false,
    };

    let retries = config.clone().unwrap_or(default_retries);

    let retries = gasket::retries::Policy {
        max_retries: retries.max_retries,
        backoff_unit: Duration::from_secs(retries.backoff_unit_sec),
        backoff_factor: retries.backoff_factor,
        max_backoff: Duration::from_secs(retries.max_backoff_sec),
        dismissible: retries.dismissible,
    };

    gasket::runtime::Policy {
        // TODO: we skip checking timeouts to avoid stalling the pipeline on slow work units. The
        // long-term solution is to scope work units to fit within a particular quota.
        tick_timeout: None,
        bootstrap_retry: retries.clone(),
        work_retry: retries.clone(),
        teardown_retry: retries.clone(),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn pipeline(
    config: &SyncConfig,
    upstream: &UpstreamConfig,
    domain: DomainAdapter,
    retries: &Option<RetryConfig>,
) -> Result<Vec<gasket::runtime::Tether>, Error> {
    match upstream {
        UpstreamConfig::Peer(cfg) => sync(config, cfg, domain.clone(), retries),
        UpstreamConfig::Emulator(cfg) => devnet(cfg, domain.clone(), retries),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn sync(
    config: &SyncConfig,
    upstream: &PeerConfig,
    domain: DomainAdapter,
    retries: &Option<RetryConfig>,
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
    retries: &Option<RetryConfig>,
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
