use dolos_cardano::networks::ResolvedNetwork;
use dolos_core::config::{
    EmulatorConfig, PeerConfig, RetryConfig, RootConfig, SyncConfig, UpstreamConfig,
};
use std::time::Duration;

use crate::adapters::DomainAdapter;
use crate::prelude::*;

pub mod apply;
pub mod emulator;
pub mod pull;
pub mod submit;

const HOUSEKEEPING_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// When a sync stops pulling: an invocation input, never configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum SyncLimit {
    #[default]
    NoLimit,
    UntilTip,
    MaxBlocks(u64),
}

fn define_gasket_policy(config: &RetryConfig) -> gasket::runtime::Policy {
    let retries = gasket::retries::Policy {
        max_retries: config.max_retries,
        backoff_unit: Duration::from_secs(config.backoff_unit_seconds),
        backoff_factor: config.backoff_factor,
        max_backoff: Duration::from_secs(config.max_backoff_seconds),
        dismissible: config.dismissible,
    };

    gasket::runtime::Policy {
        // TODO: we skip checking timeouts to avoid stalling the pipeline on slow work units. The
        // long-term solution is to scope work units to fit within a particular quota. (#1041)
        tick_timeout: None,
        bootstrap_retry: retries.clone(),
        work_retry: retries.clone(),
        teardown_retry: retries.clone(),
    }
}

/// The configured peer, or the network's default one.
pub fn peer_address(config: &RootConfig, network: &ResolvedNetwork) -> Result<String, Error> {
    let configured = match &config.upstream {
        Some(UpstreamConfig::Peer(PeerConfig { peer_address })) => peer_address.clone(),
        _ => None,
    };

    configured
        .or_else(|| network.default_peer().map(str::to_string))
        .ok_or_else(|| Error::ConfigError("`upstream.peer_address` is not set".into()))
}

pub fn pipeline(
    config: &RootConfig,
    limit: SyncLimit,
    domain: DomainAdapter,
) -> Result<Vec<gasket::runtime::Tether>, Error> {
    let network = ResolvedNetwork::resolve(&config.chain)
        .map_err(|error| Error::ConfigError(error.to_string()))?;

    match &config.upstream {
        Some(UpstreamConfig::Emulator(cfg)) => devnet(cfg, domain, &config.sync.retry),
        _ => sync(
            &config.sync,
            network.magic(),
            peer_address(config, &network)?,
            limit,
            domain,
        ),
    }
}

pub fn sync(
    config: &SyncConfig,
    network_magic: u64,
    peer_address: String,
    limit: SyncLimit,
    domain: DomainAdapter,
) -> Result<Vec<gasket::runtime::Tether>, Error> {
    let mut pull = pull::Stage::new(
        config,
        peer_address.clone(),
        network_magic,
        limit,
        domain.wal().clone(),
    );

    let mut apply = apply::Stage::new(domain.clone(), HOUSEKEEPING_INTERVAL);

    let submit = submit::Stage::new(peer_address, network_magic, domain.mempool().clone());

    let (to_apply, from_pull) = gasket::messaging::tokio::mpsc_channel(50);
    pull.downstream.connect(to_apply);
    apply.upstream.connect(from_pull);

    // output to outside of out pipeline
    // apply.downstream.connect(output);

    let policy = define_gasket_policy(&config.retry);

    let pull = gasket::runtime::spawn_stage(pull, policy.clone());
    let apply = gasket::runtime::spawn_stage(apply, policy.clone());
    let submit = gasket::runtime::spawn_stage(submit, policy.clone());

    Ok(vec![pull, apply, submit])
}

pub fn devnet(
    emulator_cfg: &EmulatorConfig,
    domain: DomainAdapter,
    retries: &RetryConfig,
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
