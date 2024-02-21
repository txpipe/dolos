use std::{path::PathBuf, sync::Arc};

use gasket::{
    messaging::{RecvPort, SendPort},
    runtime::Policy,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, RwLock};

use pallas::{
    crypto::hash::Hash,
    network::miniprotocols::txsubmission::{EraTxBody, EraTxId, TxIdAndSize},
    storage::rolldb::wal,
};

use crate::{prelude::*, sync};

use self::mempool::Monitor;

mod endpoints;
mod mempool;
mod monitor;
mod propagator;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Transaction {
    hash: Hash<32>,
    era: u16,
    bytes: Vec<u8>,
}

impl Into<TxIdAndSize<EraTxId>> for Transaction {
    fn into(self) -> TxIdAndSize<EraTxId> {
        TxIdAndSize(
            EraTxId(self.era, self.hash.to_vec()),
            self.bytes.len() as u32,
        )
    }
}

impl Into<EraTxBody> for Transaction {
    fn into(self) -> EraTxBody {
        EraTxBody(self.era, self.bytes)
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    pub listen_address: String,
    tls_client_ca_root: Option<PathBuf>,
    prune_after_confirmations: u64,
    peer_addresses: Vec<String>,
    peer_magic: u64,
}

pub fn pipeline(
    config: Config,
    wal: wal::Store,
    sync: bool,
) -> Result<gasket::daemon::Daemon, Error> {
    let mempool_monitor = Arc::new(RwLock::new(Monitor::new()));
    let change_notifier = Arc::new(Notify::new());

    let (grpc_send_txs_channel, grpc_receive_txs_channel) =
        gasket::messaging::tokio::mpsc_channel(64);

    let cursor = wal.find_tip().map_err(Error::storage)?;

    let last_wal_seq = if let Some(c) = cursor {
        wal.find_wal_seq(c).unwrap_or_default()
    } else {
        0
    };

    // create stages

    let mut tethers = vec![];

    // spawn pull/roll stage if we need to handle syncing the wal (not being run
    // in conjunction with sync stage)
    if sync {
        let mut pull = sync::pull::Stage::new(
            config.peer_addresses[0].clone(),
            config.peer_magic,
            sync::pull::Intersection::Tip,
        );

        let mut roll = sync::roll::Stage::new(wal.clone(), cursor, cursor);

        let (pull_to_roll_send, pull_to_roll_recv) = gasket::messaging::tokio::mpsc_channel(64);
        pull.downstream.connect(pull_to_roll_send);
        roll.upstream.connect(pull_to_roll_recv);

        tethers.push(gasket::runtime::spawn_stage(pull, Policy::default()));
        tethers.push(gasket::runtime::spawn_stage(roll, Policy::default()));
    }

    let endpoints_stage = endpoints::Stage::new(
        config.listen_address,
        config.tls_client_ca_root,
        grpc_send_txs_channel,
        mempool_monitor.clone(),
        change_notifier.clone(),
    );

    let mut mempool_stage = mempool::Stage::new(
        mempool_monitor.clone(),
        change_notifier.clone(),
        config.prune_after_confirmations,
    );

    let mut propagator_stage = propagator::Stage::new(config.peer_addresses, config.peer_magic);

    let mut monitor_stage = monitor::Stage::new(wal, last_wal_seq);

    // connect mempool stage to gRPC service
    // mempool stage (sc) has a single consumer receiving messages (txs to add
    // to mempool) from many different gRPC tasks (mp)
    mempool_stage
        .upstream_submit_endpoint
        .connect(grpc_receive_txs_channel);

    // connect mempool and propagator stage

    let (mempool_to_propagator_send, mempool_to_propagator_recv) =
        gasket::messaging::tokio::mpsc_channel(64);

    mempool_stage
        .downstream_propagator
        .connect(mempool_to_propagator_send);

    propagator_stage
        .upstream_mempool
        .connect(mempool_to_propagator_recv);

    // connect mempool stage and monitor stage

    let (monitor_to_mempool_send, monitor_to_mempool_recv) =
        gasket::messaging::tokio::mpsc_channel(64);

    monitor_stage
        .downstream_mempool
        .connect(monitor_to_mempool_send);

    mempool_stage
        .upstream_block_monitor
        .connect(monitor_to_mempool_recv);

    tethers.push(gasket::runtime::spawn_stage(
        endpoints_stage,
        Policy::default(),
    ));
    tethers.push(gasket::runtime::spawn_stage(
        mempool_stage,
        Policy::default(),
    ));
    tethers.push(gasket::runtime::spawn_stage(
        propagator_stage,
        Policy::default(),
    ));
    tethers.push(gasket::runtime::spawn_stage(
        monitor_stage,
        Policy::default(),
    ));

    Ok(gasket::daemon::Daemon(tethers))
}
