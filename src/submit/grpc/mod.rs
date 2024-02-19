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

use crate::{
    prelude::*,
    sync::{self, roll},
};

/*
    create notifier, mempool stage controls notifier, grpc clones receiver into each endpoint
    create mpsc, gRPC stage clones sender into each endpoint, mempool stage receives new txs
    create RwLock of propagated vec, used to instantiate mempool stage, cloned into each grpc endpoint for reads

    gRPC stage clones a mpsc sender into each endpoint task, which sends newTxs to mempool stage
    gRPC stage clones a RwLock of the mempool stage propagated field
    gRPC stage clones a notifier receiver into each endpoint task (WaitForStreams), wait for tip change then .read the propagated field
*/

mod endpoints;
mod mempool;
mod monitor;
mod propagator;

#[derive(Clone, Debug)]
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
    prune_after_slots: u64,
    peer_addresses: Vec<String>,
    peer_magic: u64,
}

pub fn pipeline(
    config: Config,
    wal: wal::Store,
    sync: bool,
) -> Result<gasket::daemon::Daemon, Error> {
    let mempool_txs = Arc::new(RwLock::new(Vec::new()));
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
        mempool_txs.clone(),
        change_notifier.clone(),
    );

    let mut mempool_stage = mempool::Stage::new(
        mempool_txs.clone(),
        change_notifier.clone(),
        config.prune_after_slots,
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
