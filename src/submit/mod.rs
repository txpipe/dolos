use gasket::messaging::{tokio::ChannelRecvAdapter, RecvPort, SendPort};
use pallas::{
    crypto::hash::Hash,
    network::miniprotocols::txsubmission::{EraTxBody, EraTxId, TxIdAndSize},
    storage::rolldb::wal,
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};

use crate::prelude::*;

mod mempool;
mod monitor;
mod propagator;

pub use self::mempool::MempoolState;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Transaction {
    pub hash: Hash<32>,
    pub era: u16,
    pub bytes: Vec<u8>,
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
    prune_after_confirmations: u64,
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
    upstream: &UpstreamConfig,
    wal: wal::Store,
    mempool: Arc<mempool::MempoolState>,
    txs_in: ChannelRecvAdapter<Vec<Transaction>>,
    retries: &Option<gasket::retries::Policy>,
) -> Result<Vec<gasket::runtime::Tether>, Error> {
    let cursor = wal.find_tip().map_err(Error::storage)?;

    let last_wal_seq = if let Some(c) = cursor {
        wal.find_wal_seq(&[c])
            .map_err(Error::storage)?
            .unwrap_or_default()
    } else {
        0
    };

    let mut mempool = mempool::Stage::new(mempool, config.prune_after_confirmations);

    let mut propagator =
        propagator::Stage::new(vec![upstream.peer_address.clone()], upstream.network_magic);

    let mut monitor = monitor::Stage::new(wal, last_wal_seq);

    // connect mempool stage to gRPC service
    // mempool stage (sc) has a single consumer receiving messages (txs to add
    // to mempool) from many different gRPC tasks (mp)
    mempool.upstream_submit_endpoint.connect(txs_in);

    // connect mempool and propagator stage

    let (from_mempool, to_propagator) = gasket::messaging::tokio::mpsc_channel(64);
    mempool.downstream_propagator.connect(from_mempool);
    propagator.upstream_mempool.connect(to_propagator);

    // connect mempool stage and monitor stage

    let (from_monitor, to_mempool) = gasket::messaging::tokio::mpsc_channel(64);
    monitor.downstream_mempool.connect(from_monitor);
    mempool.upstream_block_monitor.connect(to_mempool);

    let policy = define_gasket_policy(retries);

    let mempool = gasket::runtime::spawn_stage(mempool, policy.clone());
    let propagator = gasket::runtime::spawn_stage(propagator, policy.clone());
    let monitor = gasket::runtime::spawn_stage(monitor, policy.clone());

    Ok(vec![mempool, propagator, monitor])
}
