use std::collections::BTreeMap;

use gasket::framework::*;
use pallas::codec::minicbor;
use pallas::codec::utils::{Bytes, Nullable};
use pallas::ledger::traverse::{ComputeHash, Era, MultiEraBlock, MultiEraTx};
use pallas::network::miniprotocols::chainsync::Tip;
use tracing::info;

use crate::mempool::Mempool;
use crate::prelude::*;
use crate::wal::redb::WalStore;
use crate::wal::{self, ChainPoint, WalReader};

pub type DownstreamPort = gasket::messaging::OutputPort<PullEvent>;

pub fn empty_bytes() -> Bytes {
    Bytes::from(vec![])
}

pub struct Worker {
    block_production_timer: tokio::time::Interval,
    mempool: Mempool,
}
impl Worker {
    pub fn create_next_block(
        &self,
        current: Option<wal::RawBlock>,
    ) -> Result<(Tip, BlockBody), WorkerError> {
        let (block_number, slot, prev_hash) = match current {
            Some(raw) => {
                let block = MultiEraBlock::decode(&raw.body).unwrap();
                (block.number() + 1, block.slot() + 20, Some(block.hash()))
            }
            None => (1, 20, None),
        };

        let mut transaction_bodies = vec![];
        let mut transaction_witness_sets = vec![];
        let mut auxiliary_data_set = BTreeMap::new();

        for (i, tx) in self.mempool.request(10).iter().enumerate() {
            info!(tx = hex::encode(tx.hash), "adding tx to emulated block");
            let MultiEraTx::Conway(conway) = MultiEraTx::decode(&tx.bytes).or_panic()? else {
                return Err(WorkerError::Panic);
            };

            // Encode and decode to remove all intermediate representations
            transaction_bodies
                .push(minicbor::decode(conway.transaction_body.raw_cbor()).or_panic()?);
            transaction_witness_sets
                .push(minicbor::decode(conway.transaction_witness_set.raw_cbor()).or_panic()?);
            if let Nullable::Some(aux) = &conway.auxiliary_data {
                auxiliary_data_set.insert(i as u32, minicbor::decode(aux.raw_cbor()).or_panic()?);
            }
        }

        self.mempool.acknowledge(transaction_bodies.len());

        let block = pallas::ledger::primitives::conway::Block {
            header: pallas::ledger::primitives::babbage::Header {
                header_body: pallas::ledger::primitives::babbage::HeaderBody {
                    block_number,
                    slot,
                    prev_hash,
                    block_body_hash: pallas::ledger::primitives::Hash::new([0; 32]),
                    issuer_vkey: empty_bytes(),
                    vrf_vkey: empty_bytes(),
                    vrf_result: pallas::ledger::primitives::babbage::VrfCert(
                        empty_bytes(),
                        empty_bytes(),
                    ),
                    block_body_size: 0,
                    operational_cert: pallas::ledger::primitives::conway::OperationalCert {
                        operational_cert_hot_vkey: empty_bytes(),
                        operational_cert_sequence_number: 0,
                        operational_cert_kes_period: 0,
                        operational_cert_sigma: empty_bytes(),
                    },
                    protocol_version: (1, 0),
                },
                body_signature: empty_bytes(),
            },
            transaction_bodies,
            transaction_witness_sets,
            auxiliary_data_set,
            invalid_transactions: None,
        };

        let tip = Tip(
            pallas::network::miniprotocols::Point::Specific(
                slot,
                block.header.compute_hash().to_vec(),
            ),
            block_number,
        );
        let era: u16 = Era::Conway.into();
        Ok((tip, minicbor::to_vec((era, block)).or_panic()?))
    }
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        Ok(Self {
            block_production_timer: tokio::time::interval(stage.block_production_interval),
            mempool: stage.mempool.clone(),
        })
    }

    async fn schedule(&mut self, _stage: &mut Stage) -> Result<WorkSchedule<()>, WorkerError> {
        self.block_production_timer.tick().await;
        info!("creating new block");
        Ok(WorkSchedule::Unit(()))
    }

    async fn execute(&mut self, _unit: &(), stage: &mut Stage) -> Result<(), WorkerError> {
        let current_tip = match stage.wal.find_tip().or_panic()? {
            Some((_, point)) => match point {
                ChainPoint::Origin => None,
                _ => Some(stage.wal.read_block(&point).or_panic()?),
            },
            None => None,
        };
        let (tip, block) = self.create_next_block(current_tip)?;

        stage.flush_block(block).await?;
        stage.track_tip(&tip);
        Ok(())
    }
}

#[derive(Stage)]
#[stage(name = "emulator", unit = "()", worker = "Worker")]
pub struct Stage {
    block_production_interval: std::time::Duration,
    wal: WalStore,
    mempool: Mempool,

    pub downstream: DownstreamPort,

    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    chain_tip: gasket::metrics::Gauge,
}

impl Stage {
    pub fn new(
        wal: WalStore,
        mempool: Mempool,
        block_production_interval: std::time::Duration,
    ) -> Self {
        Self {
            downstream: Default::default(),
            block_count: Default::default(),
            chain_tip: Default::default(),
            block_production_interval,
            wal,
            mempool,
        }
    }

    async fn flush_block(&mut self, block: BlockBody) -> Result<(), WorkerError> {
        let payload = {
            let decoded = MultiEraBlock::decode(&block).or_panic()?;
            RawBlock {
                slot: decoded.slot(),
                hash: decoded.hash(),
                era: decoded.era(),
                body: block,
            }
        };

        self.downstream
            .send(PullEvent::RollForward(payload).into())
            .await
            .or_panic()?;
        Ok(())
    }

    fn track_tip(&self, tip: &Tip) {
        self.chain_tip.set(tip.0.slot_or_default() as i64);
    }
}
