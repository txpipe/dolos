use std::collections::BTreeMap;
use std::sync::Arc;

use gasket::framework::*;
use pallas::codec::minicbor;
use pallas::codec::utils::{Bytes, Nullable};
use pallas::ledger::traverse::{ComputeHash, Era, MultiEraBlock, MultiEraTx};
use pallas::network::miniprotocols::chainsync::Tip;
use tracing::info;

use crate::adapters::WalAdapter;
use dolos_core::builtin::EphemeralMempool;
use crate::prelude::*;

pub type DownstreamPort = gasket::messaging::OutputPort<PullEvent>;

pub fn empty_bytes() -> Bytes {
    Bytes::from(vec![])
}

pub struct Worker {
    block_production_interval_seconds: u64,
    block_production_timer: tokio::time::Interval,
    mempool: EphemeralMempool,
}

impl Worker {
    pub fn create_next_block(
        &self,
        current: Option<RawBlock>,
    ) -> Result<(Tip, RawBlock), WorkerError> {
        let (block_number, slot, prev_hash) = match current {
            Some(raw) => {
                if raw.is_empty() {
                    (1, self.block_production_interval_seconds, None)
                } else {
                    let block = MultiEraBlock::decode(&raw).unwrap();
                    (
                        block.number() + 1,
                        block.slot() + self.block_production_interval_seconds,
                        Some(block.hash()),
                    )
                }
            }
            None => (1, self.block_production_interval_seconds, None),
        };

        let mut transaction_bodies = vec![];
        let mut transaction_witness_sets = vec![];
        let mut auxiliary_data_set = BTreeMap::new();

        let txs = self.mempool.peek_pending(10);

        for (i, tx) in txs.iter().enumerate() {
            info!(tx = hex::encode(tx.hash), "adding tx to emulated block");

            let EraCbor(era, cbor) = &tx.payload;

            let era = pallas::ledger::traverse::Era::try_from(*era).or_panic()?;

            let tx = MultiEraTx::decode_for_era(era, &cbor).or_panic()?;

            let Some(conway) = tx.as_conway() else {
                return Err(WorkerError::Panic);
            };

            // Encode and decode to remove all intermediate representations
            transaction_bodies.push(conway.transaction_body.raw_cbor().to_vec());
            transaction_witness_sets.push(conway.transaction_witness_set.raw_cbor().to_vec());
            if let Nullable::Some(aux) = &conway.auxiliary_data {
                auxiliary_data_set.insert(i as u32, aux.raw_cbor().to_vec());
            }
        }

        let hashes: Vec<TxHash> = txs.iter().map(|tx| tx.hash).collect();
        self.mempool.mark_inflight(&hashes);
        self.mempool.mark_acknowledged(&hashes);

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
            }
            .into(),
            transaction_bodies: transaction_bodies
                .iter()
                .map(|x| minicbor::decode(x).or_panic())
                .collect::<Result<Vec<_>, WorkerError>>()?,
            transaction_witness_sets: transaction_witness_sets
                .iter()
                .map(|x| minicbor::decode(x).or_panic())
                .collect::<Result<Vec<_>, WorkerError>>()?,
            auxiliary_data_set: auxiliary_data_set
                .iter()
                .map(|(i, x)| {
                    let decoded = minicbor::decode(x).or_panic()?;
                    Ok((*i, decoded))
                })
                .collect::<Result<BTreeMap<u32, _>, WorkerError>>()?,
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

        let cbor = minicbor::to_vec((era, block)).or_panic()?;

        Ok((tip, Arc::new(cbor)))
    }
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        Ok(Self {
            block_production_interval_seconds: stage.block_production_interval,
            block_production_timer: tokio::time::interval(std::time::Duration::from_secs(
                stage.block_production_interval,
            )),
            mempool: stage.mempool.clone(),
        })
    }

    async fn schedule(&mut self, _stage: &mut Stage) -> Result<WorkSchedule<()>, WorkerError> {
        self.block_production_timer.tick().await;
        info!("creating new block");
        Ok(WorkSchedule::Unit(()))
    }

    async fn execute(&mut self, _unit: &(), stage: &mut Stage) -> Result<(), WorkerError> {
        let tip = stage
            .wal
            .find_tip()
            .or_panic()?
            .map(|(_, log)| log)
            .map(|x| Arc::new(x.block.clone()));

        let (tip, block) = self.create_next_block(tip)?;

        stage.flush_block(block).await?;
        stage.track_tip(&tip);

        Ok(())
    }
}

#[derive(Stage)]
#[stage(name = "emulator", unit = "()", worker = "Worker")]
pub struct Stage {
    // block_production_interval: std::time::Duration,
    block_production_interval: u64,
    wal: WalAdapter,
    mempool: EphemeralMempool,

    pub downstream: DownstreamPort,

    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    chain_tip: gasket::metrics::Gauge,
}

impl Stage {
    pub fn new(wal: WalAdapter, mempool: EphemeralMempool, block_production_interval: u64) -> Self {
        Self {
            downstream: Default::default(),
            block_count: Default::default(),
            chain_tip: Default::default(),
            block_production_interval,
            wal,
            mempool,
        }
    }

    async fn flush_block(&mut self, block: RawBlock) -> Result<(), WorkerError> {
        self.downstream
            .send(PullEvent::RollForward(block).into())
            .await
            .or_panic()?;

        Ok(())
    }

    fn track_tip(&self, tip: &Tip) {
        self.chain_tip.set(tip.0.slot_or_default() as i64);
    }
}
