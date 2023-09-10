use gasket::framework::*;
use tracing::{info, instrument, warn};

use crate::prelude::*;
use crate::storage::applydb::{ApplyDB, UtxoRef};

pub type UpstreamPort = gasket::messaging::tokio::InputPort<RollEvent>;

#[derive(Stage)]
#[stage(name = "apply", unit = "RollEvent", worker = "Worker")]
pub struct Stage {
    applydb: ApplyDB,

    pub upstream: UpstreamPort,

    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    wal_count: gasket::metrics::Counter,
}

impl Stage {
    pub fn new(applydb: ApplyDB) -> Self {
        Self {
            applydb,
            upstream: Default::default(),
            // downstream: Default::default(),
            block_count: Default::default(),
            wal_count: Default::default(),
        }
    }
}

impl Stage {
    #[instrument(skip_all)]
    fn apply_block(&mut self, cbor: &[u8]) -> Result<(), WorkerError> {
        let block = pallas::ledger::traverse::MultiEraBlock::decode(cbor).or_panic()?;
        let slot = block.slot();
        let hash = block.hash();

        let mut batch = self.applydb.start_block(slot);

        for tx in block.txs() {
            for consumed in tx.consumes() {
                batch
                    .spend_utxo(*consumed.hash(), consumed.index())
                    // TODO: since we don't have genesis utxos, it's reasonable to get missed hits.
                    // This needs to go away once the genesis block processing is implemented.
                    .or_else(|x| match x {
                        crate::storage::kvtable::Error::NotFound => {
                            warn!("skipping missing utxo");
                            Ok(())
                        }
                        x => Err(x),
                    })
                    .or_panic()?;
            }

            for (idx, produced) in tx.produces() {
                let body = produced.encode();
                batch.insert_utxo(tx.hash(), idx as u64, body);
            }
        }

        let tombstones = block
            .txs()
            .iter()
            .flat_map(|x| x.consumes())
            .map(|x| UtxoRef(*x.hash(), x.index()))
            .collect();

        batch.insert_slot(block.hash(), tombstones);

        self.applydb.commit_block(batch).or_panic()?;

        info!(slot, ?hash, "applied block");

        Ok(())
    }

    #[instrument(skip_all)]
    fn undo_block(&mut self, cbor: &[u8]) -> Result<(), WorkerError> {
        let block = pallas::ledger::traverse::MultiEraBlock::decode(cbor).or_panic()?;

        let mut batch = self.applydb.start_block(block.slot());

        for tx in block.txs() {
            for consumed in tx.consumes() {
                batch
                    .unspend_stxi(*consumed.hash(), consumed.index())
                    .or_panic()?;
            }

            for (idx, _) in tx.produces() {
                batch.delete_utxo(tx.hash(), idx as u64);
            }
        }

        batch.delete_slot();

        self.applydb.commit_block(batch).or_panic()?;

        Ok(())
    }
}

pub struct Worker;

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(_stage: &Stage) -> Result<Self, WorkerError> {
        Ok(Self)
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<RollEvent>, WorkerError> {
        let msg = stage.upstream.recv().await.or_panic()?;

        Ok(WorkSchedule::Unit(msg.payload))
    }

    async fn execute(&mut self, unit: &RollEvent, stage: &mut Stage) -> Result<(), WorkerError> {
        match unit {
            RollEvent::Apply(_, _, cbor) => stage.apply_block(cbor)?,
            RollEvent::Undo(_, _, cbor) => stage.undo_block(cbor)?,
            RollEvent::Reset(_) => todo!(),
        };

        Ok(())
    }
}
