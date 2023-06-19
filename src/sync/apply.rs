use gasket::framework::*;

use crate::prelude::*;
use crate::storage::statedb::StateDB;

pub type UpstreamPort = gasket::messaging::tokio::InputPort<RollEvent>;
//pub type DownstreamPort = gasket::messaging::tokio::OutputPort<???>;

#[derive(Stage)]
#[stage(name = "reducer", unit = "RollEvent", worker = "Worker")]
pub struct Stage {
    statedb: StateDB,

    pub upstream: UpstreamPort,

    // placeholder
    //pub downstream: DownstreamPort,
    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    wal_count: gasket::metrics::Counter,
}

impl Stage {
    pub fn new(statedb: StateDB) -> Self {
        Self {
            statedb,
            upstream: Default::default(),
            block_count: Default::default(),
            wal_count: Default::default(),
        }
    }
}

impl Stage {
    fn apply_block(&mut self, cbor: &[u8]) -> Result<(), WorkerError> {
        let block = pallas::ledger::traverse::MultiEraBlock::decode(&cbor).or_panic()?;

        let mut batch = self.statedb.block_apply(block.slot(), block.hash());

        for tx in block.txs() {
            for consumed in tx.consumes() {
                batch.consume_utxo(consumed.hash().clone(), consumed.index());
            }

            for (idx, produced) in tx.produces() {
                let body = produced.encode();
                batch.insert_utxo(tx.hash(), idx as u64, body);
            }
        }

        batch.commit().or_panic()?;

        Ok(())
    }

    fn undo_block(&mut self, cbor: &[u8]) -> Result<(), WorkerError> {
        let block = pallas::ledger::traverse::MultiEraBlock::decode(&cbor).or_panic()?;

        let mut batch = self.statedb.block_apply(block.slot(), block.hash());

        batch.delete_slot();

        for tx in block.txs() {
            for (idx, produced) in tx.produces() {
                let body = produced.encode();
                batch.delete_utxo(tx.hash(), idx as u64);
            }
        }

        batch.commit().or_panic()?;

        Ok(())
    }
}

pub struct Worker;

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
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
            RollEvent::Apply(_, _, cbor) => stage.apply_block(cbor),
            RollEvent::Undo(_, _, cbor) => stage.undo_block(cbor),
            RollEvent::Reset(_) => todo!(),
        };

        Ok(())
    }
}
