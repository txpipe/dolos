use gasket::framework::*;
use pallas::ledger::configs::byron::GenesisFile;
use pallas::storage::rolldb::chain;

use crate::prelude::*;
use crate::storage::applydb::ApplyDB;

pub type UpstreamPort = gasket::messaging::tokio::InputPort<RollEvent>;

#[derive(Stage)]
#[stage(name = "apply", unit = "RollEvent", worker = "Worker")]
pub struct Stage {
    ledger: ApplyDB,
    chain: chain::Store,
    genesis: GenesisFile,

    pub upstream: UpstreamPort,

    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    wal_count: gasket::metrics::Counter,
}

impl Stage {
    pub fn new(ledger: ApplyDB, chain: chain::Store, genesis: GenesisFile) -> Self {
        Self {
            ledger,
            chain,
            genesis,
            upstream: Default::default(),
            // downstream: Default::default(),
            block_count: Default::default(),
            wal_count: Default::default(),
        }
    }
}

impl Stage {}

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
            RollEvent::Apply(slot, hash, cbor) => {
                stage
                    .chain
                    .roll_forward(*slot, *hash, cbor.clone())
                    .or_panic()?;

                stage.ledger.apply_block(cbor).or_panic()?;
            }
            RollEvent::Undo(slot, _, cbor) => {
                stage.chain.roll_back(*slot).or_panic()?;
                stage.ledger.undo_block(cbor).or_panic()?;
            }
            RollEvent::Origin => {
                stage.chain.roll_back_origin().or_panic()?;
                stage.ledger.apply_origin(&stage.genesis).or_panic()?
            }
            RollEvent::Reset(_) => todo!(),
        };

        Ok(())
    }
}
