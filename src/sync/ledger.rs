use std::borrow::Cow;
use std::collections::HashMap;

use gasket::framework::*;
use pallas::applying::{validate, UTxOs};
use pallas::ledger::configs::byron::GenesisFile;
use pallas::ledger::traverse::{Era, MultiEraBlock, MultiEraInput, MultiEraOutput};
use tracing::{info, warn};

use crate::prelude::*;
use crate::storage::applydb::ApplyDB;

pub fn execute_phase1_validation(
    ledger: &ApplyDB,
    block: &MultiEraBlock<'_>,
) -> Result<(), WorkerError> {
    let mut utxos = HashMap::new();
    ledger
        .resolve_inputs_for_block(&block, &mut utxos)
        .or_panic()?;

    let mut utxos2 = UTxOs::new();

    for (ref_, output) in utxos.iter() {
        let txin = pallas::ledger::primitives::byron::TxIn::Variant0(
            pallas::codec::utils::CborWrap((ref_.0.clone(), ref_.1 as u32)),
        );

        let key = MultiEraInput::Byron(
            <Box<Cow<'_, pallas::ledger::primitives::byron::TxIn>>>::from(Cow::Owned(txin)),
        );

        let era = Era::try_from(output.0).or_panic()?;
        let value = MultiEraOutput::decode(era, &output.1).or_panic()?;

        utxos2.insert(key, value);
    }

    let env = ledger.get_active_pparams(block.slot()).or_panic()?;

    for tx in block.txs().iter() {
        let res = validate(&tx, &utxos2, &env);

        if let Err(err) = res {
            warn!(?err, "validation error");
        }
    }

    Ok(())
}

pub type UpstreamPort = gasket::messaging::tokio::InputPort<RollEvent>;

#[derive(Stage)]
#[stage(name = "ledger", unit = "RollEvent", worker = "Worker")]
pub struct Stage {
    ledger: ApplyDB,
    genesis: GenesisFile,

    pub upstream: UpstreamPort,

    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    wal_count: gasket::metrics::Counter,
}

impl Stage {
    pub fn new(ledger: ApplyDB, genesis: GenesisFile) -> Self {
        Self {
            ledger,
            genesis,
            upstream: Default::default(),
            block_count: Default::default(),
            wal_count: Default::default(),
        }
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
            RollEvent::Apply(slot, _, cbor) => {
                info!(slot, "applying block");

                let block = MultiEraBlock::decode(cbor).or_panic()?;

                execute_phase1_validation(&stage.ledger, &block)?;

                stage.ledger.apply_block(&block).or_panic()?;
            }
            RollEvent::Undo(slot, _, cbor) => {
                info!(slot, "undoing block");
                stage.ledger.undo_block(cbor).or_panic()?;
            }
            RollEvent::Origin => {
                info!("applying origin");
                stage.ledger.apply_origin(&stage.genesis).or_panic()?;
            }
        };

        Ok(())
    }
}
