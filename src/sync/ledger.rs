use gasket::framework::*;
use pallas::applying::{validate, Environment, UTxOs};
use pallas::ledger::configs::{byron, shelley};
use pallas::ledger::traverse::wellknown::GenesisValues;
use pallas::ledger::traverse::{Era, MultiEraBlock, MultiEraInput, MultiEraOutput};
use std::borrow::Cow;
use std::collections::HashMap;
use tracing::{debug, info, warn};

use crate::prelude::*;
use crate::storage::applydb::ApplyDB;

pub fn execute_phase1_validation(
    ledger: &ApplyDB,
    block: &MultiEraBlock<'_>,
) -> Result<(), WorkerError> {
    let mut utxos = HashMap::new();
    ledger
        .resolve_inputs_for_block(block, &mut utxos)
        .or_panic()?;

    let mut utxos2 = UTxOs::new();

    for (ref_, output) in utxos.iter() {
        let txin = pallas::ledger::primitives::byron::TxIn::Variant0(
            pallas::codec::utils::CborWrap((ref_.0, ref_.1 as u32)),
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
        let res = validate(tx, &utxos2, &env);

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
    byron: byron::GenesisFile,
    shelley: shelley::GenesisFile,

    current_pparams: Option<(u64, Environment)>,

    // HACK: until multi-era genesis
    genesis_values: GenesisValues,

    phase1_validation_enabled: bool,

    pub upstream: UpstreamPort,

    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    wal_count: gasket::metrics::Counter,
}

impl Stage {
    pub fn new(
        ledger: ApplyDB,
        byron: byron::GenesisFile,
        shelley: shelley::GenesisFile,
        phase1_validation_enabled: bool,
    ) -> Self {
        Self {
            ledger,
            genesis_values: pallas::ledger::traverse::wellknown::GenesisValues::from_magic(
                byron.protocol_consts.protocol_magic as u64,
            )
            .unwrap(),
            byron,
            shelley,
            current_pparams: None,
            phase1_validation_enabled,
            upstream: Default::default(),
            block_count: Default::default(),
            wal_count: Default::default(),
        }
    }

    fn ensure_pparams(&mut self, epoch: u64) -> Result<(), WorkerError> {
        if self
            .current_pparams
            .as_ref()
            .is_some_and(|(current, _)| *current == epoch)
        {
            return Ok(());
        }

        let pparams = super::pparams::compute_pparams(
            super::pparams::Genesis {
                byron: &self.byron,
                shelley: &self.shelley,
            },
            &self.ledger,
            epoch,
        )?;

        warn!(?pparams, "pparams for new epoch");

        self.current_pparams = Some((epoch, pparams));

        Ok(())
    }

    pub fn execute_phase1_validation(&self, block: &MultiEraBlock<'_>) -> Result<(), WorkerError> {
        let mut utxos = HashMap::new();
        self.ledger
            .resolve_inputs_for_block(block, &mut utxos)
            .or_panic()?;

        let mut utxos2 = UTxOs::new();

        for (ref_, output) in utxos.iter() {
            let txin = pallas::ledger::primitives::byron::TxIn::Variant0(
                pallas::codec::utils::CborWrap((ref_.0, ref_.1 as u32)),
            );

            let key = MultiEraInput::Byron(
                <Box<Cow<'_, pallas::ledger::primitives::byron::TxIn>>>::from(Cow::Owned(txin)),
            );

            let era = Era::try_from(output.0).or_panic()?;
            let value = MultiEraOutput::decode(era, &output.1).or_panic()?;

            utxos2.insert(key, value);
        }

        let pparams = self
            .current_pparams
            .as_ref()
            .map(|(_, x)| x)
            .ok_or("no pparams available")
            .or_panic()?;

        for tx in block.txs().iter() {
            let res = validate(tx, &utxos2, pparams);

            if let Err(err) = res {
                warn!(?err, "validation error");
            }
        }

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
            RollEvent::Apply(slot, _, cbor) => {
                info!(slot, "applying block");

                let block = MultiEraBlock::decode(cbor).or_panic()?;

                if stage.phase1_validation_enabled {
                    debug!("performing phase-1 validations");
                    let (epoch, _) = block.epoch(&stage.genesis_values);
                    stage.ensure_pparams(epoch)?;
                    stage.execute_phase1_validation(&block)?;
                }

                stage.ledger.apply_block(&block).or_panic()?;
            }
            RollEvent::Undo(slot, _, cbor) => {
                info!(slot, "undoing block");
                stage.ledger.undo_block(cbor).or_panic()?;
            }
            RollEvent::Origin => {
                info!("applying origin");
                stage.ledger.apply_origin(&stage.byron).or_panic()?;
            }
        };

        Ok(())
    }
}
