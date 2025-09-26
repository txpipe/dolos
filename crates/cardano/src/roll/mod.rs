use std::collections::HashMap;

use dolos_core::{
    batch::{WorkBlock, WorkDeltas},
    ChainError, InvariantViolation, StateError, TxoRef,
};
use pallas::{
    codec::utils::KeepRaw,
    ledger::{
        primitives::PlutusData,
        traverse::{
            MultiEraBlock, MultiEraCert, MultiEraInput, MultiEraOutput, MultiEraPolicyAssets,
            MultiEraTx, MultiEraUpdate,
        },
    },
};

use crate::{owned::OwnedMultiEraOutput, CardanoLogic, PParamsSet};

use super::TrackConfig;

pub mod accounts;
pub mod assets;
pub mod dreps;
pub mod epochs;
pub mod pools;
pub mod txs;

use accounts::AccountVisitor;
use assets::AssetStateVisitor;
use dreps::DRepStateVisitor;
use epochs::EpochStateVisitor;
use pools::PoolStateVisitor;
use txs::TxLogVisitor;

pub trait BlockVisitor {
    #[allow(unused_variables)]
    fn visit_root(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        block: &MultiEraBlock,
        pparams: &PParamsSet,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_tx(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_input(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        input: &MultiEraInput,
        resolved: &MultiEraOutput,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_output(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        index: u32,
        output: &MultiEraOutput,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_mint(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        mint: &MultiEraPolicyAssets,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_cert(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_withdrawal(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        account: &[u8],
        amount: u64,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_update(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        block: &MultiEraBlock,
        tx: Option<&MultiEraTx>,
        update: &MultiEraUpdate,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    /// Visit plutus data available in the tx witness set. IMPORTANT: this does
    /// not include inline-plutus data (visit the outputs for that).
    #[allow(unused_variables)]
    fn visit_datums(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        data: &KeepRaw<'_, PlutusData>,
    ) -> Result<(), ChainError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn flush(&mut self, deltas: &mut WorkDeltas<CardanoLogic>) -> Result<(), ChainError> {
        Ok(())
    }
}

macro_rules! maybe_visit {
    ($self:expr, $deltas:expr, $visitor:ident, $method:ident, $($args:tt)*) => {{
        if $self.config.$visitor {
            $self.$visitor.$method(&mut $deltas, $($args)*)?;
        }
    }};
}

macro_rules! visit_all {
    ($self:ident, $deltas:expr, $method:ident, $($args:tt)*) => {
        maybe_visit!($self, $deltas, account_state, $method, $($args)*);
        maybe_visit!($self, $deltas, asset_state, $method, $($args)*);
        maybe_visit!($self, $deltas, drep_state, $method, $($args)*);
        maybe_visit!($self, $deltas, epoch_state, $method, $($args)*);
        maybe_visit!($self, $deltas, pool_state, $method, $($args)*);
        maybe_visit!($self, $deltas, tx_logs, $method, $($args)*);
    };
}

pub struct DeltaBuilder<'a> {
    config: TrackConfig,
    work: &'a mut WorkBlock<CardanoLogic>,
    active_params: &'a PParamsSet,
    utxos: &'a HashMap<TxoRef, OwnedMultiEraOutput>,

    account_state: AccountVisitor,
    asset_state: AssetStateVisitor,
    drep_state: DRepStateVisitor,
    epoch_state: EpochStateVisitor,
    pool_state: PoolStateVisitor,
    tx_logs: TxLogVisitor,
}

impl<'a> DeltaBuilder<'a> {
    pub fn new(
        config: TrackConfig,
        active_params: &'a PParamsSet,
        work: &'a mut WorkBlock<CardanoLogic>,
        utxos: &'a HashMap<TxoRef, OwnedMultiEraOutput>,
    ) -> Self {
        Self {
            config,
            work,
            active_params,
            utxos,
            account_state: Default::default(),
            asset_state: Default::default(),
            drep_state: Default::default(),
            epoch_state: Default::default(),
            pool_state: Default::default(),
            tx_logs: Default::default(),
        }
    }

    pub fn crawl(&mut self) -> Result<(), ChainError> {
        let block = self.work.unwrap_decoded();
        let block = block.view();
        let mut deltas = WorkDeltas::default();

        visit_all!(self, deltas, visit_root, block, self.active_params);

        for tx in block.txs() {
            visit_all!(self, deltas, visit_tx, block, &tx);

            for input in tx.consumes() {
                let txoref = TxoRef::from(&input);

                let resolved = self.utxos.get(&txoref).ok_or_else(|| {
                    StateError::InvariantViolation(InvariantViolation::InputNotFound(txoref))
                })?;

                resolved.with_dependent(|_, resolved| {
                    visit_all!(self, deltas, visit_input, block, &tx, &input, &resolved);
                    Result::<_, ChainError>::Ok(())
                })?;
            }

            for (index, output) in tx.produces() {
                visit_all!(
                    self,
                    deltas,
                    visit_output,
                    block,
                    &tx,
                    index as u32,
                    &output
                );
            }

            for mint in tx.mints() {
                visit_all!(self, deltas, visit_mint, block, &tx, &mint);
            }

            for cert in tx.certs() {
                visit_all!(self, deltas, visit_cert, block, &tx, &cert,);
            }

            for (account, amount) in tx.withdrawals().collect::<Vec<_>>() {
                visit_all!(self, deltas, visit_withdrawal, block, &tx, &account, amount);
            }

            if let Some(update) = tx.update() {
                visit_all!(self, deltas, visit_update, block, Some(&tx), &update);
            }

            for datum in tx.plutus_data() {
                visit_all!(self, deltas, visit_datums, block, &tx, &datum);
            }
        }

        if let Some(update) = block.update() {
            visit_all!(self, deltas, visit_update, block, None, &update);
        }

        visit_all!(self, deltas, flush,);

        self.work.deltas = deltas;

        Ok(())
    }
}
