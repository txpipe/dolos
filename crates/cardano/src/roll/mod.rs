use std::{collections::HashMap, sync::Arc};

use dolos_core::{ChainError, InvariantViolation, StateError, TxOrder, TxoRef};

use crate::CardanoDomain;
use pallas::{
    codec::utils::KeepRaw,
    ledger::{
        primitives::{Epoch, PlutusData},
        traverse::{
            MultiEraBlock, MultiEraCert, MultiEraInput, MultiEraOutput, MultiEraPolicyAssets,
            MultiEraProposal, MultiEraRedeemer, MultiEraTx, MultiEraUpdate,
        },
    },
};
use tracing::{debug, instrument};

use crate::{
    load_effective_pparams, owned::OwnedMultiEraOutput, roll::proposals::ProposalVisitor, utxoset,
    Cache, PParamsSet,
};

// Sub-modules
pub mod accounts;
pub mod assets;
pub mod batch;
pub mod datums;
pub mod dreps;
pub mod epochs;
pub mod pools;
pub mod proposals;
pub mod txs;
pub mod work_unit;

// Re-exports
pub use batch::{WorkBatch, WorkBlock, WorkDeltas};
pub use work_unit::RollWorkUnit;

use accounts::AccountVisitor;
use assets::AssetStateVisitor;
use datums::DatumVisitor;
use dreps::DRepStateVisitor;
use epochs::EpochStateVisitor;
use pools::PoolStateVisitor;
use txs::TxLogVisitor;

pub trait BlockVisitor {
    #[allow(unused_variables)]
    #[allow(clippy::too_many_arguments)]
    fn visit_root(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        genesis: &crate::CardanoGenesis,
        pparams: &PParamsSet,
        epoch: Epoch,
        epoch_start: u64,
        protocol: u16,
    ) -> Result<(), ChainError<crate::CardanoError>> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_tx(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        utxos: &HashMap<TxoRef, OwnedMultiEraOutput>,
    ) -> Result<(), ChainError<crate::CardanoError>> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_input(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        input: &MultiEraInput,
        resolved: &MultiEraOutput,
    ) -> Result<(), ChainError<crate::CardanoError>> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_output(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        index: u32,
        output: &MultiEraOutput,
    ) -> Result<(), ChainError<crate::CardanoError>> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_mint(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        mint: &MultiEraPolicyAssets,
    ) -> Result<(), ChainError<crate::CardanoError>> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_cert(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        order: &TxOrder,
        cert: &MultiEraCert,
    ) -> Result<(), ChainError<crate::CardanoError>> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_withdrawal(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        account: &[u8],
        amount: u64,
    ) -> Result<(), ChainError<crate::CardanoError>> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_update(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: Option<&MultiEraTx>,
        update: &MultiEraUpdate,
    ) -> Result<(), ChainError<crate::CardanoError>> {
        Ok(())
    }

    /// Visit plutus data available in the tx witness set. IMPORTANT: this does
    /// not include inline-plutus data (visit the outputs for that).
    #[allow(unused_variables)]
    fn visit_datums(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        data: &KeepRaw<'_, PlutusData>,
    ) -> Result<(), ChainError<crate::CardanoError>> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_proposal(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        proposal: &MultiEraProposal,
        idx: usize,
    ) -> Result<(), ChainError<crate::CardanoError>> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_redeemers(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        proposal: &MultiEraRedeemer,
    ) -> Result<(), ChainError<crate::CardanoError>> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn flush(&mut self, deltas: &mut WorkDeltas) -> Result<(), ChainError<crate::CardanoError>> {
        Ok(())
    }
}

pub struct DeltaBuilder<'a> {
    genesis: Arc<crate::CardanoGenesis>,
    work: &'a mut WorkBlock,
    active_params: &'a PParamsSet,
    epoch: Epoch,
    epoch_start: u64,
    protocol: u16,
    utxos: &'a HashMap<TxoRef, OwnedMultiEraOutput>,

    account_state: AccountVisitor,
    asset_state: AssetStateVisitor,
    datum_state: DatumVisitor,
    drep_state: DRepStateVisitor,
    epoch_state: EpochStateVisitor,
    pool_state: PoolStateVisitor,
    tx_logs: TxLogVisitor,
    proposal_logs: ProposalVisitor,
}

impl<'a> DeltaBuilder<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        genesis: Arc<crate::CardanoGenesis>,
        protocol: u16,
        active_params: &'a PParamsSet,
        epoch: Epoch,
        epoch_start: u64,
        work: &'a mut WorkBlock,
        utxos: &'a HashMap<TxoRef, OwnedMultiEraOutput>,
    ) -> Self {
        Self {
            genesis,
            work,
            active_params,
            epoch,
            epoch_start,
            protocol,
            utxos,
            account_state: Default::default(),
            asset_state: Default::default(),
            datum_state: Default::default(),
            drep_state: Default::default(),
            epoch_state: Default::default(),
            pool_state: Default::default(),
            tx_logs: Default::default(),
            proposal_logs: Default::default(),
        }
    }

    pub fn crawl(&mut self) -> Result<(), ChainError<crate::CardanoError>> {
        let block = self.work.decoded();
        let block = block.view();
        let mut deltas = WorkDeltas::default();

        self.account_state.visit_root(
            &mut deltas,
            block,
            &self.genesis,
            self.active_params,
            self.epoch,
            self.epoch_start,
            self.protocol,
        )?;
        self.asset_state.visit_root(
            &mut deltas,
            block,
            &self.genesis,
            self.active_params,
            self.epoch,
            self.epoch_start,
            self.protocol,
        )?;
        self.datum_state.visit_root(
            &mut deltas,
            block,
            &self.genesis,
            self.active_params,
            self.epoch,
            self.epoch_start,
            self.protocol,
        )?;
        self.drep_state.visit_root(
            &mut deltas,
            block,
            &self.genesis,
            self.active_params,
            self.epoch,
            self.epoch_start,
            self.protocol,
        )?;
        self.epoch_state.visit_root(
            &mut deltas,
            block,
            &self.genesis,
            self.active_params,
            self.epoch,
            self.epoch_start,
            self.protocol,
        )?;
        self.pool_state.visit_root(
            &mut deltas,
            block,
            &self.genesis,
            self.active_params,
            self.epoch,
            self.epoch_start,
            self.protocol,
        )?;
        self.tx_logs.visit_root(
            &mut deltas,
            block,
            &self.genesis,
            self.active_params,
            self.epoch,
            self.epoch_start,
            self.protocol,
        )?;
        self.proposal_logs.visit_root(
            &mut deltas,
            block,
            &self.genesis,
            self.active_params,
            self.epoch,
            self.epoch_start,
            self.protocol,
        )?;

        for (order, tx) in block.txs().iter().enumerate() {
            self.account_state
                .visit_tx(&mut deltas, block, tx, self.utxos)?;
            self.asset_state
                .visit_tx(&mut deltas, block, tx, self.utxos)?;
            self.datum_state
                .visit_tx(&mut deltas, block, tx, self.utxos)?;
            self.drep_state
                .visit_tx(&mut deltas, block, tx, self.utxos)?;
            self.epoch_state
                .visit_tx(&mut deltas, block, tx, self.utxos)?;
            self.pool_state
                .visit_tx(&mut deltas, block, tx, self.utxos)?;
            self.tx_logs.visit_tx(&mut deltas, block, tx, self.utxos)?;
            self.proposal_logs
                .visit_tx(&mut deltas, block, tx, self.utxos)?;

            for input in tx.consumes() {
                let txoref = crate::txo_ref_from_input(&input);

                let resolved = self
                    .utxos
                    .get(&txoref)
                    .ok_or(StateError::InvariantViolation(
                        InvariantViolation::InputNotFound(txoref),
                    ))?;

                resolved.with_dependent(|_, resolved| {
                    self.account_state
                        .visit_input(&mut deltas, block, tx, &input, resolved)?;
                    self.asset_state
                        .visit_input(&mut deltas, block, tx, &input, resolved)?;
                    self.datum_state
                        .visit_input(&mut deltas, block, tx, &input, resolved)?;
                    self.drep_state
                        .visit_input(&mut deltas, block, tx, &input, resolved)?;
                    self.epoch_state
                        .visit_input(&mut deltas, block, tx, &input, resolved)?;
                    self.pool_state
                        .visit_input(&mut deltas, block, tx, &input, resolved)?;
                    self.tx_logs
                        .visit_input(&mut deltas, block, tx, &input, resolved)?;
                    self.proposal_logs
                        .visit_input(&mut deltas, block, tx, &input, resolved)?;
                    Result::<_, ChainError<crate::CardanoError>>::Ok(())
                })?;
            }

            for (index, output) in tx.produces() {
                self.account_state
                    .visit_output(&mut deltas, block, tx, index as u32, &output)?;
                self.asset_state
                    .visit_output(&mut deltas, block, tx, index as u32, &output)?;
                self.datum_state
                    .visit_output(&mut deltas, block, tx, index as u32, &output)?;
                self.drep_state
                    .visit_output(&mut deltas, block, tx, index as u32, &output)?;
                self.epoch_state
                    .visit_output(&mut deltas, block, tx, index as u32, &output)?;
                self.pool_state
                    .visit_output(&mut deltas, block, tx, index as u32, &output)?;
                self.tx_logs
                    .visit_output(&mut deltas, block, tx, index as u32, &output)?;
                self.proposal_logs
                    .visit_output(&mut deltas, block, tx, index as u32, &output)?;
            }

            for mint in tx.mints() {
                self.account_state
                    .visit_mint(&mut deltas, block, tx, &mint)?;
                self.asset_state.visit_mint(&mut deltas, block, tx, &mint)?;
                self.datum_state.visit_mint(&mut deltas, block, tx, &mint)?;
                self.drep_state.visit_mint(&mut deltas, block, tx, &mint)?;
                self.epoch_state.visit_mint(&mut deltas, block, tx, &mint)?;
                self.pool_state.visit_mint(&mut deltas, block, tx, &mint)?;
                self.tx_logs.visit_mint(&mut deltas, block, tx, &mint)?;
                self.proposal_logs
                    .visit_mint(&mut deltas, block, tx, &mint)?;
            }

            for cert in tx.certs() {
                self.account_state
                    .visit_cert(&mut deltas, block, tx, &order, &cert)?;
                self.asset_state
                    .visit_cert(&mut deltas, block, tx, &order, &cert)?;
                self.datum_state
                    .visit_cert(&mut deltas, block, tx, &order, &cert)?;
                self.drep_state
                    .visit_cert(&mut deltas, block, tx, &order, &cert)?;
                self.epoch_state
                    .visit_cert(&mut deltas, block, tx, &order, &cert)?;
                self.pool_state
                    .visit_cert(&mut deltas, block, tx, &order, &cert)?;
                self.tx_logs
                    .visit_cert(&mut deltas, block, tx, &order, &cert)?;
                self.proposal_logs
                    .visit_cert(&mut deltas, block, tx, &order, &cert)?;
            }

            for (account, amount) in tx.withdrawals().collect::<Vec<_>>() {
                self.account_state
                    .visit_withdrawal(&mut deltas, block, tx, account, amount)?;
                self.asset_state
                    .visit_withdrawal(&mut deltas, block, tx, account, amount)?;
                self.datum_state
                    .visit_withdrawal(&mut deltas, block, tx, account, amount)?;
                self.drep_state
                    .visit_withdrawal(&mut deltas, block, tx, account, amount)?;
                self.epoch_state
                    .visit_withdrawal(&mut deltas, block, tx, account, amount)?;
                self.pool_state
                    .visit_withdrawal(&mut deltas, block, tx, account, amount)?;
                self.tx_logs
                    .visit_withdrawal(&mut deltas, block, tx, account, amount)?;
                self.proposal_logs
                    .visit_withdrawal(&mut deltas, block, tx, account, amount)?;
            }

            if let Some(update) = tx.update() {
                self.account_state
                    .visit_update(&mut deltas, block, Some(tx), &update)?;
                self.asset_state
                    .visit_update(&mut deltas, block, Some(tx), &update)?;
                self.datum_state
                    .visit_update(&mut deltas, block, Some(tx), &update)?;
                self.drep_state
                    .visit_update(&mut deltas, block, Some(tx), &update)?;
                self.epoch_state
                    .visit_update(&mut deltas, block, Some(tx), &update)?;
                self.pool_state
                    .visit_update(&mut deltas, block, Some(tx), &update)?;
                self.tx_logs
                    .visit_update(&mut deltas, block, Some(tx), &update)?;
                self.proposal_logs
                    .visit_update(&mut deltas, block, Some(tx), &update)?;
            }

            for datum in tx.plutus_data() {
                self.account_state
                    .visit_datums(&mut deltas, block, tx, datum)?;
                self.asset_state
                    .visit_datums(&mut deltas, block, tx, datum)?;
                self.datum_state
                    .visit_datums(&mut deltas, block, tx, datum)?;
                self.drep_state
                    .visit_datums(&mut deltas, block, tx, datum)?;
                self.epoch_state
                    .visit_datums(&mut deltas, block, tx, datum)?;
                self.pool_state
                    .visit_datums(&mut deltas, block, tx, datum)?;
                self.tx_logs.visit_datums(&mut deltas, block, tx, datum)?;
                self.proposal_logs
                    .visit_datums(&mut deltas, block, tx, datum)?;
            }

            for (idx, proposal) in tx.gov_proposals().iter().enumerate() {
                self.account_state
                    .visit_proposal(&mut deltas, block, tx, proposal, idx)?;
                self.asset_state
                    .visit_proposal(&mut deltas, block, tx, proposal, idx)?;
                self.datum_state
                    .visit_proposal(&mut deltas, block, tx, proposal, idx)?;
                self.drep_state
                    .visit_proposal(&mut deltas, block, tx, proposal, idx)?;
                self.epoch_state
                    .visit_proposal(&mut deltas, block, tx, proposal, idx)?;
                self.pool_state
                    .visit_proposal(&mut deltas, block, tx, proposal, idx)?;
                self.tx_logs
                    .visit_proposal(&mut deltas, block, tx, proposal, idx)?;
                self.proposal_logs
                    .visit_proposal(&mut deltas, block, tx, proposal, idx)?;
            }

            for redeemer in tx.redeemers() {
                self.account_state
                    .visit_redeemers(&mut deltas, block, tx, &redeemer)?;
                self.asset_state
                    .visit_redeemers(&mut deltas, block, tx, &redeemer)?;
                self.datum_state
                    .visit_redeemers(&mut deltas, block, tx, &redeemer)?;
                self.drep_state
                    .visit_redeemers(&mut deltas, block, tx, &redeemer)?;
                self.epoch_state
                    .visit_redeemers(&mut deltas, block, tx, &redeemer)?;
                self.pool_state
                    .visit_redeemers(&mut deltas, block, tx, &redeemer)?;
                self.tx_logs
                    .visit_redeemers(&mut deltas, block, tx, &redeemer)?;
                self.proposal_logs
                    .visit_redeemers(&mut deltas, block, tx, &redeemer)?;
            }
        }

        if let Some(update) = block.update() {
            self.account_state
                .visit_update(&mut deltas, block, None, &update)?;
            self.asset_state
                .visit_update(&mut deltas, block, None, &update)?;
            self.datum_state
                .visit_update(&mut deltas, block, None, &update)?;
            self.drep_state
                .visit_update(&mut deltas, block, None, &update)?;
            self.epoch_state
                .visit_update(&mut deltas, block, None, &update)?;
            self.pool_state
                .visit_update(&mut deltas, block, None, &update)?;
            self.tx_logs
                .visit_update(&mut deltas, block, None, &update)?;
            self.proposal_logs
                .visit_update(&mut deltas, block, None, &update)?;
        }

        self.account_state.flush(&mut deltas)?;
        self.asset_state.flush(&mut deltas)?;
        self.datum_state.flush(&mut deltas)?;
        self.drep_state.flush(&mut deltas)?;
        self.epoch_state.flush(&mut deltas)?;
        self.pool_state.flush(&mut deltas)?;
        self.tx_logs.flush(&mut deltas)?;
        self.proposal_logs.flush(&mut deltas)?;

        self.work.deltas = deltas;

        Ok(())
    }
}

#[instrument(name = "roll", skip_all)]
pub fn compute_delta<D: CardanoDomain>(
    genesis: Arc<crate::CardanoGenesis>,
    cache: &Cache,
    state: &D::State,
    batch: &mut WorkBatch,
) -> Result<(), ChainError<crate::CardanoError>> {
    let (epoch, _) = cache.eras.slot_epoch(batch.first_slot());

    let (protocol, _) = cache.eras.protocol_and_era_for_epoch(epoch);
    let epoch_start = cache.eras.epoch_start(epoch);

    debug!(
        from = batch.first_slot(),
        to = batch.last_slot(),
        epoch,
        "computing delta"
    );

    let active_params = load_effective_pparams::<D>(state)?;

    for block in batch.blocks.iter_mut() {
        let mut builder = DeltaBuilder::new(
            genesis.clone(),
            *protocol,
            &active_params,
            epoch,
            epoch_start,
            block,
            &batch.utxos_decoded,
        );

        builder.crawl()?;

        // TODO: we treat the UTxO set differently due to tech-debt. We should migrate
        // this into the entity system.
        let blockd = block.decoded();
        let blockd = blockd.view();
        let utxos = utxoset::compute_apply_delta(blockd, &batch.utxos_decoded)?;
        block.utxo_delta = Some(utxos);
    }

    Ok(())
}
