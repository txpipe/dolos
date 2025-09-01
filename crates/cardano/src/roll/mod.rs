use std::collections::HashMap;

use dolos_core::{InvariantViolation, State3Error, StateDelta, TxoRef};
use pallas::ledger::traverse::{
    MultiEraBlock, MultiEraCert, MultiEraInput, MultiEraOutput, MultiEraPolicyAssets, MultiEraTx,
};

use crate::{
    model::CardanoDelta,
    owned::{OwnedMultiEraBlock, OwnedMultiEraOutput},
};

use super::TrackConfig;

pub mod accounts;
pub mod assets;
pub mod dreps;
pub mod epochs;
pub mod pools;

use accounts::AccountVisitor;
use assets::AssetStateVisitor;
use dreps::DRepStateVisitor;
use epochs::EpochStateVisitor;
use pools::PoolStateVisitor;

pub trait BlockVisitor {
    #[allow(unused_variables)]
    fn visit_root(&mut self, block: &MultiEraBlock) -> Result<(), State3Error> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_input(
        &mut self,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        input: &MultiEraInput,
        resolved: &MultiEraOutput,
    ) -> Result<(), State3Error> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_output(
        &mut self,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        index: u32,
        output: &MultiEraOutput,
    ) -> Result<(), State3Error> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_mint(
        &mut self,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        mint: &MultiEraPolicyAssets,
    ) -> Result<(), State3Error> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn visit_cert(
        &mut self,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        Ok(())
    }
}

pub fn crawl_block<'a, T: BlockVisitor>(
    block: &OwnedMultiEraBlock,
    inputs: &HashMap<TxoRef, OwnedMultiEraOutput>,
    visitor: &mut T,
) -> Result<(), State3Error> {
    let block = block.view();

    visitor.visit_root(block)?;

    for tx in block.txs() {
        for input in tx.consumes() {
            let txoref = TxoRef::from(&input);

            let resolved = inputs.get(&txoref).ok_or_else(|| {
                State3Error::InvariantViolation(InvariantViolation::InputNotFound(txoref))
            })?;

            resolved
                .with_dependent(|_, resolved| visitor.visit_input(block, &tx, &input, &resolved))?;
        }

        for (index, output) in tx.produces() {
            visitor.visit_output(block, &tx, index as u32, &output)?;
        }

        for mint in tx.mints() {
            visitor.visit_mint(block, &tx, &mint)?;
        }

        for cert in tx.certs() {
            visitor.visit_cert(block, &tx, &cert)?;
        }
    }

    Ok(())
}

macro_rules! maybe_visit {
    ($self:expr, $config:ident, $type:tt, $method:ident, $($args:tt)*) => {{
        if $self.config.$config {
            $type::from(&mut $self.delta).$method($($args)*)?;
        }
    }};
}

macro_rules! visit_all {
    ($self:ident, $method:ident, $($args:tt)*) => {
        maybe_visit!($self, account_state, AccountVisitor, $method, $($args)*);
        maybe_visit!($self, asset_state, AssetStateVisitor, $method, $($args)*);
        maybe_visit!($self, epoch_state, EpochStateVisitor, $method, $($args)*);
        maybe_visit!($self, drep_state, DRepStateVisitor, $method, $($args)*);
        maybe_visit!($self, pool_state, PoolStateVisitor, $method, $($args)*);
    };
}

pub struct DeltaBuilder {
    config: TrackConfig,
    delta: StateDelta<CardanoDelta>,
}

impl DeltaBuilder {
    pub fn new(config: TrackConfig) -> Self {
        Self {
            config,
            delta: StateDelta::default(),
        }
    }

    pub fn unwrap(self) -> StateDelta<CardanoDelta> {
        self.delta
    }
}

impl BlockVisitor for DeltaBuilder {
    fn visit_root(&mut self, block: &MultiEraBlock) -> Result<(), State3Error> {
        self.delta.set_cursor(block.slot());

        visit_all!(self, visit_root, block);

        Ok(())
    }

    fn visit_input(
        &mut self,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        input: &MultiEraInput,
        resolved: &MultiEraOutput,
    ) -> Result<(), State3Error> {
        visit_all!(self, visit_input, block, tx, input, resolved);

        Ok(())
    }

    fn visit_output(
        &mut self,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        index: u32,
        output: &MultiEraOutput,
    ) -> Result<(), State3Error> {
        visit_all!(self, visit_output, block, tx, index, output);

        Ok(())
    }

    fn visit_mint(
        &mut self,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        mint: &MultiEraPolicyAssets,
    ) -> Result<(), State3Error> {
        visit_all!(self, visit_mint, block, tx, mint);

        Ok(())
    }

    fn visit_cert(
        &mut self,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        visit_all!(self, visit_cert, block, tx, cert);

        Ok(())
    }
}
