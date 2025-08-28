use std::collections::HashMap;

use dolos_core::{
    EraCbor, InvariantViolation, LedgerSlice, State3Error, State3Store, StateDelta, StateSlice,
    StateSliceView, TxoRef,
};
use pallas::ledger::{
    addresses::Network,
    traverse::{
        Era, MultiEraBlock, MultiEraCert, MultiEraInput, MultiEraOutput, MultiEraPolicyAssets,
        MultiEraTx,
    },
};

use rayon::iter::{IntoParallelIterator as _, ParallelIterator as _};

use crate::pparams::ChainSummary;

use super::TrackConfig;

mod accounts;
mod assets;
mod epochs;
mod pools;

use accounts::{AccountActivityVisitor, DelegationVisitor, SeenAddressesVisitor};
use assets::AssetStateVisitor;
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

fn load_input<'a>(
    input: MultiEraInput<'a>,
    utxo_slice: &'a LedgerSlice,
) -> Result<(MultiEraInput<'a>, MultiEraOutput<'a>), State3Error> {
    let txoref = TxoRef::from(&input);

    let EraCbor(era, cbor) = utxo_slice
        .resolved_inputs
        .get(&txoref)
        .ok_or(InvariantViolation::InputNotFound(txoref))?;

    let era = Era::try_from(*era)?;

    let resolved = MultiEraOutput::decode(era, cbor)?;

    Ok((input, resolved))
}

pub fn crawl_block<'a, T: BlockVisitor>(
    block: &MultiEraBlock<'a>,
    utxo_slice: &LedgerSlice,
    visitor: &mut T,
) -> Result<(), State3Error> {
    visitor.visit_root(block)?;

    for tx in block.txs() {
        let consumed = tx
            .consumes()
            .into_par_iter()
            .map(|input| load_input(input, utxo_slice))
            .collect::<Result<Vec<_>, _>>()?;

        for (input, resolved) in consumed {
            visitor.visit_input(block, &tx, &input, &resolved)?;
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
            $type::from(&mut *$self).$method($($args)*)?;
        }
    }};
}

macro_rules! visit_all {
    ($self:ident, $method:ident, $($args:tt)*) => {
        maybe_visit!($self, seen_addresses, SeenAddressesVisitor, $method, $($args)*);
        maybe_visit!($self, asset_state, AssetStateVisitor, $method, $($args)*);
        maybe_visit!($self, pool_state, PoolStateVisitor, $method, $($args)*);
        maybe_visit!($self, pool_delegator, DelegationVisitor, $method, $($args)*);
        maybe_visit!($self, epoch_state, EpochStateVisitor, $method, $($args)*);
        maybe_visit!($self, account_activity, AccountActivityVisitor, $method, $($args)*);
    };
}

pub struct DeltaBuilder<'a> {
    config: &'a TrackConfig,
    state: StateSliceView<'a>,
    delta: StateDelta,
    network: Network,
}

impl<'a> DeltaBuilder<'a> {
    pub fn new(
        config: &'a TrackConfig,
        state: StateSliceView<'a>,
        delta: StateDelta,
        network: Network,
    ) -> Self {
        Self {
            config,
            state,
            delta,
            network,
        }
    }

    pub fn delta_mut(&mut self) -> &mut StateDelta {
        &mut self.delta
    }

    pub fn slice(&self) -> &StateSliceView<'a> {
        &self.state
    }

    pub fn build(self) -> StateDelta {
        self.delta
    }
}

impl<'a> BlockVisitor for DeltaBuilder<'a> {
    fn visit_root(&mut self, block: &MultiEraBlock) -> Result<(), State3Error> {
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

pub struct SliceBuilder<'a, S: State3Store> {
    config: &'a TrackConfig,
    store: &'a S,
    slice: StateSliceView<'a>,
    network: Network,
    chain_summary: std::sync::Arc<ChainSummary>,
}

impl<'a, S: State3Store> SliceBuilder<'a, S> {
    pub fn new(
        config: &'a TrackConfig,
        store: &'a S,
        unapplied_deltas: &'a [StateDelta],
        network: Network,
        chain_summary: std::sync::Arc<ChainSummary>,
    ) -> Self {
        Self {
            config,
            store,
            slice: StateSliceView::new(StateSlice::default(), unapplied_deltas),
            network,
            chain_summary,
        }
    }

    pub fn store(&self) -> &S {
        self.store
    }

    pub fn build(self) -> StateSlice {
        self.slice.unwrap()
    }
}

impl<'a, S: State3Store> BlockVisitor for SliceBuilder<'a, S> {
    fn visit_root(&mut self, block: &MultiEraBlock) -> Result<(), State3Error> {
        visit_all!(self, visit_root, block);

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
