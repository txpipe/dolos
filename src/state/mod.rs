use itertools::Itertools as _;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraTx};
use std::collections::{HashMap, HashSet};

use dolos_cardano::{compute_delta, lastest_immutable_slot};
use dolos_core::{Genesis, StateStore};

use crate::prelude::*;

pub mod redb;

/// A persistent store for ledger state
#[derive(Clone)]
#[non_exhaustive]
pub enum LedgerStore {
    Redb(redb::LedgerStore),
}

impl StateStore for LedgerStore {
    fn cursor(&self) -> Result<Option<ChainPoint>, StateError> {
        let out = match self {
            LedgerStore::Redb(x) => x.cursor()?,
        };

        Ok(out)
    }

    fn is_empty(&self) -> Result<bool, StateError> {
        let out = match self {
            LedgerStore::Redb(x) => x.is_empty()?,
        };

        Ok(out)
    }

    fn get_pparams(&self, until: BlockSlot) -> Result<Vec<EraCbor>, StateError> {
        let out = match self {
            LedgerStore::Redb(x) => x.get_pparams(until)?,
        };

        Ok(out)
    }

    fn get_utxos(&self, refs: Vec<TxoRef>) -> Result<UtxoMap, StateError> {
        let out = match self {
            LedgerStore::Redb(x) => x.get_utxos(refs)?,
        };

        Ok(out)
    }

    fn get_utxo_by_address(&self, address: &[u8]) -> Result<UtxoSet, StateError> {
        let out = match self {
            LedgerStore::Redb(x) => x.get_utxo_by_address(address)?,
        };

        Ok(out)
    }

    fn get_utxo_by_payment(&self, payment: &[u8]) -> Result<UtxoSet, StateError> {
        let out = match self {
            LedgerStore::Redb(x) => x.get_utxo_by_payment(payment)?,
        };

        Ok(out)
    }

    fn get_utxo_by_stake(&self, stake: &[u8]) -> Result<UtxoSet, StateError> {
        let out = match self {
            LedgerStore::Redb(x) => x.get_utxo_by_stake(stake)?,
        };

        Ok(out)
    }

    fn get_utxo_by_policy(&self, policy: &[u8]) -> Result<UtxoSet, StateError> {
        let out = match self {
            LedgerStore::Redb(x) => x.get_utxo_by_policy(policy)?,
        };

        Ok(out)
    }

    fn get_utxo_by_asset(&self, asset: &[u8]) -> Result<UtxoSet, StateError> {
        let out = match self {
            LedgerStore::Redb(x) => x.get_utxo_by_asset(asset)?,
        };

        Ok(out)
    }

    fn apply(&self, deltas: &[LedgerDelta]) -> Result<(), StateError> {
        match self {
            LedgerStore::Redb(x) => x.apply(deltas)?,
        };

        Ok(())
    }

    fn finalize(&self, until: BlockSlot) -> Result<(), StateError> {
        match self {
            LedgerStore::Redb(x) => x.finalize(until)?,
        };

        Ok(())
    }

    fn upgrade(self) -> Result<Self, StateError> {
        let out = match self {
            LedgerStore::Redb(x) => LedgerStore::Redb(x.upgrade()?),
        };

        Ok(out)
    }

    fn copy(&self, target: &Self) -> Result<(), StateError> {
        match (self, target) {
            (Self::Redb(x), Self::Redb(target)) => x.copy(target)?,
        }

        Ok(())
    }
}

impl From<redb::LedgerStore> for LedgerStore {
    fn from(value: redb::LedgerStore) -> Self {
        Self::Redb(value)
    }
}

impl pallas::interop::utxorpc::LedgerContext for LedgerStore {
    fn get_utxos<'a>(
        &self,
        refs: &[pallas::interop::utxorpc::TxoRef],
    ) -> Option<pallas::interop::utxorpc::UtxoMap> {
        let refs: Vec<_> = refs.iter().map(|x| TxoRef::from(*x)).collect();

        let some = dolos_core::StateStore::get_utxos(self, refs)
            .ok()?
            .into_iter()
            .map(|(k, v)| {
                let era = v.0.try_into().expect("era out of range");
                (k.into(), (era, v.1))
            })
            .collect();

        Some(some)
    }
}

pub fn load_slice_for_block(
    block: &MultiEraBlock,
    store: &LedgerStore,
    unapplied_deltas: &[LedgerDelta],
) -> Result<LedgerSlice, StateError> {
    let txs: HashMap<_, _> = block.txs().into_iter().map(|tx| (tx.hash(), tx)).collect();

    // TODO: turn this into "referenced utxos" intead of just consumed.
    let consumed: HashSet<_> = txs
        .values()
        .flat_map(MultiEraTx::consumes)
        .map(|utxo| TxoRef(*utxo.hash(), utxo.index() as u32))
        .collect();

    let consumed_same_block: HashMap<_, _> = txs
        .iter()
        .flat_map(|(tx_hash, tx)| {
            tx.produces()
                .into_iter()
                .map(|(idx, utxo)| (TxoRef(*tx_hash, idx as u32), utxo.into()))
        })
        .filter(|(x, _)| consumed.contains(x))
        .collect();

    let consumed_unapplied_deltas: HashMap<_, _> = unapplied_deltas
        .iter()
        .flat_map(|d| d.produced_utxo.iter().chain(d.recovered_stxi.iter()))
        .filter(|(x, _)| consumed.contains(x))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    let to_fetch = consumed
        .into_iter()
        .filter(|x| !consumed_same_block.contains_key(x))
        .filter(|x| !consumed_unapplied_deltas.contains_key(x))
        .collect_vec();

    let mut resolved_inputs = store.get_utxos(to_fetch)?;
    resolved_inputs.extend(consumed_same_block);
    resolved_inputs.extend(consumed_unapplied_deltas);

    // TODO: include reference scripts and collateral

    Ok(LedgerSlice { resolved_inputs })
}

pub fn calculate_block_batch_deltas<'a>(
    blocks: impl IntoIterator<Item = &'a MultiEraBlock<'a>>,
    store: &LedgerStore,
) -> Result<Vec<LedgerDelta>, StateError> {
    let mut deltas: Vec<LedgerDelta> = vec![];

    for block in blocks {
        let context = load_slice_for_block(block, store, &deltas)?;
        let delta = compute_delta(block, context).map_err(StateError::BrokenInvariant)?;

        deltas.push(delta);
    }
    Ok(deltas)
}

pub fn apply_delta_batch(
    deltas: Vec<LedgerDelta>,
    store: &LedgerStore,
    genesis: &Genesis,
    max_ledger_history: Option<u64>,
) -> Result<(), StateError> {
    store.apply(&deltas)?;

    let tip = deltas
        .last()
        .and_then(|x| x.new_position.as_ref())
        .map(|x| x.0)
        .unwrap();

    let to_finalize = max_ledger_history
        .map(|x| tip - x)
        .unwrap_or(lastest_immutable_slot(tip, genesis));

    store.finalize(to_finalize)?;

    Ok(())
}

pub fn apply_block_batch<'a>(
    blocks: impl IntoIterator<Item = &'a MultiEraBlock<'a>>,
    store: &LedgerStore,
    genesis: &Genesis,
    max_ledger_history: Option<u64>,
) -> Result<(), StateError> {
    let deltas = calculate_block_batch_deltas(blocks, store)?;
    apply_delta_batch(deltas, store, genesis, max_ledger_history)
}
