//! The Shelley→Allegra AVVM reclamation.
//!
//! Crossing into Allegra the Haskell ledger's `translateEra` deletes every
//! unredeemed Byron genesis AVVM UTxO and returns its value to `reserves`.
//! The two halves are one event, and [`AvvmReclamation`] is what keeps them
//! one: the pot arithmetic in [`crate::pots::apply_delta`] takes its
//! `avvm_reclamation` from [`AvvmReclamation::total`] and the deletion staged
//! in `WorkContext::commit_finalize` takes its refs from
//! [`AvvmReclamation::utxos`], both from the same read of the UTxO set, in the
//! same commit. A store cannot end up with one applied and not the other.
//!
//! The refs are derived from the Byron genesis on every run — never from a
//! recorded list, which is a census of one tip rather than an input. A network
//! whose Byron genesis carries an empty `avvmDistr` (preprod and preview: their
//! whole supply sits in `nonAvvmBalances`) derives nothing, so the reclamation
//! is a no-op there and mainnet is the only network on which it has ever moved
//! anything.
//!
//! # Rollback
//!
//! The deletion carries no `recovered_stxi` / `undone_utxo` counterpart, and
//! deliberately not. [`dolos_core::sync::SyncExt::rollback`] unwinds *blocks*:
//! it walks the WAL entries after the target point and undoes each one's entity
//! and UTxO deltas. Epoch-boundary work units write no WAL entry — ESTART's pot
//! recalculation, snapshot rotation, nonce transition and era transition are
//! none of them undone by a rollback today — so a deletion staged in the
//! boundary commit is exactly as recoverable as everything else the boundary
//! does. Rolling back *across* an epoch boundary is not an operation this node
//! supports, and giving this one boundary effect an undo path would not make it
//! one. The boundary in question is mainnet epoch 236 (December 2020), several
//! thousand epochs behind any rollback window a node keeps.

use dolos_core::{
    ChainError, ChainPoint, Domain, EntityKey, Genesis, IndexStore, IndexWriter, StateStore,
    StateWriter, TxoRef, UtxoMap, UtxoSetDelta,
};
use pallas::ledger::traverse::MultiEraOutput;

use crate::{model::EraSummary, EraProtocol, FixedNamespace as _};

/// The protocol major Shelley runs at. The end of that era *is* the
/// reclamation boundary.
const SHELLEY: u16 = 2;

/// The unredeemed Byron genesis AVVM UTxOs of one store, and the lovelace
/// they hold.
///
/// Default (empty, zero) is the honest value everywhere the reclamation does
/// not apply: every boundary that is not Shelley→Allegra, and every network
/// whose Byron genesis has no `avvmDistr`.
#[derive(Default, Clone)]
pub struct AvvmReclamation {
    /// The refs still in the UTxO set, with their bodies — what the boundary
    /// deletes.
    pub utxos: UtxoMap,

    /// What those refs hold — what the boundary subtracts from the `utxos`
    /// pot and adds to `reserves`.
    pub total: u64,
}

impl AvvmReclamation {
    /// Every AVVM UTxO ref the Byron genesis distributes, redeemed or not.
    ///
    /// The one derivation both the boundary and `dolos doctor reclaim-avvm`
    /// go through, so the repair can never delete a ref the boundary would
    /// have kept.
    pub fn genesis_refs(genesis: &Genesis) -> Vec<TxoRef> {
        pallas::interop::hardano::configs::byron::genesis_avvm_utxos(&genesis.byron)
            .iter()
            .map(|(tx, _, _)| TxoRef(*tx, 0))
            .collect()
    }

    /// Read which of the genesis AVVM refs the store still holds, and sum
    /// them.
    ///
    /// An output that fails to decode is an error rather than a zero: the
    /// total funds a pot movement that has to match the deletion to the
    /// lovelace, and a store holding a Byron genesis output it cannot decode
    /// is broken in a way this boundary must not paper over.
    pub fn unredeemed<D: Domain>(state: &D::State, genesis: &Genesis) -> Result<Self, ChainError> {
        let refs = Self::genesis_refs(genesis);

        if refs.is_empty() {
            return Ok(Self::default());
        }

        let derived = refs.len();
        let utxos = state.get_utxos(refs)?;

        let mut total = 0u64;

        for utxo in utxos.values() {
            let output = MultiEraOutput::try_from(utxo.as_ref())?;
            total += output.value().coin();
        }

        tracing::debug!(
            derived,
            unredeemed = utxos.len(),
            total,
            "AVVM reclamation census"
        );

        Ok(Self { utxos, total })
    }

    /// The reclamation for the boundary closing the current epoch: the
    /// census above at the Shelley→Allegra transition, and nothing anywhere
    /// else.
    pub fn at_boundary<D: Domain>(state: &D::State, genesis: &Genesis) -> Result<Self, ChainError> {
        let ended_state = crate::load_epoch::<D>(state)?;

        let entering_allegra = ended_state
            .pparams
            .era_transition()
            .is_some_and(|x| x.entering_allegra());

        if !entering_allegra {
            return Ok(Self::default());
        }

        Self::unredeemed::<D>(state, genesis)
    }

    /// Whether the store has already been through the Shelley→Allegra
    /// boundary.
    ///
    /// The Shelley era summary gains its `end` at that boundary and only
    /// there, so its presence is the store's own record of having crossed.
    /// A store that never had a Shelley era at all — a devnet forced straight
    /// into a later protocol — never crossed it either, and reads `false`.
    pub fn boundary_crossed<D: Domain>(state: &D::State) -> Result<bool, ChainError> {
        let shelley = state.read_entity_typed::<EraSummary>(
            EraSummary::NS,
            &EntityKey::from(EraProtocol::from(SHELLEY)),
        )?;

        Ok(shelley.is_some_and(|x| x.end.is_some()))
    }

    pub fn is_empty(&self) -> bool {
        self.utxos.is_empty()
    }

    /// The UTxO-set delta that removes them.
    ///
    /// `consumed_utxo` is the existing write path for "this ref is no longer
    /// unspent"; the boundary is not a transaction, but the effect on the set
    /// is the same one, and reusing it keeps every backend's ordering rules
    /// (`StateWriter::apply_utxoset`) in force.
    pub fn deletion_delta(&self) -> UtxoSetDelta {
        UtxoSetDelta {
            consumed_utxo: self.utxos.clone(),
            ..Default::default()
        }
    }

    /// Delete this census from the state store and from the UTxO filter
    /// indexes, leaving every pot untouched.
    ///
    /// This is the repair path — `dolos doctor reclaim-avvm` against a store
    /// an earlier binary built, where the boundary already moved the value
    /// and left the rows. The boundary itself does not come through here: it
    /// stages the same deletion inside its own commit, alongside the pot
    /// delta it must be atomic with.
    ///
    /// Indexes follow the state commit, the order the block path uses. A
    /// crash between the two leaves an index entry pointing at a ref the
    /// state no longer holds, which readers already tolerate — they resolve
    /// refs against the state and drop what is not there. Re-running the
    /// repair after such a crash finds nothing unspent and does nothing, so
    /// the stale tags outlive it; the other order would hide a live UTxO
    /// instead, which is worse.
    pub fn apply_deletion<D: Domain>(
        &self,
        state: &D::State,
        indexes: &D::Indexes,
    ) -> Result<(), ChainError> {
        if self.is_empty() {
            return Ok(());
        }

        let delta = self.deletion_delta();

        let writer = state.start_writer()?;
        writer.apply_utxoset(&delta)?;
        writer.commit()?;

        // The delta carries the index's own cursor: this changes what the
        // index holds, never how far it has been advanced.
        let cursor = indexes.cursor()?.unwrap_or(ChainPoint::Origin);

        let index_writer = indexes.start_writer()?;
        index_writer.apply(&crate::indexes::index_delta_from_utxo_delta(cursor, &delta))?;
        index_writer.commit()?;

        Ok(())
    }
}
