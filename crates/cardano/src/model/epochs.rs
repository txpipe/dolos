use std::{collections::HashSet, sync::Arc};

use dolos_core::{BlockSlot, EntityKey, Genesis, NsKey};
use pallas::{
    codec::minicbor::{self, Decode, Encode},
    crypto::{
        hash::Hash,
        nonce::{generate_epoch_nonce, generate_rolling_nonce},
    },
    ledger::primitives::Epoch,
};
use serde::{Deserialize, Serialize};

use super::{
    epoch_value::{EpochValue, TransitionDefault},
    eras::EraTransition,
    pools::PoolHash,
    pparams::PParamsSet,
    FixedNamespace as _,
};
use crate::pots::{EpochIncentives, Pots};

pub type Lovelace = u64;

pub const CURRENT_EPOCH_KEY: &[u8] = b"0";

#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Nonces {
    #[n(0)]
    pub active: Hash<32>,

    #[n(1)]
    pub evolving: Hash<32>,

    #[n(2)]
    pub candidate: Hash<32>,

    #[n(3)]
    pub tail: Option<Hash<32>>,
}

impl Nonces {
    pub fn bootstrap(shelley_hash: Hash<32>) -> Self {
        Self {
            active: shelley_hash,
            evolving: shelley_hash,
            candidate: shelley_hash,
            tail: None,
        }
    }

    pub fn roll(
        &self,
        update_candidate: bool,
        nonce_vrf_output: &[u8],
        tail: Option<Hash<32>>,
    ) -> Nonces {
        let evolving = generate_rolling_nonce(self.evolving, nonce_vrf_output);

        Self {
            active: self.active,
            evolving,
            candidate: if update_candidate {
                evolving
            } else {
                self.candidate
            },
            tail,
        }
    }

    /// Compute active nonce for next epoch.
    pub fn sweep(&self, previous_tail: Option<Hash<32>>, extra_entropy: Option<&[u8]>) -> Self {
        Self {
            active: match previous_tail {
                Some(tail) => generate_epoch_nonce(self.candidate, tail, extra_entropy),
                None => self.candidate,
            },
            candidate: self.evolving,
            evolving: self.evolving,
            tail: self.tail,
        }
    }
}

/// Epoch data that is gathered as part of the block rolling process
#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct RollingStats {
    #[n(2)]
    pub produced_utxos: Lovelace,

    #[n(3)]
    pub consumed_utxos: Lovelace,

    #[n(4)]
    pub gathered_fees: Lovelace,

    #[n(5)]
    pub new_accounts: u64,

    #[n(6)]
    pub removed_accounts: u64,

    #[n(7)]
    pub withdrawals: Lovelace,

    #[n(8)]
    pub registered_pools: HashSet<PoolHash>,

    #[n(13)]
    pub blocks_minted: u32,

    #[n(14)]
    pub drep_deposits: Lovelace,

    #[n(15)]
    pub proposal_deposits: Lovelace,

    #[n(16)]
    pub drep_refunds: Lovelace,

    // TODO: deprecate
    #[n(17)]
    pub __proposal_refunds: Lovelace,

    #[n(18)]
    #[cbor(default)]
    pub treasury_donations: Lovelace,

    #[n(19)]
    #[cbor(default)]
    pub reserve_mirs: Lovelace,

    /// Blocks minted in non-overlay slots (includes pools + genesis delegates).
    #[n(20)]
    #[cbor(default)]
    pub non_overlay_blocks_minted: u32,

    /// MIR sourced from treasury.
    #[n(21)]
    #[cbor(default)]
    pub treasury_mirs: Lovelace,
}

impl TransitionDefault for RollingStats {
    fn next_value(_: Option<&Self>) -> Option<Self> {
        Some(Self::default())
    }
}

/// Stats that are gathered at the end of the epoch
#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EndStats {
    #[n(0)]
    pub pool_deposit_count: u64,

    #[n(1)]
    pub pool_refund_count: u64,

    #[n(2)]
    pub pool_invalid_refund_count: u64,

    #[n(3)]
    pub epoch_incentives: EpochIncentives,

    #[n(4)]
    pub effective_rewards: u64,

    /// Unspendable rewards that go to treasury.
    #[n(5)]
    pub unspendable_to_treasury: u64,

    /// Unspendable rewards that return to reserves.
    #[n(10)]
    #[cbor(default)]
    pub unspendable_to_reserves: u64,

    /// Effective MIR sourced from treasury (only to registered accounts).
    #[n(11)]
    #[cbor(default)]
    pub treasury_mirs: Lovelace,

    /// Effective MIR sourced from reserves (only to registered accounts).
    #[n(12)]
    #[cbor(default)]
    pub reserve_mirs: Lovelace,

    /// MIRs to unregistered accounts (stays in treasury, not transferred).
    #[n(13)]
    #[cbor(default)]
    pub invalid_treasury_mirs: Lovelace,

    /// MIRs to unregistered accounts (stays in reserves, not transferred).
    #[n(14)]
    #[cbor(default)]
    pub invalid_reserve_mirs: Lovelace,

    #[n(6)]
    pub proposal_invalid_refunds: Lovelace,

    #[n(7)]
    pub proposal_refunds: Lovelace,

    // TODO: deprecate
    #[n(8)]
    pub __drep_deposits: Lovelace,

    // TODO: deprecate
    #[n(9)]
    pub __drep_refunds: Lovelace,
}

#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct EpochState {
    #[n(0)]
    pub number: Epoch,

    #[n(1)]
    pub initial_pots: Pots,

    #[n(2)]
    pub rolling: EpochValue<RollingStats>,

    #[n(9)]
    pub pparams: EpochValue<PParamsSet>,

    #[n(10)]
    pub largest_stable_slot: BlockSlot,

    #[n(11)]
    pub previous_nonce_tail: Option<Hash<32>>,

    #[n(12)]
    pub nonces: Option<Nonces>,

    #[n(13)]
    pub end: Option<EndStats>,

    /// Epoch incentives computed during RUPD, used for pot calculations at epoch boundary.
    #[n(14)]
    #[cbor(default)]
    pub incentives: Option<EpochIncentives>,
}

impl Default for EpochState {
    fn default() -> Self {
        Self {
            number: 0,
            initial_pots: Pots::default(),
            rolling: EpochValue::new(0),
            pparams: EpochValue::new(0),
            largest_stable_slot: 0,
            previous_nonce_tail: None,
            nonces: None,
            end: None,
            incentives: None,
        }
    }
}

entity_boilerplate!(EpochState, "epochs");

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::model::epoch_value::testing::any_epoch_value;
    use crate::model::pparams::testing::any_pparams_set;
    use crate::model::testing as root;
    use crate::pots::testing::{any_epoch_incentives, any_pots};
    use proptest::prelude::*;

    prop_compose! {
        pub fn any_nonces()(
            active in root::any_hash_32(),
            evolving in root::any_hash_32(),
            candidate in root::any_hash_32(),
            tail in prop::option::of(root::any_hash_32()),
        ) -> Nonces {
            Nonces { active, evolving, candidate, tail }
        }
    }

    prop_compose! {
        pub fn any_rolling_stats()(
            produced_utxos in root::any_lovelace(),
            consumed_utxos in root::any_lovelace(),
            gathered_fees in root::any_lovelace(),
            blocks_minted in 0u32..1000u32,
        ) -> RollingStats {
            let mut stats = RollingStats::default();
            stats.produced_utxos = produced_utxos;
            stats.consumed_utxos = consumed_utxos;
            stats.gathered_fees = gathered_fees;
            stats.blocks_minted = blocks_minted;
            stats
        }
    }

    prop_compose! {
        pub fn any_end_stats()(
            pool_deposit_count in 0u64..100u64,
            pool_refund_count in 0u64..100u64,
            pool_invalid_refund_count in 0u64..100u64,
            epoch_incentives in any_epoch_incentives(),
            effective_rewards in root::any_lovelace(),
        ) -> EndStats {
            EndStats {
                pool_deposit_count,
                pool_refund_count,
                pool_invalid_refund_count,
                epoch_incentives,
                effective_rewards,
                unspendable_to_treasury: 0,
                unspendable_to_reserves: 0,
                treasury_mirs: 0,
                reserve_mirs: 0,
                invalid_treasury_mirs: 0,
                invalid_reserve_mirs: 0,
                proposal_invalid_refunds: 0,
                proposal_refunds: 0,
                __drep_deposits: 0,
                __drep_refunds: 0,
            }
        }
    }

    prop_compose! {
        pub fn any_epoch_state()(
            number in root::any_epoch(),
            initial_pots in any_pots(),
            rolling in any_epoch_value(any_rolling_stats().boxed()),
            pparams in any_epoch_value(any_pparams_set().boxed()),
            largest_stable_slot in root::any_slot(),
            previous_nonce_tail in prop::option::of(root::any_hash_32()),
            nonces in prop::option::of(any_nonces()),
            end in prop::option::of(any_end_stats()),
            incentives in prop::option::of(any_epoch_incentives()),
        ) -> EpochState {
            EpochState {
                number,
                initial_pots,
                rolling,
                pparams,
                largest_stable_slot,
                previous_nonce_tail,
                nonces,
                end,
                incentives,
            }
        }
    }
}

// --- Deltas ---

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EpochStatsUpdate {
    pub(crate) epoch: Epoch,
    pub(crate) block_fees: u64,
    pub(crate) utxo_delta: i64,
    pub(crate) new_accounts: u64,
    pub(crate) removed_accounts: u64,
    pub(crate) withdrawals: u64,
    pub(crate) registered_pools: HashSet<PoolHash>,
    pub(crate) drep_deposits: Lovelace,
    pub(crate) proposal_deposits: Lovelace,
    pub(crate) drep_refunds: Lovelace,
    pub(crate) treasury_donations: Lovelace,
    pub(crate) reserve_mirs: Lovelace,
    pub(crate) treasury_mirs: Lovelace,
    pub(crate) non_overlay_blocks_minted: u32,

    // undo: did apply create rolling.live from default? Plus the pre-union pool set, which
    // can't be recovered by set subtraction (a pool in both prev and self would be removed).
    pub(crate) was_new: bool,
    pub(crate) prev_registered_pools: HashSet<PoolHash>,
}

impl dolos_core::EntityDelta for EpochStatsUpdate {
    type Entity = EpochState;

    fn key(&self) -> NsKey {
        NsKey::from((EpochState::NS, CURRENT_EPOCH_KEY))
    }

    fn apply(&mut self, entity: &mut Option<EpochState>) {
        let entity = entity.as_mut().expect("existing epoch");

        let live_slot = entity.rolling.live_mut(self.epoch);
        self.was_new = live_slot.is_none();
        let stats = live_slot.get_or_insert_default();

        self.prev_registered_pools = stats.registered_pools.clone();

        stats.blocks_minted += 1;

        if self.utxo_delta > 0 {
            stats.produced_utxos += self.utxo_delta.unsigned_abs();
        } else {
            stats.consumed_utxos += self.utxo_delta.unsigned_abs();
        }

        stats.gathered_fees += self.block_fees;
        stats.new_accounts += self.new_accounts;
        stats.removed_accounts += self.removed_accounts;
        stats.withdrawals += self.withdrawals;
        stats.proposal_deposits += self.proposal_deposits;
        stats.drep_deposits += self.drep_deposits;
        stats.drep_refunds += self.drep_refunds;
        stats.treasury_donations += self.treasury_donations;
        stats.reserve_mirs += self.reserve_mirs;
        stats.treasury_mirs += self.treasury_mirs;
        stats.non_overlay_blocks_minted += self.non_overlay_blocks_minted;

        stats.registered_pools = stats
            .registered_pools
            .union(&self.registered_pools)
            .cloned()
            .collect();
    }

    fn undo(&self, entity: &mut Option<EpochState>) {
        let entity = entity.as_mut().expect("existing epoch");

        if self.was_new {
            *entity.rolling.live_mut(self.epoch) = None;
            return;
        }

        let live_slot = entity.rolling.live_mut(self.epoch);
        let stats = live_slot.as_mut().expect("rolling.live populated");

        stats.blocks_minted -= 1;

        if self.utxo_delta > 0 {
            stats.produced_utxos -= self.utxo_delta.unsigned_abs();
        } else {
            stats.consumed_utxos -= self.utxo_delta.unsigned_abs();
        }

        stats.gathered_fees -= self.block_fees;
        stats.new_accounts -= self.new_accounts;
        stats.removed_accounts -= self.removed_accounts;
        stats.withdrawals -= self.withdrawals;
        stats.proposal_deposits -= self.proposal_deposits;
        stats.drep_deposits -= self.drep_deposits;
        stats.drep_refunds -= self.drep_refunds;
        stats.treasury_donations -= self.treasury_donations;
        stats.reserve_mirs -= self.reserve_mirs;
        stats.treasury_mirs -= self.treasury_mirs;
        stats.non_overlay_blocks_minted -= self.non_overlay_blocks_minted;

        stats.registered_pools = self.prev_registered_pools.clone();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoncesUpdate {
    pub(crate) slot: u64,
    pub(crate) tail: Option<Hash<32>>,
    pub(crate) nonce_vrf_output: Vec<u8>,

    pub(crate) previous: Option<Nonces>,
}

impl NoncesUpdate {
    pub fn new(slot: u64, tail: Option<Hash<32>>, nonce_vrf_output: Vec<u8>) -> Self {
        Self {
            slot,
            tail,
            nonce_vrf_output,
            previous: None,
        }
    }
}

impl dolos_core::EntityDelta for NoncesUpdate {
    type Entity = EpochState;

    fn key(&self) -> NsKey {
        NsKey::from((EpochState::NS, CURRENT_EPOCH_KEY))
    }

    fn apply(&mut self, entity: &mut Option<EpochState>) {
        let Some(entity) = entity else { return };
        if let Some(nonces) = entity.nonces.as_ref() {
            self.previous = Some(nonces.clone());
            entity.nonces = Some(nonces.roll(
                self.slot < entity.largest_stable_slot,
                &self.nonce_vrf_output,
                self.tail,
            ));
        }
    }

    fn undo(&self, entity: &mut Option<EpochState>) {
        // apply is a no-op when entity was None or when nonces were None, so in those
        // cases undo is also a no-op.
        let Some(entity) = entity else { return };
        if self.previous.is_some() {
            entity.nonces = self.previous.clone();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PParamsUpdate {
    pub(crate) to_update: PParamsSet,

    // undo
    pub(crate) prev_pparams: Option<EpochValue<PParamsSet>>,
}

impl PParamsUpdate {
    pub fn new(to_update: PParamsSet) -> Self {
        Self {
            to_update,
            prev_pparams: None,
        }
    }
}

impl dolos_core::EntityDelta for PParamsUpdate {
    type Entity = EpochState;

    fn key(&self) -> NsKey {
        NsKey::from((EpochState::NS, CURRENT_EPOCH_KEY))
    }

    fn apply(&mut self, entity: &mut Option<EpochState>) {
        let entity = entity.as_mut().expect("epoch state missing");

        tracing::debug!(value = ?self.to_update, "applying pparam update");

        self.prev_pparams = Some(entity.pparams.clone());

        let next = entity.pparams.scheduled_or_default();

        next.merge(self.to_update.clone());
    }

    fn undo(&self, entity: &mut Option<EpochState>) {
        let entity = entity.as_mut().expect("epoch state missing");
        entity.pparams = self.prev_pparams.clone().expect("apply captured pparams");
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochWrapUp {
    pub(crate) stats: EndStats,

    // undo
    pub(crate) prev_rolling: Option<EpochValue<RollingStats>>,
    pub(crate) prev_pparams: Option<EpochValue<PParamsSet>>,
    pub(crate) prev_end: Option<EndStats>,
}

impl EpochWrapUp {
    pub fn new(stats: EndStats) -> Self {
        Self {
            stats,
            prev_rolling: None,
            prev_pparams: None,
            prev_end: None,
        }
    }
}

impl dolos_core::EntityDelta for EpochWrapUp {
    type Entity = EpochState;

    fn key(&self) -> NsKey {
        NsKey::from((EpochState::NS, CURRENT_EPOCH_KEY))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing epoch");

        self.prev_rolling = Some(entity.rolling.clone());
        self.prev_pparams = Some(entity.pparams.clone());
        self.prev_end = entity.end.clone();

        entity.rolling.scheduled_or_default();
        entity.pparams.scheduled_or_default();
        entity.end = Some(self.stats.clone());
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing epoch");
        entity.rolling = self.prev_rolling.clone().expect("apply captured rolling");
        entity.pparams = self.prev_pparams.clone().expect("apply captured pparams");
        entity.end = self.prev_end.clone();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NonceTransition {
    pub(crate) next_nonce: Option<Nonces>,
    pub(crate) next_slot: BlockSlot,

    // undo
    pub(crate) prev_previous_nonce_tail: Option<Hash<32>>,
    pub(crate) prev_nonces: Option<Nonces>,
    pub(crate) prev_largest_stable_slot: BlockSlot,
}

impl NonceTransition {
    pub fn new(next_nonce: Option<Nonces>, next_slot: BlockSlot) -> Self {
        Self {
            next_nonce,
            next_slot,
            prev_previous_nonce_tail: None,
            prev_nonces: None,
            prev_largest_stable_slot: 0,
        }
    }
}

impl dolos_core::EntityDelta for NonceTransition {
    type Entity = EpochState;

    fn key(&self) -> NsKey {
        NsKey::from((EpochState::NS, CURRENT_EPOCH_KEY))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing epoch");

        self.prev_previous_nonce_tail = entity.previous_nonce_tail;
        self.prev_nonces = entity.nonces.clone();
        self.prev_largest_stable_slot = entity.largest_stable_slot;

        entity.previous_nonce_tail = entity.nonces.as_ref().and_then(|n| n.tail);
        entity.nonces = self.next_nonce.clone();
        entity.largest_stable_slot = self.next_slot;
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing epoch");
        entity.previous_nonce_tail = self.prev_previous_nonce_tail;
        entity.nonces = self.prev_nonces.clone();
        entity.largest_stable_slot = self.prev_largest_stable_slot;
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct EpochTransition {
    pub(crate) new_epoch: Epoch,
    pub(crate) new_pots: Pots,
    pub(crate) era_transition: Option<EraTransition>,

    #[serde(skip)]
    pub(crate) genesis: Option<Arc<Genesis>>,

    // undo
    pub(crate) prev_number: Epoch,
    pub(crate) prev_initial_pots: Option<Pots>,
    pub(crate) prev_rolling: Option<EpochValue<RollingStats>>,
    pub(crate) prev_pparams: Option<EpochValue<PParamsSet>>,
}

impl EpochTransition {
    pub fn new(
        new_epoch: Epoch,
        new_pots: Pots,
        era_transition: Option<EraTransition>,
        genesis: Option<Arc<Genesis>>,
    ) -> Self {
        Self {
            new_epoch,
            new_pots,
            era_transition,
            genesis,
            prev_number: 0,
            prev_initial_pots: None,
            prev_rolling: None,
            prev_pparams: None,
        }
    }
}

impl std::fmt::Debug for EpochTransition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EpochTransition")?;
        Ok(())
    }
}

impl dolos_core::EntityDelta for EpochTransition {
    type Entity = EpochState;

    fn key(&self) -> NsKey {
        NsKey::from((EpochState::NS, CURRENT_EPOCH_KEY))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing epoch");

        debug_assert!(self
            .new_pots
            .is_consistent(entity.initial_pots.max_supply()));

        // save undo info (snapshot whole EpochValues so rotation + any era migration are
        // both covered)
        self.prev_number = entity.number;
        self.prev_initial_pots = Some(entity.initial_pots.clone());
        self.prev_rolling = Some(entity.rolling.clone());
        self.prev_pparams = Some(entity.pparams.clone());

        entity.number = self.new_epoch;
        entity.initial_pots = self.new_pots.clone();
        entity.rolling.default_transition(self.new_epoch);
        entity.pparams.default_transition(self.new_epoch);

        // if we have an era transition, we need to migrate the pparams
        if let Some(transition) = &self.era_transition {
            let current = entity.pparams.unwrap_live_mut();

            *current = crate::forks::migrate_pparams_version(
                transition.prev_version.into(),
                transition.new_version.into(),
                current,
                self.genesis.as_ref().expect("genesis not set"),
            );
        }
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing epoch");

        entity.rolling = self.prev_rolling.clone().expect("apply captured rolling");
        entity.pparams = self.prev_pparams.clone().expect("apply captured pparams");
        entity.number = self.prev_number;
        entity.initial_pots = self
            .prev_initial_pots
            .clone()
            .expect("apply captured initial_pots");
    }
}

/// Delta to set epoch incentives on the current epoch state.
/// Applied by RUPD after computing rewards to store incentives metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetEpochIncentives {
    pub incentives: EpochIncentives,
    pub(crate) prev_incentives: Option<EpochIncentives>,
}

impl SetEpochIncentives {
    pub fn new(incentives: EpochIncentives) -> Self {
        Self {
            incentives,
            prev_incentives: None,
        }
    }
}

impl dolos_core::EntityDelta for SetEpochIncentives {
    type Entity = EpochState;

    fn key(&self) -> NsKey {
        NsKey::from((EpochState::NS, EntityKey::from(CURRENT_EPOCH_KEY)))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("EpochState must exist");
        self.prev_incentives = entity.incentives.take();
        entity.incentives = Some(self.incentives.clone());
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("EpochState must exist");
        entity.incentives = self.prev_incentives.clone();
    }
}

#[cfg(test)]
mod prop_tests {
    use super::*;
    use super::testing::{any_end_stats, any_epoch_state, any_nonces};
    use crate::model::epoch_value::testing::{any_epoch_value, any_epoch_value_no_next};
    use crate::model::pparams::testing::any_pparams_set;
    use crate::model::testing::{self as root, assert_delta_roundtrip};
    use crate::pots::testing::{any_epoch_incentives, any_pots};
    use proptest::prelude::*;

    /// `EpochStatsUpdate::apply` calls `rolling.live_mut` which asserts `next` is None,
    /// so we need a specialized generator that keeps `rolling.next` empty.
    prop_compose! {
        fn any_epoch_state_no_rolling_next()(
            number in root::any_epoch(),
            initial_pots in any_pots(),
            rolling in any_epoch_value_no_next(super::testing::any_rolling_stats().boxed()),
            pparams in any_epoch_value(any_pparams_set().boxed()),
            largest_stable_slot in root::any_slot(),
            previous_nonce_tail in prop::option::of(root::any_hash_32()),
            nonces in prop::option::of(any_nonces()),
            end in prop::option::of(any_end_stats()),
            incentives in prop::option::of(any_epoch_incentives()),
        ) -> EpochState {
            EpochState {
                number,
                initial_pots,
                rolling,
                pparams,
                largest_stable_slot,
                previous_nonce_tail,
                nonces,
                end,
                incentives,
            }
        }
    }

    prop_compose! {
        fn any_epoch_stats_update()(
            epoch in root::any_epoch(),
            block_fees in root::any_lovelace(),
            utxo_delta in -1_000_000i64..1_000_000i64,
            new_accounts in 0u64..100u64,
            removed_accounts in 0u64..100u64,
            withdrawals in root::any_lovelace(),
        ) -> EpochStatsUpdate {
            EpochStatsUpdate {
                epoch, block_fees, utxo_delta,
                new_accounts, removed_accounts, withdrawals,
                ..EpochStatsUpdate::default()
            }
        }
    }

    prop_compose! {
        fn any_nonces_update()(
            slot in root::any_slot(),
            tail in prop::option::of(root::any_hash_32()),
            nonce_vrf_output in prop::collection::vec(any::<u8>(), 32..=32),
        ) -> NoncesUpdate {
            NoncesUpdate::new(slot, tail, nonce_vrf_output)
        }
    }

    fn any_pparams_update() -> impl Strategy<Value = PParamsUpdate> {
        Just(PParamsUpdate::new(crate::model::pparams::PParamsSet::default()))
    }

    prop_compose! {
        fn any_epoch_wrap_up()(
            stats in any_end_stats(),
        ) -> EpochWrapUp {
            EpochWrapUp::new(stats)
        }
    }

    prop_compose! {
        fn any_nonce_transition()(
            next_nonce in prop::option::of(any_nonces()),
            next_slot in root::any_slot(),
        ) -> NonceTransition {
            NonceTransition::new(next_nonce, next_slot)
        }
    }

    prop_compose! {
        fn any_epoch_transition()(
            new_epoch in root::any_epoch(),
        ) -> EpochTransition {
            // new_pots is filled in by the test harness from the entity's initial_pots
            // so that `new_pots.max_supply() == entity.initial_pots.max_supply()` holds
            // (which `apply`'s debug_assert requires).
            EpochTransition::new(new_epoch, crate::pots::Pots::default(), None, None)
        }
    }

    prop_compose! {
        fn any_set_epoch_incentives()(
            incentives in any_epoch_incentives(),
        ) -> SetEpochIncentives {
            SetEpochIncentives::new(incentives)
        }
    }

    proptest! {
        #[test]
        fn epoch_stats_update_roundtrip(
            entity in any_epoch_state_no_rolling_next(),
            delta in any_epoch_stats_update(),
        ) {
            assert_delta_roundtrip(Some(entity), delta);
        }

        #[test]
        fn nonces_update_roundtrip(
            entity in prop::option::of(any_epoch_state()),
            delta in any_nonces_update(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn pparams_update_roundtrip(
            entity in any_epoch_state(),
            delta in any_pparams_update(),
        ) {
            assert_delta_roundtrip(Some(entity), delta);
        }

        #[test]
        fn epoch_wrap_up_roundtrip(
            entity in any_epoch_state(),
            delta in any_epoch_wrap_up(),
        ) {
            assert_delta_roundtrip(Some(entity), delta);
        }

        #[test]
        fn nonce_transition_roundtrip(
            entity in any_epoch_state(),
            delta in any_nonce_transition(),
        ) {
            assert_delta_roundtrip(Some(entity), delta);
        }

        #[test]
        fn epoch_transition_roundtrip(
            entity in any_epoch_state(),
            mut delta in any_epoch_transition(),
        ) {
            // align new_pots with the entity's initial_pots so apply's max_supply
            // consistency debug_assert holds.
            delta.new_pots = entity.initial_pots.clone();
            assert_delta_roundtrip(Some(entity), delta);
        }

        #[test]
        fn set_epoch_incentives_roundtrip(
            entity in any_epoch_state(),
            delta in any_set_epoch_incentives(),
        ) {
            assert_delta_roundtrip(Some(entity), delta);
        }
    }
}
