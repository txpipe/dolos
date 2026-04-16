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

#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize, Default)]
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
#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize)]
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
}

impl dolos_core::EntityDelta for EpochStatsUpdate {
    type Entity = EpochState;

    fn key(&self) -> NsKey {
        NsKey::from((EpochState::NS, CURRENT_EPOCH_KEY))
    }

    fn apply(&mut self, entity: &mut Option<EpochState>) {
        let entity = entity.as_mut().expect("existing epoch");

        let stats = entity.rolling.live_mut(self.epoch).get_or_insert_default();

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

    fn undo(&self, _entity: &mut Option<EpochState>) {
        // TODO: implement undo
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoncesUpdate {
    pub(crate) slot: u64,
    pub(crate) tail: Option<Hash<32>>,
    pub(crate) nonce_vrf_output: Vec<u8>,

    pub(crate) previous: Option<Nonces>,
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

    fn undo(&self, _entity: &mut Option<EpochState>) {
        // no-op: undo not yet comprehensively implemented
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PParamsUpdate {
    pub(crate) to_update: PParamsSet,
}

impl PParamsUpdate {
    pub fn new(to_update: PParamsSet) -> Self {
        Self { to_update }
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

        let next = entity.pparams.scheduled_or_default();

        next.merge(self.to_update.clone());
    }

    fn undo(&self, _entity: &mut Option<EpochState>) {
        // Placeholder undo logic. Ensure this does not panic.
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochWrapUp {
    pub(crate) stats: EndStats,
}

impl dolos_core::EntityDelta for EpochWrapUp {
    type Entity = EpochState;

    fn key(&self) -> NsKey {
        NsKey::from((EpochState::NS, CURRENT_EPOCH_KEY))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing epoch");

        entity.rolling.scheduled_or_default();
        entity.pparams.scheduled_or_default();
        entity.end = Some(self.stats.clone());
    }

    fn undo(&self, _entity: &mut Option<Self::Entity>) {
        // no-op: undo not yet comprehensively implemented
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NonceTransition {
    pub(crate) next_nonce: Option<Nonces>,
    pub(crate) next_slot: BlockSlot,
}

impl dolos_core::EntityDelta for NonceTransition {
    type Entity = EpochState;

    fn key(&self) -> NsKey {
        NsKey::from((EpochState::NS, CURRENT_EPOCH_KEY))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing epoch");

        entity.previous_nonce_tail = entity.nonces.as_ref().and_then(|n| n.tail);
        entity.nonces = self.next_nonce.clone();
        entity.largest_stable_slot = self.next_slot;
    }

    fn undo(&self, _entity: &mut Option<Self::Entity>) {
        // Placeholder undo logic. Ensure this does not panic.
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct EpochTransition {
    pub(crate) new_epoch: Epoch,
    pub(crate) new_pots: Pots,
    pub(crate) era_transition: Option<EraTransition>,

    #[serde(skip)]
    pub(crate) genesis: Option<Arc<Genesis>>,
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

    fn undo(&self, _entity: &mut Option<Self::Entity>) {
        // Placeholder undo logic. Ensure this does not panic.
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

    fn undo(&self, _entity: &mut Option<Self::Entity>) {
        // no-op: undo not yet comprehensively implemented
    }
}
