use dolos_core::{BlockSlot, EntityKey, NsKey};
use pallas::{
    codec::minicbor::{self, Decode, Encode},
    crypto::hash::Hash,
    ledger::primitives::{Coin, Epoch, ProtocolVersion, StakeCredential},
};
use serde::{Deserialize, Serialize};

use super::{epochs::Lovelace, pparams::PParamsSet, FixedNamespace as _};
use crate::hacks::{self, proposals::ProposalOutcome};

#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ProposalAction {
    #[n(0)]
    ParamChange(#[n(0)] PParamsSet),

    #[n(1)]
    HardFork(#[n(0)] ProtocolVersion),

    #[n(2)]
    TreasuryWithdrawal(#[n(0)] Vec<(StakeCredential, Coin)>),

    /// Used to track any other proposal that isn't relevant for Dolos' purposes.
    #[n(3)]
    Other,
}

#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProposalState {
    #[n(0)]
    pub slot: BlockSlot,

    #[n(1)]
    pub tx: Hash<32>,

    #[n(2)]
    pub idx: u32,

    #[n(3)]
    pub action: ProposalAction,

    /// Set at the initialization of the proposal representing the last valid epoch. The existence of a value doesn't mean the proposal has expired.
    #[n(4)]
    pub max_epoch: Option<Epoch>,

    #[n(5)]
    pub ratified_epoch: Option<Epoch>,

    #[n(6)]
    pub canceled_epoch: Option<Epoch>,

    #[n(7)]
    pub deposit: Option<Lovelace>,

    #[n(8)]
    pub reward_account: Option<StakeCredential>,
}

entity_boilerplate!(ProposalState, "proposals");

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::model::testing as root;
    use proptest::prelude::*;

    prop_compose! {
        pub fn any_proposal_state()(
            slot in root::any_slot(),
            tx in root::any_hash_32(),
            idx in 0u32..16u32,
            deposit in prop::option::of(root::any_lovelace()),
            reward_account in prop::option::of(root::any_stake_credential()),
            max_epoch in prop::option::of(root::any_epoch()),
            ratified_epoch in prop::option::of(root::any_epoch()),
            canceled_epoch in prop::option::of(root::any_epoch()),
        ) -> ProposalState {
            ProposalState {
                slot,
                tx,
                idx,
                action: ProposalAction::Other,
                max_epoch,
                ratified_epoch,
                canceled_epoch,
                deposit,
                reward_account,
            }
        }
    }
}

impl ProposalState {
    pub fn key(&self) -> EntityKey {
        Self::build_entity_key(self.tx, self.idx)
    }

    /// Build the ID of the proposal in its string form, as found on explorers.
    pub fn id(tx: Hash<32>, idx: u32) -> String {
        format!("{}#{}", hex::encode(tx), idx)
    }

    /// Get ID of the proposal in its string form, as found on explorers.
    pub fn id_as_string(&self) -> String {
        Self::id(self.tx, self.idx)
    }

    pub fn build_entity_key(tx: Hash<32>, idx: u32) -> EntityKey {
        EntityKey::from([idx.to_be_bytes().as_slice(), tx.as_slice()].concat())
    }

    pub fn expires_at(&self) -> Option<Epoch> {
        self.max_epoch.map(|x| x + 1)
    }

    pub fn has_expired(&self, current_epoch: Epoch) -> bool {
        let expires_at = self.expires_at();
        expires_at.is_some_and(|x| x <= current_epoch)
    }

    pub fn was_enacted(&self, current_epoch: Epoch) -> bool {
        if let Some(ratified_epoch) = self.ratified_epoch {
            if current_epoch > ratified_epoch + 1 {
                return true;
            }
        }

        false
    }

    pub fn was_canceled(&self, current_epoch: Epoch) -> bool {
        if let Some(canceled_epoch) = self.canceled_epoch {
            if current_epoch > canceled_epoch {
                return true;
            }
        }

        false
    }

    /// Returns true if the proposal is still beign evaluated. Not to confuse with `is_enacted`.
    pub fn is_active(&self, current_epoch: Epoch) -> bool {
        if self.was_enacted(current_epoch) {
            return false;
        }

        if self.was_canceled(current_epoch) {
            return false;
        }

        if let Some(expires_at) = self.expires_at() {
            // +1 after the expiration to allow for the drop epoch
            return current_epoch <= expires_at + 1;
        }

        true
    }

    /// Returns true if the proposal should be enacted at the starting epoch. It does a strict comparision with the ratified epoch, it will return false if asked at a later epoch.
    pub fn should_enact(&self, starting_epoch: Epoch) -> bool {
        self.ratified_epoch.is_some_and(|x| x + 1 == starting_epoch)
    }

    pub fn should_drop(&self, starting_epoch: Epoch) -> bool {
        if self.ratified_epoch.is_some() {
            return false;
        }

        if let Some(canceled_epoch) = self.canceled_epoch {
            return starting_epoch == canceled_epoch;
        }

        if let Some(expires) = self.expires_at() {
            return starting_epoch == expires + 1;
        }

        false
    }
}

// --- Deltas ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewProposal {
    pub(crate) slot: BlockSlot,
    pub(crate) tx: Hash<32>,
    pub(crate) idx: u32,
    pub(crate) action: ProposalAction,
    pub(crate) deposit: Option<Lovelace>,
    pub(crate) reward_account: Option<StakeCredential>,
    pub(crate) validity_period: Option<u64>,
    pub(crate) current_epoch: Epoch,
    pub(crate) network_magic: u32,
    pub(crate) protocol: u16,

    pub(crate) prev: Option<ProposalState>,
}

impl NewProposal {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        slot: BlockSlot,
        tx: Hash<32>,
        idx: u32,
        action: ProposalAction,
        deposit: Option<Lovelace>,
        reward_account: Option<StakeCredential>,
        validity_period: Option<u64>,
        current_epoch: Epoch,
        network_magic: u32,
        protocol: u16,
    ) -> Self {
        Self {
            slot,
            tx,
            idx,
            action,
            deposit,
            reward_account,
            validity_period,
            current_epoch,
            network_magic,
            protocol,
            prev: None,
        }
    }
}

impl dolos_core::EntityDelta for NewProposal {
    type Entity = ProposalState;

    fn key(&self) -> NsKey {
        NsKey::from((
            ProposalState::NS,
            ProposalState::build_entity_key(self.tx, self.idx),
        ))
    }

    fn apply(&mut self, entity: &mut Option<ProposalState>) {
        self.prev = entity.clone();

        let id = ProposalState::id(self.tx, self.idx);

        let outcome = hacks::proposals::outcome(self.network_magic, self.protocol, &id);

        let max_epoch = self.validity_period.map(|x| self.current_epoch + x);

        let ratified_epoch = match &outcome {
            ProposalOutcome::Ratified(epoch) => Some(*epoch),
            ProposalOutcome::RatifiedCurrentEpoch => Some(self.current_epoch),
            _ => None,
        };

        let canceled_epoch = match &outcome {
            ProposalOutcome::Canceled(epoch) => Some(*epoch),
            _ => None,
        };

        let state = ProposalState {
            slot: self.slot,
            tx: self.tx,
            idx: self.idx,
            action: self.action.clone(),
            reward_account: self.reward_account.clone(),
            deposit: self.deposit,
            max_epoch,
            ratified_epoch,
            canceled_epoch,
        };

        let _ = entity.insert(state);
    }

    fn undo(&self, entity: &mut Option<ProposalState>) {
        *entity = self.prev.clone();
    }
}

#[cfg(test)]
mod prop_tests {
    use super::*;
    use super::testing::any_proposal_state;
    use crate::model::testing::{self as root, assert_delta_roundtrip};
    use proptest::prelude::*;

    prop_compose! {
        fn any_new_proposal()(
            slot in root::any_slot(),
            tx in root::any_hash_32(),
            idx in 0u32..16u32,
            deposit in prop::option::of(root::any_lovelace()),
            reward_account in prop::option::of(root::any_stake_credential()),
            validity_period in prop::option::of(1u64..10u64),
            current_epoch in root::any_epoch(),
            network_magic in any::<u32>(),
            protocol in 0u16..10u16,
        ) -> NewProposal {
            NewProposal::new(
                slot, tx, idx,
                ProposalAction::Other,
                deposit, reward_account, validity_period,
                current_epoch, network_magic, protocol,
            )
        }
    }

    proptest! {
        #[test]
        fn new_proposal_roundtrip(
            entity in prop::option::of(any_proposal_state()),
            delta in any_new_proposal(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }
    }
}
