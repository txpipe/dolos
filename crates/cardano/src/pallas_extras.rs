use std::ops::Deref as _;

use dolos_core::BlockSlot;
use pallas::crypto::hash::Hash;
use pallas::ledger::addresses::{
    Address, Network, ShelleyAddress, ShelleyDelegationPart, StakeAddress, StakePayload,
};
use pallas::ledger::primitives::conway::{
    CostModels, DRep, DRepVotingThresholds, PoolVotingThresholds,
};
use pallas::ledger::primitives::{
    alonzo::Certificate as AlonzoCert, conway::Certificate as ConwayCert, PoolMetadata,
    RationalNumber, Relay, StakeCredential,
};
use pallas::ledger::primitives::{ExUnitPrices, ExUnits, Nonce, NonceVariant};
use pallas::ledger::traverse::MultiEraCert;
use serde::{Deserialize, Serialize};

use crate::eras::ChainSummary;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiEraPoolRegistration {
    pub operator: Hash<28>,
    pub vrf_keyhash: Hash<32>,
    pub pledge: u64,
    pub cost: u64,
    pub margin: RationalNumber,
    pub reward_account: Vec<u8>,
    pub pool_owners: Vec<Hash<28>>,
    pub relays: Vec<Relay>,
    pub pool_metadata: Option<PoolMetadata>,
}

pub fn cert_as_pool_registration(cert: &MultiEraCert) -> Option<MultiEraPoolRegistration> {
    match cert {
        MultiEraCert::AlonzoCompatible(cow) => match cow.deref().deref() {
            AlonzoCert::PoolRegistration {
                operator,
                vrf_keyhash,
                pledge,
                cost,
                margin,
                reward_account,
                pool_owners,
                relays,
                pool_metadata,
            } => Some(MultiEraPoolRegistration {
                operator: *operator,
                vrf_keyhash: *vrf_keyhash,
                pledge: *pledge,
                cost: *cost,
                margin: margin.clone(),
                reward_account: reward_account.to_vec(),
                pool_owners: pool_owners.clone(),
                relays: relays.clone(),
                pool_metadata: pool_metadata.clone(),
            }),
            _ => None,
        },
        MultiEraCert::Conway(cow) => match cow.deref().deref() {
            ConwayCert::PoolRegistration {
                operator,
                vrf_keyhash,
                pledge,
                cost,
                margin,
                reward_account,
                pool_owners,
                relays,
                pool_metadata,
            } => Some(MultiEraPoolRegistration {
                operator: *operator,
                vrf_keyhash: *vrf_keyhash,
                pledge: *pledge,
                cost: *cost,
                margin: margin.clone(),
                reward_account: reward_account.to_vec(),
                pool_owners: Vec::from_iter(pool_owners.iter().cloned()),
                relays: relays.clone(),
                pool_metadata: pool_metadata.clone(),
            }),
            _ => None,
        },
        _ => None,
    }
}

pub struct MultiEraVoteDelegation {
    pub delegator: StakeCredential,
    pub drep: DRep,
}

pub fn cert_as_vote_delegation(cert: &MultiEraCert) -> Option<MultiEraVoteDelegation> {
    match cert {
        MultiEraCert::Conway(cow) => match cow.deref().deref() {
            ConwayCert::VoteDeleg(delegator, drep) => Some(MultiEraVoteDelegation {
                delegator: delegator.clone(),
                drep: drep.clone(),
            }),
            ConwayCert::VoteRegDeleg(delegator, drep, _) => Some(MultiEraVoteDelegation {
                delegator: delegator.clone(),
                drep: drep.clone(),
            }),
            ConwayCert::StakeVoteRegDeleg(delegator, _, drep, _) => Some(MultiEraVoteDelegation {
                delegator: delegator.clone(),
                drep: drep.clone(),
            }),
            _ => None,
        },
        _ => None,
    }
}

pub struct MultiEraStakeDelegation {
    pub delegator: StakeCredential,
    pub pool: Hash<28>,
}

pub fn cert_as_stake_delegation(cert: &MultiEraCert) -> Option<MultiEraStakeDelegation> {
    match cert {
        MultiEraCert::AlonzoCompatible(cow) => match cow.deref().deref() {
            AlonzoCert::StakeDelegation(delegator, pool) => Some(MultiEraStakeDelegation {
                delegator: delegator.clone(),
                pool: *pool,
            }),
            _ => None,
        },
        MultiEraCert::Conway(cow) => match cow.deref().deref() {
            ConwayCert::StakeDelegation(delegator, pool) => Some(MultiEraStakeDelegation {
                delegator: delegator.clone(),
                pool: *pool,
            }),
            ConwayCert::StakeRegDeleg(delegator, pool, _) => Some(MultiEraStakeDelegation {
                delegator: delegator.clone(),
                pool: *pool,
            }),
            ConwayCert::StakeVoteRegDeleg(delegator, pool, _, _) => Some(MultiEraStakeDelegation {
                delegator: delegator.clone(),
                pool: *pool,
            }),
            _ => None,
        },
        _ => None,
    }
}

pub fn cert_as_stake_registration(cert: &MultiEraCert) -> Option<StakeCredential> {
    match cert {
        MultiEraCert::AlonzoCompatible(cow) => match cow.deref().deref() {
            AlonzoCert::StakeRegistration(credential) => Some(credential.clone()),
            _ => None,
        },
        MultiEraCert::Conway(cow) => match cow.deref().deref() {
            ConwayCert::StakeRegistration(credential) => Some(credential.clone()),
            ConwayCert::Reg(cred, _) => Some(cred.clone()),
            ConwayCert::StakeRegDeleg(cred, _, _) => Some(cred.clone()),
            ConwayCert::VoteRegDeleg(cred, _, _) => Some(cred.clone()),
            ConwayCert::StakeVoteRegDeleg(cred, _, _, _) => Some(cred.clone()),
            _ => None,
        },
        _ => None,
    }
}

pub fn cert_as_stake_deregistration(cert: &MultiEraCert) -> Option<StakeCredential> {
    match cert {
        MultiEraCert::AlonzoCompatible(cow) => match cow.deref().deref() {
            AlonzoCert::StakeDeregistration(credential) => Some(credential.clone()),
            _ => None,
        },
        MultiEraCert::Conway(cow) => match cow.deref().deref() {
            ConwayCert::StakeDeregistration(credential) => Some(credential.clone()),
            ConwayCert::UnReg(cred, _) => Some(cred.clone()),
            _ => None,
        },
        _ => None,
    }
}

pub fn stake_credential_to_address(network: Network, credential: &StakeCredential) -> StakeAddress {
    match credential {
        StakeCredential::ScriptHash(x) => StakeAddress::new(network, StakePayload::Script(*x)),
        StakeCredential::AddrKeyhash(x) => StakeAddress::new(network, StakePayload::Stake(*x)),
    }
}

pub fn stake_address_to_cred(address: &StakeAddress) -> StakeCredential {
    match address.payload() {
        StakePayload::Stake(x) => StakeCredential::AddrKeyhash(*x),
        StakePayload::Script(x) => StakeCredential::ScriptHash(*x),
    }
}

pub fn shelley_address_to_stake_cred(address: &ShelleyAddress) -> Option<StakeCredential> {
    match address.delegation() {
        ShelleyDelegationPart::Key(x) => Some(StakeCredential::AddrKeyhash(*x)),
        ShelleyDelegationPart::Script(x) => Some(StakeCredential::ScriptHash(*x)),
        _ => None,
    }
}

pub fn shelley_address_to_stake_address(address: &ShelleyAddress) -> Option<StakeAddress> {
    match address.delegation() {
        ShelleyDelegationPart::Key(x) => Some(StakeAddress::new(
            address.network(),
            StakePayload::Stake(*x),
        )),
        ShelleyDelegationPart::Script(x) => Some(StakeAddress::new(
            address.network(),
            StakePayload::Script(*x),
        )),
        _ => None,
    }
}

pub fn address_as_stake_cred(address: &Address) -> Option<StakeCredential> {
    match &address {
        Address::Shelley(x) => shelley_address_to_stake_cred(x),
        Address::Stake(x) => Some(stake_address_to_cred(x)),
        _ => None,
    }
}

pub fn epoch_boundary(
    chain_summary: &ChainSummary,
    prev_slot: BlockSlot,
    next_slot: BlockSlot,
) -> Option<BlockSlot> {
    let (prev_epoch, _) = chain_summary.slot_epoch(prev_slot);
    let (next_epoch, _) = chain_summary.slot_epoch(next_slot);

    if prev_epoch != next_epoch {
        let boundary = chain_summary.epoch_start(next_epoch);
        Some(boundary)
    } else {
        None
    }
}

pub fn rupd_boundary(
    stability_window: u64,
    chain_summary: &ChainSummary,
    prev_slot: BlockSlot,
    next_slot: BlockSlot,
) -> Option<BlockSlot> {
    let (prev_epoch, _) = chain_summary.slot_epoch(prev_slot);

    let epoch_start = chain_summary.epoch_start(prev_epoch);

    let boundary = epoch_start + stability_window;

    if prev_slot <= boundary && boundary < next_slot {
        Some(boundary)
    } else {
        None
    }
}

pub fn default_rational_number() -> RationalNumber {
    RationalNumber {
        numerator: 0,
        denominator: 1,
    }
}

pub fn default_pool_voting_thresholds() -> PoolVotingThresholds {
    PoolVotingThresholds {
        motion_no_confidence: default_rational_number(),
        committee_normal: default_rational_number(),
        committee_no_confidence: default_rational_number(),
        hard_fork_initiation: default_rational_number(),
        security_voting_threshold: default_rational_number(),
    }
}

pub fn default_drep_voting_thresholds() -> DRepVotingThresholds {
    DRepVotingThresholds {
        motion_no_confidence: default_rational_number(),
        committee_normal: default_rational_number(),
        committee_no_confidence: default_rational_number(),
        hard_fork_initiation: default_rational_number(),
        pp_network_group: default_rational_number(),
        pp_economic_group: default_rational_number(),
        pp_technical_group: default_rational_number(),
        treasury_withdrawal: default_rational_number(),
        update_constitution: default_rational_number(),
        pp_governance_group: default_rational_number(),
    }
}

pub fn default_nonce() -> Nonce {
    Nonce {
        variant: NonceVariant::NeutralNonce,
        hash: None,
    }
}

pub fn default_ex_units() -> ExUnits {
    ExUnits { mem: 0, steps: 0 }
}

pub fn default_ex_unit_prices() -> ExUnitPrices {
    ExUnitPrices {
        mem_price: default_rational_number(),
        step_price: default_rational_number(),
    }
}

pub fn default_cost_models() -> CostModels {
    CostModels {
        plutus_v1: None,
        plutus_v2: None,
        plutus_v3: None,
        unknown: Default::default(),
    }
}

pub const DREP_KEY_PREFIX: u8 = 0b00100010;
pub const DREP_SCRIPT_PREFIX: u8 = 0b00100011;

/// Check that the first byte of the drep id finishes with the 0011 bytes.
pub fn drep_id_is_script(drep_id: &[u8]) -> bool {
    let first = drep_id.first().unwrap();
    first & 0b00001111 == 0b00000011
}

pub fn stake_cred_to_drep(cred: &StakeCredential) -> DRep {
    match cred {
        StakeCredential::AddrKeyhash(key) => DRep::Key(*key),
        StakeCredential::ScriptHash(key) => DRep::Script(*key),
    }
}

pub fn pool_reward_account(reward_account: &[u8]) -> Option<StakeCredential> {
    let pool_address = Address::from_bytes(reward_account).ok()?;
    address_as_stake_cred(&pool_address)
}

pub fn keyhash_to_stake_cred(keyhash: Hash<28>) -> StakeCredential {
    StakeCredential::AddrKeyhash(keyhash)
}

#[cfg(test)]
mod tests {
    use super::*;

    static REWARD_ACCOUNT: [u8; 29] = [
        224, 185, 111, 206, 243, 185, 53, 26, 246, 131, 75, 216, 80, 227, 169, 120, 89, 215, 189,
        91, 114, 157, 36, 191, 54, 70, 174, 172, 207,
    ];

    #[test]
    fn test_pool_reward_account() {
        let parsed = pool_reward_account(&REWARD_ACCOUNT).unwrap();
        dbg!(&parsed);
    }
}
