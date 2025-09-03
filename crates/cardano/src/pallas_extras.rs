use std::ops::Deref as _;

use dolos_core::BlockSlot;
use pallas::crypto::hash::Hash;
use pallas::ledger::addresses::{Network, StakeAddress, StakePayload};
use pallas::ledger::primitives::{
    alonzo::Certificate as AlonzoCert, conway::Certificate as ConwayCert, PoolMetadata,
    RationalNumber, Relay, StakeCredential,
};
use pallas::ledger::traverse::MultiEraCert;
use serde::{Deserialize, Serialize};

use crate::pparams::ChainSummary;

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

pub fn cert_to_pool_state(cert: &MultiEraCert) -> Option<MultiEraPoolRegistration> {
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
            _ => None,
        },
        _ => None,
    }
}

pub fn cert_as_stake_registration(cert: &MultiEraCert) -> Option<StakeCredential> {
    match cert {
        MultiEraCert::AlonzoCompatible(cow) => match cow.deref().deref() {
            AlonzoCert::StakeRegistration(credential) => Some(credential.clone()),
            AlonzoCert::StakeDeregistration(credential) => Some(credential.clone()),
            AlonzoCert::StakeDelegation(credential, _) => Some(credential.clone()),

            _ => None,
        },
        MultiEraCert::Conway(cow) => match cow.deref().deref() {
            ConwayCert::StakeRegistration(credential) => Some(credential.clone()),
            ConwayCert::StakeDeregistration(credential) => Some(credential.clone()),
            ConwayCert::StakeDelegation(credential, _) => Some(credential.clone()),
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
            _ => None,
        },
        _ => None,
    }
}

pub fn stake_credential_to_address(network: Network, credential: &StakeCredential) -> StakeAddress {
    match credential {
        StakeCredential::ScriptHash(x) => {
            StakeAddress::new(network, StakePayload::Script(x.clone()))
        }
        StakeCredential::AddrKeyhash(x) => {
            StakeAddress::new(network, StakePayload::Stake(x.clone()))
        }
    }
}

pub fn next_epoch_boundary(chain_summary: &ChainSummary, after: BlockSlot) -> BlockSlot {
    let era = chain_summary.era_for_slot(after);
    let epoch_length = era.pparams.epoch_length();
    let (_, epoch_slot) = super::utils::slot_epoch(after, chain_summary);

    let missing = epoch_length - epoch_slot as u64;

    after + missing
}
