use std::ops::Deref as _;

use dolos_core::BlockSlot;
use pallas::crypto::hash::Hash;
use pallas::ledger::addresses::{
    Address, Network, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart, StakeAddress,
    StakePayload,
};
use pallas::ledger::primitives::alonzo::MoveInstantaneousReward;
use pallas::ledger::primitives::conway::{
    CostModels, DRep, DRepVotingThresholds, GovAction, PoolVotingThresholds, RedeemerTag,
    ScriptRef, Voter,
};
use pallas::ledger::primitives::{
    alonzo::Certificate as AlonzoCert, conway::Certificate as ConwayCert, PoolMetadata,
    RationalNumber, Relay, StakeCredential,
};
use pallas::ledger::primitives::{Epoch, ExUnitPrices, ExUnits, Nonce, NonceVariant};
use pallas::ledger::traverse::{
    ComputeHash, MultiEraCert, MultiEraInput, MultiEraRedeemer, MultiEraTx, OriginalHash,
};
use serde::{Deserialize, Serialize};

use crate::eras::ChainSummary;
use crate::{hacks, Lovelace};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MultiEraPoolRetirement {
    pub operator: Hash<28>,
    pub epoch: Epoch,
}

pub fn cert_as_pool_retirement(cert: &MultiEraCert) -> Option<MultiEraPoolRetirement> {
    match cert {
        MultiEraCert::AlonzoCompatible(cow) => match cow.deref().deref() {
            AlonzoCert::PoolRetirement(operator, epoch) => Some(MultiEraPoolRetirement {
                operator: *operator,
                epoch: *epoch,
            }),
            _ => None,
        },
        MultiEraCert::Conway(cow) => match cow.deref().deref() {
            ConwayCert::PoolRetirement(operator, epoch) => Some(MultiEraPoolRetirement {
                operator: *operator,
                epoch: *epoch,
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
            ConwayCert::StakeVoteDeleg(delegator, _, drep) => Some(MultiEraVoteDelegation {
                delegator: delegator.clone(),
                drep: drep.clone(),
            }),
            _ => None,
        },
        _ => None,
    }
}

pub struct MultiEraDRepRegistration {
    pub cred: StakeCredential,
    pub deposit: Lovelace,
}

pub fn cert_as_drep_registration(cert: &MultiEraCert) -> Option<MultiEraDRepRegistration> {
    match cert {
        MultiEraCert::Conway(cow) => match cow.deref().deref() {
            ConwayCert::RegDRepCert(cred, deposit, _) => Some(MultiEraDRepRegistration {
                cred: cred.clone(),
                deposit: *deposit,
            }),
            _ => None,
        },
        _ => None,
    }
}

pub type MultiEraDRepUnRegistration = MultiEraDRepRegistration;

pub fn cert_as_drep_unregistration(cert: &MultiEraCert) -> Option<MultiEraDRepUnRegistration> {
    match cert {
        MultiEraCert::Conway(cow) => match cow.deref().deref() {
            ConwayCert::UnRegDRepCert(cred, deposit) => Some(MultiEraDRepRegistration {
                cred: cred.clone(),
                deposit: *deposit,
            }),
            _ => None,
        },
        _ => None,
    }
}

pub struct MultiEraCommitteeAuth {
    pub cold: StakeCredential,
    pub hot: StakeCredential,
}

pub fn cert_as_committee_auth(cert: &MultiEraCert) -> Option<MultiEraCommitteeAuth> {
    match cert {
        MultiEraCert::Conway(cow) => match cow.deref().deref() {
            ConwayCert::AuthCommitteeHot(cold, hot) => Some(MultiEraCommitteeAuth {
                cold: cold.clone(),
                hot: hot.clone(),
            }),
            _ => None,
        },
        _ => None,
    }
}

pub struct MultiEraCommitteeResign {
    pub cold: StakeCredential,
    pub anchor: Option<pallas::ledger::primitives::conway::Anchor>,
}

pub fn cert_as_committee_resign(cert: &MultiEraCert) -> Option<MultiEraCommitteeResign> {
    match cert {
        MultiEraCert::Conway(cow) => match cow.deref().deref() {
            ConwayCert::ResignCommitteeCold(cold, anchor) => Some(MultiEraCommitteeResign {
                cold: cold.clone(),
                anchor: anchor.clone(),
            }),
            _ => None,
        },
        _ => None,
    }
}

#[derive(Debug)]
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
            ConwayCert::StakeVoteDeleg(delegator, pool, _) => Some(MultiEraStakeDelegation {
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

pub fn cert_as_mir_certificate(cert: &MultiEraCert) -> Option<MoveInstantaneousReward> {
    match cert {
        MultiEraCert::AlonzoCompatible(cow) => match cow.deref().deref() {
            AlonzoCert::MoveInstantaneousRewardsCert(mir) => Some(mir.clone()),
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

pub fn shelley_address_to_stake_cred(
    address: &ShelleyAddress,
) -> Option<(StakeCredential, IsPointer)> {
    match address.delegation() {
        ShelleyDelegationPart::Key(x) => Some((StakeCredential::AddrKeyhash(*x), false)),
        ShelleyDelegationPart::Script(x) => Some((StakeCredential::ScriptHash(*x), false)),
        ShelleyDelegationPart::Pointer(x) => hacks::pointers::pointer_to_cred(x).map(|x| (x, true)),
        ShelleyDelegationPart::Null => None,
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

pub type IsPointer = bool;

pub fn address_as_stake_cred(address: &Address) -> Option<(StakeCredential, IsPointer)> {
    match &address {
        Address::Shelley(x) => shelley_address_to_stake_cred(x),
        Address::Stake(x) => Some((stake_address_to_cred(x), false)),
        _ => None,
    }
}

/// The script credential that witnesses a certificate, if any.
///
/// A certificate can name a script credential. The script must then validate
/// the certificate. A cert-purpose redeemer points at that script. Pool
/// certificates carry only key hashes.
pub fn cert_script_hash(cert: &MultiEraCert) -> Option<Hash<28>> {
    let credential = match cert {
        MultiEraCert::AlonzoCompatible(cow) => match cow.deref().deref() {
            AlonzoCert::StakeRegistration(cred)
            | AlonzoCert::StakeDeregistration(cred)
            | AlonzoCert::StakeDelegation(cred, _) => Some(cred),
            _ => None,
        },
        MultiEraCert::Conway(cow) => match cow.deref().deref() {
            ConwayCert::StakeRegistration(cred)
            | ConwayCert::StakeDeregistration(cred)
            | ConwayCert::StakeDelegation(cred, _)
            | ConwayCert::Reg(cred, _)
            | ConwayCert::UnReg(cred, _)
            | ConwayCert::VoteDeleg(cred, _)
            | ConwayCert::StakeVoteDeleg(cred, _, _)
            | ConwayCert::StakeRegDeleg(cred, _, _)
            | ConwayCert::VoteRegDeleg(cred, _, _)
            | ConwayCert::StakeVoteRegDeleg(cred, _, _, _)
            | ConwayCert::AuthCommitteeHot(cred, _)
            | ConwayCert::ResignCommitteeCold(cred, _)
            | ConwayCert::RegDRepCert(cred, _, _)
            | ConwayCert::UnRegDRepCert(cred, _)
            | ConwayCert::UpdateDRepCert(cred, _) => Some(cred),
            _ => None,
        },
        _ => None,
    }?;

    match credential {
        StakeCredential::ScriptHash(hash) => Some(*hash),
        StakeCredential::AddrKeyhash(_) => None,
    }
}

/// The script that votes at the given redeemer index, if the voter is a
/// script.
///
/// The ledger indexes voters by its `Map Voter` order: committee before
/// dreps before pools, script credentials before key credentials inside
/// each group. Pallas declares the `Voter` variants in that order, so the
/// decoded `BTreeMap` already iterates like the ledger. Do not sort by the
/// cbor encoding: its tag order puts key credentials first.
fn vote_script_hash(tx: &MultiEraTx<'_>, index: usize) -> Option<Hash<28>> {
    let body = &tx.as_conway()?.transaction_body;
    let procedures = body.voting_procedures.as_ref()?;

    match procedures.keys().nth(index)? {
        Voter::ConstitutionalCommitteeScript(hash) | Voter::DRepScript(hash) => Some(*hash),
        _ => None,
    }
}

/// The guardrails script of the proposal at the given redeemer index, if the
/// proposed action names one.
fn propose_script_hash(tx: &MultiEraTx<'_>, index: usize) -> Option<Hash<28>> {
    let body = &tx.as_conway()?.transaction_body;
    let proposals = body.proposal_procedures.as_ref()?;
    let proposal = proposals.get(index)?;

    match &proposal.gov_action {
        GovAction::ParameterChange(_, _, policy) => *policy,
        GovAction::TreasuryWithdrawals(_, policy) => *policy,
        _ => None,
    }
}

/// The hash of the script that a redeemer points at.
///
/// The redeemer's tag and index select an entity in the tx. A spend redeemer
/// indexes the sorted input set. Its script hash is the payment credential of
/// the consumed output's address. That output lives in another tx, so the
/// injected `resolve_input_address` supplies its address. All other purposes
/// resolve inside the tx itself.
pub fn redeemer_script_hash<E, F>(
    tx: &MultiEraTx<'_>,
    redeemer: &MultiEraRedeemer<'_>,
    resolve_input_address: &mut F,
) -> Result<Option<Hash<28>>, E>
where
    F: FnMut(&MultiEraInput<'_>) -> Result<Option<Address>, E>,
{
    let index = redeemer.index() as usize;

    match redeemer.tag() {
        RedeemerTag::Spend => {
            let inputs = tx.inputs_sorted_set();
            let Some(input) = inputs.get(index) else {
                return Ok(None);
            };

            let Some(address) = resolve_input_address(input)? else {
                return Ok(None);
            };

            match address {
                Address::Shelley(x) => match x.payment() {
                    ShelleyPaymentPart::Script(hash) => Ok(Some(*hash)),
                    _ => Ok(None),
                },
                _ => Ok(None),
            }
        }
        RedeemerTag::Mint => {
            let mints = tx.mints_sorted_set();
            Ok(mints.get(index).map(|x| x.policy()).cloned())
        }
        RedeemerTag::Cert => Ok(tx.certs().get(index).and_then(cert_script_hash)),
        RedeemerTag::Reward => {
            let withdrawals = tx.withdrawals_sorted_set();
            let Some((account, _)) = withdrawals.get(index) else {
                return Ok(None);
            };

            match Address::from_bytes(account) {
                Ok(Address::Stake(stake)) => match stake.payload() {
                    StakePayload::Script(hash) => Ok(Some(*hash)),
                    StakePayload::Stake(_) => Ok(None),
                },
                _ => Ok(None),
            }
        }
        RedeemerTag::Vote => Ok(vote_script_hash(tx, index)),
        RedeemerTag::Propose => Ok(propose_script_hash(tx, index)),
    }
}

pub fn epoch_boundary(
    chain_summary: &ChainSummary,
    prev_slot: BlockSlot,
    next_slot: BlockSlot,
) -> Option<(Epoch, BlockSlot, Epoch)> {
    let (prev_epoch, _) = chain_summary.slot_epoch(prev_slot);
    let (next_epoch, _) = chain_summary.slot_epoch(next_slot);

    if prev_epoch != next_epoch {
        let boundary = chain_summary.epoch_start(next_epoch);
        Some((prev_epoch, boundary, next_epoch))
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

/// Compute the on-chain script hash of a reference script.
///
/// Each language hashes its own tagged serialization, so the match must stay
/// per-variant instead of hashing the raw bytes once.
pub fn script_ref_hash(script_ref: &ScriptRef) -> Hash<28> {
    match script_ref {
        ScriptRef::NativeScript(x) => x.original_hash(),
        ScriptRef::PlutusV1Script(x) => x.compute_hash(),
        ScriptRef::PlutusV2Script(x) => x.compute_hash(),
        ScriptRef::PlutusV3Script(x) => x.compute_hash(),
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

pub fn parse_reward_account(reward_account: &[u8]) -> Option<StakeCredential> {
    let pool_address = Address::from_bytes(reward_account).ok()?;
    let (cred, _) = address_as_stake_cred(&pool_address)?;

    Some(cred)
}

pub fn keyhash_to_stake_cred(keyhash: Hash<28>) -> StakeCredential {
    StakeCredential::AddrKeyhash(keyhash)
}

pub fn cred_matches_hash(cred: &StakeCredential, hash: &str) -> bool {
    let hash: Hash<28> = hash.parse().unwrap();

    match cred {
        StakeCredential::AddrKeyhash(x) => x == &hash,
        StakeCredential::ScriptHash(x) => x == &hash,
    }
}

pub fn tx_treasury_donation(tx: &MultiEraTx) -> Option<Lovelace> {
    match tx {
        MultiEraTx::Conway(x) => x.transaction_body.donation.map(|x| x.into()),
        MultiEraTx::AlonzoCompatible(..) => None,
        MultiEraTx::Babbage(..) => None,
        MultiEraTx::Byron(..) => None,
        _ => panic!("unexpected tx era"),
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::model::pools::testing::any_pool_params;
    use crate::model::testing as root;
    use proptest::prelude::*;

    prop_compose! {
        pub fn any_multi_era_pool_registration()(
            operator in root::any_hash_28(),
            params in any_pool_params(),
        ) -> MultiEraPoolRegistration {
            MultiEraPoolRegistration {
                operator,
                vrf_keyhash: params.vrf_keyhash,
                pledge: params.pledge,
                cost: params.cost,
                margin: params.margin,
                reward_account: params.reward_account,
                pool_owners: params.pool_owners,
                relays: params.relays,
                pool_metadata: params.pool_metadata,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    use pallas::{
        codec::minicbor,
        codec::utils::{Bytes, Int, KeepRaw, NonEmptySet, NonZeroInt, Nullable, Set},
        ledger::{
            primitives::{
                conway::{
                    Anchor, GovActionId, ProposalProcedure, Redeemer, Redeemers, TransactionBody,
                    TransactionInput, Tx, Vote, VotingProcedure, WitnessSet,
                },
                BigInt, PlutusData,
            },
            traverse::Era,
        },
    };

    static REWARD_ACCOUNT: [u8; 29] = [
        224, 185, 111, 206, 243, 185, 53, 26, 246, 131, 75, 216, 80, 227, 169, 120, 89, 215, 189,
        91, 114, 157, 36, 191, 54, 70, 174, 172, 207,
    ];

    #[test]
    fn test_pool_reward_account() {
        let parsed = parse_reward_account(&REWARD_ACCOUNT).unwrap();
        dbg!(&parsed);
    }

    const SCRIPT_HASH: [u8; 28] = [0xAA; 28];

    fn redeemer(tag: RedeemerTag, index: u32) -> Redeemer {
        Redeemer {
            tag,
            index,
            data: PlutusData::BigInt(BigInt::Int(Int::from(0))),
            ex_units: ExUnits {
                mem: 10,
                steps: 100,
            },
        }
    }

    /// A conway tx with one redeemer of every purpose. Each redeemer points
    /// at an entity under [`SCRIPT_HASH`]: the first input (resolved by the
    /// injected closure), a mint of its policy, a stake deregistration of
    /// its stake credential, a withdrawal from its reward account, a vote by
    /// it as a script DRep, and a treasury withdrawal guarded by it.
    fn tx_with_redeemers() -> Vec<u8> {
        let script = Hash::<28>::from(SCRIPT_HASH);

        let input = TransactionInput {
            transaction_id: Hash::from([9u8; 32]),
            index: 0,
        };

        let asset_name = Bytes::from(b"UNIT".to_vec());
        let mint_amount = NonZeroInt::try_from(-1).expect("non-zero mint");
        let mint =
            BTreeMap::from_iter([(script, BTreeMap::from_iter([(asset_name, mint_amount)]))]);

        let cert = ConwayCert::StakeDeregistration(StakeCredential::ScriptHash(script));

        let reward_account = StakeAddress::new(Network::Testnet, StakePayload::Script(script));
        let withdrawals = BTreeMap::from_iter([(Bytes::from(reward_account.to_vec()), 0)]);

        let ballot = BTreeMap::from_iter([(
            GovActionId {
                transaction_id: Hash::from([7u8; 32]),
                action_index: 0,
            },
            VotingProcedure {
                vote: Vote::Yes,
                anchor: None,
            },
        )]);
        let voting_procedures = BTreeMap::from_iter([(Voter::DRepScript(script), ballot.clone())]);

        let proposal = ProposalProcedure {
            deposit: 0,
            reward_account: Bytes::from(reward_account.to_vec()),
            gov_action: GovAction::TreasuryWithdrawals(BTreeMap::new(), Some(script)),
            anchor: Anchor {
                url: "https://example.com".to_string(),
                content_hash: Hash::from([8u8; 32]),
            },
        };

        let body = TransactionBody {
            inputs: Set::from(vec![input]),
            outputs: vec![],
            fee: 0,
            ttl: None,
            certificates: Some(NonEmptySet::try_from(vec![cert]).expect("non-empty certs")),
            withdrawals: Some(withdrawals),
            auxiliary_data_hash: None,
            validity_interval_start: None,
            mint: Some(mint),
            script_data_hash: None,
            collateral: None,
            required_signers: None,
            network_id: None,
            collateral_return: None,
            total_collateral: None,
            reference_inputs: None,
            voting_procedures: Some(voting_procedures),
            proposal_procedures: Some(
                NonEmptySet::try_from(vec![proposal]).expect("non-empty proposals"),
            ),
            treasury_value: None,
            donation: None,
        };

        let redeemers = Redeemers::List(vec![
            redeemer(RedeemerTag::Spend, 0),
            redeemer(RedeemerTag::Mint, 0),
            redeemer(RedeemerTag::Cert, 0),
            redeemer(RedeemerTag::Reward, 0),
            redeemer(RedeemerTag::Vote, 0),
            redeemer(RedeemerTag::Propose, 0),
        ]);

        let witness_set = WitnessSet {
            vkeywitness: None,
            native_script: None,
            bootstrap_witness: None,
            plutus_v1_script: None,
            plutus_data: None,
            redeemer: Some(KeepRaw::from(redeemers)),
            plutus_v2_script: None,
            plutus_v3_script: None,
        };

        let body_cbor = minicbor::to_vec(&body).expect("failed to encode body");
        let body = minicbor::decode::<KeepRaw<'_, TransactionBody<'_>>>(&body_cbor)
            .expect("failed to decode body")
            .to_owned();

        let tx = Tx {
            transaction_body: body,
            transaction_witness_set: KeepRaw::from(witness_set),
            success: true,
            auxiliary_data: Nullable::Null,
        };

        minicbor::to_vec(tx).expect("failed to encode tx")
    }

    fn shelley_address(payment: ShelleyPaymentPart) -> Address {
        Address::Shelley(ShelleyAddress::new(
            Network::Testnet,
            payment,
            ShelleyDelegationPart::Null,
        ))
    }

    #[test]
    fn redeemer_script_hash_resolves_every_purpose() {
        let bytes = tx_with_redeemers();
        let tx = MultiEraTx::decode_for_era(Era::Conway, &bytes).expect("decodable tx");

        let script = Hash::<28>::from(SCRIPT_HASH);
        let redeemers = tx.redeemers();
        assert_eq!(redeemers.len(), 6);

        for redeemer in &redeemers {
            let resolved = redeemer_script_hash(&tx, redeemer, &mut |input| {
                assert_eq!(input.hash().as_slice(), [9u8; 32].as_slice());
                Ok::<_, ()>(Some(shelley_address(ShelleyPaymentPart::Script(script))))
            })
            .expect("resolution failed");

            assert_eq!(resolved, Some(script), "purpose {:?}", redeemer.tag());
        }
    }

    #[test]
    fn redeemer_script_hash_ignores_key_held_inputs() {
        let bytes = tx_with_redeemers();
        let tx = MultiEraTx::decode_for_era(Era::Conway, &bytes).expect("decodable tx");

        let redeemers = tx.redeemers();
        let spend = redeemers
            .iter()
            .find(|x| matches!(x.tag(), RedeemerTag::Spend))
            .expect("spend redeemer");

        let resolved = redeemer_script_hash(&tx, spend, &mut |_| {
            Ok::<_, ()>(Some(shelley_address(ShelleyPaymentPart::key_hash(
                Hash::from([0xBBu8; 28]),
            ))))
        })
        .expect("resolution failed");

        assert_eq!(resolved, None, "a key-held input names no script");

        let resolved =
            redeemer_script_hash(&tx, spend, &mut |_| Ok::<_, ()>(None)).expect("resolution");

        assert_eq!(resolved, None, "an unresolvable input names no script");
    }

    #[test]
    fn governance_lookups_handle_out_of_range_indexes() {
        let bytes = tx_with_redeemers();
        let tx = MultiEraTx::decode_for_era(Era::Conway, &bytes).expect("decodable tx");

        // the fixture carries one voter and one proposal, both at index 0
        assert_eq!(vote_script_hash(&tx, 1), None);
        assert_eq!(propose_script_hash(&tx, 1), None);
    }

    /// A conway tx whose only content is one voter of every kind.
    fn tx_with_mixed_voters() -> Vec<u8> {
        let ballot = BTreeMap::from_iter([(
            GovActionId {
                transaction_id: Hash::from([7u8; 32]),
                action_index: 0,
            },
            VotingProcedure {
                vote: Vote::Yes,
                anchor: None,
            },
        )]);

        let voting_procedures = BTreeMap::from_iter([
            (
                Voter::ConstitutionalCommitteeKey([1u8; 28].into()),
                ballot.clone(),
            ),
            (
                Voter::ConstitutionalCommitteeScript([2u8; 28].into()),
                ballot.clone(),
            ),
            (Voter::DRepKey([3u8; 28].into()), ballot.clone()),
            (Voter::DRepScript([4u8; 28].into()), ballot.clone()),
            (Voter::StakePoolKey([5u8; 28].into()), ballot),
        ]);

        let body = TransactionBody {
            inputs: Set::from(vec![]),
            outputs: vec![],
            fee: 0,
            ttl: None,
            certificates: None,
            withdrawals: None,
            auxiliary_data_hash: None,
            validity_interval_start: None,
            mint: None,
            script_data_hash: None,
            collateral: None,
            required_signers: None,
            network_id: None,
            collateral_return: None,
            total_collateral: None,
            reference_inputs: None,
            voting_procedures: Some(voting_procedures),
            proposal_procedures: None,
            treasury_value: None,
            donation: None,
        };

        let witness_set = WitnessSet {
            vkeywitness: None,
            native_script: None,
            bootstrap_witness: None,
            plutus_v1_script: None,
            plutus_data: None,
            redeemer: None,
            plutus_v2_script: None,
            plutus_v3_script: None,
        };

        let body_cbor = minicbor::to_vec(&body).expect("failed to encode body");
        let body = minicbor::decode::<KeepRaw<'_, TransactionBody<'_>>>(&body_cbor)
            .expect("failed to decode body")
            .to_owned();

        let tx = Tx {
            transaction_body: body,
            transaction_witness_set: KeepRaw::from(witness_set),
            success: true,
            auxiliary_data: Nullable::Null,
        };

        minicbor::to_vec(tx).expect("failed to encode tx")
    }

    /// The ledger orders voters by group, then script before key. The cbor
    /// tag order puts keys first, so an encoding sort resolves the wrong
    /// voter. This pins the ledger order per index.
    #[test]
    fn vote_script_hash_follows_ledger_voter_order() {
        let bytes = tx_with_mixed_voters();
        let tx = MultiEraTx::decode_for_era(Era::Conway, &bytes).expect("decodable tx");

        // committee script, committee key, drep script, drep key, pool
        assert_eq!(vote_script_hash(&tx, 0), Some([2u8; 28].into()));
        assert_eq!(vote_script_hash(&tx, 1), None);
        assert_eq!(vote_script_hash(&tx, 2), Some([4u8; 28].into()));
        assert_eq!(vote_script_hash(&tx, 3), None);
        assert_eq!(vote_script_hash(&tx, 4), None);
        assert_eq!(vote_script_hash(&tx, 5), None);
    }
}
