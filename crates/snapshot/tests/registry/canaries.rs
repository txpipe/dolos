//! The seventeen canaries: one fully-populated value per namespace.
//!
//! A canary is a value in which **every field is present and distinctive** —
//! every `Option` is `Some`, every collection is non-empty, every nested type
//! is itself a canary. That is what makes the pinned encoding a *field table*
//! rather than a sample: a field that is renumbered, removed, widened or
//! silently re-encoded moves bytes that a partly-defaulted value would have
//! left as nulls.
//!
//! Writing these constructors is also the entity-encoding audit ADR-004's
//! Limitations section asks for: every field of every model type reachable
//! from a stele is named here, so a type that cannot be built deterministically
//! cannot be pinned and shows up as a failing determinism assertion instead of
//! as a golden.
//!
//! Values are built through the model's **public** API only. `EpochValue`'s
//! slots in particular are filled by driving `schedule`/`transition` the way
//! the ledger does, not by a test-only raw constructor — the encoding a stele
//! carries is the encoding the ledger writes.

use std::collections::{BTreeMap, BTreeSet};

use dolos_cardano::{
    model::{
        AccountEpochLog, AccountStakeLog, AccountState, AssetState, AuthHistory, Committee,
        CommitteeAuthorization, Constitution, DRepDelegation, DRepExpiry, DRepState, DatumState,
        EndStats, EpochState, EpochValue, EraBoundary, EraSummary, GovDistr, GovRoots, GovState,
        LeaderRewardLog, MemberRewardLog, Nonces, PParamValue, PParamsSet, PendingMirState,
        PendingRewardState, PoolDelegation, PoolDepositRefundLog, PoolParams, PoolSnapshot,
        PoolState, ProposalAction, ProposalState, RollingStats, ShardProgress, Stake, StakeLog,
        VoteHistory,
    },
    pallas::{
        codec::utils::Bytes,
        crypto::hash::Hash,
        ledger::primitives::{
            conway::{Anchor, DRep, GovActionId, RationalNumber, Vote},
            Epoch, ExUnitPrices, ExUnits, Nonce, NonceVariant, PoolMetadata, Relay,
            StakeCredential,
        },
    },
    pots::{EpochIncentives, Pots},
};
use dolos_core::{cbor, EraCbor};

// Every byte differs from its neighbours, so a field that shifts by one
// position, or a hash that is truncated rather than carried whole, changes the
// pinned bytes instead of landing on a repeated value that hides the move.

pub fn bytes_of<const N: usize>(seed: u8) -> [u8; N] {
    let mut out = [0u8; N];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = seed.wrapping_add(i as u8);
    }
    out
}

pub fn hash28(seed: u8) -> Hash<28> {
    Hash::new(bytes_of::<28>(seed))
}

pub fn hash32(seed: u8) -> Hash<32> {
    Hash::new(bytes_of::<32>(seed))
}

fn rational(numerator: u64, denominator: u64) -> RationalNumber {
    RationalNumber {
        numerator,
        denominator,
    }
}

fn anchor(seed: u8) -> Anchor {
    Anchor {
        url: format!("https://canary.invalid/{seed}"),
        content_hash: hash32(seed),
    }
}

fn gov_action_id(seed: u8, index: u32) -> GovActionId {
    GovActionId {
        transaction_id: hash32(seed),
        action_index: index,
    }
}

/// An `EpochValue` with all six of its fields populated, filled the way the
/// ledger fills one: three `schedule`/`transition` rounds and a final
/// `schedule`, leaving `go`, `set`, `mark`, `live` and `next` distinct and the
/// position three epochs past `start`.
///
/// Driving the public API rather than the `cfg(test)` raw constructor is the
/// point — a canary built by a back door would pin an arrangement the ledger
/// cannot actually produce.
fn filled_epoch_value<T: Clone + std::fmt::Debug>(start: Epoch, slots: [T; 5]) -> EpochValue<T> {
    let [go, set, mark, live, next] = slots;

    let mut value = EpochValue::with_live(start, go);

    for (offset, scheduled) in [set, mark, live].into_iter().enumerate() {
        let epoch = start + offset as Epoch;
        value.schedule(epoch, Some(scheduled));
        value.transition(epoch + 1);
    }

    value.schedule(start + 3, Some(next));

    value
}

/// Every field populated, both lists at two entries: the arity the merge
/// exists to make expressible, and the one the four namespaces it replaces
/// could not carry.
pub fn account_epoch_log() -> AccountEpochLog {
    AccountEpochLog {
        active_stake: Some(9_876_543_210),
        pool_id: Some(hash28(0x11)),
        member_reward: Some(2_345_678_901),
        leader_rewards: vec![(hash28(0x21), 1_234_567_890), (hash28(0x22), 987_654_321)],
        deposit_refunds: vec![(hash28(0x41), 500_000_000), (hash28(0x42), 500_000_000)],
    }
}

pub fn account_stake_log() -> AccountStakeLog {
    AccountStakeLog {
        amount: 9_876_543_210,
        pool_id: bytes_of::<28>(0x11).to_vec(),
    }
}

pub fn leader_reward_log() -> LeaderRewardLog {
    LeaderRewardLog {
        amount: 1_234_567_890,
        pool_id: bytes_of::<28>(0x21).to_vec(),
    }
}

pub fn member_reward_log() -> MemberRewardLog {
    MemberRewardLog {
        amount: 2_345_678_901,
        pool_id: bytes_of::<28>(0x31).to_vec(),
    }
}

pub fn pool_deposit_refund_log() -> PoolDepositRefundLog {
    PoolDepositRefundLog {
        amount: 500_000_000,
        pool_id: bytes_of::<28>(0x41).to_vec(),
    }
}

/// The one float in the whole profile.
///
/// `relative_size` is pinned at an exactly-representable power of two, so the
/// golden fixes the IEEE-754 bits and nothing else: a value that needed
/// rounding would pin the rounding rather than the field.
pub fn stake_log() -> StakeLog {
    StakeLog {
        blocks_minted: 42,
        total_stake: 123_456_789_000,
        relative_size: 0.015625,
        delegators_count: 317,
        live_pledge: 30_000_000_000,
        declared_pledge: 25_000_000_000,
        total_rewards: 4_400_000_000,
        operator_share: 340_000_000,
        fixed_cost: 170_000_000,
        margin_cost: Some(rational(3, 100)),
    }
}

fn stake(seed: u64) -> Stake {
    Stake {
        utxo_sum: 1_000_000 + seed,
        rewards_sum: 2_000_000 + seed,
        withdrawals_sum: 3_000 + seed,
        utxo_sum_at_pointer_addresses: 40_000 + seed,
    }
}

pub fn account_state() -> AccountState {
    AccountState {
        registered_at: Some(11_111),
        stake: filled_epoch_value(300, [stake(1), stake(2), stake(3), stake(4), stake(5)]),
        pool: filled_epoch_value(
            300,
            [
                PoolDelegation::Pool(hash28(0x50)),
                PoolDelegation::NotDelegated,
                PoolDelegation::Pool(hash28(0x51)),
                PoolDelegation::Pool(hash28(0x52)),
                PoolDelegation::Pool(hash28(0x53)),
            ],
        ),
        drep: filled_epoch_value(
            300,
            [
                DRepDelegation::Delegated(DRep::Key(hash28(0x60))),
                DRepDelegation::NotDelegated,
                DRepDelegation::Delegated(DRep::Script(hash28(0x61))),
                DRepDelegation::Delegated(DRep::Abstain),
                DRepDelegation::Delegated(DRep::NoConfidence),
            ],
        ),
        vote_delegated_at: Some((22_222, 7)),
        deregistered_at: Some(33_333),
        credential: StakeCredential::AddrKeyhash(hash28(0x70)),
        retired_pool: Some(hash28(0x71)),
    }
}

pub fn asset_state() -> AssetState {
    AssetState {
        quantity_bytes: bytes_of::<16>(0x80),
        initial_tx: Some(hash32(0x81)),
        initial_slot: Some(44_444),
        mint_tx_count: 19,
        metadata_tx: Some(hash32(0x82)),
    }
}

pub fn datum_state() -> DatumState {
    DatumState {
        refcount: 23,
        bytes: bytes_of::<24>(0x90).to_vec(),
    }
}

pub fn drep_state() -> DRepState {
    DRepState {
        registered_at: Some((55_555, 3)),
        voting_power: 777_000_000,
        last_active_slot: Some(66_666),
        unregistered_at: Some((77_777, 11)),
        expired: true,
        deposit: 500_000_000,
        identifier: DRep::Key(hash28(0xa0)),
        anchor: Some(anchor(0xa1)),
        expiry: Some(DRepExpiry {
            current: 412,
            updated_in: 409,
            prev: Some(400),
        }),
    }
}

fn pots() -> Pots {
    Pots {
        reserves: 8_000_000_000_000,
        treasury: 1_500_000_000_000,
        utxos: 30_000_000_000_000,
        rewards: 400_000_000_000,
        fees: 900_000_000,
        pool_count: 3_100,
        account_count: 1_300_000,
        deposit_per_pool: 500_000_000,
        deposit_per_account: 2_000_000,
        nominal_deposits: 123_000_000,
        drep_deposits: 45_000_000,
        proposal_deposits: 67_000_000,
    }
}

fn epoch_incentives(seed: u64) -> EpochIncentives {
    EpochIncentives {
        total: 30_000_000_000 + seed,
        treasury_tax: 6_000_000_000 + seed,
        available_rewards: 24_000_000_000 + seed,
        used_fees: 800_000_000 + seed,
    }
}

fn rolling_stats(seed: u64) -> RollingStats {
    RollingStats {
        produced_utxos: 10_000_000 + seed,
        consumed_utxos: 9_000_000 + seed,
        gathered_fees: 800_000 + seed,
        new_accounts: 700 + seed,
        removed_accounts: 60 + seed,
        withdrawals: 50_000 + seed,
        registered_pools: BTreeSet::from([hash28(0xb0), hash28(0xb1), hash28(0xb2)]),
        blocks_minted: 21_600 + seed as u32,
        drep_deposits: 4_000 + seed,
        proposal_deposits: 300 + seed,
        drep_refunds: 20 + seed,
        __proposal_refunds: 1 + seed,
        treasury_donations: 11_000 + seed,
        reserve_mirs: 12_000 + seed,
        non_overlay_blocks_minted: 21_000 + seed as u32,
        treasury_mirs: 13_000 + seed,
        tx_count: 140_000 + seed,
        output: cbor::U128::new(340_282_366_920_938_463_463u128 + seed as u128),
        first_block_slot: 86_400_000 + seed,
        last_block_slot: 86_831_999 + seed,
    }
}

fn pparams_set(seed: u64) -> PParamsSet {
    let mut set = PParamsSet::default();

    set.set(PParamValue::MinFeeA(44 + seed));
    set.set(PParamValue::ProtocolVersion((10, 1)));
    set.set(PParamValue::ExecutionCosts(ExUnitPrices {
        mem_price: rational(577, 10_000),
        step_price: rational(721, 10_000_000),
    }));
    set.set(PParamValue::MaxTxExUnits(ExUnits {
        mem: 14_000_000,
        steps: 10_000_000_000,
    }));

    set
}

fn end_stats() -> EndStats {
    EndStats {
        pool_deposit_count: 12,
        pool_refund_count: 5,
        pool_invalid_refund_count: 1,
        epoch_incentives: epoch_incentives(7),
        effective_rewards: 23_000_000_000,
        unspendable_to_treasury: 900_000,
        unspendable_to_reserves: 800_000,
        treasury_mirs: 700_000,
        reserve_mirs: 600_000,
        invalid_treasury_mirs: 500_000,
        invalid_reserve_mirs: 400_000,
        treasury_withdrawals: 300_000,
        invalid_treasury_withdrawals: 200_000,
        proposal_invalid_refunds: 100_000,
        proposal_refunds: 90_000,
        __drep_deposits: 80_000,
        __drep_refunds: 70_000,
    }
}

pub fn epoch_state() -> EpochState {
    EpochState {
        number: 402,
        initial_pots: pots(),
        rolling: filled_epoch_value(
            399,
            [
                rolling_stats(1),
                rolling_stats(2),
                rolling_stats(3),
                rolling_stats(4),
                rolling_stats(5),
            ],
        ),
        pparams: filled_epoch_value(
            399,
            [
                pparams_set(1),
                pparams_set(2),
                pparams_set(3),
                pparams_set(4),
                pparams_set(5),
            ],
        ),
        largest_stable_slot: 88_888_888,
        previous_nonce_tail: Some(hash32(0xc0)),
        nonces: Some(Nonces {
            active: hash32(0xc1),
            evolving: hash32(0xc2),
            candidate: hash32(0xc3),
            tail: Some(hash32(0xc4)),
        }),
        end: Some(end_stats()),
        incentives: Some(epoch_incentives(9)),
        ewrap_progress: Some(ShardProgress {
            committed: 3,
            total: 8,
        }),
        estart_progress: Some(ShardProgress {
            committed: 5,
            total: 8,
        }),
        rupd_progress: Some(ShardProgress {
            committed: 7,
            total: 8,
        }),
    }
}

pub fn era_summary() -> EraSummary {
    EraSummary {
        start: EraBoundary {
            epoch: 208,
            slot: 4_492_800,
            timestamp: 1_596_059_091,
        },
        end: Some(EraBoundary {
            epoch: 236,
            slot: 16_588_800,
            timestamp: 1_608_155_091,
        }),
        epoch_length: 432_000,
        slot_length: 1,
        protocol: 4,
    }
}

pub fn gov_state() -> GovState {
    let cold_a = StakeCredential::AddrKeyhash(hash28(0xd0));
    let cold_b = StakeCredential::ScriptHash(hash28(0xd1));

    let auths: BTreeMap<StakeCredential, AuthHistory> = BTreeMap::from([
        (
            cold_a.clone(),
            vec![
                (
                    100_000u64,
                    CommitteeAuthorization::HotCredential(StakeCredential::AddrKeyhash(hash28(
                        0xd2,
                    ))),
                ),
                (
                    200_000u64,
                    CommitteeAuthorization::Resigned(Some(anchor(0xd3))),
                ),
            ],
        ),
        (
            cold_b.clone(),
            vec![(300_000u64, CommitteeAuthorization::Resigned(None))],
        ),
    ]);

    GovState {
        constitution: Some(Constitution {
            anchor: anchor(0xd4),
            guardrail_script: Some(hash28(0xd5)),
        }),
        committee: Some(Committee {
            members: BTreeMap::from([(cold_a, 430u64), (cold_b, 440u64)]),
            threshold: rational(2, 3),
        }),
        committee_auths: auths,
        prev_gov_action_ids: GovRoots {
            pparam_update: Some(gov_action_id(0xd6, 0)),
            hard_fork: Some(gov_action_id(0xd7, 1)),
            committee: Some(gov_action_id(0xd8, 2)),
            constitution: Some(gov_action_id(0xd9, 3)),
        },
        num_dormant_epochs: 6,
        active_since: Some(507),
        distr: Some(gov_distr(0xda, 4)),
        prev_distr: Some(gov_distr(0xdb, 8)),
    }
}

fn gov_distr(seed: u8, epoch_offset: Epoch) -> GovDistr {
    GovDistr {
        closing_epoch: 500 + epoch_offset,
        committed_shards: 6,
        total_shards: 8,
        drep_distr: BTreeMap::from([
            (DRep::Key(hash28(seed)), 111_000_000u64),
            (DRep::Script(hash28(seed.wrapping_add(1))), 222_000_000),
            (DRep::Abstain, 333_000_000),
            (DRep::NoConfidence, 444_000_000),
        ]),
        pool_distr: BTreeMap::from([
            (hash28(seed.wrapping_add(2)), 555_000_000u64),
            (hash28(seed.wrapping_add(3)), 666_000_000),
        ]),
        pool_total: 1_221_000_000,
    }
}

pub fn pending_mir_state() -> PendingMirState {
    PendingMirState {
        credential: StakeCredential::ScriptHash(hash28(0xe0)),
        from_reserves: 7_000_000,
        from_treasury: 8_000_000,
    }
}

pub fn pending_reward_state() -> PendingRewardState {
    PendingRewardState {
        credential: StakeCredential::AddrKeyhash(hash28(0xe1)),
        is_spendable: true,
        as_leader: vec![(hash28(0xe2), 900_000u64), (hash28(0xe3), 800_000)],
        as_delegator: vec![(hash28(0xe4), 700_000u64), (hash28(0xe5), 600_000)],
    }
}

fn pool_params(seed: u8) -> PoolParams {
    PoolParams {
        vrf_keyhash: hash32(seed),
        pledge: 100_000_000_000,
        cost: 170_000_000,
        margin: rational(3, 100),
        reward_account: bytes_of::<29>(seed.wrapping_add(1)).to_vec(),
        pool_owners: vec![hash28(seed.wrapping_add(2)), hash28(seed.wrapping_add(3))],
        // All three `Relay` shapes in one canary: the pinned bytes then carry
        // the whole variant table of a type this profile does not own.
        relays: vec![
            Relay::SingleHostAddr(
                Some(3001),
                Some(Bytes::from(bytes_of::<4>(0x01).to_vec())),
                Some(Bytes::from(bytes_of::<16>(0x02).to_vec())),
            ),
            Relay::SingleHostName(Some(3002), "relay.canary.invalid".to_string()),
            Relay::MultiHostName("_cardano._tcp.canary.invalid".to_string()),
        ],
        pool_metadata: Some(PoolMetadata {
            url: "https://canary.invalid/pool.json".to_string(),
            hash: Bytes::from(bytes_of::<32>(seed.wrapping_add(4)).to_vec()),
        }),
    }
}

fn pool_snapshot(seed: u8) -> PoolSnapshot {
    PoolSnapshot {
        is_retired: seed.is_multiple_of(2),
        blocks_minted: 128 + seed as u32,
        params: pool_params(seed),
        is_new: seed.is_multiple_of(3),
    }
}

pub fn pool_state() -> PoolState {
    PoolState {
        operator: hash28(0xf0),
        snapshot: filled_epoch_value(
            401,
            [
                pool_snapshot(0x01),
                pool_snapshot(0x02),
                pool_snapshot(0x03),
                pool_snapshot(0x04),
                pool_snapshot(0x05),
            ],
        ),
        blocks_minted_total: 4_242,
        register_slot: 55_555_555,
        retiring_epoch: Some(777),
        deposit: 500_000_000,
    }
}

fn vote_history(seed: u8) -> VoteHistory {
    vec![
        (400_000u64 + seed as u64, Vote::Yes),
        (500_000u64 + seed as u64, Vote::No),
        (600_000u64 + seed as u64, Vote::Abstain),
    ]
}

pub fn proposal_state() -> ProposalState {
    ProposalState {
        slot: 99_999_999,
        tx: hash32(0x12),
        idx: 3,
        action: ProposalAction::UpdateCommittee {
            to_remove: vec![
                StakeCredential::AddrKeyhash(hash28(0x13)),
                StakeCredential::ScriptHash(hash28(0x14)),
            ],
            to_add: vec![
                (StakeCredential::AddrKeyhash(hash28(0x15)), 520u64),
                (StakeCredential::ScriptHash(hash28(0x16)), 530u64),
            ],
            threshold: rational(2, 3),
        },
        max_epoch: Some(515),
        ratified_epoch: Some(512),
        canceled_epoch: Some(513),
        deposit: Some(100_000_000_000),
        reward_account: Some(StakeCredential::AddrKeyhash(hash28(0x17))),
        proposed_in: Some(508),
        parent: Some(gov_action_id(0x18, 4)),
        purpose: Some(dolos_cardano::model::GovPurpose::Committee),
        anchor: Some(anchor(0x19)),
        cc_votes: BTreeMap::from([
            (StakeCredential::AddrKeyhash(hash28(0x1a)), vote_history(1)),
            (StakeCredential::ScriptHash(hash28(0x1b)), vote_history(2)),
        ]),
        drep_votes: BTreeMap::from([
            (StakeCredential::AddrKeyhash(hash28(0x1c)), vote_history(3)),
            (StakeCredential::ScriptHash(hash28(0x1d)), vote_history(4)),
        ]),
        spo_votes: BTreeMap::from([
            (hash28(0x1e), vote_history(5)),
            (hash28(0x1f), vote_history(6)),
        ]),
    }
}

/// The one namespace whose value the profile *builds* rather than carries: the
/// `[era, body]` wrapper of `crate::layers::state`.
pub fn utxo_value() -> EraCbor {
    EraCbor(6, bytes_of::<48>(0x33).to_vec())
}

// The values the enum tables pin; nothing else builds them.

pub fn every_pparam_value() -> Vec<(&'static str, PParamValue)> {
    vec![
        ("SystemStart", PParamValue::SystemStart(1_506_203_091)),
        ("EpochLength", PParamValue::EpochLength(432_000)),
        ("SlotLength", PParamValue::SlotLength(1)),
        ("MinFeeA", PParamValue::MinFeeA(44)),
        ("MinFeeB", PParamValue::MinFeeB(155_381)),
        ("MaxBlockBodySize", PParamValue::MaxBlockBodySize(90_112)),
        (
            "MaxTransactionSize",
            PParamValue::MaxTransactionSize(16_384),
        ),
        ("MaxBlockHeaderSize", PParamValue::MaxBlockHeaderSize(1_100)),
        ("KeyDeposit", PParamValue::KeyDeposit(2_000_000)),
        ("PoolDeposit", PParamValue::PoolDeposit(500_000_000)),
        (
            "DesiredNumberOfStakePools",
            PParamValue::DesiredNumberOfStakePools(500),
        ),
        ("ProtocolVersion", PParamValue::ProtocolVersion((10, 1))),
        ("MinUtxoValue", PParamValue::MinUtxoValue(1_000_000)),
        ("MinPoolCost", PParamValue::MinPoolCost(170_000_000)),
        (
            "ExpansionRate",
            PParamValue::ExpansionRate(rational(3, 1_000)),
        ),
        (
            "TreasuryGrowthRate",
            PParamValue::TreasuryGrowthRate(rational(2, 10)),
        ),
        ("MaximumEpoch", PParamValue::MaximumEpoch(18)),
        (
            "PoolPledgeInfluence",
            PParamValue::PoolPledgeInfluence(rational(3, 10)),
        ),
        (
            "DecentralizationConstant",
            PParamValue::DecentralizationConstant(rational(0, 1)),
        ),
        (
            "ExtraEntropy",
            PParamValue::ExtraEntropy(Nonce {
                variant: NonceVariant::Nonce,
                hash: Some(hash32(0x44)),
            }),
        ),
        ("AdaPerUtxoByte", PParamValue::AdaPerUtxoByte(4_310)),
        (
            "ExecutionCosts",
            PParamValue::ExecutionCosts(ExUnitPrices {
                mem_price: rational(577, 10_000),
                step_price: rational(721, 10_000_000),
            }),
        ),
        (
            "MaxTxExUnits",
            PParamValue::MaxTxExUnits(ExUnits {
                mem: 14_000_000,
                steps: 10_000_000_000,
            }),
        ),
        (
            "MaxBlockExUnits",
            PParamValue::MaxBlockExUnits(ExUnits {
                mem: 62_000_000,
                steps: 20_000_000_000,
            }),
        ),
        ("MaxValueSize", PParamValue::MaxValueSize(5_000)),
        (
            "CollateralPercentage",
            PParamValue::CollateralPercentage(150),
        ),
        ("MaxCollateralInputs", PParamValue::MaxCollateralInputs(3)),
        (
            "PoolVotingThresholds",
            PParamValue::PoolVotingThresholds(
                dolos_cardano::pallas::ledger::primitives::conway::PoolVotingThresholds {
                    motion_no_confidence: rational(51, 100),
                    committee_normal: rational(52, 100),
                    committee_no_confidence: rational(53, 100),
                    hard_fork_initiation: rational(54, 100),
                    security_voting_threshold: rational(55, 100),
                },
            ),
        ),
        (
            "DrepVotingThresholds",
            PParamValue::DrepVotingThresholds(
                dolos_cardano::pallas::ledger::primitives::conway::DRepVotingThresholds {
                    motion_no_confidence: rational(61, 100),
                    committee_normal: rational(62, 100),
                    committee_no_confidence: rational(63, 100),
                    update_constitution: rational(64, 100),
                    hard_fork_initiation: rational(65, 100),
                    pp_network_group: rational(66, 100),
                    pp_economic_group: rational(67, 100),
                    pp_technical_group: rational(68, 100),
                    pp_governance_group: rational(69, 100),
                    treasury_withdrawal: rational(70, 100),
                },
            ),
        ),
        ("MinCommitteeSize", PParamValue::MinCommitteeSize(7)),
        ("CommitteeTermLimit", PParamValue::CommitteeTermLimit(146)),
        (
            "GovernanceActionValidityPeriod",
            PParamValue::GovernanceActionValidityPeriod(6),
        ),
        (
            "GovernanceActionDeposit",
            PParamValue::GovernanceActionDeposit(100_000_000_000),
        ),
        ("DrepDeposit", PParamValue::DrepDeposit(500_000_000)),
        (
            "DrepInactivityPeriod",
            PParamValue::DrepInactivityPeriod(20),
        ),
        (
            "MinFeeRefScriptCostPerByte",
            PParamValue::MinFeeRefScriptCostPerByte(rational(15, 1)),
        ),
        (
            "CostModelsPlutusV1",
            PParamValue::CostModelsPlutusV1(vec![100_788, -1, 420]),
        ),
        (
            "CostModelsPlutusV2",
            PParamValue::CostModelsPlutusV2(vec![100_789, -2, 421]),
        ),
        (
            "CostModelsPlutusV3",
            PParamValue::CostModelsPlutusV3(vec![100_790, -3, 422]),
        ),
        (
            "CostModelsUnknown",
            PParamValue::CostModelsUnknown(BTreeMap::from([
                (4u64, vec![1i64, 2, 3]),
                (5u64, vec![4i64, 5, 6]),
            ])),
        ),
    ]
}

pub fn every_proposal_action() -> Vec<(&'static str, ProposalAction)> {
    vec![
        ("ParamChange", ProposalAction::ParamChange(pparams_set(1))),
        ("HardFork", ProposalAction::HardFork((10, 2))),
        (
            "TreasuryWithdrawal",
            ProposalAction::TreasuryWithdrawal(vec![
                (StakeCredential::AddrKeyhash(hash28(0x22)), 1_000_000u64),
                (StakeCredential::ScriptHash(hash28(0x23)), 2_000_000),
            ]),
        ),
        ("Other", ProposalAction::Other),
        ("NoConfidence", ProposalAction::NoConfidence),
        (
            "UpdateCommittee",
            ProposalAction::UpdateCommittee {
                to_remove: vec![StakeCredential::AddrKeyhash(hash28(0x24))],
                to_add: vec![(StakeCredential::ScriptHash(hash28(0x25)), 540u64)],
                threshold: rational(2, 3),
            },
        ),
        (
            "NewConstitution",
            ProposalAction::NewConstitution {
                anchor: anchor(0x26),
                guardrail_script: Some(hash28(0x27)),
            },
        ),
        ("Info", ProposalAction::Info),
    ]
}

pub fn every_gov_purpose() -> Vec<(&'static str, dolos_cardano::model::GovPurpose)> {
    use dolos_cardano::model::GovPurpose::*;

    vec![
        ("PParamUpdate", PParamUpdate),
        ("HardFork", HardFork),
        ("Committee", Committee),
        ("Constitution", Constitution),
    ]
}

pub fn every_pool_delegation() -> Vec<(&'static str, PoolDelegation)> {
    vec![
        ("Pool", PoolDelegation::Pool(hash28(0x28))),
        ("NotDelegated", PoolDelegation::NotDelegated),
    ]
}

pub fn every_drep_delegation() -> Vec<(&'static str, DRepDelegation)> {
    vec![
        (
            "Delegated",
            DRepDelegation::Delegated(DRep::Key(hash28(0x29))),
        ),
        ("NotDelegated", DRepDelegation::NotDelegated),
    ]
}

pub fn every_committee_authorization() -> Vec<(&'static str, CommitteeAuthorization)> {
    vec![
        (
            "HotCredential",
            CommitteeAuthorization::HotCredential(StakeCredential::AddrKeyhash(hash28(0x2a))),
        ),
        (
            "Resigned",
            CommitteeAuthorization::Resigned(Some(anchor(0x2b))),
        ),
    ]
}

pub fn every_drep() -> Vec<(&'static str, DRep)> {
    vec![
        ("Key", DRep::Key(hash28(0x2c))),
        ("Script", DRep::Script(hash28(0x2d))),
        ("Abstain", DRep::Abstain),
        ("NoConfidence", DRep::NoConfidence),
    ]
}

pub fn every_stake_credential() -> Vec<(&'static str, StakeCredential)> {
    vec![
        ("AddrKeyhash", StakeCredential::AddrKeyhash(hash28(0x2e))),
        ("ScriptHash", StakeCredential::ScriptHash(hash28(0x2f))),
    ]
}

pub fn every_vote() -> Vec<(&'static str, Vote)> {
    vec![
        ("No", Vote::No),
        ("Yes", Vote::Yes),
        ("Abstain", Vote::Abstain),
    ]
}

pub fn every_relay() -> Vec<(&'static str, Relay)> {
    vec![
        (
            "SingleHostAddr",
            Relay::SingleHostAddr(
                Some(3001),
                Some(Bytes::from(bytes_of::<4>(0x01).to_vec())),
                Some(Bytes::from(bytes_of::<16>(0x02).to_vec())),
            ),
        ),
        (
            "SingleHostName",
            Relay::SingleHostName(Some(3002), "relay.canary.invalid".to_string()),
        ),
        (
            "MultiHostName",
            Relay::MultiHostName("_cardano._tcp.canary.invalid".to_string()),
        ),
    ]
}

pub fn every_nonce_variant() -> Vec<(&'static str, Nonce)> {
    vec![
        (
            "NeutralNonce",
            Nonce {
                variant: NonceVariant::NeutralNonce,
                hash: None,
            },
        ),
        (
            "Nonce",
            Nonce {
                variant: NonceVariant::Nonce,
                hash: Some(hash32(0x39)),
            },
        ),
    ]
}
