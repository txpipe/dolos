use pallas::{
    codec::minicbor::{self, Decode, Encode},
    ledger::primitives::conway::RationalNumber,
};

use super::pools::PoolHash;

/// Everything one account did in one epoch, as one archive log row.
///
/// Replaces the four identically-keyed namespaces that preceded it —
/// `leader-rewards`, `member-rewards`, `pool-deposit-refunds` and
/// `account-stakes` (ADR-0027). Written by EWRAP alone, keyed by
/// `(epoch_start(C-1), credential)` when the boundary closing epoch `C` runs:
/// the stake temporal key three of the four already used, so the merge
/// deduplicates the `LogKey` and the pool hash the stake leg and the member
/// reward share.
///
/// # Absence
///
/// Absence is encoded per field, never by the row: `active_stake: Some(0)` is
/// a real zero-stake delegator that `zero_stake_delegators_are_kept` requires
/// to exist, while a missing row means "this account did nothing this epoch".
/// The reward legs keep the opposite policy — the `> 0` filter lives in the
/// read path, as it did across the three reward namespaces.
///
/// # Repetition
///
/// The two list fields are lists because they can legitimately repeat: an
/// account that is the reward account of N pools earns N leader rewards in one
/// epoch, and an operator retiring several pools into one reward account
/// collects N refunds. Under the four namespaces those collapsed onto one
/// `LogKey` and all but one were lost; here they are elements. Both are sorted
/// by pool id, because the ledger holds them in a `HashMap` and a stele's bytes
/// have to be reproducible across publishers.
#[derive(Debug, Clone, PartialEq, Eq, Decode, Encode, Default)]
pub struct AccountEpochLog {
    /// Active stake in Lovelaces, at the snapshot epoch the boundary rewards.
    #[n(0)]
    pub active_stake: Option<u64>,

    /// Pool the account delegated to at the snapshot epoch — and therefore the
    /// payer of `member_reward`, which is computed off the same snapshot.
    #[n(1)]
    pub pool_id: Option<PoolHash>,

    /// Reward earned as a delegator of [`Self::pool_id`].
    #[n(2)]
    pub member_reward: Option<u64>,

    /// Rewards earned as the operator of each named pool.
    #[n(3)]
    pub leader_rewards: Vec<(PoolHash, u64)>,

    /// Pool deposits refunded to this account by each named pool's retirement.
    #[n(4)]
    pub deposit_refunds: Vec<(PoolHash, u64)>,
}

impl AccountEpochLog {
    /// Whether the row carries anything at all — the test for writing it.
    ///
    /// A row that says nothing is not written, which is what keeps the
    /// namespace's population "accounts that did something this epoch" rather
    /// than "every account that has ever existed".
    pub fn is_empty(&self) -> bool {
        self.active_stake.is_none()
            && self.member_reward.is_none()
            && self.leader_rewards.is_empty()
            && self.deposit_refunds.is_empty()
    }

    /// Put the two list fields in pool order.
    ///
    /// Both are assembled from `HashMap` iteration, whose order varies per
    /// process. Two honest publishers of identical state have to produce
    /// identical bytes (ADR-004), so the order is fixed here rather than left
    /// to the map.
    pub fn sort(&mut self) {
        self.leader_rewards.sort_unstable();
        self.deposit_refunds.sort_unstable();
    }
}

entity_boilerplate!(AccountEpochLog, "account-epochs");

#[derive(Debug, Clone, PartialEq, Eq, Decode, Encode, Default)]
pub struct LeaderRewardLog {
    #[n(0)]
    pub amount: u64,

    #[n(1)]
    pub pool_id: Vec<u8>,
}

entity_boilerplate!(LeaderRewardLog, "leader-rewards");

#[derive(Debug, Clone, PartialEq, Eq, Decode, Encode, Default)]
pub struct MemberRewardLog {
    #[n(0)]
    pub amount: u64,

    #[n(1)]
    pub pool_id: Vec<u8>,
}

entity_boilerplate!(MemberRewardLog, "member-rewards");

#[derive(Debug, Clone, PartialEq, Eq, Decode, Encode, Default)]
pub struct PoolDepositRefundLog {
    #[n(0)]
    pub amount: u64,

    #[n(1)]
    pub pool_id: Vec<u8>,
}

entity_boilerplate!(PoolDepositRefundLog, "pool-deposit-refunds");

/// Per-account snapshot of the stake that was active during an epoch.
///
/// Written by RUPD under the same temporal key as the per-pool [`StakeLog`]
/// (the epoch whose active stake the snapshot describes), keyed by the
/// account's credential, with the pool in the value.
///
/// # Access patterns
///
/// The key layout is chosen for the two cheap cases and accepts the third:
///
/// - epoch-wide distribution — one prefix scan over the epoch's temporal key
/// - one account across epochs — one point read per epoch
/// - one pool within an epoch — **scans the epoch and filters on `pool_id`**
///
/// The pool-scoped case is a deliberate, deferred tradeoff, not an oversight.
/// Keying by pool instead would turn the per-account lookup into a full scan,
/// and a composite `pool ++ credential` key does not fit the fixed-size entity
/// half of a `LogKey` without truncating one of them. A pool-keyed secondary
/// namespace would fix it at the cost of duplicating every row (~1.3M per
/// epoch on mainnet); revisit only if pool-scoped traffic justifies that.
#[derive(Debug, Clone, PartialEq, Eq, Decode, Encode, Default)]
pub struct AccountStakeLog {
    /// Active stake in Lovelaces
    #[n(0)]
    pub amount: u64,

    /// Pool the account delegated to at the snapshot epoch
    #[n(1)]
    pub pool_id: Vec<u8>,
}

entity_boilerplate!(AccountStakeLog, "account-stakes");

#[derive(Debug, Clone, PartialEq, Decode, Encode, Default)]
pub struct StakeLog {
    /// Number of blocks created by pool
    #[n(0)]
    pub blocks_minted: u64,

    /// Total stake in Lovelaces
    #[n(1)]
    pub total_stake: u64,

    /// Pool size (percentage) of overall active stake at that epoch
    #[n(2)]
    pub relative_size: f64,

    /// Number of delegators for epoch
    #[n(3)]
    pub delegators_count: u64,

    /// Live pledge
    #[n(6)]
    pub live_pledge: u64,

    /// Declared pledge
    #[n(7)]
    pub declared_pledge: u64,

    /// Total rewards for epoch
    #[n(8)]
    pub total_rewards: u64,

    /// Total fees for epoch
    #[n(9)]
    pub operator_share: u64,

    /// Fixed cost
    #[n(10)]
    pub fixed_cost: u64,

    /// Margin cost
    #[n(11)]
    pub margin_cost: Option<RationalNumber>,
}

entity_boilerplate!(StakeLog, "stakes");
