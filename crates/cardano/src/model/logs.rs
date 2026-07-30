use pallas::{
    codec::minicbor::{self, Decode, Encode},
    ledger::primitives::conway::RationalNumber,
};

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
