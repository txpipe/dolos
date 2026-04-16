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
