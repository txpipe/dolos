use pallas::{
    codec::minicbor::{self, Decode, Encode},
    ledger::primitives::conway::RationalNumber,
};
use serde::Serialize;

use super::pools::PoolHash;

/// Everything one account did in one epoch, as one archive log row.
///
/// Replaces the four identically-keyed namespaces that preceded it —
/// `leader-rewards`, `member-rewards`, `pool-deposit-refunds` and
/// `account-stakes` (ADR-0027). Keyed by `(epoch_start(E), credential)` for
/// epoch `E`: the stake temporal key three of the four already used, so the
/// merge deduplicates the `LogKey` and the pool hash the stake leg and the
/// member reward share.
///
/// Two writers fill the key. ESTART opening `E` writes the row with the stake
/// leg alone, from the distribution active in `E`. The EWRAP closing `E + 1`
/// writes the full row over it, with the same stake leg and the reward legs.
/// So at tip `C` the rows of `C - 1` and `C` carry stake only, and the older
/// ones are final.
///
/// # Absence
///
/// Absence is encoded per field, never by the row: `active_stake: Some(0)` is
/// a real zero-stake delegator that `zero_stake_delegators_are_kept` requires
/// to exist. On a final row, a missing row means "this account did nothing this
/// epoch". The reward legs keep the opposite policy — the `> 0` filter lives in
/// the read path, as it did across the three reward namespaces — and on a
/// stake-only row they mean "not computed yet".
///
/// For the two latest epochs, a missing row can also mean ESTART's row is
/// gone or was never written: a rollback to the epoch's start or before cuts
/// it, a restart after a crash mid-ESTART keeps only the shards still to run,
/// and a store upgraded mid-epoch has none for the epoch it upgraded in. Each
/// heals when EWRAP writes the full rows one boundary later.
///
/// # Repetition
///
/// The two list fields are lists because they can legitimately repeat: an
/// account that is the reward account of N pools earns N leader rewards in one
/// epoch, and an operator retiring several pools into one reward account
/// collects N refunds. Under the four namespaces those collapsed onto one
/// `LogKey` and all but one were lost; here they are elements. Both encode
/// sorted by pool id, then amount, whatever their order in memory, because the
/// ledger holds them in a `HashMap` and a stele's bytes have to be reproducible
/// across publishers.
#[derive(Debug, Clone, PartialEq, Eq, Decode, Encode, Default, Serialize)]
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
    #[cbor(encode_with = "encode_sorted")]
    pub leader_rewards: Vec<(PoolHash, u64)>,

    /// Pool deposits refunded to this account by each named pool's retirement.
    #[n(4)]
    #[cbor(encode_with = "encode_sorted")]
    pub deposit_refunds: Vec<(PoolHash, u64)>,
}

fn encode_sorted<Ctx, W: minicbor::encode::Write>(
    list: &[(PoolHash, u64)],
    e: &mut minicbor::Encoder<W>,
    ctx: &mut Ctx,
) -> Result<(), minicbor::encode::Error<W::Error>> {
    let mut sorted: Vec<&(PoolHash, u64)> = list.iter().collect();
    sorted.sort_unstable();

    e.array(sorted.len() as u64)?;

    for item in sorted {
        item.encode(e, ctx)?;
    }

    Ok(())
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

    /// Put the two list fields in the order they encode in, so the row equals
    /// its own decode.
    ///
    /// Both are assembled from `HashMap` iteration, whose order varies per
    /// process.
    pub fn sort(&mut self) {
        self.leader_rewards.sort_unstable();
        self.deposit_refunds.sort_unstable();
    }
}

entity_boilerplate!(AccountEpochLog, "account-epochs", log = super::AccountKey);

#[derive(Debug, Clone, PartialEq, Decode, Encode, Default, Serialize)]
pub struct PoolEpochLog {
    /// Number of blocks created by pool
    #[n(0)]
    pub blocks_minted: u64,

    /// Total stake in Lovelaces
    #[n(1)]
    pub total_stake: u64,

    /// Pool size (percentage) of overall active stake at that epoch
    #[n(2)]
    pub relative_size: f64,

    /// Number of delegators with stake above zero for epoch
    #[n(3)]
    pub delegators_count: u64,

    /// Live pledge
    #[n(6)]
    pub live_pledge: u64,

    /// Total rewards for epoch
    #[n(8)]
    pub total_rewards: u64,

    /// Fixed cost
    #[n(10)]
    pub fixed_cost: u64,

    /// Margin cost
    #[n(11)]
    pub margin_cost: Option<RationalNumber>,
}

entity_boilerplate!(PoolEpochLog, "pool-epochs", log = super::PoolKey);

/// One DRep's voting power in one completed DRep distribution.
///
/// Written by EWRAP's finalize pass for every entry of the distribution the
/// boundary closing epoch `n` completes, Abstain and NoConfidence included,
/// keyed by `(epoch_start(n), drep)`, like the `epochs` log. Koios labels that
/// distribution `epoch_no = n + 1`, the key's epoch plus one, and the
/// boundary closing `n + 1` tallies it.
#[derive(Debug, Clone, PartialEq, Eq, Decode, Encode, Default, Serialize)]
pub struct DRepEpochLog {
    /// Stake in Lovelaces the distribution attributes to the DRep.
    #[n(0)]
    pub voting_power: u64,

    /// Whether the DRep was registered at the boundary; always true for the
    /// Abstain and NoConfidence pseudo-DReps.
    #[n(1)]
    pub registered: bool,
}

entity_boilerplate!(DRepEpochLog, "drep-epochs", log = super::DRepKey);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_epoch_lists_encode_sorted() {
        let pool = |byte: u8| PoolHash::from([byte; 28]);

        let row = AccountEpochLog {
            active_stake: Some(1),
            pool_id: Some(pool(1)),
            member_reward: Some(2),
            leader_rewards: vec![(pool(9), 30), (pool(3), 10), (pool(3), 5)],
            deposit_refunds: vec![(pool(8), 500), (pool(2), 500)],
        };

        let mut sorted = row.clone();
        sorted.sort();
        assert_ne!(row, sorted);

        let bytes = minicbor::to_vec(&row).unwrap();
        assert_eq!(bytes, minicbor::to_vec(&sorted).unwrap());
        assert_eq!(minicbor::decode::<AccountEpochLog>(&bytes).unwrap(), sorted);
    }
}
