use num_bigint::BigInt;
use num_rational::BigRational;
use pallas::ledger::primitives::RationalNumber;

pub type Ratio = num_rational::BigRational;
pub type PallasRatio = pallas::ledger::primitives::RationalNumber;

macro_rules! big_ratio {
    ($numer:expr, $denom:expr) => {{
        let numer = BigInt::from($numer);
        let denom = BigInt::from($denom);
        BigRational::new(numer, denom)
    }};
}

macro_rules! from_pallas_ratio {
    ($x:expr) => {{
        big_ratio!($x.numerator, $x.denominator)
    }};
}

macro_rules! into_ratio {
    ($x:expr) => {{
        let x = BigInt::from($x);
        BigRational::from_integer(x)
    }};
}

macro_rules! floor_int {
    ($x:expr, $ty:ty) => {
        <$ty>::try_from($x.floor().to_integer()).unwrap()
    };
}

#[derive(Debug)]
pub struct PotDelta {
    pub incentives: u64,
    pub treasury_tax: u64,
    pub available_rewards: u64,
}

/// Calculate eta using the decentralisation parameter and the formula:
///
/// ```text
/// η(blocks, d) = {
///   1,                                    if d ≥ 0.8
///   min(1, blocks / expected_blocks),     otherwise
/// }
///
/// where:
///   blocks = total_blocks_in_epoch_by_pools
///   d = decentralization_parameter
///   expected_blocks = (1 - d) × slots_per_epoch × active_slot_coefficient
/// ```
///
/// This implements the eta calculation from the Cardano Shelley delegation
/// specification. Eta represents the ratio between actual blocks produced by
/// pools and the expected number of blocks under ideal conditions.
///
/// The number of expected blocks will be the number of slots per epoch times
/// the active slots coefficient See: Non-Updatable Parameters: https://cips.cardano.org/cips/cip9/
///
/// decentralizationParameter is the proportion of blocks that are expected to
/// be produced by stake pools instead of the OBFT (Ouroboros Byzantine Fault
/// Tolerance) nodes. It was introduced close before the Shelley era: https://github.com/input-output-hk/cardano-ledger/commit/c4f10d286faadcec9e4437411bce9c6c3b6e51c2
///
/// # Arguments
///
/// * `minted_blocks` - Total number of blocks produced by stake pools in the
///   epoch
/// * `d` - Decentralization parameter (proportion of blocks expected to be
///   produced by stake pools)
/// * `f` - Active slot coefficient for the network
/// * `epoch_length` - Expected number of slots per epoch
///
/// # Returns
///
/// The calculated eta value, capped at 1.0
///
/// # References
///
/// - [Cardano Ledger Implementation](https://github.com/input-output-hk/cardano-ledger/commit/c4f10d286faadcec9e4437411bce9c6c3b6e51c2)
/// - [Shelley Delegation Specification](https://hydra.iohk.io/build/2166577/download/1/delegation_design_spec.pdf)
///   section 5.4.3
/// - [CF Java Rewards Calculation](https://github.com/cardano-foundation/cf-java-rewards-calculation/blob/b05eddf495af6dc12d96c49718f27c34fa2042b1/calculation/src/main/java/org/cardanofoundation/rewards/calculation/TreasuryCalculation.java#L117)
pub fn calculate_eta(minted_blocks: u32, d: PallasRatio, f: f32, epoch_length: u64) -> Ratio {
    let one = into_ratio!(1);
    let d = from_pallas_ratio!(d);

    let d_threshold = big_ratio!(8, 10); // 0.8

    if d >= d_threshold {
        return one;
    }

    let f = Ratio::from_float(f).expect("invalid active slot coefficient");

    let epoch_length = into_ratio!(epoch_length);
    let expected_blocks = f.clone() * epoch_length.clone();

    let expected_non_obft_blocks = expected_blocks.clone() * (&one - d.clone());

    // eta is the ratio between the number of blocks that have been produced during
    // the epoch, and the expectation value of blocks that should have been
    // produced during the epoch under ideal conditions.

    let minted_blocks = into_ratio!(minted_blocks);

    let eta = minted_blocks / expected_non_obft_blocks;

    // spec: η = min(1, minted/expected)
    if eta > one {
        one
    } else {
        eta
    }
}

pub fn compute_pot_delta(
    reserves: u64,     // current reserves at snapshot
    fee_ss: u64,       // fee snapshot ("feeSS") for the epoch being rewarded
    rho: &PallasRatio, // monetaryExpansion (ρ)
    tau: &PallasRatio, // treasuryCut (τ)
    eta: Ratio,        // from calculate_eta (already capped to ≤ 1)
) -> PotDelta {
    let rho = from_pallas_ratio!(rho);
    let tau = from_pallas_ratio!(tau);
    let reserves = into_ratio!(reserves);

    // Δr1 = floor( min(1,η) * ρ * reserves )
    let incentives_q = eta * rho * reserves;
    let delta_r1 = floor_int!(incentives_q, u64);

    // rewardPot = feeSS + Δr1
    let reward_pot = fee_ss + delta_r1;

    // Δt1 = floor( τ * rewardPot )
    let treasury_tax = floor_int!(tau * into_ratio!(reward_pot), u64);

    // R = rewardPot - Δt1
    let available_rewards = reward_pot - treasury_tax;

    PotDelta {
        incentives: delta_r1, // this is Δr1 (minted from reserves)
        treasury_tax,         // Δt1 (to treasury)
        available_rewards,    // R (to be distributed)
    }
}
