
pub type Ratio = num_rational::Ratio<i128>;

pub type PallasRatio = pallas::ledger::primitives::RationalNumber;

macro_rules! from_pallas_ratio {
    ($x:expr) => {{
        let numerator = $x.numerator as i128;
        let denominator = $x.denominator as i128;
        Ratio::new(numerator, denominator)
    }};
}

macro_rules! into_ratio {
    ($x:expr) => {{
        num_rational::Ratio::from_integer($x as i128)
    }};
}

macro_rules! floor_int {
    ($x:expr) => {
        $x.floor().to_integer()
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
    let d = from_pallas_ratio!(d);

    let d_threshold = Ratio::new(8, 10); // 0.8

    if d >= d_threshold {
        return Ratio::from_integer(1);
    }

    // The number of expected blocks will be the number of slots per epoch times the
    // active slots coefficient See: Non-Updatable Parameters: https://cips.cardano.org/cips/cip9/

    // decentralizationParameter is the proportion of blocks that are expected to be
    // produced by stake pools instead of the OBFT (Ouroboros Byzantine Fault
    // Tolerance) nodes. It was introduced close before the Shelley era: https://github.com/input-output-hk/cardano-ledger/commit/c4f10d286faadcec9e4437411bce9c6c3b6e51c2
    let one = Ratio::from_integer(1);

    let f = Ratio::approximate_float(f).expect("invalid active slot coefficient");

    let epoch_length = into_ratio!(epoch_length);
    let expected_blocks = f * epoch_length;

    let expected_non_obft_blocks = expected_blocks * (one - d);

    // eta is the ratio between the number of blocks that have been produced during
    // the epoch, and the expectation value of blocks that should have been
    // produced during the epoch under ideal conditions.

    let minted_blocks = Ratio::from_integer(minted_blocks as i128);

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
    let delta_r1 = floor_int!(incentives_q) as u64;

    // rewardPot = feeSS + Δr1
    let reward_pot = fee_ss + delta_r1;

    // Δt1 = floor( τ * rewardPot )
    let treasury_tax = floor_int!(tau * into_ratio!(reward_pot)) as u64;

    // R = rewardPot - Δt1
    let available_rewards = reward_pot - treasury_tax;

    PotDelta {
        incentives: delta_r1, // this is Δr1 (minted from reserves)
        treasury_tax,         // Δt1 (to treasury)
        available_rewards,    // R (to be distributed)
    }
}
