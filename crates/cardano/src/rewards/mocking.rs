use pallas::ledger::primitives::{RationalNumber, StakeCredential};
use std::collections::HashMap;

use crate::{
    pallas_ratio,
    pots::{EpochIncentives, Pots},
    PParamsSet, PoolHash, PoolParams,
};

use serde::{Deserialize, Deserializer};

/// Helper function to deserialize a PoolHash from a hex string
fn deserialize_pool_hash<'de, D>(deserializer: D) -> Result<PoolHash, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    let bytes = hex::decode(&s).map_err(serde::de::Error::custom)?;
    if bytes.len() != 28 {
        return Err(serde::de::Error::custom(format!(
            "PoolHash must be 28 bytes, got {}",
            bytes.len()
        )));
    }
    let mut hash = [0u8; 28];
    hash.copy_from_slice(&bytes);
    Ok(pallas::crypto::hash::Hash::from(hash))
}

/// Helper function to deserialize a StakeCredential from a hex string
/// Assumes all credentials are AddrKeyhash (key hash) type
fn deserialize_stake_credential<'de, D>(deserializer: D) -> Result<StakeCredential, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    let bytes = hex::decode(&s).map_err(serde::de::Error::custom)?;

    if bytes.len() != 28 {
        return Err(serde::de::Error::custom(format!(
            "Key hash credential must be 28 bytes, got {}",
            bytes.len()
        )));
    }

    let mut hash = [0u8; 28];
    hash.copy_from_slice(&bytes);
    Ok(StakeCredential::AddrKeyhash(
        pallas::crypto::hash::Hash::from(hash),
    ))
}

/// Pool parameters with simplified reward account as hex string
/// VRF key hash, relays, and metadata are hardcoded for testing simplicity
#[derive(Debug, Deserialize)]
struct PoolParamsSimple {
    pledge: u64,
    cost: u64,
    margin: RationalNumber,
    /// Reward account as hex string (28 bytes = 56 hex chars)
    reward_account: String,
    pool_owners: Vec<String>,
}

impl PoolParamsSimple {
    /// Convert to full PoolParams
    fn to_pool_params(&self) -> Result<PoolParams, Box<dyn std::error::Error>> {
        // Hardcode VRF key hash to zeroed bytes (not needed for rewards tests)
        let vrf_hash = [0u8; 32];

        // Convert key hash to stake address format
        // Stake addresses are: 1 byte network tag + 28 bytes key hash
        let reward_key_bytes = hex::decode(&self.reward_account)?;
        if reward_key_bytes.len() != 28 {
            return Err(format!(
                "Reward account key hash must be 28 bytes, got {}",
                reward_key_bytes.len()
            )
            .into());
        }

        // Create stake address: 0xe1 (mainnet stake address) + key hash
        let mut reward_bytes = vec![0xe1];
        reward_bytes.extend_from_slice(&reward_key_bytes);

        let pool_owners = self
            .pool_owners
            .iter()
            .map(|hex| {
                let bytes = hex::decode(hex)?;
                if bytes.len() != 28 {
                    return Err(
                        format!("Pool owner hash must be 28 bytes, got {}", bytes.len()).into(),
                    );
                }
                let mut hash = [0u8; 28];
                hash.copy_from_slice(&bytes);
                Ok(pallas::crypto::hash::Hash::from(hash))
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;

        Ok(PoolParams {
            vrf_keyhash: pallas::crypto::hash::Hash::from(vrf_hash),
            pledge: self.pledge,
            cost: self.cost,
            margin: self.margin.clone(),
            reward_account: reward_bytes,
            pool_owners,
            relays: vec![],      // Hardcoded to empty for testing
            pool_metadata: None, // Hardcoded to None for testing
        })
    }
}

/// Pool data for testing rewards calculations
#[derive(Debug, Deserialize)]
struct PoolData {
    /// Number of blocks minted by this pool in the epoch
    blocks_minted: u64,
    /// Pool parameters (pledge, cost, margin, etc.)
    params: PoolParamsSimple,
}

/// Account data for testing stake and delegation
/// Note: An account is considered registered if it appears in the accounts
/// map
#[derive(Debug, Deserialize)]
struct AccountData {
    /// Total stake controlled by this account (in lovelaces)
    stake: u64,
    /// Pool to which this account delegates (pool hash hex)
    pool: Option<String>,
}

/// MockContext for testing rewards calculations
///
/// This context can be loaded from a JSON file to test rewards distribution
/// with realistic scenarios.
///
/// # JSON Format
///
/// - Pool hashes are hex strings (28 bytes = 56 hex chars)
/// - Stake credentials are hex strings (28 bytes = 56 hex chars, key hash only)
/// - Reward accounts are hex strings (28 bytes = 56 hex chars, key hash)
/// - RationalNumber values are tuples: [numerator, denominator]
/// - All stake and ada amounts are in lovelaces
/// - Accounts are registered if they appear in the accounts map
/// - Each account delegates to exactly one pool
///
/// # Example
///
/// See `test_data/rewards_context1.json` for a complete example with:
/// - 3 pools with different configurations
/// - 9 delegator accounts
/// - Realistic mainnet-like values
#[derive(Debug, Deserialize)]
pub struct MockContext {
    /// Current pots state
    pots: Pots,
    /// Whether this is pre-Allegra era (affects reward merging behavior)
    /// Number of blocks produced in the epoch
    epoch_blocks: u64,
    /// Fee snapshot for the epoch
    epoch_fee_ss: u64,
    /// Eta for the epoch
    epoch_eta: RationalNumber,
    /// Pool data indexed by pool hash (hex string)
    #[serde(default)]
    pools: HashMap<String, PoolData>, // pool hash hex -> pool data
    /// Account data indexed by stake credential (hex string)
    #[serde(default)]
    accounts: HashMap<String, AccountData>, // stake credential hex -> account data
    /// Protocol parameters
    pparams: PParamsSet,
    /// Converted pool params for efficient lookup
    #[serde(skip)]
    pool_params_converted: HashMap<String, PoolParams>,
    /// Computed pot delta for the rewards calculation
    #[serde(skip)]
    incentives: Option<EpochIncentives>,
}

impl MockContext {
    /// Load MockContext from a JSON file
    pub fn from_json_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let file = std::fs::File::open(path)?;
        let mut context: MockContext = serde_json::from_reader(file)?;

        // Pre-convert all pool params
        let mut converted = HashMap::new();

        for (pool_hex, pool_data) in &context.pools {
            if let Ok(params) = pool_data.params.to_pool_params() {
                converted.insert(pool_hex.clone(), params);
            }
        }

        context.pool_params_converted = converted;

        context.incentives = Some(crate::pots::epoch_incentives(
            context.pots.reserves,
            context.epoch_fee_ss,
            pallas_ratio!(context.pparams.ensure_rho()?),
            pallas_ratio!(context.pparams.ensure_tau()?),
            pallas_ratio!(context.epoch_eta),
        ));

        Ok(context)
    }

    /// Helper to convert hex string to PoolHash
    fn hex_to_pool_hash(hex: &str) -> Result<PoolHash, Box<dyn std::error::Error>> {
        let bytes = hex::decode(hex)?;
        if bytes.len() != 28 {
            return Err(format!("PoolHash must be 28 bytes, got {}", bytes.len()).into());
        }
        let mut hash = [0u8; 28];
        hash.copy_from_slice(&bytes);
        Ok(pallas::crypto::hash::Hash::from(hash))
    }

    /// Helper to convert hex string to StakeCredential
    /// Assumes all credentials are AddrKeyhash (key hash) type
    fn hex_to_stake_credential(hex: &str) -> Result<StakeCredential, Box<dyn std::error::Error>> {
        let bytes = hex::decode(hex)?;

        if bytes.len() != 28 {
            return Err(format!("Key hash must be 28 bytes, got {}", bytes.len()).into());
        }

        let mut hash = [0u8; 28];
        hash.copy_from_slice(&bytes);
        Ok(StakeCredential::AddrKeyhash(
            pallas::crypto::hash::Hash::from(hash),
        ))
    }
}

impl super::RewardsContext for MockContext {
    fn incentives(&self) -> &EpochIncentives {
        self.incentives
            .as_ref()
            .expect("epoch incentives not computed")
    }

    fn pots(&self) -> &Pots {
        &self.pots
    }

    fn pre_allegra(&self) -> bool {
        self.pparams.protocol_major().unwrap_or(0) < 3
    }

    fn active_stake(&self) -> u64 {
        self.accounts
            .values()
            .filter(|acc| acc.pool.is_some())
            .map(|acc| acc.stake)
            .sum()
    }

    fn epoch_blocks(&self) -> u64 {
        self.epoch_blocks
    }

    fn pool_blocks(&self, pool: PoolHash) -> u64 {
        let pool_hex = hex::encode(pool.as_ref());
        self.pools
            .get(&pool_hex)
            .map(|p| p.blocks_minted)
            .unwrap_or(0)
    }

    fn pool_stake(&self, pool: PoolHash) -> u64 {
        let pool_hex = hex::encode(pool.as_ref());

        // Pool stake is the sum of all accounts delegated to this pool
        self.accounts
            .values()
            .filter(|acc| acc.pool.as_ref() == Some(&pool_hex))
            .map(|acc| acc.stake)
            .sum()
    }

    fn account_stake(&self, pool: &PoolHash, account: &StakeCredential) -> u64 {
        let account_hex = match account {
            StakeCredential::AddrKeyhash(hash) => hex::encode(hash.as_ref()),
            StakeCredential::ScriptHash(hash) => hex::encode(hash.as_ref()),
        };

        let pool_hex = hex::encode(pool.as_ref());

        self.accounts
            .get(&account_hex)
            .filter(|acc| acc.pool.as_ref() == Some(&pool_hex))
            .map(|acc| acc.stake)
            .unwrap_or(0)
    }

    fn is_account_registered(&self, account: &StakeCredential) -> bool {
        let account_hex = match account {
            StakeCredential::AddrKeyhash(hash) => hex::encode(hash.as_ref()),
            StakeCredential::ScriptHash(hash) => hex::encode(hash.as_ref()),
        };

        // Account is registered if it appears in the map
        self.accounts.contains_key(&account_hex)
    }

    fn iter_all_pools(&self) -> impl Iterator<Item = PoolHash> {
        self.pool_params_converted
            .keys()
            .filter_map(|hex| Self::hex_to_pool_hash(hex).ok())
    }

    fn pool_params(&self, pool: PoolHash) -> &PoolParams {
        self.pool_params_converted
            .get(&hex::encode(pool.as_ref()))
            .unwrap()
    }

    fn pool_delegators(&self, pool_id: PoolHash) -> impl Iterator<Item = StakeCredential> {
        let pool_hex = hex::encode(pool_id.as_ref());

        // Collect all stake credentials that delegate to this pool
        let delegators: Vec<StakeCredential> = self
            .accounts
            .iter()
            .filter(|(_, acc)| acc.pool.as_ref() == Some(&pool_hex))
            .filter_map(|(account_hex, _)| Self::hex_to_stake_credential(account_hex).ok())
            .collect();

        delegators.into_iter()
    }

    fn pparams(&self) -> &PParamsSet {
        &self.pparams
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke_test_mock_context() {
        use std::path::PathBuf;

        let test_data = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test_data")
            .join("synthetic")
            .join("rewards")
            .join("context1.json");

        let ctx = MockContext::from_json_file(test_data.to_str().unwrap())
            .expect("Failed to load mock context");

        let reward_map = crate::rewards::define_rewards(&ctx).unwrap();

        dbg!(&reward_map);
    }
}
