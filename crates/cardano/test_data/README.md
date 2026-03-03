# Rewards Context Test Data

This directory contains JSON files for testing Cardano rewards calculations using the `MockContext` implementation.

## Overview

The `MockContext` allows you to load test scenarios from JSON files to verify rewards distribution logic with realistic data without needing a full blockchain state.

## JSON Format

### Structure

```json
{
  "pot_delta": { ... },           // Pot changes for the epoch
  "pots": { ... },                // Current pot state
  "pre_allegra": false,           // Era flag
  "total_stake": 30000000000000000,
  "active_stake": 22000000000000000,
  "epoch_blocks": 21600,
  "pools": { ... },               // Pool configurations
  "accounts": { ... },            // Delegator accounts
  "pparams": { ... }              // Protocol parameters
}
```

### Data Types

#### Pool Hash (28 bytes)
- Format: 56 hex characters
- Example: `"a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8"`

#### Stake Credential (28 bytes, key hash only)
- Format: 56 hex characters
- Only AddrKeyhash (key hash) credentials are supported
- Example: `"1234567890abcdef1234567890abcdef1234567890abcdef12345678"`

#### Reward Account (28 bytes, key hash)
- Format: 56 hex characters (key hash only)
- Will be automatically converted to a stake address
- Example: `"1234567890abcdef1234567890abcdef1234567890abcdef12345678"`

#### RationalNumber / UnitInterval
- Format: `[numerator, denominator]`
- Examples:
  - 3% margin: `[3, 100]`
  - a0 (pool influence): `[3, 10]`

#### Pool Owner Hash (28 bytes)
- Format: 56 hex characters
- Example: `"1234567890abcdef1234567890abcdef1234567890abcdef12345678"`

### Pools Section

Each pool is keyed by its pool hash (hex string) and contains:

```json
"pool_hash_hex": {
  "blocks_minted": 150,           // Blocks produced by pool in epoch
  "params": {
    "pledge": 50000000000000,     // Pool pledge (lovelaces)
    "cost": 340000000,            // Fixed cost per epoch (lovelaces)
    "margin": [3, 100],           // Pool margin as [num, denom]
    "reward_account": "...",      // Reward key hash (56 hex chars)
    "pool_owners": ["..."]        // Array of owner hashes (56 hex chars each)
  }
}
```

**Note:** 
- Pool stake is automatically computed from accounts that delegate to the pool
- VRF key hash, relays, and pool metadata are hardcoded (not needed for rewards tests)

### Accounts Section

Each account is keyed by stake credential (hex string, 56 chars) and contains:

**Note:** An account is considered registered if it appears in the accounts map. If an account is not in the map, it is considered unregistered.

**Delegation:** Each account delegates to exactly one pool specified by the `pool` field.

```json
"stake_cred_hex": {
  "stake": 50000000000,           // Total stake controlled by account (lovelaces)
  "pool": "pool_hash_hex"         // Pool to which this account delegates
}
```

### PParams Section

Protocol parameters as an array of variant objects:

```json
{
  "values": [
    { "DesiredNumberOfStakePools": 500 },
    { "PoolPledgeInfluence": [3, 10] },
    { "DecentralizationConstant": [0, 1] },
    { "ExpansionRate": [3, 1000] },
    { "TreasuryGrowthRate": [20, 100] },
    { "MinPoolCost": 340000000 },
    { "ProtocolVersion": [8, 0] }
  ]
}
```

## Usage Example

```rust
use std::path::PathBuf;

let test_file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    .join("test_data")
    .join("rewards_context1.json");

let ctx = MockContext::from_json_file(test_file.to_str().unwrap())?;

// Use with define_rewards to calculate rewards
let reward_map = define_rewards(&ctx)?;
```

## Example Files

### `rewards_context1.json`

A realistic test scenario with:
- 3 pools with different pledge amounts and performance
- 9 registered delegator accounts (all accounts in the map are registered)
- Mainnet-like stake distribution (~30 billion ADA total, 22 billion active)
- Typical protocol parameters (k=500, a0=0.3)

Pool configurations:
1. High pledge (50M ADA), good performance (150 blocks)
2. Medium pledge (10M ADA), moderate performance (80 blocks)
3. Very high pledge (100M ADA), excellent performance (200 blocks)

## Notes

- All ADA amounts are in lovelaces (1 ADA = 1,000,000 lovelaces)
- Pool hashes must be exactly 56 hex characters (28 bytes)
- Stake credentials must be exactly 56 hex characters (28 bytes, key hash only)
- Reward accounts must be exactly 56 hex characters (28 bytes key hash, auto-converted to stake address)
- Accounts appearing in the accounts map are considered registered
- RationalNumber types (margin, influence, rates) use tuple format: `[numerator, denominator]`
