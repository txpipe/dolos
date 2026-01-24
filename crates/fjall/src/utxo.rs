//! UTxO filter index operations for fjall.
//!
//! These indexes map lookup keys (addresses, policies, assets) to sets of TxoRefs.
//! Each entry is stored as a composite key: `lookup_key ++ txo_ref` with an empty value.
//! Queries use prefix scanning to find all TxoRefs for a given lookup key.

use std::collections::HashSet;

use dolos_core::{TxoRef, UtxoSet, UtxoSetDelta};
use fjall::{Keyspace, OwnedWriteBatch};
use pallas::ledger::{addresses::ShelleyDelegationPart, traverse::MultiEraOutput};

use crate::keys::{decode_txo_ref_from_suffix, utxo_composite_key, TXO_REF_SIZE};
use crate::Error;

/// Result of splitting an address into its components
struct SplitAddressResult {
    /// Full address bytes
    address: Option<Vec<u8>>,
    /// Payment credential bytes
    payment: Option<Vec<u8>>,
    /// Stake credential bytes
    stake: Option<Vec<u8>>,
}

/// Split an address into its components (full address, payment, stake)
fn split_address(utxo: &MultiEraOutput) -> Result<SplitAddressResult, Error> {
    use pallas::ledger::addresses::Address;

    match utxo.address() {
        Ok(address) => match &address {
            Address::Shelley(x) => {
                let address = x.to_vec();
                let payment = x.payment().to_vec();

                let stake = match x.delegation() {
                    ShelleyDelegationPart::Key(..)
                    | ShelleyDelegationPart::Script(..)
                    | ShelleyDelegationPart::Pointer(..) => Some(x.delegation().to_vec()),
                    ShelleyDelegationPart::Null => None,
                };

                Ok(SplitAddressResult {
                    address: Some(address),
                    payment: Some(payment),
                    stake,
                })
            }
            Address::Stake(x) => {
                let addr = x.to_vec();
                Ok(SplitAddressResult {
                    address: Some(addr.clone()),
                    payment: None,
                    stake: Some(addr),
                })
            }
            Address::Byron(x) => {
                let addr = x.to_vec();
                Ok(SplitAddressResult {
                    address: Some(addr),
                    payment: None,
                    stake: None,
                })
            }
        },
        Err(err) => Err(Error::Codec(err.to_string())),
    }
}

/// References to all UTxO filter keyspaces
pub struct UtxoKeyspaces<'a> {
    pub address: &'a Keyspace,
    pub payment: &'a Keyspace,
    pub stake: &'a Keyspace,
    pub policy: &'a Keyspace,
    pub asset: &'a Keyspace,
}

/// Insert a UTxO entry into a keyspace
fn insert_entry(batch: &mut OwnedWriteBatch, keyspace: &Keyspace, key: &[u8], txo: &TxoRef) {
    let composite = utxo_composite_key(key, txo);
    batch.insert(keyspace, composite, []);
}

/// Remove a UTxO entry from a keyspace
fn remove_entry(batch: &mut OwnedWriteBatch, keyspace: &Keyspace, key: &[u8], txo: &TxoRef) {
    let composite = utxo_composite_key(key, txo);
    batch.remove(keyspace, composite);
}

/// Apply UTxO set delta to the filter indexes
pub fn apply(
    batch: &mut OwnedWriteBatch,
    keyspaces: &UtxoKeyspaces,
    delta: &UtxoSetDelta,
) -> Result<(), Error> {
    // Process produced and recovered UTxOs (add to indexes)
    let trackable = delta
        .produced_utxo
        .iter()
        .chain(delta.recovered_stxi.iter());

    for (txo_ref, body) in trackable {
        let body =
            MultiEraOutput::try_from(body.as_ref()).map_err(|e| Error::Codec(e.to_string()))?;

        let SplitAddressResult {
            address,
            payment,
            stake,
        } = split_address(&body)?;

        if let Some(addr) = address {
            insert_entry(batch, keyspaces.address, &addr, txo_ref);
        }

        if let Some(pay) = payment {
            insert_entry(batch, keyspaces.payment, &pay, txo_ref);
        }

        if let Some(stk) = stake {
            insert_entry(batch, keyspaces.stake, &stk, txo_ref);
        }

        // Index by policy and asset
        let value = body.value();
        let assets = value.assets();

        for policy_assets in assets {
            let policy = policy_assets.policy();
            insert_entry(batch, keyspaces.policy, policy.as_slice(), txo_ref);

            for asset in policy_assets.assets() {
                let mut subject = asset.policy().to_vec();
                subject.extend(asset.name());
                insert_entry(batch, keyspaces.asset, &subject, txo_ref);
            }
        }
    }

    // Process consumed and undone UTxOs (remove from indexes)
    let forgettable = delta.consumed_utxo.iter().chain(delta.undone_utxo.iter());

    for (txo_ref, body) in forgettable {
        let body =
            MultiEraOutput::try_from(body.as_ref()).map_err(|e| Error::Codec(e.to_string()))?;

        let SplitAddressResult {
            address,
            payment,
            stake,
        } = split_address(&body)?;

        if let Some(addr) = address {
            remove_entry(batch, keyspaces.address, &addr, txo_ref);
        }

        if let Some(pay) = payment {
            remove_entry(batch, keyspaces.payment, &pay, txo_ref);
        }

        if let Some(stk) = stake {
            remove_entry(batch, keyspaces.stake, &stk, txo_ref);
        }

        // Remove from policy and asset indexes
        let value = body.value();
        let assets = value.assets();

        for policy_assets in assets {
            let policy = policy_assets.policy();
            remove_entry(batch, keyspaces.policy, policy.as_slice(), txo_ref);

            for asset in policy_assets.assets() {
                let mut subject = asset.policy().to_vec();
                subject.extend(asset.name());
                remove_entry(batch, keyspaces.asset, &subject, txo_ref);
            }
        }
    }

    Ok(())
}

/// Get all TxoRefs for a given lookup key using prefix scanning
pub fn get_by_key(keyspace: &Keyspace, lookup_key: &[u8]) -> Result<UtxoSet, Error> {
    let mut result = HashSet::new();

    // Prefix scan: all keys starting with lookup_key
    // fjall's prefix() returns an iterator of Guard items
    // Guard::key() consumes the guard and returns Result<UserKey>
    for guard in keyspace.prefix(lookup_key) {
        let key = guard.key()?;

        // Key format: lookup_key ++ txo_ref
        // We need to extract txo_ref from the suffix
        if key.len() >= lookup_key.len() + TXO_REF_SIZE {
            let txo_ref = decode_txo_ref_from_suffix(&key);
            result.insert(txo_ref);
        }
    }

    Ok(result)
}
