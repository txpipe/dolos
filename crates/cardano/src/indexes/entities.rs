//! The entity kinds Cardano declares for the archive's entity index: the
//! entities whose first-seen slot the seal records (decision 0064).
//!
//! The first-script seal writes `SCRIPT` entries and the first-mint seal
//! `ASSET` entries.
//!
//! The widths are the key types' own, so a kind and the id the witness hands
//! the index cannot disagree.

use dolos_core::{EntityKind, FixedKey};

use crate::model::{AssetKey, ScriptKey};

/// An asset, by its `AssetKey`: `blake2b256(policy ‖ name)`, 32 bytes
/// (ruling 21 of the data-model freeze).
pub const ASSET: EntityKind = EntityKind::new("asset", AssetKey::LEN);

/// A script, by its 28-byte hash.
pub const SCRIPT: EntityKind = EntityKind::new("script", ScriptKey::LEN);

/// Every kind, in name order: the list a stele export walks and a restore
/// holds a stele's declaration against.
pub const ALL: [EntityKind; 2] = [ASSET, SCRIPT];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_is_sorted_by_name() {
        assert!(ALL.windows(2).all(|pair| pair[0].name() < pair[1].name()));
    }

    /// The names are hashed into stored keys and name the stele layers, so a
    /// rename that looks cosmetic orphans every entry of the kind.
    #[test]
    fn names_and_widths_are_storage_format() {
        assert_eq!((ASSET.name(), ASSET.key_len()), ("asset", 32));
        assert_eq!((SCRIPT.name(), SCRIPT.key_len()), ("script", 28));
    }
}
