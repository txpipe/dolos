//! Cardano-specific index dimension names.
//!
//! Dimensions are static strings that identify the type of index table.
//! Each dimension corresponds to a separate table in the storage backend.

/// UTxO filter index dimensions.
///
/// These dimensions are used for querying the current UTxO set by various
/// criteria.
pub mod utxo {
    use dolos_core::TagDimension;

    /// Full address (any type: Shelley, Byron, Stake)
    pub const ADDRESS: TagDimension = "address";

    /// Payment credential (Shelley addresses only)
    pub const PAYMENT: TagDimension = "payment";

    /// Stake credential (Shelley addresses with delegation, or stake addresses)
    pub const STAKE: TagDimension = "stake";

    /// Native asset policy ID
    pub const POLICY: TagDimension = "policy";

    /// Native asset subject (policy ID + asset name)
    pub const ASSET: TagDimension = "asset";
}

/// Declare a dimension registry: the constants and the `ALL` list that has to
/// contain them, from one list.
///
/// Index stores keep a *hash* of the dimension name rather than the name, so
/// the set of dimensions is not discoverable from disk: any bulk traversal of
/// archive tags (`IndexStore::iter_archive_tags`) has to be driven by `ALL`,
/// and a dimension missing from it silently stops being exported.
///
/// Writing the two out separately made that a comment's job. Here a constant
/// cannot exist without being in `ALL`, because the same list emits both.
macro_rules! dimension_registry {
    (
        $(#[$all_meta:meta])*
        ALL;
        $($(#[$meta:meta])* $name:ident = $value:expr;)+
    ) => {
        $(
            $(#[$meta])*
            pub const $name: TagDimension = $value;
        )+

        $(#[$all_meta])*
        pub const ALL: [TagDimension; [$(stringify!($name)),+].len()] = [$($name),+];
    };
}

/// Archive index dimensions.
///
/// These dimensions are used for querying historical blocks by various
/// criteria.
pub mod archive {
    use dolos_core::TagDimension;

    dimension_registry! {
        /// Every archive dimension, in declaration order.
        ///
        /// Emitted from the same list as the constants above, so a dimension
        /// that exists is a dimension that is exported.
        ALL;

        /// Full address (any type: Shelley, Byron, Stake)
        ADDRESS = "address";

        /// Payment credential (Shelley addresses only)
        PAYMENT = "payment";

        /// Stake credential (Shelley addresses with delegation, or stake addresses)
        STAKE = "stake";

        /// Native asset subject (policy ID + asset name)
        ASSET = "asset";

        /// Native asset policy ID
        POLICY = "policy";

        /// Plutus datum hash
        DATUM = "datum";

        /// Script hash (Plutus or native)
        SCRIPT = "script";

        /// Spent TxO reference (tx_hash + output_index)
        SPENT_TXO = "spent_txo";

        /// Account certificates (stake registration, deregistration, delegation)
        ACCOUNT_CERTS = "account_certs";

        /// Pool certificates (registration/update and retirement)
        POOL_CERTS = "pool_certs";

        /// Stake-account withdrawals by reward account bytes
        ACCOUNT_WITHDRAWALS = "account_withdrawals";

        /// Transaction metadata labels.
        ///
        /// Sourced from core rather than spelled again: it stays a
        /// dimension-list entry like any other, but the string itself has one
        /// definition, because this is the dimension whose logical key the
        /// stores keep verbatim instead of hashing (see
        /// [`dolos_core::key_hash`]).
        METADATA = dolos_core::VERBATIM_KEY_DIMENSION;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archive_all_has_no_duplicates() {
        let mut seen = archive::ALL.to_vec();
        seen.sort_unstable();
        seen.dedup();

        assert_eq!(seen.len(), archive::ALL.len());
    }

    /// Pins the literal strings. They are hashed into on-disk keys, so a rename
    /// that looks cosmetic orphans every existing entry of that dimension —
    /// the same reason `ExactKind`'s names are pinned in dolos-core. Every
    /// other test compares a constant against itself and would survive such a
    /// rename; this one exists to fail it.
    #[test]
    fn archive_dimension_names_are_storage_format() {
        assert_eq!(
            archive::ALL,
            [
                "address",
                "payment",
                "stake",
                "asset",
                "policy",
                "datum",
                "script",
                "spent_txo",
                "account_certs",
                "pool_certs",
                "account_withdrawals",
                "metadata",
            ]
        );
    }
}
